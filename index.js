const { Client } = require('discord.js-selfbot-v13');
const { GoogleGenerativeAI } = require('@google/generative-ai');
const moment = require('moment');
const os = require('os');
const osu = require('os-utils');
const axios = require('axios');
const fs = require('fs');
const path = require('path');
const zlib = require('zlib');
const si = require('systeminformation');
const Database = require('better-sqlite3');
const config = require('./config.json');

const client = new Client({ checkUpdate: false });
const genAI  = new GoogleGenerativeAI(config.geminiApiKey);

// Primary model + 2-tier fallback — auto-switch on quota/ratelimit
// Auto-restore: sau 60 phút sẽ thử lại primary, nếu ok thì về tier 0
const MODEL_PRIMARY   = 'gemini-3-flash-preview';
const MODEL_FALLBACK1 = 'gemini-2.5-flash';
const MODEL_FALLBACK2 = 'gemini-2.5-flash-lite';
const RESTORE_AFTER_MS = 60 * 60 * 1000; // thử restore về primary sau 60 phút

let currentModel  = genAI.getGenerativeModel({ model: MODEL_PRIMARY });
let fallbackTier  = 0;
let restoreTimer  = null;

function scheduleRestore() {
    if (restoreTimer) return; // đã có timer rồi, không đặt lại
    restoreTimer = setTimeout(async () => {
        restoreTimer = null;
        if (fallbackTier === 0) return; // đã ở primary rồi
        try {
            const probe = genAI.getGenerativeModel({ model: MODEL_PRIMARY });
            await probe.generateContent('ping');
            currentModel = probe;
            fallbackTier = 0;
            sessionLog(`✅ [AI] Restored to primary: ${MODEL_PRIMARY}`);
            console.log(`✅ [AI] Restored to primary: ${MODEL_PRIMARY}`);
        } catch (_) {
            // Primary vẫn còn quota → giữ tier hiện tại, lên lịch thử lại lần nữa
            sessionLog(`⚠️ [AI] Primary still unavailable — retry in 60 min`);
            console.warn(`⚠️ [AI] Primary still unavailable — retry in 60 min`);
            scheduleRestore();
        }
    }, RESTORE_AFTER_MS);
}

async function generateContent(payload) {
    try {
        return await currentModel.generateContent(payload);
    } catch (err) {
        const msg     = err?.message?.toLowerCase() || '';
        const isQuota = msg.includes('quota') || msg.includes('429') || msg.includes('rate') || msg.includes('resource_exhausted');
        if (isQuota && fallbackTier < 2) {
            fallbackTier++;
            const next = fallbackTier === 1 ? MODEL_FALLBACK1 : MODEL_FALLBACK2;
            currentModel = genAI.getGenerativeModel({ model: next });
            sessionLog(`⚠️ [AI] Quota/ratelimit — switched to tier ${fallbackTier}: ${next}`);
            console.warn(`⚠️ [AI] Switched to tier ${fallbackTier}: ${next}`);
            scheduleRestore(); // bắt đầu đếm ngược về primary
            return await currentModel.generateContent(payload);
        }
        throw err;
    }
}

const downloadFolder = path.join(__dirname, 'downloads');
const logFolder      = path.join(__dirname, 'logs');
if (!fs.existsSync(downloadFolder)) fs.mkdirSync(downloadFolder);
if (!fs.existsSync(logFolder))      fs.mkdirSync(logFolder);

// ================================================================
// SESSION LOGGER
// Each bot run = 1 txt file: logs/session_YYYY-MM-DD_HH-mm-ss.txt
// On startup, compress all previous uncompressed session files → .gz
// ================================================================
const sessionFile = path.join(logFolder, `session_${moment().format('YYYY-MM-DD_HH-mm-ss')}.txt`);
const sessionStream = fs.createWriteStream(sessionFile, { flags: 'a' });

function sessionLog(line) {
    const ts = moment().format('HH:mm:ss');
    sessionStream.write(`[${ts}] ${line}\n`);
}

// Compress old session txt files from previous runs
function compressOldSessions() {
    try {
        const files = fs.readdirSync(logFolder).filter(f =>
            f.startsWith('session_') && f.endsWith('.txt') && path.join(logFolder, f) !== sessionFile
        );
        for (const file of files) {
            const src  = path.join(logFolder, file);
            const dest = src + '.gz';
            if (fs.existsSync(dest)) { fs.unlinkSync(src); continue; }
            const input  = fs.createReadStream(src);
            const output = fs.createWriteStream(dest);
            input.pipe(zlib.createGzip()).pipe(output);
            output.on('finish', () => {
                fs.unlinkSync(src);
                console.log(`📦 Compressed old session: ${file}.gz`);
            });
        }
    } catch (e) {
        console.warn('⚠️ Could not compress old sessions:', e.message);
    }
}

compressOldSessions();

// Patch console → also write to session file (strip ANSI escape codes for clean txt)
const stripAnsi = (s) => String(s).replace(/\x1B\[[0-9;]*m/g, '');
const _log   = console.log.bind(console);
const _warn  = console.warn.bind(console);
const _error = console.error.bind(console);
console.log   = (...a) => { _log(...a);   sessionLog(a.map(stripAnsi).join(' ')); };
console.warn  = (...a) => { _warn(...a);  sessionLog('[WARN] ' + a.map(stripAnsi).join(' ')); };
console.error = (...a) => { _error(...a); sessionLog('[ERROR] ' + a.map(stripAnsi).join(' ')); };

// ================================================================
// DATABASE
// ================================================================
const db = new Database(path.join(__dirname, 'selfbot.db'));

db.exec(`
    CREATE TABLE IF NOT EXISTS guild_activity (
        guild_id   TEXT PRIMARY KEY,
        guild_name TEXT,
        msg_count  INTEGER DEFAULT 0,
        last_seen  INTEGER DEFAULT 0
    );
    CREATE TABLE IF NOT EXISTS state (
        key   TEXT PRIMARY KEY,
        value TEXT
    );
    CREATE TABLE IF NOT EXISTS snipe_history (
        channel_id  TEXT PRIMARY KEY,
        author_tag  TEXT,
        content     TEXT,
        image       TEXT,
        time        TEXT,
        saved_at    INTEGER DEFAULT 0
    );
    CREATE TABLE IF NOT EXISTS message_log (
        id          INTEGER PRIMARY KEY AUTOINCREMENT,
        guild_name  TEXT,
        channel_name TEXT,
        author_tag  TEXT,
        content     TEXT,
        has_attach  INTEGER DEFAULT 0,
        deleted_at  TEXT
    );
`);

const dbGet      = db.prepare('SELECT value FROM state WHERE key = ?');
const dbSet      = db.prepare('INSERT OR REPLACE INTO state (key, value) VALUES (?, ?)');
const dbGetSnipe = db.prepare('SELECT * FROM snipe_history WHERE channel_id = ?');
const dbSetSnipe = db.prepare('INSERT OR REPLACE INTO snipe_history VALUES (?, ?, ?, ?, ?, ?)');

const snipeMap     = new Map();
const editSnipeMap = new Map();
const startTime    = Date.now();
const sleep        = (ms) => new Promise(resolve => setTimeout(resolve, ms));

// BOT_MARKER: ký tự tàng hình gắn vào cuối mọi tin nhắn do bot tự gửi
// Dùng để phân biệt chủ nhắn thật vs bot tự reply — người dùng không thấy, code detect được
const BOT_MARKER    = '\u200B';
const isBotMessage  = (content) => typeof content === 'string' && content.includes(BOT_MARKER);

const savedAfk = dbGet.get('afk_state');
let isAfk        = savedAfk ? JSON.parse(savedAfk.value).active : false;
let afkReason    = savedAfk ? JSON.parse(savedAfk.value).reason : "";
let afkToggledAt = 0;

// ================================================================
// STATUS PERSISTENCE
// Bot giữ nguyên status 24/7 qua lệnh .ss
// Không thể tự detect Discord client đổi status vì selfbot = chính là client
// → chỉ có thể override bằng lệnh, không sync ngược
// ================================================================
const VALID_STATUSES = ['online', 'idle', 'dnd', 'invisible'];
const STATUS_EMOJI   = { online: '🟢', idle: '🟡', dnd: '🔴', invisible: '⚫' };
const STATUS_LABEL   = { online: 'Online', idle: 'Idle', dnd: 'Do Not Disturb', invisible: 'Invisible (Offline)' };

const savedStatusRaw = dbGet.get('user_status');
let statusState = savedStatusRaw
    ? JSON.parse(savedStatusRaw.value)
    : { status: 'online' };

function applyStatus(status) {
    try { client.user.setPresence({ status }); } catch (_) {}
}

function saveStatusState() {
    dbSet.run('user_status', JSON.stringify(statusState));
}

// Apply ngay lập tức khi set, interval 2 phút chỉ để chống Discord server tự reset
setInterval(() => applyStatus(statusState.status), 2 * 60 * 1000);

for (const row of db.prepare('SELECT * FROM snipe_history').all()) {
    snipeMap.set(row.channel_id, {
        content: row.content,
        author: { tag: row.author_tag },
        image: row.image,
        time: row.time
    });
}

// ================================================================
// COOLDOWN
// ================================================================
const cooldowns = new Map();
function isOnCooldown(userId, command, ms) {
    const key  = `${userId}:${command}`;
    const last = cooldowns.get(key) || 0;
    if (Date.now() - last < ms) return true;
    cooldowns.set(key, Date.now());
    return false;
}

// ================================================================
// MESSAGE LOGGER — ghi tin nhắn bị xóa vào DB + file log hằng ngày
// ================================================================
const dbInsertLog = db.prepare('INSERT INTO message_log (guild_name, channel_name, author_tag, content, has_attach, deleted_at) VALUES (?, ?, ?, ?, ?, ?)');

function logDeletedMessage(source, message) {
    const isDM = !message.guildId;
    if (!isDM && !activeGuilds.has(message.guildId)) return;
    if (!source.author || source.author.bot) return;

    const guildName   = isDM ? 'DM' : (message.guild?.name || message.guildId);
    const channelName = isDM ? source.author?.tag : (message.channel?.name || message.channelId);
    const authorTag   = source.author?.tag || 'Unknown';
    const content     = source.content || '';
    const hasAttach   = (source.attachments?.size > 0) ? 1 : 0;
    const deletedAt   = moment().format('YYYY-MM-DD HH:mm:ss');

    try { dbInsertLog.run(guildName, channelName, authorTag, content, hasAttach, deletedAt); } catch (_) {}

    const logFile = path.join(logFolder, `${moment().format('YYYY-MM-DD')}.log`);
    const line    = `[${deletedAt}] [${guildName} / #${channelName}] ${authorTag}: ${content}${hasAttach ? ' [📎 có file]' : ''}\n`;
    fs.appendFile(logFile, line, () => {});
}

// ================================================================
// GUILD WHITELIST — tự động theo dõi top server hay nhắn nhất
// ================================================================
const TOP_GUILD_LIMIT = 8;
const MSG_CACHE_LIMIT = 30;

const guildActivity  = new Map();
const activeGuilds   = new Set();
const recentMsgCache = new Map();

for (const row of db.prepare('SELECT * FROM guild_activity').all()) {
    guildActivity.set(row.guild_id, {
        count: row.msg_count,
        lastSeen: row.last_seen,
        name: row.guild_name
    });
}

let rebuildTimer = null;
function scheduleRebuild() {
    if (rebuildTimer) return;
    rebuildTimer = setTimeout(() => { rebuildActiveGuilds(); rebuildTimer = null; }, 5000);
}

function trackActivity(message) {
    if (!message.guildId || message.author?.id !== client.user?.id) return;
    const g = guildActivity.get(message.guildId) || { count: 0, lastSeen: 0, name: '' };
    g.count++;
    g.lastSeen = Date.now();
    g.name = message.guild?.name || message.guildId;
    guildActivity.set(message.guildId, g);
    db.prepare('INSERT OR REPLACE INTO guild_activity VALUES (?, ?, ?, ?)').run(message.guildId, g.name, g.count, g.lastSeen);
    scheduleRebuild();
}

function rebuildActiveGuilds() {
    const scored = [...guildActivity.entries()].map(([id, data]) => {
        const recencyBonus = Math.max(0, 1 - (Date.now() - data.lastSeen) / (7 * 24 * 60 * 60 * 1000));
        return { id, score: data.count + recencyBonus * 50, name: data.name };
    }).sort((a, b) => b.score - a.score);

    const newTop = new Set(scored.slice(0, TOP_GUILD_LIMIT).map(g => g.id));

    for (const guildId of activeGuilds) {
        if (!newTop.has(guildId)) {
            for (const [channelId] of recentMsgCache) {
                const ch = client.channels.cache.get(channelId);
                if (ch?.guildId === guildId) recentMsgCache.delete(channelId);
            }
            console.log(`📤 [WHITELIST] Bỏ theo dõi: ${guildActivity.get(guildId)?.name || guildId}`);
        }
    }
    for (const g of newTop) {
        if (!activeGuilds.has(g)) console.log(`📥 [WHITELIST] Theo dõi: ${guildActivity.get(g)?.name || g}`);
    }

    activeGuilds.clear();
    newTop.forEach(id => activeGuilds.add(id));
}

async function initActivityFromHistory() {
    if (guildActivity.size > 0) {
        rebuildActiveGuilds();
        console.log(`✅ [DB] Load whitelist: [${[...activeGuilds].map(id => guildActivity.get(id)?.name).join(', ')}]`);
        return;
    }

    console.log("🔍 Lần đầu chạy — quét lịch sử để xác định server hay nhắn nhất...");
    for (const guild of client.guilds.cache.values()) {
        try {
            const channels = guild.channels.cache.filter(c => c.isText?.() && c.permissionsFor?.(client.user)?.has('VIEW_CHANNEL'));
            for (const channel of channels.values()) {
                try {
                    const msgs   = await channel.messages.fetch({ limit: 50 });
                    const myMsgs = msgs.filter(m => m.author.id === client.user.id);
                    if (myMsgs.size === 0) continue;
                    const g = guildActivity.get(guild.id) || { count: 0, lastSeen: 0, name: guild.name };
                    g.count   += myMsgs.size;
                    g.lastSeen = Math.max(g.lastSeen, myMsgs.first()?.createdTimestamp || 0);
                    g.name     = guild.name;
                    guildActivity.set(guild.id, g);
                    db.prepare('INSERT OR REPLACE INTO guild_activity VALUES (?, ?, ?, ?)').run(guild.id, g.name, g.count, g.lastSeen);
                } catch (_) {}
                await sleep(300);
            }
        } catch (_) {}
    }

    rebuildActiveGuilds();
    console.log(`✅ Whitelist tự động: [${[...activeGuilds].map(id => guildActivity.get(id)?.name).join(', ')}]`);
}

// Map lưu file đã pre-download: messageId → fileName
const predownloadedFiles = new Map();

async function cacheMessage(message) {
    if (!message.author || message.author.bot) return;
    if (message.guildId && !activeGuilds.has(message.guildId)) return;
    if (!recentMsgCache.has(message.channelId)) recentMsgCache.set(message.channelId, []);
    const arr = recentMsgCache.get(message.channelId);

    // Pre-download ảnh/file ngay khi message tới — CDN URL còn sống
    // Nếu đợi đến lúc messageDelete thì URL đã 404
    let preFile = null;
    const attachments     = message.attachments;
    const firstAttachment = attachments?.first ? attachments.first() : (attachments?.values ? [...attachments.values()][0] : null);
    if (firstAttachment && firstAttachment.size <= 8388608) {
        try {
            const extension = path.extname(firstAttachment.name) || '.png';
            const fileName  = `snipe_${moment().format('HH-mm-ss')}_${message.author.username}${extension}`;
            const filePath  = path.join(downloadFolder, fileName);
            const response  = await axios({ method: 'GET', url: firstAttachment.url, responseType: 'stream' });
            const writer    = fs.createWriteStream(filePath);
            response.data.pipe(writer);
            await new Promise((res, rej) => { writer.on('finish', res); writer.on('error', rej); });
            preFile = fileName;
            predownloadedFiles.set(message.id, fileName);
        } catch (_) {}
    }

    arr.push({ id: message.id, content: message.content, author: message.author, attachments: message.attachments, preFile, time: moment().format('HH:mm:ss') });
    if (arr.length > MSG_CACHE_LIMIT) {
        const removed = arr.shift();
        // Xóa file pre-download nếu message cũ bị đẩy ra khỏi cache (không bị xóa)
        if (removed?.preFile) predownloadedFiles.delete(removed.id);
    }
}

// Dọn file cũ hơn 48h trong downloads mỗi 12 tiếng
setInterval(() => {
    console.log("🧹 Đang dọn dẹp file cũ trong folder downloads...");
    fs.readdir(downloadFolder, (err, files) => {
        if (err) return;
        files.forEach(file => {
            const filePath = path.join(downloadFolder, file);
            fs.stat(filePath, (err, stats) => {
                if (err) return;
                if (Date.now() > new Date(stats.ctime).getTime() + 172800000) {
                    fs.unlink(filePath, () => console.log(`🗑️ Đã xóa file cũ: ${file}`));
                }
            });
        });
    });
}, 12 * 60 * 60 * 1000);

// ================================================================
// EVENTS
// ================================================================
client.on('ready', async () => {
    console.clear();
    const cpuList = os.cpus();
    const cpuName = (cpuList && cpuList.length > 0) ? cpuList[0].model : "Unknown CPU";
    const shortCpu = cpuName.length > 37 ? cpuName.substring(0, 34) + "..." : cpuName;

    console.log(`▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄`);
    console.log(`█  🤖 SELFBOT V3 - OPTIMIZED FOR 24/7             █`);
    console.log(`█  👤 User: ${client.user.tag.padEnd(36)}  █`);
    console.log(`█  💻 CPU: ${shortCpu.padEnd(38)} █`);
    console.log(`█  ✅ Status: ONLINE | Protection: ON             █`);
    console.log(`▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀`);

    // Khôi phục status từ DB (giữ đúng status trước khi restart/offline)
    applyStatus(statusState.status);
    console.log(`${STATUS_EMOJI[statusState.status]} [STATUS] Khôi phục: ${STATUS_LABEL[statusState.status]}${statusState.intentional ? ' (override 24/7)' : ''}`);

    await initActivityFromHistory();
});

client.on('messageDelete', async (message) => {
    const channelCache = recentMsgCache.get(message.channelId) || [];
    const cached = channelCache.find(m => m.id === message.id);
    const source = cached || message;

    if (!source.author || source.author.bot) return;

    // File đã được tải trước trong cacheMessage — lấy ra dùng luôn, không tải lại (CDN đã 404)
    const savedFile = cached?.preFile || null;
    predownloadedFiles.delete(message.id);

    snipeMap.set(message.channelId, {
        content: source.content,
        author: source.author,
        image: savedFile,
        time: moment().format('HH:mm:ss')
    });

    dbSetSnipe.run(message.channelId, source.author?.tag || 'Unknown', source.content || '', savedFile || null, moment().format('HH:mm:ss'), Date.now());
    logDeletedMessage(source, message);

    if (cached) {
        const arr = recentMsgCache.get(message.channelId);
        const idx = arr.indexOf(cached);
        if (idx !== -1) arr.splice(idx, 1);
    }
});

client.on('messageUpdate', async (oldMessage, newMessage) => {
    if (!oldMessage.author || oldMessage.author.bot) return;
    if (oldMessage.content === newMessage.content) return;
    const channelCache = recentMsgCache.get(oldMessage.channelId) || [];
    const cached = channelCache.find(m => m.id === oldMessage.id);
    editSnipeMap.set(oldMessage.channelId, {
        content: cached?.content || oldMessage.content,
        author: cached?.author || oldMessage.author,
        time: moment().format('HH:mm:ss')
    });
    if (cached) cached.content = newMessage.content;
});

client.on('messageCreate', async (message) => {
    await cacheMessage(message);
    trackActivity(message);

    const isCommand    = message.content.startsWith(config.prefix);
    const isOwnedByBot = isBotMessage(message.content);

    // Tự tắt AFK khi chủ nhắn thật (bỏ qua lệnh và tin nhắn do bot tự gửi)
    if (isAfk && message.author.id === client.user.id && !isCommand && !isOwnedByBot && (Date.now() - afkToggledAt > 3000)) {
        isAfk     = false;
        afkReason = "";
        dbSet.run('afk_state', JSON.stringify({ active: false, reason: "" }));
        console.log(`👋 [AFK] Tự động TẮT — Chủ đã nhắn tin`);
        if (message.channel.type !== 'DM') {
            const notice = await message.channel.send('👋 **AFK tự động tắt**' + BOT_MARKER).catch(() => {});
            if (notice) setTimeout(() => notice.delete().catch(() => {}), 1000);
        }
    }

    // AFK auto-reply: chỉ khi bị mention hoặc nhắn DM
    if (isAfk && message.author.id !== client.user.id && (message.mentions.users.has(client.user.id) || message.channel.type === 'DM')) {
        const isDM     = message.channel.type === 'DM';
        const source   = isDM ? 'DM' : 'MENTION';
        const location = isDM ? 'DM' : `#${message.channel?.name || message.channelId} (${message.guild?.name || message.guildId})`;
        const preview  = (message.content || '').substring(0, 120) + (message.content?.length > 120 ? '...' : '');

        console.log(`\n┌─────────────────────────────────────────────`);
        console.log(`│ 📬 [AFK/${source}] Có tin nhắn mới!`);
        console.log(`│ 👤 Từ      : ${message.author.tag} (${message.author.id})`);
        console.log(`│ 📍 Nơi     : ${location}`);
        console.log(`│ 💬 Nội dung: ${preview || '[không có text]'}`);
        if (message.attachments?.size > 0) console.log(`│ 📎 File    : ${[...message.attachments.values()].map(a => a.name).join(', ')}`);
        console.log(`└─────────────────────────────────────────────`);

        await message.channel.sendTyping();
        await sleep(Math.floor(Math.random() * 600) + 600);

        try {
            const systemPrompt = `Mày là AI đang trực thay cho ${client.user.username}, chủ mày đang bận: "${afkReason}".
Tính cách: hài hước, lầy lội, nói chuyện kiểu gen Z, dùng tiếng Việt, thỉnh thoảng chêm tiếng Anh cho ngầu. Không được nghiêm túc quá.
Trả lời ngắn gọn thôi (1-2 câu), nhớ mention lý do chủ mày bận nếu liên quan. Nếu người ta hỏi gì gấp thì bảo để nhắn lại sau.`;

            const parts    = [];
            const userText = message.content?.trim();
            if (userText) parts.push({ text: `${systemPrompt}\n\nNgười ta nhắn: "${userText}"` });
            else          parts.push({ text: systemPrompt + '\n\nNgười ta chỉ gửi ảnh, không kèm text.' });

            // Fetch ảnh và convert base64 để Gemini Vision đọc được
            const SUPPORTED_IMG = ['image/jpeg', 'image/png', 'image/webp', 'image/gif'];
            const images = [...(message.attachments?.values() || [])].filter(a => {
                const mime = a.contentType?.split(';')[0]?.trim() || '';
                return SUPPORTED_IMG.includes(mime) && a.size <= 10485760;
            });

            for (const img of images) {
                try {
                    const res      = await axios.get(img.url, { responseType: 'arraybuffer' });
                    const base64   = Buffer.from(res.data).toString('base64');
                    const mimeType = img.contentType?.split(';')[0]?.trim() || 'image/jpeg';
                    parts.push({ inlineData: { data: base64, mimeType } });
                    console.log(`│ 🖼️  Đã đọc ảnh: ${img.name} (${(img.size / 1024).toFixed(1)}KB)`);
                } catch (imgErr) {
                    console.warn(`⚠️ [AFK] Không fetch được ảnh ${img.name}: ${imgErr.message}`);
                }
            }

            if (images.length > 0) parts[0].text += '\n(Nếu có ảnh, hãy nhận xét/phản ứng về ảnh đó theo đúng tính cách của mày.)';

            const result   = await generateContent({ contents: [{ role: 'user', parts }] });
            const botReply = result.response.text();
            await message.reply(botReply + BOT_MARKER);
            console.log(`🤖 [AFK/BOT] Đã reply ${message.author.tag}: ${botReply.substring(0, 80)}${botReply.length > 80 ? '...' : ''}`);
        } catch (e) {
            console.error("Lỗi AI:", e);
        }
    }

    if (!message.content.startsWith(config.prefix) || message.author.id !== config.ownerId) return;

    const args    = message.content.slice(config.prefix.length).trim().split(/ +/);
    const command = args.shift().toLowerCase();

    if (command === 'help') {
        const helpText = `\`\`\`asciidoc
=== 📜 MENU LỆNH SELF BOT V3 ===
${config.prefix}snipe        :: Xem tin nhắn/ảnh vừa xóa
${config.prefix}esnipe       :: Xem tin nhắn trước khi sửa
${config.prefix}afk [lý do]  :: Bật/tắt tự động trả lời AI
${config.prefix}ss [status]  :: Set status 24/7 (on/idle/dnd/off)
${config.prefix}cs           :: Reset status về online
${config.prefix}ghost @user  :: Tag rồi xóa (ghost ping)
${config.prefix}ask [câu hỏi]:: Hỏi AI trực tiếp
${config.prefix}tr [lang]    :: Dịch (reply hoặc .tr en [text])
${config.prefix}logs [n]     :: Xem n tin nhắn bị xóa gần nhất
${config.prefix}logs clear   :: Xóa toàn bộ log
${config.prefix}avatar @user :: Lấy avatar
${config.prefix}user @user   :: Xem thông tin user
${config.prefix}ping         :: Xem độ trễ mạng
${config.prefix}stats        :: Xem CPU/RAM/GPU/Uptime
${config.prefix}purge [n]    :: Xóa n tin nhắn của mình
${config.prefix}cleandl      :: Xóa file trong folder downloads
\`\`\``;
        message.edit(helpText).catch(() => message.channel.send(helpText));
    }

    if (command === 'ask' || command === 'ai') {
        if (isOnCooldown(message.author.id, 'ask', 5000))
            return message.edit("⏳ Chờ 5 giây giữa các lần hỏi!").catch(() => {});
        const question = args.join(' ');
        if (!question) return message.edit("❌ Ví dụ: .ask Hôm nay ăn gì?");
        await message.edit(`🤔 **Đang nghĩ:** "${question}"...`);
        try {
            const result = await generateContent(`Bạn là AI thông minh. Hãy trả lời ngắn gọn: ${question}`);
            let res = result.response.text();
            const header = `❓ **${question}**\n🤖 `;
            res = res.length > 1900 - header.length ? res.substring(0, 1900 - header.length) + "..." : res;
            await message.edit(`${header}${res}`);
        } catch (e) {
            console.error("\n❌ LỖI AI CHI TIẾT:", e);
            message.edit(`❌ Lỗi AI: \`${e?.message || String(e)}\``).catch(() => {});
        }
    }

    if (command === 'stats') {
        const msg = await message.edit("🔄 Đang quét phần cứng...");
        try {
            const [gpuData, diskData, netData] = await Promise.all([si.graphics(), si.fsSize(), si.networkStats()]);

            osu.cpuUsage(async function(v) {
                const totalRAM  = (os.totalmem() / 1024 / 1024 / 1024).toFixed(2);
                const freeRAM   = (os.freemem() / 1024 / 1024 / 1024).toFixed(2);
                const usedRAM   = (totalRAM - freeRAM).toFixed(2);
                const ramPercent = ((usedRAM / totalRAM) * 100).toFixed(1);
                const uptime    = moment.duration(Date.now() - startTime);
                const cpuList   = os.cpus();
                const cpuName   = (cpuList && cpuList.length > 0) ? cpuList[0].model : "Unknown";
                const cpuCores  = cpuList ? cpuList.length : 0;

                const gpus     = gpuData.controllers || [];
                const gpuLines = gpus.length > 0
                    ? gpus.map((g, i) => {
                        const vramGB  = g.vram ? (g.vram / 1024).toFixed(1) + 'GB' : 'N/A';
                        const gpuName = (g.model || 'Unknown').substring(0, 35);
                        return `GPU ${i}   : ${gpuName} (${vramGB} VRAM)`;
                    }).join('\n')
                    : 'GPU     : Không phát hiện GPU';

                const mainDisk   = diskData[0];
                const diskUsed   = mainDisk ? (mainDisk.used / 1024 / 1024 / 1024).toFixed(1) : '?';
                const diskTotal  = mainDisk ? (mainDisk.size / 1024 / 1024 / 1024).toFixed(1) : '?';
                const diskPercent = mainDisk ? ((mainDisk.used / mainDisk.size) * 100).toFixed(1) : '?';

                const net   = netData[0];
                const netRx = net ? (net.rx_sec / 1024).toFixed(1) + ' KB/s' : 'N/A';
                const netTx = net ? (net.tx_sec / 1024).toFixed(1) + ' KB/s' : 'N/A';

                const stats = `\`\`\`yaml
💻 HARDWARE INFO
-------------------------------------------
OS      : ${os.type()} ${os.release()} (${os.arch()})
CPU     : ${cpuName}
Cores   : ${cpuCores} Threads | Load: ${(v * 100).toFixed(1)}%
RAM     : ${usedRAM}GB / ${totalRAM}GB (${ramPercent}%)
${gpuLines}
Disk    : ${diskUsed}GB / ${diskTotal}GB (${diskPercent}%)
Network : ↓ ${netRx} | ↑ ${netTx}

⚙️ BOT STATUS
-------------------------------------------
Uptime  : ${uptime.days()}d ${uptime.hours()}h ${uptime.minutes()}m ${uptime.seconds()}s
Ping    : ${client.ws.ping}ms (API)
Cache   : ${recentMsgCache.size} channels | Theo dõi: ${activeGuilds.size}/${TOP_GUILD_LIMIT} servers
\`\`\``;
                msg.edit(stats).catch(() => {});
            });
        } catch (e) {
            console.error("Stats error:", e);
            msg.edit("❌ Lỗi khi quét phần cứng: " + e.message).catch(() => {});
        }
    }

    if (command === 'cleandl') {
        fs.readdir(downloadFolder, (err, files) => {
            if (err) return message.edit("❌ Lỗi đọc folder");
            if (files.length === 0) return message.edit("✅ Folder downloads đã sạch!");
            let count = 0;
            files.forEach(file => { fs.unlinkSync(path.join(downloadFolder, file)); count++; });
            message.edit(`🗑️ Đã xóa sạch **${count}** file media rác!`);
        });
    }

    if (command === 'snipe') {
        const msg = snipeMap.get(message.channelId);
        if (!msg) return message.edit('❌ Không có gì để snipe!').catch(() => {});
        let content = `🕵️ **Snipe (${msg.time})**\n👤 **${msg.author.tag}**: ${msg.content || '[không có text]'}`;
        if (msg.image) content += `\n📁 File: \`${msg.image}\``;
        await message.delete().catch(() => {});
        message.channel.send(content).catch(() => {});
    }

    if (command === 'esnipe') {
        const msg = editSnipeMap.get(message.channelId);
        if (!msg) return message.edit('❌ Không có gì để esnipe!').catch(() => {});
        await message.delete().catch(() => {});
        message.channel.send(`📝 **Edit Snipe (${msg.time})**\n👤 **${msg.author.tag}**: ${msg.content}`).catch(() => {});
    }

    if (command === 'purge') {
        const amount = parseInt(args[0]) || 5;
        message.delete().catch(() => {});
        const fetched = await message.channel.messages.fetch({ limit: 100 });
        const myMsgs  = [...fetched.filter(m => m.author.id === client.user.id).values()].slice(0, amount);
        for (const m of myMsgs) { await m.delete().catch(() => {}); await sleep(800); }
    }

    if (command === 'afk') {
        isAfk        = !isAfk;
        afkReason    = args.join(' ') || "Bận";
        afkToggledAt = Date.now();
        dbSet.run('afk_state', JSON.stringify({ active: isAfk, reason: afkReason }));
        console.log(isAfk ? `💤 [AFK] BẬT — lý do: ${afkReason}` : `👋 [AFK] TẮT thủ công`);
        const notice = await message.channel.send(isAfk ? `💤 **AFK BẬT**: ${afkReason}` + BOT_MARKER : `👋 **AFK TẮT**` + BOT_MARKER).catch(() => {});
        await message.delete().catch(() => {});
        if (notice) setTimeout(() => notice.delete().catch(() => {}), 1000);
    }

    if (command === 'setstatus' || command === 'ss') {
        const aliases = { on: 'online', off: 'invisible', invis: 'invisible', busy: 'dnd' };
        const input   = args[0]?.toLowerCase();
        const target  = aliases[input] || input;

        if (!target || !VALID_STATUSES.includes(target)) {
            return message.edit(
                `❌ Status không hợp lệ!\n` +
                `🟢 \`online\` / \`on\`     →  Online\n` +
                `🟡 \`idle\`              →  Idle\n` +
                `🔴 \`dnd\` / \`busy\`      →  Do Not Disturb\n` +
                `⚫ \`invisible\` / \`off\`  →  Offline (tàng hình)`
            ).catch(() => {});
        }

        statusState.status = target;
        saveStatusState();
        applyStatus(target);

        console.log(`${STATUS_EMOJI[target]} [STATUS] Set 24/7 → ${STATUS_LABEL[target]}`);
        await message.edit(`${STATUS_EMOJI[target]} **Status: ${STATUS_LABEL[target]}** — giữ 24/7, kể cả khi restart`).catch(() => {});
    }

    // Reset về online (xóa override cũ)
    if (command === 'clearstatus' || command === 'cs') {
        statusState.status = 'online';
        saveStatusState();
        applyStatus('online');
        console.log(`🔄 [STATUS] Reset → Online`);
        await message.edit(`🟢 **Status reset về Online**`).catch(() => {});
    }

    if (command === 'tr' || command === 'translate') {
        if (isOnCooldown(message.author.id, 'translate', 4000))
            return message.edit("⏳ Chờ 4 giây!").catch(() => {});
        const targetLang     = args[0] || 'vi';
        const replyMsg       = message.reference ? await message.channel.messages.fetch(message.reference.messageId).catch(() => null) : null;
        const textToTranslate = replyMsg ? replyMsg.content : args.slice(1).join(' ');
        if (!textToTranslate) return message.edit("❌ Reply vào tin nhắn cần dịch, hoặc: `.tr en [text]`").catch(() => {});
        await message.edit(`🔄 Đang dịch...`);
        try {
            const result     = await generateContent(`Dịch đoạn văn sau sang "${targetLang}". Chỉ trả về bản dịch, không giải thích, không thêm gì khác:\n\n${textToTranslate}`);
            const translated = result.response.text().trim();
            const source     = replyMsg ? `\n> ${textToTranslate.substring(0, 80)}${textToTranslate.length > 80 ? '...' : ''}` : '';
            await message.edit(`🌐 **[${targetLang.toUpperCase()}]**${source}\n${translated}`);
        } catch (e) {
            console.error("Translate error:", e);
            message.edit(`❌ Lỗi dịch: \`${e?.message || e}\``).catch(() => {});
        }
    }

    if (command === 'logs') {
        if (args[0] === 'clear') {
            db.prepare('DELETE FROM message_log').run();
            return message.edit('🗑️ Đã xóa toàn bộ log trong DB!').catch(() => {});
        }
        const limit = Math.min(parseInt(args[0]) || 10, 25);
        const rows  = db.prepare('SELECT * FROM message_log ORDER BY id DESC LIMIT ?').all(limit);
        if (rows.length === 0) return message.edit('📭 Chưa có log nào.').catch(() => {});
        const lines = rows.reverse().map(r =>
            `[${r.deleted_at}] **${r.guild_name}/#${r.channel_name}** | ${r.author_tag}: ${(r.content || '').substring(0, 60)}${r.has_attach ? ' 📎' : ''}`
        ).join('\n');
        const out = `📋 **${rows.length} tin nhắn bị xóa gần nhất:**\n${lines}`;
        message.edit(out.substring(0, 1900)).catch(() => message.channel.send(out.substring(0, 1900)).catch(() => {}));
    }

    if (command === 'ghost') {
        if (!message.mentions.users.size) return;
        await sleep(30);
        await message.delete().catch(() => {});
    }

    if (command === 'avatar' || command === 'av') {
        const user = message.mentions.users.first() || client.user;
        message.edit(`🖼️ **Avatar của ${user.tag}:**\n${user.displayAvatarURL({ dynamic: true, size: 4096 })}`).catch(() => {});
    }

    if (command === 'ping') {
        const start = Date.now();
        await message.edit('🏓 Pinging...');
        message.edit(`🏓 **Pong!**\nLatency: ${Date.now() - start}ms | API: ${client.ws.ping}ms`);
    }

    if (command === 'user') {
        const user    = message.mentions.users.first() || client.user;
        const created = moment(user.createdTimestamp).format('DD/MM/YYYY');
        message.edit(`👤 **User:** ${user.tag}\n🆔 **ID:** ${user.id}\n📅 **Ngày tạo:** ${created}`);
    }
});

// ================================================================
// ERROR HANDLING
// ================================================================
process.on('unhandledRejection', (err) => console.error('❌ Unhandled Rejection:', err?.message || err));
process.on('uncaughtException',  (err) => console.error('💥 Uncaught Exception:',  err?.message || err));

// ================================================================
// CONSOLE CLI — nhập lệnh trực tiếp từ terminal, không cần prefix
// Một số lệnh cần channel ID: snipe/esnipe/purge <channelId> [...]
// ================================================================
const readline = require('readline');

const rl = readline.createInterface({ input: process.stdin, output: process.stdout, terminal: false });

async function handleConsoleCommand(input) {
    // Strip leading prefix if user accidentally types it in console
    const stripped = input.trim().replace(/^[.!,/]/, '');
    const parts    = stripped.split(/ +/);
    const command  = parts[0]?.toLowerCase();
    const args     = parts.slice(1);

    if (!command) return;

    // --- STATUS ---
    if (command === 'ss' || command === 'setstatus') {
        const aliases = { on: 'online', off: 'invisible', invis: 'invisible', busy: 'dnd' };
        const target  = aliases[args[0]] || args[0];
        if (!target || !VALID_STATUSES.includes(target)) {
            return console.log(`❌ Dùng: ss <online|idle|dnd|invisible|on|off|busy>`);
        }
        statusState.status = target;
        saveStatusState();
        applyStatus(target);
        console.log(`${STATUS_EMOJI[target]} [STATUS] Set → ${STATUS_LABEL[target]}`);
    }

    else if (command === 'cs' || command === 'clearstatus') {
        statusState.status = 'online';
        saveStatusState();
        applyStatus('online');
        console.log(`🟢 [STATUS] Reset → Online`);
    }

    // --- AFK ---
    else if (command === 'afk') {
        isAfk        = !isAfk;
        afkReason    = args.join(' ') || "Bận";
        afkToggledAt = Date.now();
        dbSet.run('afk_state', JSON.stringify({ active: isAfk, reason: afkReason }));
        console.log(isAfk ? `💤 [AFK] BẬT — lý do: ${afkReason}` : `👋 [AFK] TẮT`);
    }

    // --- PING ---
    else if (command === 'ping') {
        console.log(`🏓 API Ping: ${client.ws.ping}ms`);
    }

    // --- STATS ---
    else if (command === 'stats') {
        try {
            const [gpuData, diskData, netData] = await Promise.all([si.graphics(), si.fsSize(), si.networkStats()]);
            osu.cpuUsage((v) => {
                const totalRAM   = (os.totalmem() / 1024 / 1024 / 1024).toFixed(2);
                const freeRAM    = (os.freemem()  / 1024 / 1024 / 1024).toFixed(2);
                const usedRAM    = (totalRAM - freeRAM).toFixed(2);
                const uptime     = moment.duration(Date.now() - startTime);
                const cpuList    = os.cpus();
                const cpuName    = cpuList?.[0]?.model || 'Unknown';
                const mainDisk   = diskData[0];
                const net        = netData[0];
                console.log(`\n💻 CPU: ${cpuName} | Load: ${(v * 100).toFixed(1)}%`);
                console.log(`🧠 RAM: ${usedRAM}GB / ${totalRAM}GB`);
                if (mainDisk) console.log(`💾 Disk: ${(mainDisk.used/1e9).toFixed(1)}GB / ${(mainDisk.size/1e9).toFixed(1)}GB`);
                if (net) console.log(`🌐 Net: ↓${(net.rx_sec/1024).toFixed(1)}KB/s ↑${(net.tx_sec/1024).toFixed(1)}KB/s`);
                if (gpuData.controllers?.[0]) console.log(`🎮 GPU: ${gpuData.controllers[0].model}`);
                console.log(`⏱️  Uptime: ${uptime.days()}d ${uptime.hours()}h ${uptime.minutes()}m | Ping: ${client.ws.ping}ms\n`);
            });
        } catch (e) { console.error('❌ Stats error:', e.message); }
    }

    // --- ASK AI ---
    else if (command === 'ask' || command === 'ai') {
        const question = args.join(' ');
        if (!question) return console.log('❌ Dùng: ask <câu hỏi>');
        if (isOnCooldown('console', 'ask', 5000)) return console.log('⏳ Chờ 5 giây!');
        console.log(`🤔 Đang hỏi AI...`);
        try {
            const result = await generateContent(`Bạn là AI thông minh. Hãy trả lời ngắn gọn: ${question}`);
            console.log(`🤖 ${result.response.text().trim()}`);
        } catch (e) { console.error('❌ AI error:', e.message); }
    }

    // --- TRANSLATE ---
    else if (command === 'tr' || command === 'translate') {
        const lang = args[0] || 'vi';
        const text = args.slice(1).join(' ');
        if (!text) return console.log('❌ Dùng: tr <lang> <text>');
        if (isOnCooldown('console', 'translate', 4000)) return console.log('⏳ Chờ 4 giây!');
        console.log(`🔄 Đang dịch...`);
        try {
            const result = await generateContent(`Dịch sang "${lang}". Chỉ trả về bản dịch:\n\n${text}`);
            console.log(`🌐 [${lang.toUpperCase()}] ${result.response.text().trim()}`);
        } catch (e) { console.error('❌ Translate error:', e.message); }
    }

    // --- LOGS ---
    else if (command === 'logs') {
        if (args[0] === 'clear') {
            db.prepare('DELETE FROM message_log').run();
            return console.log('🗑️ Đã xóa toàn bộ log!');
        }
        const limit = Math.min(parseInt(args[0]) || 10, 50);
        const rows  = db.prepare('SELECT * FROM message_log ORDER BY id DESC LIMIT ?').all(limit);
        if (rows.length === 0) return console.log('📭 Chưa có log nào.');
        console.log(`\n📋 ${rows.length} tin nhắn bị xóa gần nhất:`);
        rows.reverse().forEach(r => console.log(`  [${r.deleted_at}] ${r.guild_name}/#${r.channel_name} | ${r.author_tag}: ${(r.content||'').substring(0,80)}${r.has_attach?' 📎':''}`));
        console.log('');
    }

    // --- SNIPE (cần channelId) ---
    else if (command === 'snipe') {
        const channelId = args[0];
        if (!channelId) return console.log('❌ Dùng: snipe <channelId>');
        const msg = snipeMap.get(channelId);
        if (!msg) return console.log('❌ Không có gì để snipe trong channel này!');
        console.log(`🕵️ Snipe (${msg.time}) | ${msg.author.tag}: ${msg.content || '[không có text]'}${msg.image ? ` | 📁 ${msg.image}` : ''}`);
    }

    else if (command === 'esnipe') {
        const channelId = args[0];
        if (!channelId) return console.log('❌ Dùng: esnipe <channelId>');
        const msg = editSnipeMap.get(channelId);
        if (!msg) return console.log('❌ Không có gì để esnipe!');
        console.log(`📝 Edit Snipe (${msg.time}) | ${msg.author.tag}: ${msg.content}`);
    }

    // --- PURGE (cần channelId) ---
    else if (command === 'purge') {
        const channelId = args[0];
        const amount    = parseInt(args[1]) || 5;
        if (!channelId) return console.log('❌ Dùng: purge <channelId> [số lượng]');
        const channel = client.channels.cache.get(channelId);
        if (!channel) return console.log('❌ Không tìm thấy channel!');
        const fetched = await channel.messages.fetch({ limit: 100 });
        const myMsgs  = [...fetched.filter(m => m.author.id === client.user.id).values()].slice(0, amount);
        for (const m of myMsgs) { await m.delete().catch(() => {}); await sleep(800); }
        console.log(`🗑️ Đã xóa ${myMsgs.length} tin nhắn trong #${channel.name}`);
    }

    // --- CLEANDL ---
    else if (command === 'cleandl') {
        const files = fs.readdirSync(downloadFolder);
        if (files.length === 0) return console.log('✅ Folder downloads đã sạch!');
        files.forEach(f => fs.unlinkSync(path.join(downloadFolder, f)));
        console.log(`🗑️ Đã xóa ${files.length} file trong downloads/`);
    }

    // --- HELP ---
    else if (command === 'help') {
        console.log(`
┌─────────────────────────────────────────────────
│ 📟 CONSOLE COMMANDS (không cần prefix)
├─────────────────────────────────────────────────
│ ss <on|idle|dnd|off>      Set status 24/7
│ cs                        Reset status → online
│ afk [lý do]               Bật/tắt AFK
│ ping                      Xem ping API
│ stats                     Xem CPU/RAM/GPU
│ ask <câu hỏi>             Hỏi AI
│ tr <lang> <text>          Dịch văn bản
│ logs [n|clear]            Xem/xóa log tin nhắn xóa
│ snipe <channelId>         Xem snipe
│ esnipe <channelId>        Xem edit snipe
│ purge <channelId> [n]     Xóa tin nhắn của mình
│ cleandl                   Xóa file trong downloads/
└─────────────────────────────────────────────────`);
    }

    else {
        console.log(`❓ Lệnh không tồn tại. Gõ "help" để xem danh sách.`);
    }
}

rl.on('line', (line) => {
    if (!client.isReady()) return console.log('⏳ Bot chưa sẵn sàng...');
    handleConsoleCommand(line).catch(err => console.error('❌ Console error:', err?.message || err));
});

client.login(config.token).catch(err => {
    console.error('🔑 Login thất bại — kiểm tra lại token:', err.message);
    process.exit(1);
});
