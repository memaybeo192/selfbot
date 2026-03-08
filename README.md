# 🤖 Discord Selfbot — AI-Powered

> Discord selfbot viết bằng Node.js với AI tích hợp (Gemini), web search (Tavily), SQLite logging, snipe, noitu, và nhiều hơn nữa.  
> Sử dụng `discord.js-selfbot-v13` — chạy dưới user account.

---

> ⚠️ **Disclaimer**: Selfbot vi phạm [Discord ToS](https://discord.com/terms). Project này chỉ dành cho mục đích nghiên cứu/học tập. Sử dụng có trách nhiệm và tự chịu rủi ro.

---

## ✨ Tính năng

| Module | Mô tả |
|--------|-------|
| 🧠 **Master AI** | AI có ngữ cảnh đầy đủ, hỗ trợ directive, inject history người dùng |
| 🔎 **Web Search** | Tích hợp Tavily API hoặc Google Grounding để AI tìm kiếm real-time |
| 🎨 **Image Gen** | Tạo ảnh qua Gemini image model (`paid` + `free` key pool riêng) |
| 🔑 **Multi-Key Pool** | Xoay vòng nhiều Gemini API key, tự động fallback model, spam probe khi hết quota |
| 💬 **AFK Auto-Reply** | Tự động reply khi bị mention lúc AFK, lưu lịch sử pending |
| 🕵️ **Snipe / Edit Snipe** | Bắt tin nhắn bị xóa/sửa, pre-download ảnh đính kèm |
| 🀄 **Nối từ (Noitu)** | Chơi nối từ tự động với AI |
| 🌐 **Translate** | Dịch văn bản sang ngôn ngữ bất kỳ |
| 🚫 **Anti-Spam** | Tự động phát hiện và silence user spam mention |
| 📋 **Delete Log** | SQLite lưu toàn bộ tin nhắn bị xóa để tra cứu sau |
| 📟 **Console Commands** | Điều khiển bot qua terminal không cần chat |

---

## 📦 Yêu cầu

- **Node.js** >= 18
- **npm** >= 9
- Tài khoản Discord (user token — không phải bot token)
- [Gemini API key](https://aistudio.google.com/app/apikey) (free hoặc paid)
- *(Tùy chọn)* [Tavily API key](https://tavily.com) cho web search

---

## 🚀 Cài đặt

```bash
# 1. Clone repo
git clone https://github.com/memaybeo192/selfbot.git
cd selfbot

# 2. Cài dependencies
npm install

# 3. Cấu hình
cp config.json
# Mở config.json và điền token + API keys

# 4. Chạy
node index.js
```

---

## ⚙️ Cấu hình (`config.json`)

Điền thông tin vào config.json:

```jsonc
{
  "token": "YOUR_DISCORD_TOKEN",        // Token Discord user account
  "prefix": ".",                        // Prefix lệnh (mặc định: dấu chấm)
  "ownerId": "YOUR_DISCORD_USER_ID",    // ID Discord của bạn

  // Free Gemini keys (Google AI Studio)
  "geminiApiKeys": ["key1", "key2"],

  // Paid/billing Gemini keys — được ưu tiên dùng trước, có access Pro model
  "paidApiKeys": ["paid_key1"],

  // Web search engine
  "useTavily": true,                    // false = dùng Google Grounding thay thế
  "tavilyApiKeys": ["tvly-..."],

  "snipePredownload": true              // Pre-download ảnh khi snipe
}
```


### Lấy Discord Token

1. Mở Discord trên trình duyệt
2. F12 → Console
3. Dán đoạn sau vào console:
```js
webpackChunkdiscord_app.push([[Math.random()],{},e=>{m=[];for(let c in e.c)m.push(e.c[c])}]);
m.find(m=>m?.exports?.default?.getToken).exports.default.getToken()
```
4. Copy chuỗi token trả về

### Lấy Gemini API Key

Truy cập [Google AI Studio](https://aistudio.google.com/app/apikey) → **Create API Key** → Free tier đủ dùng cho phần lớn tính năng.

---

## 📖 Lệnh Discord

Tất cả lệnh bắt đầu bằng prefix (mặc định `.`)

### AI & Tìm kiếm

| Lệnh | Mô tả |
|------|-------|
| `.ask <câu hỏi>` | Hỏi AI nhanh (không lưu lịch sử) |
| `.sum [n]` | Tóm tắt `n` tin nhắn gần nhất trong kênh |
| `.tr <ngôn ngữ> <văn bản>` | Dịch sang ngôn ngữ chỉ định |
| `.analyse @user` | Phân tích tâm lý user (compact, ~2000 ký tự) |
| `.analysefull @user` | Phân tích chi tiết (file đính kèm, 5000–6000 ký tự) |
| `.mimic @user` | Học văn phong và bắt chước user |
| `.taoanh <mô tả>` | Tạo ảnh AI từ mô tả |

### Tiện ích

| Lệnh | Mô tả |
|------|-------|
| `.ss <on\|idle\|dnd\|off>` | Đặt status 24/7 |
| `.afk [lý do]` | Bật/tắt chế độ AFK |
| `.snipe` | Xem tin nhắn bị xóa gần nhất trong kênh |
| `.esnipe` | Xem tin nhắn bị sửa gần nhất |
| `.purge [n]` | Xóa `n` tin nhắn của mình trong kênh (mặc định: 5) |
| `.noitu` | Bắt đầu/dừng chơi nối từ tự động |
| `.ping` | Xem latency |

---

## 📟 Console Commands (Terminal)

Gõ lệnh trực tiếp vào terminal đang chạy bot:

```
┌─────────────────────────────────────────────────────
│ 🧠 MASTER AI
│  m <câu hỏi>             Hỏi Master AI — có ngữ cảnh đầy đủ
│  da <mô tả>              AI tạo directive từ ngôn ngữ tự nhiên
│  da edit <id> <mô tả>    AI sửa directive có sẵn
│  da rm <id>              Xóa directive
│  brain stats             Thống kê Master AI
│  brain reset             Reset lịch sử
│  directive list/add/rm/clear
├─────────────────────────────────────────────────────
│ ⚙️ HỆ THỐNG
│  ss <on|idle|dnd|off>    Set status
│  afk [lý do]             Bật/tắt AFK
│  ping                    Xem ping
│  stats                   CPU / RAM / GPU
│  ask <câu hỏi>           Hỏi AI đơn (không history)
│  tr <lang> <text>        Dịch văn bản
│  keypool / keys          Trạng thái Gemini key pool
│  imgkeypool / imgkeys    Trạng thái image-gen key pool
│  tavilykeys              Trạng thái Tavily key pool
│  searchmode              Search engine đang dùng
├─────────────────────────────────────────────────────
│ 🗂️ DATA
│  logs [n|clear]          Xem/xóa log tin nhắn bị xóa
│  snipe <channelId>       Xem snipe theo channel ID
│  esnipe <channelId>      Xem edit snipe theo channel ID
│  purge <channelId> [n]   Xóa tin nhắn của mình qua terminal
│  cleandl                 Xóa file trong downloads/
│  predownload on/off      Bật/tắt pre-download ảnh snipe
│  wl [list|rm|reset|clear] Quản lý whitelist server
├─────────────────────────────────────────────────────
│ 🚫 ANTI-SPAM
│  spam list               Danh sách user bị flag
│  spam flag <id> [phút]   Flag thủ công (mặc định 30p)
│  spam unflag <id>        Bỏ flag
│  spam clear              Xóa tất cả flag
└─────────────────────────────────────────────────────
```

---

## 🔑 Gemini Key Pool

Bot hỗ trợ nhiều API key xoay vòng tự động:

- **`paidApiKeys`** — ưu tiên sử dụng trước, có quyền truy cập model Pro
- **`geminiApiKeys`** — free keys, dùng cho flash / fallback
- Mỗi key có 2 tier model: `primary` → `fallback1` (Flash-Lite)
- Khi một key hết quota → tự động sang key tiếp theo
- Khi **tất cả** key hết quota → bot spam probe mỗi 15 giây, tự động tiếp tục khi có key hồi (~2h)
- Xem trạng thái realtime: gõ `keys` trong terminal

---

## 🗄️ Database (SQLite)

Bot tự tạo các file `.db` khi khởi động — không cần cài thêm gì:

| File | Nội dung |
|------|----------|
| `messages.db` | Lịch sử tin nhắn cho AI phân tích (`.analyse`, `.mimic`) |
| `deleted_messages.db` | Log tin nhắn bị xóa (lệnh `logs`) |

---

## 📁 Cấu trúc Project

```
selfbot/
├── index.js                # Toàn bộ logic bot
├── config.json             # Cấu hình cá nhân (KHÔNG commit lên git)
├── config.template.json    # Template cấu hình để share
├── package.json
├── downloads/              # Ảnh pre-download từ snipe (tự tạo)
├── messages.db             # SQLite — message context (tự tạo)
└── deleted_messages.db     # SQLite — delete log (tự tạo)
```

---

## 🛡️ `.gitignore` khuyến nghị

```
config.json
node_modules/
downloads/
*.db
```

> **Tuyệt đối không commit `config.json`** — chứa token và API keys.

---

## 🐛 Xử lý sự cố

| Vấn đề | Giải pháp |
|--------|-----------|
| `Login thất bại` | Token sai hoặc đã expire — lấy lại token mới |
| `Không có Gemini API key` | Thêm ít nhất 1 key vào `geminiApiKeys` hoặc `paidApiKeys` |
| AI không trả lời | Gõ `keys` trong terminal kiểm tra trạng thái key pool |
| Web search không hoạt động | Kiểm tra `tavilyApiKeys`, hoặc đổi `"useTavily": false` |
| Bot tự mute sau nhiều tin nhắn | Anti-spam đang hoạt động — gõ `spam clear` hoặc tăng `tuning.spamThreshold` |
| `better-sqlite3` lỗi khi cài | Chạy `npm rebuild better-sqlite3` hoặc cài lại Node.js đúng version |

---

## 📄 License

MIT License — free to use, modify and distribute.
