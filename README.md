# Discord Selfbot

Discord selfbot with Gemini AI auto-reply, snipe, status persistence, and console CLI.

---

## Setup

### 1. Install dependencies

```bash
npm install discord.js-selfbot-v13 @google/generative-ai moment os-utils axios systeminformation better-sqlite3
```

### 2. Configure `config.json`

The file is already created — just fill in your values:

```json
{
    "token": "your discord account token",
    "ownerId": "your discord user id",
    "prefix": ".",
    "geminiApiKey": "your gemini api key"
}
```

**token** — Browser Discord → F12 → Network tab → send any message → find request → copy `Authorization` header value

**ownerId** — Discord Settings → Advanced → enable Developer Mode → right-click your name → Copy User ID

**prefix** — character before every command (`.` recommended)

**geminiApiKey** — free at https://aistudio.google.com/app/apikey

### 3. Run

```bash
node index.js
```

---

## Commands

Commands work both in Discord (with prefix) and in the terminal (no prefix needed).

| Command | Description |
|---|---|
| `afk [reason]` | Toggle AFK mode — AI auto-replies when mentioned or DMed |
| `ss <status>` | Set status 24/7: `online` / `on`, `idle`, `dnd` / `busy`, `invisible` / `off` |
| `cs` | Reset status back to online |
| `ask <question>` | Ask Gemini AI directly |
| `tr <lang> <text>` | Translate text — or reply to a message with `.tr en` |
| `snipe` | Show last deleted message in channel |
| `esnipe` | Show last edited message in channel |
| `purge [n]` | Delete your last n messages (default: 5) |
| `logs [n]` | Show last n deleted messages logged (default: 10) |
| `logs clear` | Clear all logs from DB |
| `ghost @user` | Ghost ping a user |
| `avatar [@user]` | Get avatar URL |
| `user [@user]` | Show user info |
| `ping` | Show API latency |
| `stats` | Show CPU / RAM / GPU / Disk / Uptime |
| `cleandl` | Clear files in `downloads/` folder |

> Terminal-only: `snipe`, `esnipe`, `purge` require a channel ID as the first argument
> e.g. `snipe 123456789012345678`

---

## Features

**AFK AI Auto-reply**
When AFK is on, the bot replies to anyone who mentions you or sends a DM using Gemini AI. Supports reading images — if someone sends a photo, the AI will react to it too. AFK auto-disables when you type a real message. Bot messages are tagged with an invisible character (`\u200B`) so the bot can tell the difference between your real messages and its own.

**Status Persistence**
Set a status once with `.ss` and the bot holds it across restarts, re-applies it every 2 minutes, and applies it immediately every time you use the command.

**Snipe**
Deleted and edited messages are cached in memory and persisted to SQLite. Attached images under 8MB are downloaded to `downloads/` for snipe retrieval.

**Guild Whitelist**
The bot automatically tracks the top 8 servers you are most active in and only logs deleted messages from those servers.

**Console CLI**
Full command support from the terminal — no prefix needed, output goes to console instead of Discord.

---

## File Structure

```
├── index.js
├── config.json
├── selfbot.db       # auto-created — stores AFK state, status, snipe history, logs
├── downloads/       # auto-created — snipe media files (auto-cleaned after 48h)
└── logs/            # auto-created — daily deleted message log files (YYYY-MM-DD.log)
```

---

> ⚠️ Using a selfbot violates Discord's Terms of Service. Use at your own risk.
