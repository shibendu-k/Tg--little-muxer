# Tg--little-muxer

An ultra-lightweight **FFmpeg Micro-Muxer** Telegram bot built with [Pyrogram](https://pyrogram.org/).  
Hosted on an Oracle ARM Free Tier server; performs **stream-copy only** (`-c copy`) — zero CPU encoding load.

---

## Features

| Feature | Detail |
|---|---|
| **Admin-only (Ghost Mode)** | Silent ignore for every non-admin user |
| **Local Bot API** | Bypasses the 50 MB limit (up to 2 GB / 4 GB for Prime) |
| **Auto-detection** | Recognises `.mkv` / `.mp4` sent as Video or Document |
| **FFprobe scan** | Detects all audio tracks and displays codec, language, channels |
| **Real-time progress** | Throttled progress bar while downloading and uploading |
| **Stream-copy muxing** | Zero-encoding FFmpeg operations via inline keyboard |
| **Guaranteed cleanup** | `try…finally` removes input + output files after every task |

### Inline keyboard actions

| Menu | Button | FFmpeg mapping |
|---|---|---|
| Main | 🎬 Just Convert to MP4 | `-map 0 -c copy` |
| Main → Isolate Audio | 🎵 Track *N* | `-map 0:v:0 -map 0:a:{track_idx} -c copy` |
| Main → Set Default Track | 🎵 Track *N* | `-map 0 -c copy -disposition:a 0 -disposition:a:{track_idx} default` |
| Main → Edit Audio Metadata | 🎵 Track *N* | `-map 0 -c copy -metadata:s:a:{track_idx} title="<new title>"` |
| Main → Remove Specific Audios | ✅ Execute Selected | `-map 0:v:0 -map 0:a:{keep_idx} -c copy` |
| Main → Remove Specific Subtitles | ✅ Execute Selected | `-map 0:v:0 -map 0:s:{keep_idx} -c copy` |
| Main | 🧹 Remove All Subtitles | `-map 0:v -map 0:a -c copy -sn` |
| Main → Multi-Extract | ✅ Execute Selected | `-map 0:<type>:<idx> -c copy` |
| Main → Add External Track | Convert/Keep + optional metadata edit | `-map 0 -map 1 -c copy` (+ optional AAC re-encode) |
| Main | 🗑 Cancel | Removes the downloaded file immediately |

---

## Requirements

- Python 3.9+
- `ffmpeg` and `ffprobe` installed on the server
- A running [Telegram Bot API server](https://github.com/tdlib/telegram-bot-api) (for large-file support)

---

## Quick start

```bash
# 1. Clone
git clone https://github.com/shibendu-k/Tg--little-muxer.git
cd Tg--little-muxer

# 2. Install Python dependencies
pip install -r requirements.txt

# 3. Configure environment
cp .env.example .env
# Edit .env with your API_ID, API_HASH, BOT_TOKEN, LOCAL_API_URL, ADMIN_USER_ID

# 4. Run
python bot.py
```

---

## Docker

### Build & run

```bash
cp .env.example .env
# edit .env with your values
docker build -t tg-little-muxer .
docker run --env-file .env \
  --name tg-little-muxer \
  --restart unless-stopped \
  -v "$(pwd)/downloads:/app/downloads" \
  tg-little-muxer
```

### Docker Compose

```bash
cp .env.example .env
# edit .env with your values
docker compose up -d --build
```

**Note:** `LOCAL_API_URL` must be reachable from inside the container.
If your Bot API server runs on the host, use `http://host.docker.internal:8081`
on Docker Desktop, or the host's LAN IP on Linux. If it runs in another
container, point to that service name (e.g. `http://bot-api:8081`).

These Docker assets are compatible with common hosting platforms that accept
Docker images (Render, Railway, Fly.io, etc.).

### Environment variables (`.env`)

| Variable | Description |
|---|---|
| `API_ID` | Telegram API ID from https://my.telegram.org/apps |
| `API_HASH` | Telegram API hash |
| `BOT_TOKEN` | Bot token from @BotFather |
| `LOCAL_API_URL` | URL of your local Bot API server (default `http://localhost:8081`) |
| `ADMIN_USER_ID` | Your numeric Telegram user ID — the **only** user the bot responds to |

---

## Privacy & Security

- **Zero media logging** — file names, paths, and stream info are never written to the terminal.
- **Admin-only filter** — all other users are silently ignored at the Pyrogram filter level.
- **Disk cleanup** — both input and output files are deleted in a `finally` block after every operation, keeping the Oracle disk 100 % clean.

---

## License

See [LICENSE](LICENSE).
