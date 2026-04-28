"""FFmpeg Micro-Muxer Telegram Bot.

A zero-encoding, stream-copy muxer bot powered by Pyrogram.
Designed for an Oracle ARM Free Tier server.
"""
from __future__ import annotations

import asyncio
import json
import math
import os
import time
from pathlib import Path

from dotenv import load_dotenv
from pyrogram import Client, filters
from pyrogram.types import (
    CallbackQuery,
    InlineKeyboardButton,
    InlineKeyboardMarkup,
    Message,
)

load_dotenv()

# ── Configuration (strict .env) ───────────────────────────────────────────────
API_ID: int = int(os.environ["API_ID"])
API_HASH: str = os.environ["API_HASH"]
BOT_TOKEN: str = os.environ["BOT_TOKEN"]
LOCAL_API_URL: str = os.environ.get("LOCAL_API_URL", "http://localhost:8081")
ADMIN_USER_ID: int = int(os.environ["ADMIN_USER_ID"])

# ── Storage ───────────────────────────────────────────────────────────────────
DOWNLOADS_DIR = Path("downloads")
DOWNLOADS_DIR.mkdir(exist_ok=True)

# ── Pyrogram client (local Bot API server) ────────────────────────────────────
app = Client(
    "muxer_bot",
    api_id=API_ID,
    api_hash=API_HASH,
    bot_token=BOT_TOKEN,
    base_url=LOCAL_API_URL.rstrip("/") + "/",
)

# ── In-memory operation registry ─────────────────────────────────────────────
# pending_ops[op_id] = {"input": str, "tracks": list[dict], "chat_id": int}
pending_ops: dict[str, dict] = {}
_op_counter: int = 0

# Progress-bar throttle: msg_id → last_update_monotonic
_progress_ts: dict[int, float] = {}
_PROGRESS_INTERVAL = 2.0  # seconds between edits


# ── Helpers ───────────────────────────────────────────────────────────────────

def _next_op_id() -> str:
    global _op_counter
    _op_counter += 1
    return str(_op_counter)


def _make_bar(current: int, total: int, width: int = 20) -> str:
    if total <= 0:
        return f"[{'░' * width}] 0%"
    filled = int(width * current / total)
    pct = current * 100 / total if total else 0.0
    return f"[{'█' * filled}{'░' * (width - filled)}] {pct:.1f}%"


def _human_size(n: int) -> str:
    if n == 0:
        return "0 B"
    units = ("B", "KiB", "MiB", "GiB", "TiB")
    i = min(int(math.floor(math.log(n, 1024))), len(units) - 1)
    return f"{n / (1024 ** i):.2f} {units[i]}"


async def _progress(current: int, total: int, msg: Message, label: str) -> None:
    """Throttled progress bar editor."""
    now = time.monotonic()
    if current < total and now - _progress_ts.get(msg.id, 0) < _PROGRESS_INTERVAL:
        return
    _progress_ts[msg.id] = now
    try:
        await msg.edit_text(
            f"**{label}…**\n{_make_bar(current, total)}\n"
            f"{_human_size(current)} / {_human_size(total)}"
        )
    except Exception:
        pass


def _cleanup(*paths: str) -> None:
    """Silently remove files; leaves no trace on disk."""
    for p in paths:
        try:
            if p and os.path.exists(p):
                os.remove(p)
        except OSError:
            pass


# ── FFprobe / FFmpeg (async, non-blocking) ────────────────────────────────────

async def _ffprobe_audio(input_path: str) -> list[dict]:
    """Return audio stream descriptors from ffprobe (zero stderr logging)."""
    proc = await asyncio.create_subprocess_exec(
        "ffprobe", "-v", "quiet",
        "-print_format", "json",
        "-show_streams",
        "-select_streams", "a",
        input_path,
        stdout=asyncio.subprocess.PIPE,
        stderr=asyncio.subprocess.DEVNULL,
    )
    stdout, _ = await proc.communicate()
    if proc.returncode != 0:
        return []
    try:
        return json.loads(stdout).get("streams", [])
    except (json.JSONDecodeError, KeyError):
        return []


async def _run_ffmpeg(cmd: list) -> tuple[bool, str]:
    """Execute an FFmpeg command asynchronously. Returns (success, stderr_tail)."""
    proc = await asyncio.create_subprocess_exec(
        *cmd,
        stdout=asyncio.subprocess.DEVNULL,
        stderr=asyncio.subprocess.PIPE,
    )
    _, stderr_raw = await proc.communicate()
    ok = proc.returncode == 0
    # Only surface stderr on failure; never log on success (privacy)
    tail = stderr_raw[-2048:].decode(errors="replace") if not ok else ""
    return ok, tail


def _build_ffmpeg_cmd(
    input_path: str,
    output_path: str,
    mode: str,
) -> list | None:
    """Build a stream-copy FFmpeg command for the given mode."""
    base = ["ffmpeg", "-y", "-i", input_path]
    if mode == "all":
        return base + ["-map", "0", "-c", "copy", output_path]
    if mode == "t1":
        return base + ["-map", "0:v:0", "-map", "0:a:0", "-c", "copy", output_path]
    if mode == "t2":
        return base + ["-map", "0:v:0", "-map", "0:a:1", "-c", "copy", output_path]
    if mode == "hindi":
        return base + [
            "-map", "0:v:0",
            "-map", "0:a:1",
            "-c", "copy",
            "-disposition:a:0", "default",
            "-metadata:s:a:0", "title=Hindi",
            output_path,
        ]
    return None


# ── Inline keyboard ───────────────────────────────────────────────────────────

def _build_keyboard(op_id: str, track_count: int) -> InlineKeyboardMarkup:
    buttons: list[list[InlineKeyboardButton]] = [
        [InlineKeyboardButton(
            "🎬 Convert  (Keep All Tracks)",
            callback_data=f"mux:all:{op_id}",
        )],
        [InlineKeyboardButton(
            "🎵 Keep Track 1 Only (MP4)",
            callback_data=f"mux:t1:{op_id}",
        )],
    ]
    if track_count >= 2:
        buttons += [
            [InlineKeyboardButton(
                "🎵 Keep Track 2 Only (MP4)",
                callback_data=f"mux:t2:{op_id}",
            )],
            [InlineKeyboardButton(
                "🇮🇳 Set Track 2 as Default & Name it 'Hindi'",
                callback_data=f"mux:hindi:{op_id}",
            )],
        ]
    buttons += [
        [
            InlineKeyboardButton("➕ Add Audio",     callback_data=f"stub:add_audio:{op_id}"),
            InlineKeyboardButton("➖ Remove Audio",  callback_data=f"stub:rm_audio:{op_id}"),
            InlineKeyboardButton("📤 Extract Audio", callback_data=f"stub:ext_audio:{op_id}"),
        ],
        [
            InlineKeyboardButton("📝 Add Subtitle",     callback_data=f"stub:add_sub:{op_id}"),
            InlineKeyboardButton("📝 Remove Subtitle",  callback_data=f"stub:rm_sub:{op_id}"),
            InlineKeyboardButton("📤 Extract Subtitle", callback_data=f"stub:ext_sub:{op_id}"),
        ],
        [InlineKeyboardButton("🗑 Cancel & Delete", callback_data=f"cancel:{op_id}")],
    ]
    return InlineKeyboardMarkup(buttons)


# ── Message handler ───────────────────────────────────────────────────────────

@app.on_message(
    filters.user(ADMIN_USER_ID) & (filters.video | filters.document)
)
async def on_video(client: Client, message: Message) -> None:
    """Auto-detect MKV/MP4, download, probe, and present mux options."""
    file_obj = message.video or message.document
    if file_obj is None:
        return

    raw_name: str = getattr(file_obj, "file_name", None) or ""
    mime: str = getattr(file_obj, "mime_type", "") or ""
    ext = Path(raw_name).suffix.lower()

    if ext not in (".mkv", ".mp4") and not mime.startswith("video/"):
        return

    # Sanitize filename — strip any path components
    safe_name = (
        Path(raw_name).name
        if raw_name
        else f"video_{file_obj.file_unique_id}{ext or '.mp4'}"
    )
    input_path = DOWNLOADS_DIR / safe_name

    status_msg = await message.reply("⬇️ **Downloading…**")

    try:
        await client.download_media(
            message,
            file_name=str(input_path),
            progress=_progress,
            progress_args=(status_msg, "Downloading"),
        )
    except Exception:
        await status_msg.edit_text("❌ Download failed.")
        _cleanup(str(input_path))
        return

    await status_msg.edit_text("🔍 **Scanning audio tracks…**")

    tracks = await _ffprobe_audio(str(input_path))
    track_lines = "\n".join(
        f"  Track {i + 1}: `{t.get('codec_name', '?')}` | "
        f"lang=`{t.get('tags', {}).get('language', 'und')}` | "
        f"ch={t.get('channels', '?')}"
        for i, t in enumerate(tracks)
    ) or "  _(none detected)_"

    op_id = _next_op_id()
    pending_ops[op_id] = {
        "input": str(input_path),
        "tracks": tracks,
        "chat_id": message.chat.id,
    }

    await status_msg.edit_text(
        f"✅ **File ready!**\n\n"
        f"📁 `{safe_name}`\n"
        f"🎵 **Audio Tracks ({len(tracks)}):**\n{track_lines}\n\n"
        f"Choose an action:",
        reply_markup=_build_keyboard(op_id, len(tracks)),
    )


# ── Callback handler ──────────────────────────────────────────────────────────

@app.on_callback_query(filters.user(ADMIN_USER_ID))
async def on_callback(client: Client, query: CallbackQuery) -> None:
    """Dispatch inline button presses."""
    data: str = query.data or ""
    parts = data.split(":", 2)
    action = parts[0]

    # ── Cancel ────────────────────────────────────────────────────────────────
    if action == "cancel" and len(parts) == 2:
        op = pending_ops.pop(parts[1], None)
        if op:
            _cleanup(op["input"])
        await query.message.edit_text(
            "🗑 **Cancelled. Temporary files deleted.**",
            reply_markup=None,
        )
        await query.answer("Cancelled.")
        return

    # ── Feature stubs (add/remove/extract) ───────────────────────────────────
    if action == "stub" and len(parts) == 3:
        await query.answer(
            "This feature requires a follow-up file/message — coming soon.",
            show_alert=True,
        )
        return

    # ── Mux actions ───────────────────────────────────────────────────────────
    if action == "mux" and len(parts) == 3:
        _, mode, op_id = parts
        op = pending_ops.pop(op_id, None)
        if op is None:
            await query.answer("Session expired. Please resend the file.", show_alert=True)
            return

        input_path: str = op["input"]
        chat_id: int = op["chat_id"]
        base_name = Path(input_path).stem
        output_path = str(DOWNLOADS_DIR / f"{base_name}_muxed.mp4")

        await query.message.edit_text(
            "⚙️ **Muxing (stream-copy, zero encoding)…**",
            reply_markup=None,
        )

        try:
            cmd = _build_ffmpeg_cmd(input_path, output_path, mode)
            if cmd is None:
                await query.message.edit_text("❌ Unknown mux mode.")
                return

            ok, err = await _run_ffmpeg(cmd)
            if not ok:
                await query.message.edit_text(
                    f"❌ **FFmpeg failed.**\n```\n{err}\n```"
                )
                return

            await query.message.edit_text("⬆️ **Uploading…**")
            await client.send_document(
                chat_id=chat_id,
                document=output_path,
                caption="✅ **Muxing complete!**",
                progress=_progress,
                progress_args=(query.message, "Uploading"),
            )
            await query.message.edit_text("✅ **Done! File uploaded.**", reply_markup=None)

        finally:
            # Guaranteed cleanup regardless of success or failure
            _cleanup(input_path, output_path)

    await query.answer()


# ── Entry point ───────────────────────────────────────────────────────────────

if __name__ == "__main__":
    app.run()
