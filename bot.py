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
    ForceReply,
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
# pending_ops[op_id] = {
#   "input": str,
#   "tracks": list[dict],
#   "chat_id": int,
#   "safe_name": str,
#   "awaiting_metadata_for_track": int | None,
#   "awaiting_metadata_msg_id": int | None,
# }
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
    track_idx: int | None = None,
    new_title: str | None = None,
) -> list | None:
    """Build a stream-copy FFmpeg command for the given mode and track."""
    base = ["ffmpeg", "-y", "-i", input_path]

    # Convert: keep all tracks, swap container to MP4.
    if mode == "convert":
        return base + ["-map", "0", "-c", "copy", output_path]

    # Isolate: keep video + only the selected audio track.
    if mode == "isolate" and track_idx is not None:
        return base + [
            "-map", "0:v:0",
            "-map", f"0:a:{track_idx}",
            "-c", "copy",
            output_path,
        ]

    # Default: keep all tracks, set the selected audio as default.
    if mode == "default" and track_idx is not None:
        return base + [
            "-map", "0",
            "-c", "copy",
            "-disposition:a", "0",
            f"-disposition:a:{track_idx}", "default",
            output_path,
        ]

    # Metadata: keep all tracks, rename the selected audio track.
    if mode == "metadata" and track_idx is not None and new_title is not None:
        return base + [
            "-map", "0",
            "-c", "copy",
            f"-metadata:s:a:{track_idx}", f"title={new_title}",
            output_path,
        ]

    return None


# ── Dynamic track helpers ─────────────────────────────────────────────────────

def _format_track_lines(tracks: list[dict]) -> str:
    """Render a human-readable list of ffprobe audio tracks for message text.

    Expected keys per track: codec_name, channels, and tags.language (optional).
    """
    return "\n".join(
        f"  Track {i + 1}: `{t.get('codec_name', '?')}` | "
        f"lang=`{t.get('tags', {}).get('language', 'und')}` | "
        f"ch={t.get('channels', '?')}"
        for i, t in enumerate(tracks)
    ) or "  _(none detected)_"


def _track_button_label(track_idx: int, track: dict) -> str:
    """Build a friendly label for the inline track buttons.

    Args:
        track_idx: Zero-based index from the ffprobe audio list.
        track: ffprobe audio stream dict with optional "tags" such as language/title.
    """
    tags = track.get("tags") or {}
    language = (tags.get("language") or "und").title()
    title = tags.get("title")
    descriptor = f"{title} / {language}" if title else language
    return f"🎵 Track {track_idx + 1} ({descriptor})"


def _render_menu_text(safe_name: str, tracks: list[dict], menu: str) -> str:
    """Build the main/sub-menu text block shown above the inline keyboard."""
    prompt = {
        "main": "Choose an action:",
        "isolate": "Select the audio track to isolate:",
        "default": "Select the audio track to set as default:",
        "metadata": "Select the audio track to rename:",
    }.get(menu, "Choose an action:")
    track_lines = _format_track_lines(tracks)
    return (
        f"✅ **File ready!**\n\n"
        f"📁 `{safe_name}`\n"
        f"🎵 **Audio Tracks ({len(tracks)}):**\n{track_lines}\n\n"
        f"{prompt}"
    )


# ── Inline keyboard ───────────────────────────────────────────────────────────

def _build_dynamic_keyboard(
    op_id: str,
    tracks: list[dict],
    menu: str = "main",
) -> InlineKeyboardMarkup:
    """Build a multi-level, track-driven inline keyboard.

    Args:
        op_id: Operation identifier to preserve state across callbacks.
        tracks: ffprobe audio stream dicts used to build per-track buttons.
        menu: One of "main", "isolate", "default", "metadata".
    """
    # Main menu: high-level actions only.
    if menu == "main":
        return InlineKeyboardMarkup(
            [
                [InlineKeyboardButton(
                    "🎬 Just Convert to MP4",
                    callback_data=f"mux:convert:{op_id}",
                )],
                [InlineKeyboardButton(
                    "✂️ Isolate Audio",
                    callback_data=f"menu:isolate:{op_id}",
                )],
                [InlineKeyboardButton(
                    "⭐ Set Default Track",
                    callback_data=f"menu:default:{op_id}",
                )],
                [InlineKeyboardButton(
                    "📝 Edit Metadata",
                    callback_data=f"menu:metadata:{op_id}",
                )],
                [InlineKeyboardButton(
                    "🗑 Cancel",
                    callback_data=f"cancel:{op_id}",
                )],
            ]
        )

    # Sub-menus: per-track selections for isolate/default/metadata.
    if menu in {"isolate", "default", "metadata"}:
        callback_prefix = "meta" if menu == "metadata" else f"mux:{menu}"
        buttons = [
            [InlineKeyboardButton(
                _track_button_label(idx, track),
                callback_data=f"{callback_prefix}:{idx}:{op_id}",
            )]
            for idx, track in enumerate(tracks)
        ]
        buttons.append([
            InlineKeyboardButton(
                "🔙 Back to Main Menu",
                callback_data=f"menu:main:{op_id}",
            )
        ])
        return InlineKeyboardMarkup(buttons)

    # Fallback: always show a safe main menu.
    return _build_dynamic_keyboard(op_id, tracks, menu="main")


async def _execute_mux(
    client: Client,
    status_msg: Message,
    chat_id: int,
    output_path: str,
    cmd: list,
) -> None:
    """Run FFmpeg, upload the output, and keep the status message in sync."""
    await status_msg.edit_text(
        "⚙️ **Muxing (stream-copy, zero encoding)…**",
        reply_markup=None,
    )
    ok, err = await _run_ffmpeg(cmd)
    if not ok:
        await status_msg.edit_text(f"❌ **FFmpeg failed.**\n```\n{err}\n```")
        return

    await status_msg.edit_text("⬆️ **Uploading…**")
    await client.send_document(
        chat_id=chat_id,
        document=output_path,
        caption="✅ **Muxing complete!**",
        progress=_progress,
        progress_args=(status_msg, "Uploading"),
    )
    await status_msg.edit_text("✅ **Done! File uploaded.**", reply_markup=None)


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
    op_id = _next_op_id()
    pending_ops[op_id] = {
        "input": str(input_path),
        "tracks": tracks,
        "chat_id": message.chat.id,
        "safe_name": safe_name,
        "awaiting_metadata_for_track": None,
        "awaiting_metadata_msg_id": None,
    }

    await status_msg.edit_text(
        _render_menu_text(safe_name, tracks, "main"),
        reply_markup=_build_dynamic_keyboard(op_id, tracks, "main"),
    )


# ── Callback handler ──────────────────────────────────────────────────────────

@app.on_callback_query(filters.user(ADMIN_USER_ID))
async def on_callback(client: Client, query: CallbackQuery) -> None:
    """Dispatch inline button presses."""
    data: str = query.data or ""
    parts = data.split(":")
    action = parts[0] if parts else ""

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

    # ── Menu navigation (no mux yet) ─────────────────────────────────────────
    if action == "menu" and len(parts) == 3:
        _, menu, op_id = parts
        op = pending_ops.get(op_id)
        if op is None:
            await query.answer("Session expired. Please resend the file.", show_alert=True)
            return
        await query.message.edit_text(
            _render_menu_text(op["safe_name"], op["tracks"], menu),
            reply_markup=_build_dynamic_keyboard(op_id, op["tracks"], menu),
        )
        await query.answer()
        return

    # ── Metadata track selection (await text reply) ──────────────────────────
    if action == "meta" and len(parts) == 3:
        _, track_str, op_id = parts
        op = pending_ops.get(op_id)
        if op is None:
            await query.answer("Session expired. Please resend the file.", show_alert=True)
            return
        try:
            track_idx = int(track_str)
        except ValueError:
            await query.answer("Invalid track selection.", show_alert=True)
            return
        if track_idx < 0 or track_idx >= len(op["tracks"]):
            await query.answer("Track out of range.", show_alert=True)
            return

        # Remember which track is awaiting metadata so the reply handler can map it.
        op["awaiting_metadata_for_track"] = track_idx
        prompt = await query.message.reply(
            f"📝 **Send new title for Track {track_idx + 1}.**",
            reply_markup=ForceReply(selective=True),
        )
        op["awaiting_metadata_msg_id"] = prompt.id
        await query.answer("Waiting for new title…")
        return

    # ── Mux actions (convert / isolate / default) ────────────────────────────
    if action == "mux" and len(parts) in {3, 4}:
        mode = parts[1]
        op_id = parts[-1]
        selected_track_idx: int | None = None
        if len(parts) == 4:
            try:
                selected_track_idx = int(parts[2])
            except ValueError:
                await query.answer("Invalid track selection.", show_alert=True)
                return

        op = pending_ops.get(op_id)
        if op is None:
            await query.answer("Session expired. Please resend the file.", show_alert=True)
            return
        if mode in {"isolate", "default"}:
            if (
                selected_track_idx is None
                or selected_track_idx < 0
                or selected_track_idx >= len(op["tracks"])
            ):
                await query.answer("Track out of range.", show_alert=True)
                return

        # We are executing now, so remove the op from the registry.
        op = pending_ops.pop(op_id)
        input_path: str = op["input"]
        chat_id: int = op["chat_id"]
        base_name = Path(input_path).stem
        output_path = str(DOWNLOADS_DIR / f"{base_name}_muxed.mp4")

        try:
            cmd = _build_ffmpeg_cmd(
                input_path=input_path,
                output_path=output_path,
                mode=mode,
                track_idx=selected_track_idx,
            )
            if cmd is None:
                await query.message.edit_text("❌ Unknown mux mode.")
                return

            await _execute_mux(
                client=client,
                status_msg=query.message,
                chat_id=chat_id,
                output_path=output_path,
                cmd=cmd,
            )
        finally:
            # Guaranteed cleanup regardless of success or failure
            _cleanup(input_path, output_path)

    await query.answer()


# ── Metadata text reply handler ──────────────────────────────────────────────

@app.on_message(filters.user(ADMIN_USER_ID) & filters.text)
async def on_metadata_text(client: Client, message: Message) -> None:
    """Capture admin replies to metadata prompts and start muxing."""
    if not message.reply_to_message:
        return

    # Match this reply to the pending op that issued the ForceReply prompt.
    matched_op_id: str | None = None
    for op_id, op in pending_ops.items():
        if (
            op.get("awaiting_metadata_msg_id") == message.reply_to_message.id
            and op.get("chat_id") == message.chat.id
        ):
            matched_op_id = op_id
            break

    if matched_op_id is None:
        return

    new_title = (message.text or "").strip()
    if not new_title:
        await message.reply("❌ **Title cannot be empty. Reply again with a name.**")
        return

    # Remove the op from registry; we are executing the final mux now.
    op = pending_ops.pop(matched_op_id)
    track_idx = op.get("awaiting_metadata_for_track")
    if track_idx is None or track_idx < 0 or track_idx >= len(op["tracks"]):
        await message.reply("❌ **Track selection expired. Please resend the file.**")
        return

    input_path: str = op["input"]
    chat_id: int = op["chat_id"]
    base_name = Path(input_path).stem
    output_path = str(DOWNLOADS_DIR / f"{base_name}_muxed.mp4")

    try:
        cmd = _build_ffmpeg_cmd(
            input_path=input_path,
            output_path=output_path,
            mode="metadata",
            track_idx=track_idx,
            new_title=new_title,
        )
        if cmd is None:
            await message.reply("❌ **Failed to build FFmpeg command.**")
            return

        status_msg = await message.reply("⚙️ **Muxing (stream-copy, zero encoding)…**")
        await _execute_mux(
            client=client,
            status_msg=status_msg,
            chat_id=chat_id,
            output_path=output_path,
            cmd=cmd,
        )
    finally:
        # Guaranteed cleanup regardless of success or failure
        _cleanup(input_path, output_path)


# ── Entry point ───────────────────────────────────────────────────────────────

if __name__ == "__main__":
    app.run()
