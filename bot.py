"""FFmpeg Micro-Muxer Telegram Bot — Production-Ready Enterprise Edition.

Implements:
  • Multi-admin support via ADMIN_USER_IDS (comma-separated)
  • Parallel 1 MiB chunk downloader with auto-resume (fast_download)
  • Enhanced progress: live speed, ETA, and [Resuming…] indicator
  • "Shopping Cart" batch processing — accumulate all changes, execute once
  • Advanced metadata editing for Video / Audio / Subtitle (title + language)
  • Attachment management (fonts, cover art) via FFmpeg -attach + -metadata:s:t
  • Background file-retention loop + disk-space safety valve (< 10 % free)
  • Upload retry with exponential back-off
"""
from __future__ import annotations

import asyncio
import json
import math
import os
import shutil
import time
from pathlib import Path

import aiofiles
from dotenv import load_dotenv
from pyrogram import Client, filters
from pyrogram.raw import functions as raw_fns
from pyrogram.raw import types as raw_types
from pyrogram.types import (
    CallbackQuery,
    ForceReply,
    InlineKeyboardButton,
    InlineKeyboardMarkup,
    Message,
)

load_dotenv()

# ── Configuration ─────────────────────────────────────────────────────────────
API_ID: int = int(os.environ["API_ID"])
API_HASH: str = os.environ["API_HASH"]
BOT_TOKEN: str = os.environ["BOT_TOKEN"]
LOCAL_API_URL: str = os.environ.get("LOCAL_API_URL", "http://localhost:8081")

# Multiple admins: ADMIN_USER_IDS is a comma-separated string of numeric IDs.
# Falls back to the legacy ADMIN_USER_ID key for backwards compatibility.
_raw_admin_ids: str = (
    os.environ.get("ADMIN_USER_IDS") or os.environ.get("ADMIN_USER_ID", "")
)
ADMIN_USER_IDS: list[int] = [
    int(uid.strip()) for uid in _raw_admin_ids.split(",") if uid.strip().isdigit()
]
if not ADMIN_USER_IDS:
    raise RuntimeError(
        "Set ADMIN_USER_IDS (comma-separated integers) or ADMIN_USER_ID in .env"
    )

# How long to keep files in downloads/ before auto-deleting (minutes).
FILE_RETENTION_MINUTES: int = int(os.environ.get("FILE_RETENTION_MINUTES", 480))
# Disk-space safety threshold: aggressively delete if free space falls below this.
_DISK_LOW_THRESHOLD: float = 0.10  # 10 %

# ── Codec / constants ─────────────────────────────────────────────────────────

AUDIO_CODEC_EXTENSIONS = {
    "eac3": ".eac3",
    "ac3": ".ac3",
    "aac": ".m4a",
    "dts": ".dts",
    "flac": ".flac",
}
SUBTITLE_CODEC_EXTENSIONS = {
    "subrip": ".srt",
    "ass": ".ass",
    "webvtt": ".vtt",
}
# Commonly supported web audio codecs (HTML5 <audio> baseline support).
WEB_COMPATIBLE_AUDIO_CODECS = {"aac", "mp3", "opus"}
# High-bitrate AAC target to preserve multichannel audio (e.g., 5.1/7.1).
AAC_HIGH_BITRATE = "640k"
MAX_METADATA_TITLE_LEN = 100

MUX_MODE_MAP = {
    "convert": "convert",
    "isolate_audio": "isolate",
    "default_audio": "default",
}

# ── MIME types for MKV attachments (fonts and cover art) ─────────────────────
ATTACHMENT_MIME_TYPES: dict[str, str] = {
    ".ttf":  "application/x-truetype-font",
    ".otf":  "application/x-font-otf",
    ".woff": "font/woff",
    ".woff2": "font/woff2",
    ".jpg":  "image/jpeg",
    ".jpeg": "image/jpeg",
    ".png":  "image/png",
}

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
#   "streams": {"audio": list[dict], "subtitle": list[dict], "video": list[dict]},
#   "chat_id": int,
#   "safe_name": str,
#   "selected_extract": set[str],             # "a:0", "s:1", "v:0"
#   "awaiting_metadata_for_track": int | None,
#   "awaiting_metadata_stream_type": str | None,
#   "awaiting_metadata_msg_id": int | None,
#   "awaiting_metadata_for_cart": bool,       # True → store in cart; False → exec now
#   "metadata_input_path": str | None,
#   "metadata_output_path": str | None,
#   "awaiting_external_type": str | None,
#   "awaiting_external_msg_id": int | None,
#   "external_input_path": str | None,
#   "external_codec": str | None,
#   "post_add_output_path": str | None,
#   "post_add_stream_type": str | None,
#   "post_add_track_idx": int | None,
#   "awaiting_attachment_msg_id": int | None,
#   # ── Shopping cart ────────────────────────────────────────────────────────
#   "cart": {
#       "remove_audio": set[int],             # original audio track indices to drop
#       "remove_subs":  set[int],             # original subtitle track indices to drop
#       "metadata": dict[                     # keyed by (kind, orig_idx)
#           tuple[str, int],
#           dict[str, str],                   # {"title": ..., "language": ...}
#       ],
#       "attachments": list[dict],            # [{path, mimetype, filename}]
#   },
# }
pending_ops: dict[str, dict] = {}
# Reverse lookup: prompt message ID -> op_id (fast metadata reply matching).
pending_metadata_prompts: dict[int, str] = {}
# Reverse lookup: external upload prompt message ID -> op_id.
pending_external_prompts: dict[int, str] = {}
# Reverse lookup: attachment upload prompt message ID -> op_id.
pending_attachment_prompts: dict[int, str] = {}
_op_counter: int = 0

# Progress-bar throttle: msg_id → last_update_monotonic
_progress_ts: dict[int, float] = {}
# Per-message speed tracking: msg_id → {start_time, start_bytes}
_progress_speed_data: dict[int, dict] = {}
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


def _format_eta(secs: float) -> str:
    """Format a duration in seconds as a human-readable ETA string."""
    if secs <= 0 or not math.isfinite(secs):
        return "—"
    m, s = divmod(int(secs), 60)
    h, m = divmod(m, 60)
    if h > 0:
        return f"{h}h {m}m {s}s"
    if m > 0:
        return f"{m}m {s}s"
    return f"{s}s"


async def _progress(
    current: int,
    total: int,
    msg: Message,
    label: str,
    start_time: float | None = None,
    start_bytes: int = 0,
    is_resuming: bool = False,
) -> None:
    """Throttled progress bar with live speed (MiB/s), ETA and resume indicator.

    When *start_time* is provided the function computes:
      • 🚀 Speed  — bytes transferred this session / elapsed seconds
      • ⏳ ETA    — remaining bytes / current speed
    A ``[Resuming…]`` badge is shown when *is_resuming* is True.
    """
    now = time.monotonic()
    if current < total and now - _progress_ts.get(msg.id, 0) < _PROGRESS_INTERVAL:
        return
    _progress_ts[msg.id] = now

    # Speed / ETA calculation (only when the caller supplies start_time).
    speed_str = ""
    eta_str = ""
    if start_time is not None:
        if msg.id not in _progress_speed_data:
            _progress_speed_data[msg.id] = {
                "start_time": start_time,
                "start_bytes": start_bytes,
            }
        data = _progress_speed_data[msg.id]
        elapsed = now - data["start_time"]
        transferred = current - data["start_bytes"]
        speed = transferred / elapsed if elapsed > 0 else 0.0
        remaining = total - current
        eta = remaining / speed if speed > 0 else 0.0
        speed_str = f"🚀 {_human_size(int(speed))}/s"
        eta_str = f"⏳ ETA: {_format_eta(eta)}"

    resume_tag = " `[Resuming…]`" if is_resuming else ""
    extras = f"\n{speed_str}  {eta_str}".strip() if (speed_str or eta_str) else ""

    try:
        await msg.edit_text(
            f"**{label}…{resume_tag}**\n"
            f"📊 {_make_bar(current, total)}\n"
            f"📦 {_human_size(current)} / {_human_size(total)}"
            f"{extras}"
        )
    except Exception:
        pass

    if current >= total:
        _progress_ts.pop(msg.id, None)
        _progress_speed_data.pop(msg.id, None)


def _attachment_mimetype(filename: str) -> str:
    """Return the MIME type string for an attachment, based on file extension."""
    ext = Path(filename).suffix.lower()
    return ATTACHMENT_MIME_TYPES.get(ext, "application/octet-stream")


def _cart_has_changes(op: dict) -> bool:
    """Return True when the shopping cart holds at least one pending operation."""
    cart = op.get("cart", {})
    return bool(
        cart.get("remove_audio")
        or cart.get("remove_subs")
        or cart.get("metadata")
        or cart.get("attachments")
    )


def _cart_summary_lines(op: dict) -> list[str]:
    """Return a list of human-readable lines describing the current cart state."""
    cart = op.get("cart", {})
    lines: list[str] = []
    if cart.get("remove_audio"):
        lines.append(f"🗑 Remove {len(cart['remove_audio'])} audio track(s)")
    if cart.get("remove_subs"):
        lines.append(f"🗑 Remove {len(cart['remove_subs'])} subtitle track(s)")
    if cart.get("metadata"):
        lines.append(f"✏️ Metadata edits queued for {len(cart['metadata'])} track(s)")
    if cart.get("attachments"):
        lines.append(f"📎 {len(cart['attachments'])} attachment(s) pending")
    return lines


def _cleanup(*paths: str) -> None:
    """Silently remove files; leaves no trace on disk."""
    for p in paths:
        try:
            if p and os.path.exists(p):
                os.remove(p)
        except OSError:
            pass


# ── Background file-retention loop ────────────────────────────────────────────

async def _cleanup_loop() -> None:
    """Enforce file retention policy in the background.

    Runs every 60 seconds:

    1. **Normal mode** — delete files older than ``FILE_RETENTION_MINUTES``.
    2. **Safety valve** — if free disk space drops below ``_DISK_LOW_THRESHOLD``
       (10 %) delete the *oldest* files first until space is recovered.  This
       prevents OS crashes on constrained servers.
    """
    while True:
        await asyncio.sleep(60)
        try:
            cutoff = time.time() - FILE_RETENTION_MINUTES * 60

            # Collect files with mtime, oldest first.
            entries: list[tuple[float, Path]] = []
            for entry in DOWNLOADS_DIR.iterdir():
                if entry.is_file():
                    try:
                        entries.append((entry.stat().st_mtime, entry))
                    except OSError:
                        pass
            entries.sort()  # ascending mtime → oldest first

            disk = shutil.disk_usage(DOWNLOADS_DIR)
            free_ratio = disk.free / disk.total if disk.total else 1.0

            if free_ratio < _DISK_LOW_THRESHOLD:
                # Aggressive mode: purge oldest files until breathing room restored.
                for _mtime, path in entries:
                    if free_ratio >= _DISK_LOW_THRESHOLD:
                        break
                    try:
                        path.unlink(missing_ok=True)
                        disk = shutil.disk_usage(DOWNLOADS_DIR)
                        free_ratio = disk.free / disk.total if disk.total else 1.0
                    except OSError:
                        pass
            else:
                # Normal retention: remove files past the cutoff time.
                for mtime, path in entries:
                    if mtime < cutoff:
                        try:
                            path.unlink(missing_ok=True)
                        except OSError:
                            pass
        except Exception:
            pass  # Never let a background loop crash the bot.


# ── FFprobe / FFmpeg (async, non-blocking) ────────────────────────────────────

async def _ffprobe_streams(input_path: str) -> list[dict]:
    """Return all stream descriptors from ffprobe (zero stderr logging)."""
    proc = await asyncio.create_subprocess_exec(
        "ffprobe", "-v", "quiet",
        "-print_format", "json",
        "-show_streams",
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


def _split_streams(streams: list[dict]) -> dict[str, list[dict]]:
    """Group ffprobe streams by type (audio/subtitle/video)."""
    grouped = {"audio": [], "subtitle": [], "video": []}
    for stream in streams:
        kind = stream.get("codec_type")
        if kind in grouped:
            grouped[kind].append(stream)
    return grouped


def _sanitize_extension(ext: str) -> str:
    """Ensure extensions are safe and start with a dot."""
    if not ext:
        return ".bin"
    ext = ext.lower()
    return ext if ext.startswith(".") else f".{ext}"


def _codec_extension(codec_name: str | None, mapping: dict[str, str]) -> str:
    """Map codec_name to a safe extension using the provided mapping."""
    if codec_name:
        codec_key = codec_name.lower()
        if codec_key in mapping:
            return mapping[codec_key]
        if all(ch.isalnum() or ch in {"_", "-"} for ch in codec_key):
            return _sanitize_extension(codec_key)
    return ".bin"


def _video_container_extension(input_path: str) -> str:
    """Choose a container extension for video extraction."""
    return ".mp4" if Path(input_path).suffix.lower() == ".mp4" else ".mkv"


def _output_path_for(input_path: str, suffix: str, ext: str = ".mkv") -> str:
    """Build an output path in the downloads directory."""
    base_name = Path(input_path).stem
    return str(DOWNLOADS_DIR / f"{base_name}_{suffix}{ext}")


# ── Fast parallel downloader with auto-resume ─────────────────────────────────

async def _fallback_download(
    client: Client,
    message: Message,
    file_path: Path,
    status_msg: Message,
    label: str,
) -> bool:
    """High-level download_media fallback (no parallelism, no raw-API)."""
    start_time = time.monotonic()
    try:
        await client.download_media(
            message,
            file_name=str(file_path),
            progress=_progress,
            progress_args=(status_msg, label, start_time, 0, False),
        )
        return True
    except Exception:
        return False


async def fast_download(
    client: Client,
    message: Message,
    file_path: Path,
    status_msg: Message,
    label: str,
) -> bool:
    """Download *message* to *file_path* using parallel 1 MiB raw-API chunks.

    Auto-resume:
        If *file_path* already exists its byte count is floored to the last
        complete 1 MiB boundary.  Only the missing chunks from that boundary
        onward are fetched, so a bot restart transparently picks up where it
        left off.

    Concurrency:
        Up to 10 ``GetFile`` requests run simultaneously behind an
        ``asyncio.Semaphore(10)``.  ``asyncio.gather`` preserves submission
        order, so chunks are written sequentially without seeking.

    Fallback:
        Any raw-API error (DC mismatch, CDN redirect, file-reference expiry,
        …) is caught; the partial file is removed and
        ``_fallback_download`` (Pyrogram's built-in ``download_media``) is
        used instead.
    """
    file_obj = message.video or message.document
    if not file_obj:
        return False

    total_size: int = file_obj.file_size or 0
    if total_size == 0:
        return await _fallback_download(client, message, file_path, status_msg, label)

    CHUNK_SIZE = 1024 * 1024  # 1 MiB per request

    # ── Auto-resume: find the last complete 1 MiB boundary on disk ───────────
    existing_size = file_path.stat().st_size if file_path.exists() else 0
    if existing_size >= total_size:
        return True  # Already fully downloaded.

    start_offset = (existing_size // CHUNK_SIZE) * CHUNK_SIZE
    is_resuming = start_offset > 0

    # Truncate any incomplete trailing chunk so "ab" appends from a clean edge.
    if file_path.exists() and existing_size > start_offset:
        os.truncate(str(file_path), start_offset)
    elif not file_path.exists():
        start_offset = 0
        is_resuming = False

    # ── Decode the raw InputDocumentFileLocation ──────────────────────────────
    # We only need InputDocumentFileLocation because this bot deals exclusively
    # with video/document messages (not bare photos).
    try:
        from pyrogram.file_id import FileId  # type: ignore[import]
        fid = FileId.decode(file_obj.file_id)
        location: raw_types.InputDocumentFileLocation = (
            raw_types.InputDocumentFileLocation(
                id=fid.media_id,
                access_hash=fid.access_hash,
                file_reference=fid.file_reference,
                thumb_size="",
            )
        )
    except Exception:
        # Cannot decode file_id → use high-level fallback.
        return await _fallback_download(client, message, file_path, status_msg, label)

    total_chunks = math.ceil(total_size / CHUNK_SIZE)
    start_chunk = start_offset // CHUNK_SIZE
    sem = asyncio.Semaphore(10)
    start_time = time.monotonic()
    downloaded_this_session = 0

    async def _fetch(chunk_idx: int) -> tuple[int, bytes]:
        offset = chunk_idx * CHUNK_SIZE
        async with sem:
            result = await client.invoke(
                raw_fns.upload.GetFile(
                    location=location,
                    offset=offset,
                    limit=CHUNK_SIZE,
                    precise=True,
                )
            )
            if not hasattr(result, "bytes"):
                # CDN redirect or unknown result type → bail out.
                raise ValueError(
                    f"Unexpected GetFile result type: {type(result).__name__}"
                )
            return chunk_idx, result.bytes

    BATCH = 10  # max concurrent requests per gather call
    try:
        open_mode = "ab" if is_resuming else "wb"
        async with aiofiles.open(file_path, open_mode) as fh:
            for batch_start in range(start_chunk, total_chunks, BATCH):
                batch_end = min(batch_start + BATCH, total_chunks)
                # asyncio.gather returns results in submission order → safe to
                # write sequentially without seeking.
                results: list[tuple[int, bytes]] = await asyncio.gather(
                    *[_fetch(i) for i in range(batch_start, batch_end)]
                )
                for _idx, data in results:
                    await fh.write(data)
                    downloaded_this_session += len(data)
                await _progress(
                    start_offset + downloaded_this_session,
                    total_size,
                    status_msg,
                    label,
                    start_time=start_time,
                    start_bytes=start_offset,
                    is_resuming=is_resuming,
                )
        return True
    except Exception:
        # Clean up the partial file so a retry starts fresh.
        if file_path.exists():
            try:
                os.remove(str(file_path))
            except OSError:
                pass
        return await _fallback_download(client, message, file_path, status_msg, label)


# ── Upload with retry ─────────────────────────────────────────────────────────

async def _upload_with_retry(
    client: Client,
    chat_id: int,
    file_path: str,
    caption: str,
    status_msg: Message,
    max_retries: int = 3,
) -> None:
    """Upload *file_path* to *chat_id* with exponential-back-off retries.

    Pyrogram handles low-level chunked uploads internally.  This wrapper adds
    application-level retry logic for transient network drops.  Raises the
    last exception if all attempts are exhausted.
    """
    last_exc: Exception | None = None
    for attempt in range(max_retries):
        if attempt > 0:
            wait = 5 * attempt
            try:
                await status_msg.edit_text(
                    f"⬆️ **Uploading… (retry {attempt}/{max_retries - 1}, "
                    f"waiting {wait}s)**"
                )
            except Exception:
                pass
            await asyncio.sleep(wait)
        start_time = time.monotonic()
        try:
            await client.send_document(
                chat_id=chat_id,
                document=file_path,
                caption=caption,
                progress=_progress,
                progress_args=(status_msg, "Uploading", start_time, 0, False),
            )
            return  # Success — exit immediately.
        except Exception as exc:
            last_exc = exc

    raise last_exc  # type: ignore[misc]


def _parse_toggle_callback(parts: list[str]) -> tuple[str, str, int, str] | None:
    """Parse toggle callback data into (menu, stream_key, track_idx, op_id)."""
    if len(parts) < 4:
        return None
    toggle_mode = parts[1]
    if toggle_mode == "extract" and len(parts) == 5:
        stream_key = parts[2]
        op_id = parts[4]
        track_str = parts[3]
        menu = "extract"
    elif toggle_mode in {"remove_audio", "remove_subs"} and len(parts) == 4:
        stream_key = toggle_mode
        op_id = parts[3]
        track_str = parts[2]
        menu = toggle_mode
    else:
        return None
    try:
        track_idx = int(track_str)
    except ValueError:
        return None
    return menu, stream_key, track_idx, op_id


def _sanitize_filename(name: str) -> str:
    """Ensure filenames do not start with '.' or '-' to avoid hidden/flagged names."""
    if name.startswith((".", "-")):
        trimmed = name.lstrip(".-") or "upload"
        return f"file_{trimmed}"
    return name


def _build_ffmpeg_cmd(
    input_path: str,
    output_path: str,
    mode: str,
    track_idx: int | None = None,
    new_title: str | None = None,
    stream_type: str = "audio",
) -> list | None:
    """Build a stream-copy FFmpeg command for single-purpose (immediate) modes."""
    base = ["ffmpeg", "-y", "-i", input_path]

    # Convert: keep all tracks, swap container.
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

    # Metadata (immediate / post-add): keep all tracks, rename one track.
    if mode == "metadata" and track_idx is not None and new_title is not None:
        stream_selector = "a" if stream_type == "audio" else "s"
        return base + [
            "-map", "0",
            "-c", "copy",
            f"-metadata:s:{stream_selector}:{track_idx}", f"title={new_title}",
            output_path,
        ]

    return None


def _build_cart_ffmpeg_cmd(
    input_path: str,
    output_path: str,
    op: dict,
) -> list[str]:
    """Compile ALL pending cart operations into a single FFmpeg stream-copy command.

    ── MAP INDEX SHIFTING ────────────────────────────────────────────────────────
    FFmpeg's ``-metadata:s:TYPE:N`` flag references OUTPUT stream indices
    (0-based per stream type), not input indices.  When tracks are removed, the
    output indices shift.  Example:

        Input audio:  [a0, a1, a2, a3]
        Cart removal: {a1, a3}
        Output audio: [a0 → out:a:0,  a2 → out:a:1]

    We therefore:
      1. Build ``audio_keep`` / ``sub_keep`` (original indices in ascending order).
      2. Map original → output index via ``enumerate(audio_keep)``.
      3. Emit ``-metadata:s:a:OUTPUT_IDX`` (not the original index).

    This guarantees zero collision between -map arguments and metadata flags
    regardless of which or how many tracks are removed.
    ─────────────────────────────────────────────────────────────────────────────
    """
    streams = op["streams"]
    cart = op.get("cart", {})

    remove_audio: set[int] = cart.get("remove_audio", set())
    remove_subs: set[int] = cart.get("remove_subs", set())
    # cart["metadata"] keys are (kind, orig_idx), values are {title, language}
    track_metadata: dict = cart.get("metadata", {})
    attachments: list[dict] = cart.get("attachments", [])

    # ── Build lists of kept original indices (ascending) ─────────────────────
    audio_keep = [i for i in range(len(streams["audio"])) if i not in remove_audio]
    sub_keep   = [i for i in range(len(streams["subtitle"])) if i not in remove_subs]
    has_video  = bool(streams["video"])

    # ── Build original → output index maps ───────────────────────────────────
    # audio_out_map[original_input_idx] = output_audio_stream_idx
    audio_out_map: dict[int, int] = {orig: out for out, orig in enumerate(audio_keep)}
    sub_out_map:   dict[int, int] = {orig: out for out, orig in enumerate(sub_keep)}
    # Video is always kept at v:0 (multiple-video-track removal not supported).

    # ── Assemble the command ──────────────────────────────────────────────────
    cmd: list[str] = ["ffmpeg", "-y", "-i", input_path]

    # ── Stream mapping ────────────────────────────────────────────────────────
    if has_video:
        cmd += ["-map", "0:v:0"]
    for orig_idx in audio_keep:
        cmd += ["-map", f"0:a:{orig_idx}"]
    for orig_idx in sub_keep:
        cmd += ["-map", f"0:s:{orig_idx}"]

    # ── Global stream copy (no re-encoding) ──────────────────────────────────
    cmd += ["-c", "copy"]

    # ── Per-stream metadata (using OUTPUT indices after removal shift) ────────
    for (kind, orig_idx), meta in track_metadata.items():
        if kind == "video" and has_video:
            # Video is not removed, output index is always 0.
            out_sel = "v:0"
        elif kind == "audio":
            if orig_idx not in audio_out_map:
                continue  # This track was removed; skip its metadata.
            out_sel = f"a:{audio_out_map[orig_idx]}"
        elif kind == "subtitle":
            if orig_idx not in sub_out_map:
                continue  # This track was removed; skip its metadata.
            out_sel = f"s:{sub_out_map[orig_idx]}"
        else:
            continue

        title = meta.get("title", "")
        lang  = meta.get("language", "")
        if title:
            cmd += [f"-metadata:s:{out_sel}", f"title={title}"]
        if lang:
            cmd += [f"-metadata:s:{out_sel}", f"language={lang}"]

    # ── Attachments (-attach requires MKV output container) ──────────────────
    for att_idx, att in enumerate(attachments):
        cmd += ["-attach", att["path"]]
        cmd += [f"-metadata:s:t:{att_idx}", f"mimetype={att['mimetype']}"]

    cmd.append(output_path)
    return cmd


# ── Dynamic track helpers ─────────────────────────────────────────────────────

def _format_audio_tracks(tracks: list[dict]) -> str:
    """Render a human-readable list of ffprobe audio tracks for message text."""
    return "\n".join(
        f"  Track {i + 1}: `{t.get('codec_name', '?')}` | "
        f"lang=`{(t.get('tags') or {}).get('language', 'und')}` | "
        f"ch={t.get('channels', '?')}"
        for i, t in enumerate(tracks)
    ) or "  _(none detected)_"


def _format_subtitle_tracks(tracks: list[dict]) -> str:
    """Render a human-readable list of ffprobe subtitle tracks."""
    return "\n".join(
        f"  Sub {i + 1}: `{t.get('codec_name', '?')}` | "
        f"lang=`{(t.get('tags') or {}).get('language', 'und')}`"
        for i, t in enumerate(tracks)
    ) or "  _(none detected)_"


def _format_video_tracks(tracks: list[dict]) -> str:
    """Render a human-readable list of ffprobe video tracks."""
    return "\n".join(
        f"  Video {i + 1}: `{t.get('codec_name', '?')}`"
        for i, t in enumerate(tracks)
    ) or "  _(none detected)_"


def _is_track_index_in_range(track_idx: int | None, tracks: list[dict]) -> bool:
    """Validate that a selected index exists in a ffprobe track list."""
    return track_idx is not None and 0 <= track_idx < len(tracks)


def _clear_metadata_prompt(prompt_id: int | None) -> None:
    """Remove a tracked metadata prompt safely."""
    if prompt_id is not None:
        pending_metadata_prompts.pop(prompt_id, None)


def _clear_external_prompt(prompt_id: int | None) -> None:
    """Remove a tracked external upload prompt safely."""
    if prompt_id is not None:
        pending_external_prompts.pop(prompt_id, None)


def _track_button_label(track_idx: int, track: dict) -> str:
    """Build a friendly label for audio track buttons."""
    tags = track.get("tags") or {}
    language = (tags.get("language") or "und").title()
    title = tags.get("title")
    descriptor = f"{title} / {language}" if title else language
    return f"🎵 Track {track_idx + 1} ({descriptor})"


def _subtitle_button_label(track_idx: int, track: dict) -> str:
    """Build a friendly label for subtitle track buttons."""
    tags = track.get("tags") or {}
    language = (tags.get("language") or "und").title()
    descriptor = tags.get("title") or language
    return f"📝 Sub {track_idx + 1} ({descriptor})"


def _video_button_label(track: dict) -> str:
    """Build a friendly label for the video track button."""
    codec = track.get("codec_name", "?")
    return f"🎬 Video ({codec})"


def _clear_attachment_prompt(prompt_id: int | None) -> None:
    """Remove a tracked attachment upload prompt safely."""
    if prompt_id is not None:
        pending_attachment_prompts.pop(prompt_id, None)


def _render_menu_text(
    safe_name: str,
    streams: dict[str, list[dict]],
    menu: str,
    op: dict | None = None,
) -> str:
    """Build the main/sub-menu text block shown above the inline keyboard.

    When *op* is provided, a cart-summary block is appended to the main menu
    so the user can see what pending changes are queued before executing.
    """
    prompt = {
        "main":             "Choose an action:",
        "isolate_audio":    "Select the audio track to isolate:",
        "default_audio":    "Select the audio track to set as default:",
        "metadata_audio":   "Select the audio track to edit metadata:",
        "metadata_subtitle":"Select the subtitle track to edit metadata:",
        "metadata_video":   "Select the video track to edit metadata:",
        "remove_audio":     "Toggle audio tracks to remove, then confirm:",
        "remove_subs":      "Toggle subtitle tracks to remove, then confirm:",
        "extract":          "Toggle tracks to extract, then execute:",
        "add_external":     "Choose which external track to add:",
        "manage_attachments": "Manage attachments (fonts / cover art):",
    }.get(menu, "Choose an action:")

    audio_lines    = _format_audio_tracks(streams["audio"])
    subtitle_lines = _format_subtitle_tracks(streams["subtitle"])
    video_lines    = _format_video_tracks(streams["video"])
    base = (
        f"✅ **File ready!**\n\n"
        f"📁 `{safe_name}`\n"
        f"🎬 **Video Tracks ({len(streams['video'])}):**\n{video_lines}\n\n"
        f"🎵 **Audio Tracks ({len(streams['audio'])}):**\n{audio_lines}\n\n"
        f"📝 **Subtitle Tracks ({len(streams['subtitle'])}):**\n{subtitle_lines}\n\n"
        f"{prompt}"
    )

    # Show cart summary in the main menu when changes are pending.
    if menu == "main" and op and _cart_has_changes(op):
        lines = _cart_summary_lines(op)
        cart_block = "\n".join(f"  • {l}" for l in lines)
        base += f"\n\n🛒 **Pending cart changes:**\n{cart_block}"

    return base


# ── Inline keyboard ───────────────────────────────────────────────────────────

def _build_dynamic_keyboard(
    op_id: str,
    op: dict,
    menu: str = "main",
) -> InlineKeyboardMarkup:
    """Build a multi-level, track-driven inline keyboard."""
    streams = op["streams"]
    cart = op.get("cart", {})

    def _toggle_label(label: str, selected: bool) -> str:
        return f"✅ {label}" if selected else label

    def _no_tracks_row(label: str) -> list[InlineKeyboardButton]:
        return [InlineKeyboardButton(label, callback_data="noop")]

    # ── Main menu ─────────────────────────────────────────────────────────────
    if menu == "main":
        rows: list[list[InlineKeyboardButton]] = [
            [InlineKeyboardButton(
                "🎬 Just Convert to MKV/MP4",
                callback_data=f"mux:convert:{op_id}",
            )],
            [InlineKeyboardButton(
                "✂️ Isolate Audio",
                callback_data=f"menu:isolate_audio:{op_id}",
            )],
            [InlineKeyboardButton(
                "⭐ Set Default Audio Track",
                callback_data=f"menu:default_audio:{op_id}",
            )],
            [InlineKeyboardButton(
                "✏️ Edit Audio Metadata",
                callback_data=f"menu:metadata_audio:{op_id}",
            )],
            [InlineKeyboardButton(
                "✏️ Edit Subtitle Metadata",
                callback_data=f"menu:metadata_subtitle:{op_id}",
            )],
        ]
        # Show video metadata only when a video track exists.
        if streams["video"]:
            rows.append([InlineKeyboardButton(
                "✏️ Edit Video Metadata",
                callback_data=f"menu:metadata_video:{op_id}",
            )])
        rows += [
            [InlineKeyboardButton(
                "🧹 Remove Specific Audio Tracks",
                callback_data=f"menu:remove_audio:{op_id}",
            )],
            [InlineKeyboardButton(
                "🧹 Remove Specific Subtitle Tracks",
                callback_data=f"menu:remove_subs:{op_id}",
            )],
            [InlineKeyboardButton(
                "🧹 Remove All Subtitles (immediate)",
                callback_data=f"mux:remove_all_subs:{op_id}",
            )],
            [InlineKeyboardButton(
                "📤 Multi-Extract Tracks",
                callback_data=f"menu:extract:{op_id}",
            )],
            [InlineKeyboardButton(
                "➕ Add External Track",
                callback_data=f"menu:add_external:{op_id}",
            )],
            [InlineKeyboardButton(
                "📎 Manage Attachments",
                callback_data=f"menu:manage_attachments:{op_id}",
            )],
        ]
        # Show the Execute-All button only when the cart has pending changes.
        if _cart_has_changes(op):
            rows.append([InlineKeyboardButton(
                "🚀 Execute All Changes",
                callback_data=f"cart_exec:{op_id}",
            )])
            rows.append([InlineKeyboardButton(
                "🗑 Clear Cart",
                callback_data=f"cart_clear:{op_id}",
            )])
        rows.append([InlineKeyboardButton(
            "🗑 Cancel",
            callback_data=f"cancel:{op_id}",
        )])
        return InlineKeyboardMarkup(rows)

    # ── Sub-menus: per-track single selection (isolate / default / metadata) ──
    if menu in {"isolate_audio", "default_audio", "metadata_audio"}:
        callback_prefix = (
            "meta:audio" if menu == "metadata_audio" else f"mux:{menu}"
        )
        buttons = [
            [InlineKeyboardButton(
                _track_button_label(idx, track),
                callback_data=f"{callback_prefix}:{idx}:{op_id}",
            )]
            for idx, track in enumerate(streams["audio"])
        ] or [_no_tracks_row("⚠️ No audio tracks found")]
        buttons.append([InlineKeyboardButton(
            "🔙 Back to Main Menu",
            callback_data=f"menu:main:{op_id}",
        )])
        return InlineKeyboardMarkup(buttons)

    if menu == "metadata_subtitle":
        buttons = [
            [InlineKeyboardButton(
                _subtitle_button_label(idx, track),
                callback_data=f"meta:subtitle:{idx}:{op_id}",
            )]
            for idx, track in enumerate(streams["subtitle"])
        ] or [_no_tracks_row("⚠️ No subtitle tracks found")]
        buttons.append([InlineKeyboardButton(
            "🔙 Back to Main Menu",
            callback_data=f"menu:main:{op_id}",
        )])
        return InlineKeyboardMarkup(buttons)

    if menu == "metadata_video":
        buttons = [
            [InlineKeyboardButton(
                _video_button_label(track),
                callback_data=f"meta:video:{idx}:{op_id}",
            )]
            for idx, track in enumerate(streams["video"])
        ] or [_no_tracks_row("⚠️ No video tracks found")]
        buttons.append([InlineKeyboardButton(
            "🔙 Back to Main Menu",
            callback_data=f"menu:main:{op_id}",
        )])
        return InlineKeyboardMarkup(buttons)

    # ── Multi-select remove audio (adds to cart) ──────────────────────────────
    if menu == "remove_audio":
        remove_set: set[int] = cart.get("remove_audio", set())
        buttons = [
            [InlineKeyboardButton(
                _toggle_label(
                    _track_button_label(idx, track),
                    idx in remove_set,
                ),
                callback_data=f"toggle:remove_audio:{idx}:{op_id}",
            )]
            for idx, track in enumerate(streams["audio"])
        ] or [_no_tracks_row("⚠️ No audio tracks found")]
        buttons.append([
            InlineKeyboardButton(
                "✓ Apply to Cart & Back",
                callback_data=f"menu:main:{op_id}",
            ),
        ])
        return InlineKeyboardMarkup(buttons)

    # ── Multi-select remove subtitles (adds to cart) ──────────────────────────
    if menu == "remove_subs":
        remove_subs_set: set[int] = cart.get("remove_subs", set())
        buttons = [
            [InlineKeyboardButton(
                _toggle_label(
                    _subtitle_button_label(idx, track),
                    idx in remove_subs_set,
                ),
                callback_data=f"toggle:remove_subs:{idx}:{op_id}",
            )]
            for idx, track in enumerate(streams["subtitle"])
        ] or [_no_tracks_row("⚠️ No subtitle tracks found")]
        buttons.append([
            InlineKeyboardButton(
                "✓ Apply to Cart & Back",
                callback_data=f"menu:main:{op_id}",
            ),
        ])
        return InlineKeyboardMarkup(buttons)

    # ── Multi-select extraction (still immediate) ─────────────────────────────
    if menu == "extract":
        buttons = []
        if streams["video"]:
            video_selected = "v:0" in op["selected_extract"]
            buttons.append([
                InlineKeyboardButton(
                    _toggle_label(_video_button_label(streams["video"][0]), video_selected),
                    callback_data=f"toggle:extract:v:0:{op_id}",
                )
            ])
        for idx, track in enumerate(streams["audio"]):
            key = f"a:{idx}"
            buttons.append([
                InlineKeyboardButton(
                    _toggle_label(_track_button_label(idx, track), key in op["selected_extract"]),
                    callback_data=f"toggle:extract:a:{idx}:{op_id}",
                )
            ])
        for idx, track in enumerate(streams["subtitle"]):
            key = f"s:{idx}"
            buttons.append([
                InlineKeyboardButton(
                    _toggle_label(
                        _subtitle_button_label(idx, track),
                        key in op["selected_extract"],
                    ),
                    callback_data=f"toggle:extract:s:{idx}:{op_id}",
                )
            ])
        if not buttons:
            buttons.append(_no_tracks_row("⚠️ No tracks available to extract"))
        buttons.append([
            InlineKeyboardButton(
                "✅ Execute Extraction Now",
                callback_data=f"exec:extract:{op_id}",
            ),
            InlineKeyboardButton(
                "🔙 Back",
                callback_data=f"menu:main:{op_id}",
            ),
        ])
        return InlineKeyboardMarkup(buttons)

    # ── External track selection ──────────────────────────────────────────────
    if menu == "add_external":
        return InlineKeyboardMarkup(
            [
                [InlineKeyboardButton(
                    "🎵 Add External Audio",
                    callback_data=f"external:select_audio:{op_id}",
                )],
                [InlineKeyboardButton(
                    "📝 Add External Subtitle",
                    callback_data=f"external:select_sub:{op_id}",
                )],
                [InlineKeyboardButton(
                    "🔙 Back to Main Menu",
                    callback_data=f"menu:main:{op_id}",
                )],
            ]
        )

    # ── Attachment management ─────────────────────────────────────────────────
    if menu == "manage_attachments":
        attachments: list[dict] = cart.get("attachments", [])
        rows_att: list[list[InlineKeyboardButton]] = []
        for att_idx, att in enumerate(attachments):
            rows_att.append([InlineKeyboardButton(
                f"🗑 Remove: {att['filename']}",
                callback_data=f"attach:remove:{att_idx}:{op_id}",
            )])
        rows_att += [
            [InlineKeyboardButton(
                "➕ Add Attachment (font/image)",
                callback_data=f"attach:prompt:{op_id}",
            )],
            [InlineKeyboardButton(
                "🔙 Back to Main Menu",
                callback_data=f"menu:main:{op_id}",
            )],
        ]
        return InlineKeyboardMarkup(rows_att)

    # Fallback: always show a safe main menu.
    return _build_dynamic_keyboard(op_id, op, menu="main")


async def _execute_mux(
    client: Client,
    status_msg: Message,
    chat_id: int,
    output_path: str,
    cmd: list,
) -> None:
    """Run FFmpeg, upload the output with retry, and keep the status message in sync."""
    await status_msg.edit_text(
        "⚙️ **Muxing (stream-copy, zero encoding)…**",
        reply_markup=None,
    )
    ok, err = await _run_ffmpeg(cmd)
    if not ok:
        await status_msg.edit_text(f"❌ **FFmpeg failed.**\n```\n{err}\n```")
        return

    await status_msg.edit_text("⬆️ **Uploading…**")
    try:
        await _upload_with_retry(
            client=client,
            chat_id=chat_id,
            file_path=output_path,
            caption="✅ **Muxing complete!**",
            status_msg=status_msg,
        )
        await status_msg.edit_text("✅ **Done! File uploaded.**", reply_markup=None)
    except Exception as exc:
        await status_msg.edit_text(
            f"❌ **Upload failed after retries:** `{exc}`", reply_markup=None
        )


async def _issue_metadata_prompt(
    reply_target: Message,
    op_id: str,
    op: dict,
    track_idx: int,
    stream_type: str,
    input_path: str,
    output_path: str,
    prompt_text: str | None = None,
    for_cart: bool = False,
) -> None:
    """Send a ForceReply prompt and register the reply-to-op lookup.

    When *for_cart* is True the text reply handler will store the result in
    the shopping cart instead of executing FFmpeg immediately.

    Prompt format:
        ``New title (optionally: Title | lang_code)``
    """
    # Prompts are issued on the single asyncio event loop, so mapping updates are
    # serialized per update and safe for this lightweight in-memory state.
    _clear_metadata_prompt(op.get("awaiting_metadata_msg_id"))
    op["awaiting_metadata_for_track"] = track_idx
    op["awaiting_metadata_stream_type"] = stream_type
    op["awaiting_metadata_for_cart"] = for_cart
    op["metadata_input_path"] = input_path
    op["metadata_output_path"] = output_path
    prompt = await reply_target.reply(
        prompt_text
        or (
            f"📝 **Send new metadata for Track {track_idx + 1}.**\n"
            "Format: `New Title` or `New Title | lang` (e.g. `English DTS-HD | eng`)"
        ),
        reply_markup=ForceReply(selective=True),
    )
    op["awaiting_metadata_msg_id"] = prompt.id
    pending_metadata_prompts[prompt.id] = op_id


def _build_keep_map_args(
    audio_keep: list[int],
    subtitle_keep: list[int],
    has_video: bool = True,
) -> list[str]:
    """Build ffmpeg -map arguments to keep selected streams."""
    args: list[str] = []
    if has_video:
        args += ["-map", "0:v:0"]
    for idx in audio_keep:
        args += ["-map", f"0:a:{idx}"]
    for idx in subtitle_keep:
        args += ["-map", f"0:s:{idx}"]
    return args


def _build_extract_cmd(
    input_path: str,
    output_path: str,
    stream_type: str,
    track_idx: int,
) -> list:
    """Build an ffmpeg command to extract a single stream via stream copy."""
    if stream_type == "video":
        maps = ["-map", "0:v:0"]
    elif stream_type == "audio":
        maps = ["-map", f"0:a:{track_idx}"]
    else:
        maps = ["-map", f"0:s:{track_idx}"]
    return ["ffmpeg", "-y", "-i", input_path, *maps, "-c", "copy", output_path]


def _build_add_external_cmd(
    input_path: str,
    external_path: str,
    output_path: str,
    convert_to_aac: bool,
) -> list:
    """Build an ffmpeg command to add an external track."""
    if convert_to_aac:
        return [
            "ffmpeg", "-y",
            "-i", input_path,
            "-i", external_path,
            "-map", "0",
            "-map", "1:a:0",
            "-c:v", "copy",
            "-c:a", "aac",
            "-b:a", AAC_HIGH_BITRATE,
            output_path,
        ]
    return [
        "ffmpeg", "-y",
        "-i", input_path,
        "-i", external_path,
        "-map", "0",
        "-map", "1",
        "-c", "copy",
        output_path,
    ]


async def _handle_external_addition(
    client: Client,
    status_msg: Message,
    op_id: str,
    op: dict,
    convert_to_aac: bool,
) -> None:
    """Mux an external track into the main file and prompt for metadata."""
    input_path = op["input"]
    external_path = op.get("external_input_path")
    external_type = op.get("awaiting_external_type")
    if not external_path or not external_type:
        await status_msg.edit_text("❌ **External file missing. Please re-upload.**")
        return

    output_path = _output_path_for(input_path, "external")
    cmd = _build_add_external_cmd(
        input_path=input_path,
        external_path=external_path,
        output_path=output_path,
        convert_to_aac=convert_to_aac,
    )

    await status_msg.edit_text("⚙️ **Muxing external track…**", reply_markup=None)
    ok, err = await _run_ffmpeg(cmd)
    if not ok:
        await status_msg.edit_text(f"❌ **FFmpeg failed.**\n```\n{err}\n```")
        _cleanup(external_path)
        op["external_input_path"] = None
        return

    # Store post-add state for metadata prompt.
    op["post_add_output_path"] = output_path
    op["post_add_stream_type"] = external_type
    if external_type == "audio":
        op["post_add_track_idx"] = len(op["streams"]["audio"])
    else:
        op["post_add_track_idx"] = len(op["streams"]["subtitle"])

    op["awaiting_external_type"] = None
    await status_msg.edit_text(
        "✅ **Task finished. Edit the metadata/title of the new track?**",
        reply_markup=InlineKeyboardMarkup(
            [
                [
                    InlineKeyboardButton(
                        "Yes, Edit Metadata",
                        callback_data=f"postmeta:yes:{op_id}",
                    ),
                    InlineKeyboardButton(
                        "No, Upload Now",
                        callback_data=f"postmeta:no:{op_id}",
                    ),
                ]
            ]
        ),
    )


async def _upload_post_add_result(
    client: Client,
    status_msg: Message,
    op_id: str,
    op: dict,
) -> None:
    """Upload the post-add output and clean up all related files."""
    output_path = op.get("post_add_output_path")
    if not output_path:
        await status_msg.edit_text("❌ **Output missing. Please retry.**")
        return

    await status_msg.edit_text("⬆️ **Uploading…**", reply_markup=None)
    try:
        await _upload_with_retry(
            client=client,
            chat_id=op["chat_id"],
            file_path=output_path,
            caption="✅ **Muxing complete!**",
            status_msg=status_msg,
        )
        await status_msg.edit_text("✅ **Done! File uploaded.**", reply_markup=None)
    except Exception as exc:
        await status_msg.edit_text(
            f"❌ **Upload failed:** `{exc}`", reply_markup=None
        )
    # Files are retained for FILE_RETENTION_MINUTES; background loop handles deletion.
    pending_ops.pop(op_id, None)


# ── Message handler ───────────────────────────────────────────────────────────

@app.on_message(
    filters.user(ADMIN_USER_IDS) & (filters.video | filters.document)
)
async def on_video(client: Client, message: Message) -> None:
    """Auto-detect MKV/MP4, download with fast_download, probe, and present mux options."""
    # Prevent external-track reply prompts from being treated as new video tasks.
    if message.reply_to_message and message.reply_to_message.id in pending_external_prompts:
        await message.reply(
            "❌ External track prompt active. Reply with an audio/document file, not a video."
        )
        return
    # Prevent attachment upload prompts from being treated as new video tasks.
    if message.reply_to_message and message.reply_to_message.id in pending_attachment_prompts:
        return

    file_obj = message.video or message.document
    if file_obj is None:
        return

    raw_name: str = getattr(file_obj, "file_name", None) or ""
    mime: str = getattr(file_obj, "mime_type", "") or ""
    ext = Path(raw_name).suffix.lower()

    if ext not in (".mkv", ".mp4") and not mime.startswith("video/"):
        return

    # Sanitize filename — strip any path components.
    safe_name = (
        Path(raw_name).name
        if raw_name
        else f"video_{file_obj.file_unique_id}{ext or '.mkv'}"
    )
    safe_name = _sanitize_filename(safe_name)
    input_path = DOWNLOADS_DIR / safe_name

    status_msg = await message.reply("⬇️ **Downloading…**")

    ok = await fast_download(
        client=client,
        message=message,
        file_path=input_path,
        status_msg=status_msg,
        label="Downloading",
    )
    if not ok:
        await status_msg.edit_text("❌ Download failed.")
        _cleanup(str(input_path))
        return

    await status_msg.edit_text("🔍 **Scanning media tracks…**")

    streams = _split_streams(await _ffprobe_streams(str(input_path)))
    op_id = _next_op_id()
    pending_ops[op_id] = {
        "input": str(input_path),
        "streams": streams,
        "chat_id": message.chat.id,
        "safe_name": safe_name,
        "selected_extract": set(),
        "awaiting_metadata_for_track": None,
        "awaiting_metadata_stream_type": None,
        "awaiting_metadata_msg_id": None,
        "awaiting_metadata_for_cart": False,
        "metadata_input_path": None,
        "metadata_output_path": None,
        "awaiting_external_type": None,
        "awaiting_external_msg_id": None,
        "external_input_path": None,
        "external_codec": None,
        "post_add_output_path": None,
        "post_add_stream_type": None,
        "post_add_track_idx": None,
        "awaiting_attachment_msg_id": None,
        # Shopping cart — accumulate changes; compile into one FFmpeg command.
        "cart": {
            "remove_audio": set(),
            "remove_subs": set(),
            "metadata": {},       # {(kind, orig_idx): {title, language}}
            "attachments": [],    # [{path, mimetype, filename}]
        },
    }

    await status_msg.edit_text(
        _render_menu_text(safe_name, streams, "main", pending_ops[op_id]),
        reply_markup=_build_dynamic_keyboard(op_id, pending_ops[op_id], "main"),
    )


# ── Callback handler ──────────────────────────────────────────────────────────

@app.on_callback_query(filters.user(ADMIN_USER_IDS))
async def on_callback(client: Client, query: CallbackQuery) -> None:
    """Dispatch inline button presses."""
    data: str = query.data or ""
    parts = data.split(":")
    action = parts[0] if parts else ""

    # ── No-op placeholders ───────────────────────────────────────────────────
    if action == "noop":
        await query.answer("Nothing to select here.")
        return

    # ── Cancel ────────────────────────────────────────────────────────────────
    if action == "cancel" and len(parts) == 2:
        op = pending_ops.pop(parts[1], None)
        if op:
            _clear_metadata_prompt(op.get("awaiting_metadata_msg_id"))
            _clear_external_prompt(op.get("awaiting_external_msg_id"))
            _clear_attachment_prompt(op.get("awaiting_attachment_msg_id"))
            # Cleanup attachment files from cart (they are temp files).
            for att in op.get("cart", {}).get("attachments", []):
                _cleanup(att.get("path", ""))
            _cleanup(op["input"])
            _cleanup(op.get("external_input_path") or "")
            _cleanup(op.get("post_add_output_path") or "")
        await query.message.edit_text(
            "🗑 **Cancelled. Temporary files deleted.**",
            reply_markup=None,
        )
        await query.answer("Cancelled.")
        return

    # ── Cart: clear all pending changes ──────────────────────────────────────
    if action == "cart_clear" and len(parts) == 2:
        op = pending_ops.get(parts[1])
        if op is None:
            await query.answer("Session expired.", show_alert=True)
            return
        # Remove attachment files from disk (they are small temp copies).
        for att in op["cart"].get("attachments", []):
            _cleanup(att.get("path", ""))
        op["cart"] = {
            "remove_audio": set(),
            "remove_subs": set(),
            "metadata": {},
            "attachments": [],
        }
        await query.message.edit_text(
            _render_menu_text(op["safe_name"], op["streams"], "main", op),
            reply_markup=_build_dynamic_keyboard(parts[1], op, "main"),
        )
        await query.answer("🗑 Cart cleared.")
        return

    # ── Cart: execute all queued changes in ONE FFmpeg command ────────────────
    if action == "cart_exec" and len(parts) == 2:
        op_id = parts[1]
        op = pending_ops.get(op_id)
        if op is None:
            await query.answer("Session expired. Please resend the file.", show_alert=True)
            return
        if not _cart_has_changes(op):
            await query.answer("Cart is empty — nothing to execute.", show_alert=True)
            return

        input_path: str = op["input"]
        cart = op["cart"]

        # Determine output extension: use MKV when attachments are present
        # (MKV is the only common container that supports -attach natively).
        has_atts = bool(cart.get("attachments"))
        out_ext = ".mkv" if has_atts or Path(input_path).suffix.lower() == ".mkv" else ".mp4"
        output_path = _output_path_for(input_path, "cart", out_ext)

        cmd = _build_cart_ffmpeg_cmd(input_path, output_path, op)

        _clear_metadata_prompt(op.get("awaiting_metadata_msg_id"))
        _clear_external_prompt(op.get("awaiting_external_msg_id"))
        pending_ops.pop(op_id, None)

        try:
            await _execute_mux(
                client=client,
                status_msg=query.message,
                chat_id=op["chat_id"],
                output_path=output_path,
                cmd=cmd,
            )
        finally:
            # Clean up any attachment temp files after muxing.
            for att in cart.get("attachments", []):
                _cleanup(att.get("path", ""))
        await query.answer()
        return

    # ── Menu navigation (no mux yet) ─────────────────────────────────────────
    if action == "menu" and len(parts) == 3:
        _, menu, op_id = parts
        op = pending_ops.get(op_id)
        if op is None:
            await query.answer("Session expired. Please resend the file.", show_alert=True)
            return
        await query.message.edit_text(
            _render_menu_text(op["safe_name"], op["streams"], menu, op),
            reply_markup=_build_dynamic_keyboard(op_id, op, menu),
        )
        await query.answer()
        return

    # ── Toggle multi-select state (now writes directly to cart) ──────────────
    if action == "toggle" and len(parts) >= 4:
        parsed = _parse_toggle_callback(parts)
        if parsed is None:
            await query.answer("Invalid selection.", show_alert=True)
            return
        menu, stream_key, track_idx, op_id = parsed
        op = pending_ops.get(op_id)
        if op is None:
            await query.answer("Session expired. Please resend the file.", show_alert=True)
            return

        cart = op["cart"]
        if stream_key == "remove_audio":
            if not _is_track_index_in_range(track_idx, op["streams"]["audio"]):
                await query.answer("Track out of range.", show_alert=True)
                return
            # Toggle in cart.remove_audio (cart-based, not immediate).
            if track_idx in cart["remove_audio"]:
                cart["remove_audio"].discard(track_idx)
            else:
                cart["remove_audio"].add(track_idx)
        elif stream_key == "remove_subs":
            if not _is_track_index_in_range(track_idx, op["streams"]["subtitle"]):
                await query.answer("Track out of range.", show_alert=True)
                return
            if track_idx in cart["remove_subs"]:
                cart["remove_subs"].discard(track_idx)
            else:
                cart["remove_subs"].add(track_idx)
        else:
            key = f"{stream_key}:{track_idx}"
            if stream_key == "v" and not op["streams"]["video"]:
                await query.answer("No video track found.", show_alert=True)
                return
            if stream_key == "a" and not _is_track_index_in_range(track_idx, op["streams"]["audio"]):
                await query.answer("Track out of range.", show_alert=True)
                return
            if stream_key == "s" and not _is_track_index_in_range(track_idx, op["streams"]["subtitle"]):
                await query.answer("Track out of range.", show_alert=True)
                return
            if key in op["selected_extract"]:
                op["selected_extract"].remove(key)
            else:
                op["selected_extract"].add(key)

        await query.message.edit_text(
            _render_menu_text(op["safe_name"], op["streams"], menu, op),
            reply_markup=_build_dynamic_keyboard(op_id, op, menu),
        )
        await query.answer()
        return

    # ── Execute extract (still immediate — separate output files) ────────────
    if action == "exec" and len(parts) == 3:
        _, exec_mode, op_id = parts
        op = pending_ops.get(op_id)
        if op is None:
            await query.answer("Session expired. Please resend the file.", show_alert=True)
            return

        if exec_mode != "extract":
            await query.answer("Unknown execution mode.", show_alert=True)
            return

        if not op["selected_extract"]:
            await query.answer("Select at least one track.", show_alert=True)
            return

        input_path = op["input"]
        chat_id = op["chat_id"]
        selected = sorted(op["selected_extract"])
        outputs: list[str] = []
        try:
            status_msg = await query.message.reply("⚙️ **Extracting…**")
            for key in selected:
                stream_type, idx_str = key.split(":")
                track_idx = int(idx_str)
                if stream_type == "v":
                    ext = _video_container_extension(input_path)
                    extract_path = _output_path_for(input_path, "video", ext)
                    cmd = _build_extract_cmd(input_path, extract_path, "video", track_idx)
                elif stream_type == "a":
                    track = op["streams"]["audio"][track_idx]
                    ext = _codec_extension(track.get("codec_name"), AUDIO_CODEC_EXTENSIONS)
                    extract_path = _output_path_for(input_path, f"a{track_idx + 1}", ext)
                    cmd = _build_extract_cmd(input_path, extract_path, "audio", track_idx)
                else:
                    track = op["streams"]["subtitle"][track_idx]
                    ext = _codec_extension(track.get("codec_name"), SUBTITLE_CODEC_EXTENSIONS)
                    extract_path = _output_path_for(input_path, f"s{track_idx + 1}", ext)
                    cmd = _build_extract_cmd(input_path, extract_path, "subtitle", track_idx)
                outputs.append(extract_path)
                await _execute_mux(
                    client=client,
                    status_msg=status_msg,
                    chat_id=chat_id,
                    output_path=extract_path,
                    cmd=cmd,
                )
            await query.answer("Extraction complete.")
        finally:
            pending_ops.pop(op_id, None)
        return

    # ── Metadata track selection → issue ForceReply prompt (cart mode) ────────
    if action == "meta" and len(parts) == 4:
        _, stream_type, track_str, op_id = parts
        op = pending_ops.get(op_id)
        if op is None:
            await query.answer("Session expired. Please resend the file.", show_alert=True)
            return
        try:
            track_idx = int(track_str)
        except ValueError:
            await query.answer("Invalid track selection.", show_alert=True)
            return

        if stream_type == "audio":
            track_list = op["streams"]["audio"]
        elif stream_type == "subtitle":
            track_list = op["streams"]["subtitle"]
        elif stream_type == "video":
            track_list = op["streams"]["video"]
        else:
            await query.answer("Unknown stream type.", show_alert=True)
            return

        if not _is_track_index_in_range(track_idx, track_list):
            await query.answer("Track out of range.", show_alert=True)
            return

        input_path: str = op["input"]
        # Output path only needed for immediate (post-add) mode; set a placeholder.
        output_path = _output_path_for(input_path, "metadata")

        # Cart mode: store result in cart, do NOT execute FFmpeg immediately.
        await _issue_metadata_prompt(
            reply_target=query.message,
            op_id=op_id,
            op=op,
            track_idx=track_idx,
            stream_type=stream_type,
            input_path=input_path,
            output_path=output_path,
            for_cart=True,
        )
        await query.answer("Waiting for metadata…")
        return

    # ── Attachment: prompt upload or remove from cart ─────────────────────────
    if action == "attach" and len(parts) >= 3:
        attach_action = parts[1]
        op_id = parts[-1]
        op = pending_ops.get(op_id)
        if op is None:
            await query.answer("Session expired. Please resend the file.", show_alert=True)
            return

        if attach_action == "prompt":
            # Send ForceReply to request an attachment file.
            _clear_attachment_prompt(op.get("awaiting_attachment_msg_id"))
            prompt = await query.message.reply(
                "📎 **Upload your attachment file** (.ttf / .otf / .jpg / .png).",
                reply_markup=ForceReply(selective=True),
            )
            op["awaiting_attachment_msg_id"] = prompt.id
            pending_attachment_prompts[prompt.id] = op_id
            await query.answer("Waiting for file…")
            return

        if attach_action == "remove" and len(parts) == 4:
            try:
                att_idx = int(parts[2])
            except ValueError:
                await query.answer("Invalid index.", show_alert=True)
                return
            attachments = op["cart"].get("attachments", [])
            if 0 <= att_idx < len(attachments):
                removed = attachments.pop(att_idx)
                _cleanup(removed.get("path", ""))
            await query.message.edit_text(
                _render_menu_text(op["safe_name"], op["streams"], "manage_attachments", op),
                reply_markup=_build_dynamic_keyboard(op_id, op, "manage_attachments"),
            )
            await query.answer("Attachment removed from cart.")
            return

    # ── External track selection and conversion prompts ──────────────────────
    if action == "external" and len(parts) == 3:
        _, external_action, op_id = parts
        op = pending_ops.get(op_id)
        if op is None:
            await query.answer("Session expired. Please resend the file.", show_alert=True)
            return
        if external_action in {"select_audio", "select_sub"}:
            external_type = "audio" if external_action == "select_audio" else "subtitle"
            op["awaiting_external_type"] = external_type
            _clear_external_prompt(op.get("awaiting_external_msg_id"))
            prompt = await query.message.reply(
                f"📎 **Upload the external {external_type} file now.**",
                reply_markup=ForceReply(selective=True),
            )
            op["awaiting_external_msg_id"] = prompt.id
            pending_external_prompts[prompt.id] = op_id
            await query.answer("Waiting for upload…")
            return
        if external_action in {"convert_yes", "convert_no"}:
            if not op.get("external_input_path"):
                await query.answer("External file missing. Please re-upload.", show_alert=True)
                return
            convert = external_action == "convert_yes"
            await _handle_external_addition(
                client=client,
                status_msg=query.message,
                op_id=op_id,
                op=op,
                convert_to_aac=convert,
            )
            await query.answer()
            return

    # ── Post-add metadata prompt ─────────────────────────────────────────────
    if action == "postmeta" and len(parts) == 3:
        _, decision, op_id = parts
        op = pending_ops.get(op_id)
        if op is None:
            await query.answer("Session expired. Please resend the file.", show_alert=True)
            return
        if decision == "no":
            await _upload_post_add_result(client, query.message, op_id, op)
            await query.answer()
            return
        if decision == "yes":
            output_path = op.get("post_add_output_path")
            stream_type = op.get("post_add_stream_type")
            track_idx = op.get("post_add_track_idx")
            if not output_path or stream_type is None or track_idx is None:
                await query.answer("Missing post-add data.", show_alert=True)
                return
            titled_output = _output_path_for(output_path, "titled", Path(output_path).suffix)
            await _issue_metadata_prompt(
                reply_target=query.message,
                op_id=op_id,
                op=op,
                track_idx=track_idx,
                stream_type=stream_type,
                input_path=output_path,
                output_path=titled_output,
                prompt_text=(
                    "📝 **Send the new title for the added track.**\n"
                    "Format: `Title` or `Title | lang`"
                ),
                for_cart=False,  # post-add metadata executes immediately
            )
            await query.answer("Waiting for title…")
            return

    # ── Mux actions (convert / isolate / default / remove subs) ──────────────
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

        if mode in {"isolate_audio", "default_audio"}:
            if not _is_track_index_in_range(selected_track_idx, op["streams"]["audio"]):
                await query.answer("Track out of range.", show_alert=True)
                return

        # We are executing now, so remove the op from the registry.
        op = pending_ops.pop(op_id)
        _clear_metadata_prompt(op.get("awaiting_metadata_msg_id"))
        _clear_external_prompt(op.get("awaiting_external_msg_id"))
        input_path: str = op["input"]
        chat_id: int = op["chat_id"]
        # Choose extension: keep MKV for MKV inputs, convert to MKV for others
        in_ext = Path(input_path).suffix.lower()
        output_path = _output_path_for(
            input_path, mode,
            ".mp4" if mode == "convert" and in_ext == ".mp4" else ".mkv",
        )

        try:
            if mode == "remove_all_subs":
                cmd = [
                    "ffmpeg", "-y", "-i", input_path,
                    "-map", "0:v", "-map", "0:a",
                    "-c", "copy", "-sn",
                    output_path,
                ]
            else:
                ffmpeg_mode = MUX_MODE_MAP.get(mode)
                if ffmpeg_mode is None:
                    await query.message.edit_text("❌ Unknown mux mode.")
                    return
                cmd = _build_ffmpeg_cmd(
                    input_path=input_path,
                    output_path=output_path,
                    mode=ffmpeg_mode,
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
        # Files retained by background loop; no immediate cleanup here.

    await query.answer()


# ── Upload handler: external tracks AND attachments ───────────────────────────

@app.on_message(filters.user(ADMIN_USER_IDS) & (filters.document | filters.audio) & filters.reply)
async def on_external_upload(client: Client, message: Message) -> None:
    """Handle document/audio replies for both external tracks and cart attachments."""
    prompt_id = message.reply_to_message.id if message.reply_to_message else None
    if prompt_id is None:
        return

    # ── Attachment upload path ────────────────────────────────────────────────
    if prompt_id in pending_attachment_prompts:
        op_id = pending_attachment_prompts.get(prompt_id)
        if op_id is None:
            return
        op = pending_ops.get(op_id)
        if op is None or op.get("chat_id") != message.chat.id:
            _clear_attachment_prompt(prompt_id)
            return

        file_obj = message.document or message.audio
        if file_obj is None:
            return

        raw_name = getattr(file_obj, "file_name", None) or ""
        ext = Path(raw_name).suffix.lower() if raw_name else ""
        mime = _attachment_mimetype(raw_name or f"file{ext}")
        safe_name = Path(raw_name).name if raw_name else f"attach_{file_obj.file_unique_id}{ext or '.bin'}"
        safe_name = _sanitize_filename(safe_name)
        att_path = DOWNLOADS_DIR / f"attach_{op_id}_{safe_name}"

        status_msg = await message.reply("⬇️ **Downloading attachment…**")
        start_time = time.monotonic()
        try:
            await client.download_media(
                message,
                file_name=str(att_path),
                progress=_progress,
                progress_args=(status_msg, "Downloading", start_time, 0, False),
            )
        except Exception as exc:
            await status_msg.edit_text(
                f"❌ Attachment download failed ({type(exc).__name__}). Please retry."
            )
            _cleanup(str(att_path))
            return

        _clear_attachment_prompt(prompt_id)
        op["awaiting_attachment_msg_id"] = None
        op["cart"]["attachments"].append({
            "path": str(att_path),
            "mimetype": mime,
            "filename": safe_name,
        })
        await status_msg.edit_text(
            f"✅ **Attachment added to cart:** `{safe_name}` (`{mime}`)",
            reply_markup=_build_dynamic_keyboard(op_id, op, "manage_attachments"),
        )
        return

    # ── External track upload path ────────────────────────────────────────────
    op_id = pending_external_prompts.get(prompt_id)
    if op_id is None:
        return
    op = pending_ops.get(op_id)
    if op is None or op.get("chat_id") != message.chat.id:
        _clear_external_prompt(prompt_id)
        return

    file_obj = message.document or message.audio
    if file_obj is None:
        return

    raw_name = getattr(file_obj, "file_name", None) or ""
    ext = Path(raw_name).suffix.lower() if raw_name else ""
    safe_name = Path(raw_name).name if raw_name else f"external_{file_obj.file_unique_id}{ext or '.bin'}"
    safe_name = _sanitize_filename(safe_name)
    external_path = DOWNLOADS_DIR / f"external_{op_id}_{safe_name}"

    status_msg = await message.reply("⬇️ **Downloading external file…**")
    start_time_ext = time.monotonic()
    try:
        await client.download_media(
            message,
            file_name=str(external_path),
            progress=_progress,
            progress_args=(status_msg, "Downloading"),
        )
    except Exception as exc:
        await status_msg.edit_text(
            f"❌ External download failed ({type(exc).__name__}). Please retry."
        )
        _cleanup(str(external_path))
        return

    _clear_external_prompt(prompt_id)
    op["awaiting_external_msg_id"] = None
    op["external_input_path"] = str(external_path)

    streams = _split_streams(await _ffprobe_streams(str(external_path)))
    expected_type = op.get("awaiting_external_type")
    if expected_type == "audio":
        if not streams["audio"]:
            await status_msg.edit_text("❌ No audio stream found in the uploaded file.")
            _cleanup(str(external_path))
            op["external_input_path"] = None
            return
        codec_name = streams["audio"][0].get("codec_name")
        if not codec_name:
            await status_msg.edit_text("❌ Unable to detect audio codec. Please retry.")
            _cleanup(str(external_path))
            op["external_input_path"] = None
            return
        op["external_codec"] = codec_name
        if codec_name.lower() not in WEB_COMPATIBLE_AUDIO_CODECS:
            await status_msg.edit_text(
                "⚠️ **This codec is not web-browser supported.**\n"
                "Convert to high-bitrate AAC to preserve channels?",
                reply_markup=InlineKeyboardMarkup(
                    [
                        [
                            InlineKeyboardButton(
                                "Yes, Convert to AAC",
                                callback_data=f"external:convert_yes:{op_id}",
                            ),
                            InlineKeyboardButton(
                                "No, Keep Original Codec",
                                callback_data=f"external:convert_no:{op_id}",
                            ),
                        ]
                    ]
                ),
            )
            return

        await _handle_external_addition(
            client=client,
            status_msg=status_msg,
            op_id=op_id,
            op=op,
            convert_to_aac=False,
        )
        return

    if expected_type == "subtitle":
        if not streams["subtitle"]:
            await status_msg.edit_text("❌ No subtitle stream found in the uploaded file.")
            _cleanup(str(external_path))
            op["external_input_path"] = None
            return

        await _handle_external_addition(
            client=client,
            status_msg=status_msg,
            op_id=op_id,
            op=op,
            convert_to_aac=False,
        )
        return

    await status_msg.edit_text("❌ External upload state expired. Please retry.")
    _cleanup(str(external_path))


# ── Metadata text reply handler ──────────────────────────────────────────────

@app.on_message(filters.user(ADMIN_USER_ID) & filters.text & filters.reply)
async def on_metadata_text(client: Client, message: Message) -> None:
    """Capture admin replies to metadata prompts and start muxing."""
    if not message.reply_to_message:
        return

    # Fast lookup: prompt message ID -> op_id.
    prompt_id = message.reply_to_message.id
    op_id = pending_metadata_prompts.get(prompt_id)
    if op_id is None:
        return

    op = pending_ops.get(op_id)
    if op is None or op.get("chat_id") != message.chat.id:
        _clear_metadata_prompt(prompt_id)
        return

    track_idx = op.get("awaiting_metadata_for_track")
    stream_type = op.get("awaiting_metadata_stream_type")
    if stream_type is None:
        await message.reply("❌ **Metadata session expired. Please resend the file.**")
        _clear_metadata_prompt(prompt_id)
        pending_ops.pop(op_id, None)
        return
    track_list = (
        op["streams"]["audio"] if stream_type == "audio" else op["streams"]["subtitle"]
    )
    if not _is_track_index_in_range(track_idx, track_list):
        await message.reply("❌ **Track selection expired. Please resend the file.**")
        _clear_metadata_prompt(prompt_id)
        pending_ops.pop(op_id, None)
        return

    new_title = (message.text or "").strip()
    if not new_title:
        # Keep the op active and re-issue a fresh ForceReply prompt.
        _clear_metadata_prompt(prompt_id)
        await _issue_metadata_prompt(
            reply_target=message,
            op_id=op_id,
            op=op,
            track_idx=track_idx,
            stream_type=stream_type,
            input_path=op.get("metadata_input_path") or op["input"],
            output_path=op.get("metadata_output_path") or _output_path_for(op["input"], "metadata"),
            prompt_text=(
                f"❌ **Title cannot be empty. Send a name for Track {track_idx + 1}.**"
            ),
        )
        return
    if len(new_title) > MAX_METADATA_TITLE_LEN:
        _clear_metadata_prompt(prompt_id)
        await _issue_metadata_prompt(
            reply_target=message,
            op_id=op_id,
            op=op,
            track_idx=track_idx,
            stream_type=stream_type,
            input_path=op.get("metadata_input_path") or op["input"],
            output_path=op.get("metadata_output_path") or _output_path_for(op["input"], "metadata"),
            prompt_text=(
                f"❌ **Title too long. Max {MAX_METADATA_TITLE_LEN} characters.**"
            ),
        )
        return

    # Remove the op from registry; we are executing the final mux now.
    op = pending_ops.pop(op_id)
    _clear_metadata_prompt(prompt_id)

    input_path: str = op.get("metadata_input_path") or op["input"]
    chat_id: int = op["chat_id"]
    output_path = op.get("metadata_output_path") or _output_path_for(input_path, "metadata")

    try:
        cmd = _build_ffmpeg_cmd(
            input_path=input_path,
            output_path=output_path,
            mode="metadata",
            track_idx=track_idx,
            new_title=new_title,
            stream_type=stream_type,
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
        _cleanup(
            op["input"],
            input_path,
            output_path,
            op.get("external_input_path") or "",
            op.get("post_add_output_path") or "",
        )


# ── Entry point ───────────────────────────────────────────────────────────────

if __name__ == "__main__":
    app.run()
