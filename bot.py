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
#   "streams": {"audio": list[dict], "subtitle": list[dict], "video": list[dict]},
#   "chat_id": int,
#   "safe_name": str,
#   "selected_audio": set[int],
#   "selected_subtitles": set[int],
#   "selected_extract": set[str],  # "a:0", "s:1", "v:0"
#   "awaiting_metadata_for_track": int | None,
#   "awaiting_metadata_stream_type": str | None,
#   "awaiting_metadata_msg_id": int | None,
#   "metadata_input_path": str | None,
#   "metadata_output_path": str | None,
#   "awaiting_external_type": str | None,
#   "awaiting_external_msg_id": int | None,
#   "external_input_path": str | None,
#   "external_codec": str | None,
#   "post_add_output_path": str | None,
#   "post_add_stream_type": str | None,
#   "post_add_track_idx": int | None,
# }
pending_ops: dict[str, dict] = {}
# Reverse lookup: prompt message ID -> op_id (fast metadata reply matching).
pending_metadata_prompts: dict[int, str] = {}
# Reverse lookup: external upload prompt message ID -> op_id.
pending_external_prompts: dict[int, str] = {}
_op_counter: int = 0

# Progress-bar throttle: msg_id → last_update_monotonic
_progress_ts: dict[int, float] = {}
_PROGRESS_INTERVAL = 2.0  # seconds between edits

# ── Codec/extension helpers ────────────────────────────────────────────────────

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
WEB_AUDIO_CODECS = {"aac", "mp3", "opus"}


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
        if codec_key.isalnum():
            return _sanitize_extension(codec_key)
    return ".bin"


def _video_container_extension(input_path: str) -> str:
    """Choose a container extension for video extraction."""
    return ".mp4" if Path(input_path).suffix.lower() == ".mp4" else ".mkv"


def _output_path_for(input_path: str, suffix: str, ext: str = ".mp4") -> str:
    """Build an output path in the downloads directory."""
    base_name = Path(input_path).stem
    return str(DOWNLOADS_DIR / f"{base_name}_{suffix}{ext}")

def _build_ffmpeg_cmd(
    input_path: str,
    output_path: str,
    mode: str,
    track_idx: int | None = None,
    new_title: str | None = None,
    stream_type: str = "audio",
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
        stream_selector = "a" if stream_type == "audio" else "s"
        return base + [
            "-map", "0",
            "-c", "copy",
            f"-metadata:s:{stream_selector}:{track_idx}", f"title={new_title}",
            output_path,
        ]

    return None


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


def _is_track_index_valid(track_idx: int | None, tracks: list[dict]) -> bool:
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


def _render_menu_text(safe_name: str, streams: dict[str, list[dict]], menu: str) -> str:
    """Build the main/sub-menu text block shown above the inline keyboard."""
    prompt = {
        "main": "Choose an action:",
        "isolate_audio": "Select the audio track to isolate:",
        "default_audio": "Select the audio track to set as default:",
        "metadata_audio": "Select the audio track to rename:",
        "remove_audio": "Toggle audio tracks to remove, then execute:",
        "remove_subs": "Toggle subtitle tracks to remove, then execute:",
        "extract": "Toggle tracks to extract, then execute:",
        "add_external": "Choose which external track to add:",
    }.get(menu, "Choose an action:")
    audio_lines = _format_audio_tracks(streams["audio"])
    subtitle_lines = _format_subtitle_tracks(streams["subtitle"])
    video_lines = _format_video_tracks(streams["video"])
    return (
        f"✅ **File ready!**\n\n"
        f"📁 `{safe_name}`\n"
        f"🎬 **Video Tracks ({len(streams['video'])}):**\n{video_lines}\n\n"
        f"🎵 **Audio Tracks ({len(streams['audio'])}):**\n{audio_lines}\n\n"
        f"📝 **Subtitle Tracks ({len(streams['subtitle'])}):**\n{subtitle_lines}\n\n"
        f"{prompt}"
    )


# ── Inline keyboard ───────────────────────────────────────────────────────────

def _build_dynamic_keyboard(
    op_id: str,
    op: dict,
    menu: str = "main",
) -> InlineKeyboardMarkup:
    """Build a multi-level, track-driven inline keyboard."""
    streams = op["streams"]

    def _toggle_label(label: str, selected: bool) -> str:
        return f"✅ {label}" if selected else label

    def _no_tracks_row(label: str) -> list[InlineKeyboardButton]:
        return [InlineKeyboardButton(label, callback_data="noop")]

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
                    callback_data=f"menu:isolate_audio:{op_id}",
                )],
                [InlineKeyboardButton(
                    "⭐ Set Default Track",
                    callback_data=f"menu:default_audio:{op_id}",
                )],
                [InlineKeyboardButton(
                    "📝 Edit Audio Metadata",
                    callback_data=f"menu:metadata_audio:{op_id}",
                )],
                [InlineKeyboardButton(
                    "🧹 Remove Specific Audios",
                    callback_data=f"menu:remove_audio:{op_id}",
                )],
                [InlineKeyboardButton(
                    "🧹 Remove Specific Subtitles",
                    callback_data=f"menu:remove_subs:{op_id}",
                )],
                [InlineKeyboardButton(
                    "🧹 Remove All Subtitles",
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
                    "🗑 Cancel",
                    callback_data=f"cancel:{op_id}",
                )],
            ]
        )

    # Sub-menus: per-track selections for isolate/default/metadata (single select).
    if menu in {"isolate_audio", "default_audio", "metadata_audio"}:
        callback_prefix = "meta:audio" if menu == "metadata_audio" else f"mux:{menu}"
        buttons = [
            [InlineKeyboardButton(
                _track_button_label(idx, track),
                callback_data=f"{callback_prefix}:{idx}:{op_id}",
            )]
            for idx, track in enumerate(streams["audio"])
        ] or [_no_tracks_row("⚠️ No audio tracks found")]
        buttons.append([
            InlineKeyboardButton(
                "🔙 Back to Main Menu",
                callback_data=f"menu:main:{op_id}",
            )
        ])
        return InlineKeyboardMarkup(buttons)

    # Multi-select remove audio.
    if menu == "remove_audio":
        buttons = [
            [InlineKeyboardButton(
                _toggle_label(
                    _track_button_label(idx, track),
                    idx in op["selected_audio"],
                ),
                callback_data=f"toggle:remove_audio:{idx}:{op_id}",
            )]
            for idx, track in enumerate(streams["audio"])
        ] or [_no_tracks_row("⚠️ No audio tracks found")]
        buttons.append([
            InlineKeyboardButton(
                "✅ Execute Selected",
                callback_data=f"exec:remove_audio:{op_id}",
            ),
            InlineKeyboardButton(
                "🔙 Back",
                callback_data=f"menu:main:{op_id}",
            ),
        ])
        return InlineKeyboardMarkup(buttons)

    # Multi-select remove subtitles.
    if menu == "remove_subs":
        buttons = [
            [InlineKeyboardButton(
                _toggle_label(
                    _subtitle_button_label(idx, track),
                    idx in op["selected_subtitles"],
                ),
                callback_data=f"toggle:remove_subs:{idx}:{op_id}",
            )]
            for idx, track in enumerate(streams["subtitle"])
        ] or [_no_tracks_row("⚠️ No subtitle tracks found")]
        buttons.append([
            InlineKeyboardButton(
                "✅ Execute Selected",
                callback_data=f"exec:remove_subs:{op_id}",
            ),
            InlineKeyboardButton(
                "🔙 Back",
                callback_data=f"menu:main:{op_id}",
            ),
        ])
        return InlineKeyboardMarkup(buttons)

    # Multi-select extraction.
    if menu == "extract":
        buttons: list[list[InlineKeyboardButton]] = []
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
                "✅ Execute Selected",
                callback_data=f"exec:extract:{op_id}",
            ),
            InlineKeyboardButton(
                "🔙 Back",
                callback_data=f"menu:main:{op_id}",
            ),
        ])
        return InlineKeyboardMarkup(buttons)

    # External track selection.
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

    # Fallback: always show a safe main menu.
    return _build_dynamic_keyboard(op_id, op, menu="main")


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


async def _issue_metadata_prompt(
    reply_target: Message,
    op_id: str,
    op: dict,
    track_idx: int,
    stream_type: str,
    input_path: str,
    output_path: str,
    prompt_text: str | None = None,
) -> None:
    """Send a ForceReply prompt and register the reply-to-op lookup."""
    _clear_metadata_prompt(op.get("awaiting_metadata_msg_id"))
    op["awaiting_metadata_for_track"] = track_idx
    op["awaiting_metadata_stream_type"] = stream_type
    op["metadata_input_path"] = input_path
    op["metadata_output_path"] = output_path
    prompt = await reply_target.reply(
        prompt_text or f"📝 **Send new title for Track {track_idx + 1}.**",
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
            "-b:a", "640k",
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
    await client.send_document(
        chat_id=op["chat_id"],
        document=output_path,
        caption="✅ **Muxing complete!**",
        progress=_progress,
        progress_args=(status_msg, "Uploading"),
    )
    await status_msg.edit_text("✅ **Done! File uploaded.**", reply_markup=None)

    _cleanup(op["input"], output_path, op.get("external_input_path") or "")
    pending_ops.pop(op_id, None)


# ── Message handler ───────────────────────────────────────────────────────────

@app.on_message(
    filters.user(ADMIN_USER_ID) & (filters.video | filters.document)
)
async def on_video(client: Client, message: Message) -> None:
    """Auto-detect MKV/MP4, download, probe, and present mux options."""
    if message.reply_to_message and message.reply_to_message.id in pending_external_prompts:
        return
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

    await status_msg.edit_text("🔍 **Scanning media tracks…**")

    streams = _split_streams(await _ffprobe_streams(str(input_path)))
    op_id = _next_op_id()
    pending_ops[op_id] = {
        "input": str(input_path),
        "streams": streams,
        "chat_id": message.chat.id,
        "safe_name": safe_name,
        "selected_audio": set(),
        "selected_subtitles": set(),
        "selected_extract": set(),
        "awaiting_metadata_for_track": None,
        "awaiting_metadata_stream_type": None,
        "awaiting_metadata_msg_id": None,
        "metadata_input_path": None,
        "metadata_output_path": None,
        "awaiting_external_type": None,
        "awaiting_external_msg_id": None,
        "external_input_path": None,
        "external_codec": None,
        "post_add_output_path": None,
        "post_add_stream_type": None,
        "post_add_track_idx": None,
    }

    await status_msg.edit_text(
        _render_menu_text(safe_name, streams, "main"),
        reply_markup=_build_dynamic_keyboard(op_id, pending_ops[op_id], "main"),
    )


# ── Callback handler ──────────────────────────────────────────────────────────

@app.on_callback_query(filters.user(ADMIN_USER_ID))
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
            _cleanup(op["input"])
            _cleanup(op.get("external_input_path") or "")
            _cleanup(op.get("post_add_output_path") or "")
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
            _render_menu_text(op["safe_name"], op["streams"], menu),
            reply_markup=_build_dynamic_keyboard(op_id, op, menu),
        )
        await query.answer()
        return

    # ── Toggle multi-select state ────────────────────────────────────────────
    if action == "toggle" and len(parts) >= 4:
        toggle_mode = parts[1]
        if toggle_mode == "extract" and len(parts) == 5:
            stream_key = parts[2]
            track_str = parts[3]
            op_id = parts[4]
        elif toggle_mode in {"remove_audio", "remove_subs"} and len(parts) == 4:
            stream_key = toggle_mode
            track_str = parts[2]
            op_id = parts[3]
        else:
            await query.answer("Invalid selection.", show_alert=True)
            return

        op = pending_ops.get(op_id)
        if op is None:
            await query.answer("Session expired. Please resend the file.", show_alert=True)
            return
        try:
            track_idx = int(track_str)
        except ValueError:
            await query.answer("Invalid track selection.", show_alert=True)
            return

        if stream_key == "remove_audio":
            if not _is_track_index_valid(track_idx, op["streams"]["audio"]):
                await query.answer("Track out of range.", show_alert=True)
                return
            if track_idx in op["selected_audio"]:
                op["selected_audio"].remove(track_idx)
            else:
                op["selected_audio"].add(track_idx)
            menu = "remove_audio"
        elif stream_key == "remove_subs":
            if not _is_track_index_valid(track_idx, op["streams"]["subtitle"]):
                await query.answer("Track out of range.", show_alert=True)
                return
            if track_idx in op["selected_subtitles"]:
                op["selected_subtitles"].remove(track_idx)
            else:
                op["selected_subtitles"].add(track_idx)
            menu = "remove_subs"
        else:
            key = f"{stream_key}:{track_idx}"
            if stream_key == "v" and not op["streams"]["video"]:
                await query.answer("No video track found.", show_alert=True)
                return
            if stream_key == "a" and not _is_track_index_valid(track_idx, op["streams"]["audio"]):
                await query.answer("Track out of range.", show_alert=True)
                return
            if stream_key == "s" and not _is_track_index_valid(track_idx, op["streams"]["subtitle"]):
                await query.answer("Track out of range.", show_alert=True)
                return
            if key in op["selected_extract"]:
                op["selected_extract"].remove(key)
            else:
                op["selected_extract"].add(key)
            menu = "extract"

        await query.message.edit_text(
            _render_menu_text(op["safe_name"], op["streams"], menu),
            reply_markup=_build_dynamic_keyboard(op_id, op, menu),
        )
        await query.answer()
        return

    # ── Execute multi-select actions ─────────────────────────────────────────
    if action == "exec" and len(parts) == 3:
        _, exec_mode, op_id = parts
        op = pending_ops.get(op_id)
        if op is None:
            await query.answer("Session expired. Please resend the file.", show_alert=True)
            return

        input_path = op["input"]
        chat_id = op["chat_id"]
        output_path = _output_path_for(input_path, exec_mode)

        if exec_mode == "remove_audio":
            if not op["selected_audio"]:
                await query.answer("Select at least one audio track.", show_alert=True)
                return
            audio_keep = [
                idx for idx in range(len(op["streams"]["audio"]))
                if idx not in op["selected_audio"]
            ]
            subtitle_keep = list(range(len(op["streams"]["subtitle"])))
            cmd = [
                "ffmpeg", "-y", "-i", input_path,
                *_build_keep_map_args(audio_keep, subtitle_keep, bool(op["streams"]["video"])),
                "-c", "copy",
                output_path,
            ]
        elif exec_mode == "remove_subs":
            if not op["selected_subtitles"]:
                await query.answer("Select at least one subtitle track.", show_alert=True)
                return
            audio_keep = list(range(len(op["streams"]["audio"])))
            subtitle_keep = [
                idx for idx in range(len(op["streams"]["subtitle"]))
                if idx not in op["selected_subtitles"]
            ]
            cmd = [
                "ffmpeg", "-y", "-i", input_path,
                *_build_keep_map_args(audio_keep, subtitle_keep, bool(op["streams"]["video"])),
                "-c", "copy",
                output_path,
            ]
        elif exec_mode == "extract":
            if not op["selected_extract"]:
                await query.answer("Select at least one track.", show_alert=True)
                return
            selected = sorted(op["selected_extract"])
            outputs: list[str] = []
            try:
                _clear_metadata_prompt(op.get("awaiting_metadata_msg_id"))
                _clear_external_prompt(op.get("awaiting_external_msg_id"))
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
                    status_msg = await query.message.reply("⚙️ **Extracting…**")
                    await _execute_mux(
                        client=client,
                        status_msg=status_msg,
                        chat_id=chat_id,
                        output_path=extract_path,
                        cmd=cmd,
                    )
                await query.answer("Extraction complete.")
            finally:
                _cleanup(input_path, *outputs)
                pending_ops.pop(op_id, None)
            return
        else:
            await query.answer("Unknown execution mode.", show_alert=True)
            return

        # We are executing now, so remove the op from the registry.
        _clear_metadata_prompt(op.get("awaiting_metadata_msg_id"))
        _clear_external_prompt(op.get("awaiting_external_msg_id"))
        pending_ops.pop(op_id, None)
        try:
            await _execute_mux(
                client=client,
                status_msg=query.message,
                chat_id=chat_id,
                output_path=output_path,
                cmd=cmd,
            )
        finally:
            _cleanup(input_path, output_path)
        await query.answer()
        return

    # ── Metadata track selection (await text reply) ──────────────────────────
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
        track_list = op["streams"]["audio"] if stream_type == "audio" else op["streams"]["subtitle"]
        if not _is_track_index_valid(track_idx, track_list):
            await query.answer("Track out of range.", show_alert=True)
            return

        input_path: str = op["input"]
        output_path = _output_path_for(input_path, "metadata")

        # Remember which track is awaiting metadata so the reply handler can map it.
        await _issue_metadata_prompt(
            reply_target=query.message,
            op_id=op_id,
            op=op,
            track_idx=track_idx,
            stream_type=stream_type,
            input_path=input_path,
            output_path=output_path,
        )
        await query.answer("Waiting for new title…")
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
                prompt_text="📝 **Send the new title for the added track.**",
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
            if not _is_track_index_valid(selected_track_idx, op["streams"]["audio"]):
                await query.answer("Track out of range.", show_alert=True)
                return

        # We are executing now, so remove the op from the registry.
        op = pending_ops.pop(op_id)
        _clear_metadata_prompt(op.get("awaiting_metadata_msg_id"))
        _clear_external_prompt(op.get("awaiting_external_msg_id"))
        input_path: str = op["input"]
        chat_id: int = op["chat_id"]
        output_path = _output_path_for(input_path, mode)

        try:
            if mode == "remove_all_subs":
                cmd = [
                    "ffmpeg", "-y", "-i", input_path,
                    "-map", "0:v", "-map", "0:a",
                    "-c", "copy", "-sn",
                    output_path,
                ]
            else:
                cmd = _build_ffmpeg_cmd(
                    input_path=input_path,
                    output_path=output_path,
                    mode=mode.replace("_audio", ""),
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
            _cleanup(
                input_path,
                output_path,
                op.get("external_input_path") or "",
                op.get("post_add_output_path") or "",
            )

    await query.answer()


# ── External track upload handler ─────────────────────────────────────────────

@app.on_message(filters.user(ADMIN_USER_ID) & (filters.document | filters.audio) & filters.reply)
async def on_external_upload(client: Client, message: Message) -> None:
    """Handle external audio/subtitle uploads for add-track operations."""
    prompt_id = message.reply_to_message.id if message.reply_to_message else None
    if prompt_id is None:
        return
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
    external_path = DOWNLOADS_DIR / f"external_{op_id}_{safe_name}"

    status_msg = await message.reply("⬇️ **Downloading external file…**")
    try:
        await client.download_media(
            message,
            file_name=str(external_path),
            progress=_progress,
            progress_args=(status_msg, "Downloading"),
        )
    except Exception:
        await status_msg.edit_text("❌ External download failed.")
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
        codec_name = streams["audio"][0].get("codec_name", "")
        op["external_codec"] = codec_name
        if codec_name.lower() not in WEB_AUDIO_CODECS:
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
    matched_op_id = pending_metadata_prompts.get(prompt_id)
    if matched_op_id is None:
        return

    op = pending_ops.get(matched_op_id)
    if op is None or op.get("chat_id") != message.chat.id:
        _clear_metadata_prompt(prompt_id)
        return

    track_idx = op.get("awaiting_metadata_for_track")
    stream_type = op.get("awaiting_metadata_stream_type")
    if stream_type is None:
        await message.reply("❌ **Metadata session expired. Please resend the file.**")
        _clear_metadata_prompt(prompt_id)
        pending_ops.pop(matched_op_id, None)
        return
    track_list = (
        op["streams"]["audio"] if stream_type == "audio" else op["streams"]["subtitle"]
    )
    if not _is_track_index_valid(track_idx, track_list):
        await message.reply("❌ **Track selection expired. Please resend the file.**")
        _clear_metadata_prompt(prompt_id)
        pending_ops.pop(matched_op_id, None)
        return

    new_title = (message.text or "").strip()
    if not new_title:
        # Keep the op active and re-issue a fresh ForceReply prompt.
        _clear_metadata_prompt(prompt_id)
        await _issue_metadata_prompt(
            reply_target=message,
            op_id=matched_op_id,
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

    # Remove the op from registry; we are executing the final mux now.
    op = pending_ops.pop(matched_op_id)
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
