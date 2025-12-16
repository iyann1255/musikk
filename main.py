import asyncio
import re
from dataclasses import dataclass, field
from typing import Dict, List, Optional, Tuple

import aiohttp
from pyrogram import Client, filters
from pyrogram.types import (
    Message,
    CallbackQuery,
    InlineKeyboardMarkup,
    InlineKeyboardButton,
)
from pyrogram.enums import ChatType

from pytgcalls import PyTgCalls
from pytgcalls.types.input_stream import AudioPiped
from pytgcalls.exceptions import NoActiveGroupCall

from yt_dlp import YoutubeDL

import config

# =========================
# Guards
# =========================
if not (config.API_ID and config.API_HASH and config.BOT_TOKEN and config.ASSISTANT_SESSION):
    raise SystemExit("ENV wajib: API_ID, API_HASH, BOT_TOKEN, ASSISTANT_SESSION")

# =========================
# Regex & classifier
# =========================
YT_RE = re.compile(r"(youtube\.com/watch\?v=|youtu\.be/|music\.youtube\.com/|youtube\.com/shorts/)", re.I)
STREAM_EXTS = (".m3u8", ".mp3", ".aac", ".m4a", ".ogg", ".opus", ".flac", ".wav")


def is_url(s: str) -> bool:
    s = (s or "").strip().lower()
    return s.startswith("http://") or s.startswith("https://")


def is_stream_url(s: str) -> bool:
    u = (s or "").strip().lower()
    if not is_url(u):
        return False
    return any(ext in u for ext in STREAM_EXTS)


def is_youtube(s: str) -> bool:
    t = (s or "").strip().lower()
    return bool(YT_RE.search(t))


# =========================
# State
# =========================
@dataclass
class Track:
    title: str
    source: str  # direct playable url (m3u8/mp3/…)
    requester: str


@dataclass
class ChatState:
    queue: List[Track] = field(default_factory=list)
    playing: Optional[Track] = None
    paused: bool = False


STATE: Dict[int, ChatState] = {}


def st(chat_id: int) -> ChatState:
    if chat_id not in STATE:
        STATE[chat_id] = ChatState()
    return STATE[chat_id]


# =========================
# Clients
# =========================
bot = Client("musicbot-bot", api_id=config.API_ID, api_hash=config.API_HASH, bot_token=config.BOT_TOKEN)
assistant = Client(
    "musicbot-assistant",
    api_id=config.API_ID,
    api_hash=config.API_HASH,
    session_string=config.ASSISTANT_SESSION,
)
call = PyTgCalls(assistant)

# =========================
# YouTube Search via Data API (no robots.txt)
# =========================
YTS_ENDPOINT = "https://www.googleapis.com/youtube/v3/search"


async def yt_search(query: str, limit: int = 5) -> List[Tuple[str, str]]:
    if not config.YOUTUBE_API_KEY:
        return []

    params = {
        "part": "snippet",
        "q": query,
        "type": "video",
        "maxResults": max(1, min(limit, 10)),
        "key": config.YOUTUBE_API_KEY,
        "safeSearch": "none",
    }

    async with aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=12)) as session:
        async with session.get(YTS_ENDPOINT, params=params) as resp:
            data = await resp.json()

    out: List[Tuple[str, str]] = []
    for it in data.get("items", []):
        vid = (it.get("id") or {}).get("videoId")
        title = ((it.get("snippet") or {}).get("title")) or "Unknown"
        if vid:
            out.append((title, f"https://www.youtube.com/watch?v={vid}"))
    return out


# =========================
# YouTube -> direct audio url (yt-dlp)
# =========================
YDL_OPTS = {
    "quiet": True,
    "no_warnings": True,
    "noplaylist": True,
    "geo_bypass": True,
    "format": "bestaudio/best",
    # Important: kita butuh URL stream, bukan download file
    "skip_download": True,
}


def ytdlp_extract_audio_url(url: str) -> Tuple[str, str]:
    """
    Return (title, direct_url) for playback with FFmpeg.
    """
    with YoutubeDL(YDL_OPTS) as ydl:
        info = ydl.extract_info(url, download=False)
    title = info.get("title") or "YouTube Audio"
    direct = info.get("url")
    if not direct:
        raise RuntimeError("yt-dlp gagal ambil direct audio url.")
    return title, direct


# =========================
# UI
# =========================
def player_kb(chat_id: int) -> InlineKeyboardMarkup:
    s = st(chat_id)
    pause_label = "▶️ Resume" if s.paused else "⏸ Pause"
    return InlineKeyboardMarkup(
        [
            [
                InlineKeyboardButton(pause_label, callback_data=f"pl:pause:{chat_id}"),
                InlineKeyboardButton("⏭ Skip", callback_data=f"pl:skip:{chat_id}"),
            ],
            [
                InlineKeyboardButton("⏹ Stop", callback_data=f"pl:stop:{chat_id}"),
                InlineKeyboardButton("📜 Queue", callback_data=f"pl:queue:{chat_id}"),
            ],
        ]
    )


def yt_kb(results: List[Tuple[str, str]]) -> InlineKeyboardMarkup:
    rows = []
    for i, (title, url) in enumerate(results[:5], start=1):
        rows.append([InlineKeyboardButton(f"{i}. Open", url=url)])
    rows.append([InlineKeyboardButton("Open YouTube", url="https://www.youtube.com")])
    return InlineKeyboardMarkup(rows)


# =========================
# Voice Playback
# =========================
async def ensure_join_and_play(chat_id: int, announce_chat_id: int):
    s = st(chat_id)
    if s.playing or not s.queue:
        return

    nxt = s.queue.pop(0)
    s.playing = nxt
    s.paused = False

    try:
        await call.join_group_call(chat_id, AudioPiped(nxt.source))
        await bot.send_message(
            announce_chat_id,
            f"🎶 Now playing:\n**{nxt.title}**\nRequested by: {nxt.requester}",
            reply_markup=player_kb(chat_id),
        )
    except NoActiveGroupCall:
        s.playing = None
        await bot.send_message(announce_chat_id, "❌ Voice chat belum aktif. Nyalain VC dulu, lalu /play lagi.")
    except Exception as e:
        s.playing = None
        await bot.send_message(announce_chat_id, f"❌ Gagal join/play.\n`{e}`")


async def play_next(chat_id: int, announce_chat_id: int):
    s = st(chat_id)
    s.paused = False

    if not s.queue:
        s.playing = None
        try:
            await call.leave_group_call(chat_id)
        except Exception:
            pass
        await bot.send_message(announce_chat_id, "Queue habis. Keluar dari VC.")
        return

    nxt = s.queue.pop(0)
    s.playing = nxt

    await call.change_stream(chat_id, AudioPiped(nxt.source))
    await bot.send_message(
        announce_chat_id,
        f"🎶 Now playing:\n**{nxt.title}**\nRequested by: {nxt.requester}",
        reply_markup=player_kb(chat_id),
    )


@call.on_stream_end()
async def on_end(_, update):
    chat_id = update.chat_id
    s = st(chat_id)
    if s.queue:
        try:
            await play_next(chat_id, chat_id)
        except Exception:
            pass
    else:
        s.playing = None
        s.paused = False
        try:
            await call.leave_group_call(chat_id)
        except Exception:
            pass


# =========================
# Target resolver for /cplay
# =========================
async def resolve_target_chat_id(m: Message, token: Optional[str]) -> int:
    if not token:
        return m.chat.id
    t = token.strip()
    if t.startswith("@"):
        chat = await bot.get_chat(t)
        return chat.id
    return int(t)


def parse_c_command(m: Message) -> Tuple[Optional[str], str]:
    args = m.command[1:] if m.command else []
    if not args:
        return None, ""
    first = args[0]
    if first.startswith("@") or first.startswith("-100"):
        target = first
        query = " ".join(args[1:]).strip()
        return target, query
    return None, " ".join(args).strip()


# =========================
# Core handler
# =========================
async def handle_play(m: Message, target_chat_id: int, query: str):
    requester = m.from_user.mention if m.from_user else "Unknown"

    if not query:
        return await m.reply("Format: `/play <judul|url>`", quote=True)

    # Only allow playback in groups/supergroups/channels context
    # (Bot bisa reply di private tapi playback VC butuh chat target)
    # We'll still allow /cplay from private if you want later.
    # For now: keep simple.

    # 1) Direct stream URL
    if is_stream_url(query):
        s = st(target_chat_id)
        s.queue.append(Track(title=query, source=query, requester=requester))

        if not s.playing:
            await m.reply("✅ Stream masuk. Nyoba join VC...")
            return await ensure_join_and_play(target_chat_id, m.chat.id)

        return await m.reply(f"✅ Masuk queue.\nPosisi: `{len(s.queue)}`", quote=True)

    # 2) YouTube link -> extract audio direct URL -> playback
    if is_youtube(query):
        msg = await m.reply("🎧 Ambil audio YouTube (yt-dlp)...")
        try:
            title, direct = await asyncio.to_thread(ytdlp_extract_audio_url, query)
        except Exception as e:
            return await msg.edit(
                "❌ Gagal ambil audio dari YouTube.\n"
                f"`{e}`\n\n"
                "Catatan: kadang YouTube berubah & yt-dlp perlu update."
            )

        s = st(target_chat_id)
        s.queue.append(Track(title=title, source=direct, requester=requester))
        if not s.playing:
            await msg.edit("✅ Masuk. Nyoba join VC...")
            return await ensure_join_and_play(target_chat_id, m.chat.id)

        return await msg.edit(f"✅ Masuk queue: **{title}**\nPosisi: `{len(s.queue)}`")

    # 3) Keyword -> search via YouTube Data API -> show list (dan opsional play)
    if not is_url(query):
        if not config.YOUTUBE_API_KEY:
            q = query.replace(" ", "+")
            url = f"https://www.youtube.com/results?search_query={q}"
            return await m.reply(
                "Saya bisa cariin via tombol ini.\n"
                "Kalau mau hasil list rapi + bisa auto-play, set `YOUTUBE_API_KEY`.\n",
                reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("Open Search", url=url)]]),
            )

        msg = await m.reply("🔎 Cari di YouTube (API resmi)...")
        try:
            results = await yt_search(query, limit=5)
        except Exception as e:
            return await msg.edit(f"❌ Gagal search YouTube API.\n`{e}`")

        if not results:
            return await msg.edit("❌ Tidak ada hasil.")

        # Tampilkan list + tombol open (biar user bisa copy link /play link)
        text = "✅ Hasil YouTube (pilih/open link):\n" + "\n".join(
            [f"{i}. {t[0]}" for i, t in enumerate(results, 1)]
        ) + "\n\nKirim linknya ke `/play <link>` buat langsung play."
        return await msg.edit(text, reply_markup=yt_kb(results))

    # 4) URL non-stream (misal website biasa)
    return await m.reply(
        "URL itu bukan stream yang bisa diputar langsung.\n"
        "Untuk VC: kirim link **m3u8/mp3/radio stream**, atau link YouTube.",
        quote=True,
    )


# =========================
# Commands
# =========================
@bot.on_message(filters.command(["start", "help"]))
async def help_cmd(_, m: Message):
    txt = (
        "🎵 **MusicBot (versi stabil)**\n\n"
        "**Commands**\n"
        "/play <m3u8/mp3/url|judul|link yt>\n"
        "/cplay [@channel|-100id] <m3u8/mp3/url|judul|link yt>\n"
        "/pause, /resume, /skip, /stop, /queue\n\n"
        "Catatan:\n"
        "- Playback VC butuh **VC aktif**.\n"
        "- Keyword YouTube butuh `YOUTUBE_API_KEY`.\n"
        "- Link YouTube bisa langsung `/play <link>`."
    )
    await m.reply(txt)


@bot.on_message(filters.command(["play"]))
async def play_cmd(_, m: Message):
    # allow in groups/supergroups
    query = " ".join(m.command[1:]).strip() if m.command else ""
    await handle_play(m, m.chat.id, query)


@bot.on_message(filters.command(["cplay"]))
async def cplay_cmd(_, m: Message):
    target_token, query = parse_c_command(m)
    try:
        target_chat_id = await resolve_target_chat_id(m, target_token)
    except Exception as e:
        return await m.reply(f"❌ Target tidak valid.\n`{e}`")
    await handle_play(m, target_chat_id, query)


@bot.on_message(filters.command(["pause"]))
async def pause_cmd(_, m: Message):
    s = st(m.chat.id)
    if not s.playing:
        return await m.reply("Belum ada yang diputar.")
    try:
        await call.pause_stream(m.chat.id)
        s.paused = True
        await m.reply("⏸ Paused.", reply_markup=player_kb(m.chat.id))
    except Exception as e:
        await m.reply(f"❌ Gagal pause.\n`{e}`")


@bot.on_message(filters.command(["resume"]))
async def resume_cmd(_, m: Message):
    s = st(m.chat.id)
    if not s.playing:
        return await m.reply("Belum ada yang diputar.")
    try:
        await call.resume_stream(m.chat.id)
        s.paused = False
        await m.reply("▶️ Resumed.", reply_markup=player_kb(m.chat.id))
    except Exception as e:
        await m.reply(f"❌ Gagal resume.\n`{e}`")


@bot.on_message(filters.command(["skip"]))
async def skip_cmd(_, m: Message):
    s = st(m.chat.id)
    if not s.playing:
        return await m.reply("Belum ada yang diputar.")
    await m.reply("⏭ Skipping...")
    try:
        await play_next(m.chat.id, m.chat.id)
    except Exception as e:
        await m.reply(f"❌ Gagal skip.\n`{e}`")


@bot.on_message(filters.command(["stop"]))
async def stop_cmd(_, m: Message):
    s = st(m.chat.id)
    s.queue.clear()
    s.playing = None
    s.paused = False
    try:
        await call.leave_group_call(m.chat.id)
    except Exception:
        pass
    await m.reply("⏹ Stop. Keluar dari VC.")


@bot.on_message(filters.command(["queue"]))
async def queue_cmd(_, m: Message):
    s = st(m.chat.id)
    if not s.playing and not s.queue:
        return await m.reply("Queue kosong.")
    lines = []
    if s.playing:
        lines.append(f"🎶 Now: **{s.playing.title}**")
    if s.queue:
        lines.append("\n📜 Next:")
        for i, t in enumerate(s.queue[:15], 1):
            lines.append(f"{i}. {t.title}")
        if len(s.queue) > 15:
            lines.append(f"...dan `{len(s.queue)-15}` lagi.")
    await m.reply("\n".join(lines), reply_markup=player_kb(m.chat.id))


@bot.on_callback_query()
async def callbacks(_, q: CallbackQuery):
    try:
        _, action, chat_id_str = q.data.split(":")
        chat_id = int(chat_id_str)
    except Exception:
        return await q.answer("Invalid button.", show_alert=True)

    s = st(chat_id)

    if action == "pause":
        if not s.playing:
            return await q.answer("Belum ada playback.", show_alert=True)
        try:
            if s.paused:
                await call.resume_stream(chat_id)
                s.paused = False
                await q.answer("Resumed")
            else:
                await call.pause_stream(chat_id)
                s.paused = True
                await q.answer("Paused")
            await q.message.edit_reply_markup(player_kb(chat_id))
        except Exception as e:
            await q.answer(f"Gagal: {e}", show_alert=True)

    elif action == "skip":
        if not s.playing:
            return await q.answer("Belum ada playback.", show_alert=True)
        await q.answer("Skipping...")
        try:
            await play_next(chat_id, q.message.chat.id)
        except Exception as e:
            await bot.send_message(q.message.chat.id, f"❌ Gagal skip.\n`{e}`")

    elif action == "stop":
        s.queue.clear()
        s.playing = None
        s.paused = False
        try:
            await call.leave_group_call(chat_id)
        except Exception:
            pass
        await q.answer("Stopped")
        try:
            await q.message.edit_text("⏹ Stop. Keluar dari VC.")
        except Exception:
            pass

    elif action == "queue":
        await q.answer("Queue")
        if not s.playing and not s.queue:
            return await bot.send_message(q.message.chat.id, "Queue kosong.")
        lines = []
        if s.playing:
            lines.append(f"🎶 Now: **{s.playing.title}**")
        if s.queue:
            lines.append("\n📜 Next:")
            for i, t in enumerate(s.queue[:15], 1):
                lines.append(f"{i}. {t.title}")
            if len(s.queue) > 15:
                lines.append(f"...dan `{len(s.queue)-15}` lagi.")
        await bot.send_message(q.message.chat.id, "\n".join(lines))
    else:
        await q.answer("Unknown action.", show_alert=True)


async def main():
    await assistant.start()
    await bot.start()
    await call.start()
    print("MusicBot running...")
    await asyncio.Event().wait()


if __name__ == "__main__":
    asyncio.run(main())
