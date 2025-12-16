import os
import re
import asyncio
import shutil
import subprocess
from dataclasses import dataclass, field
from typing import Dict, List, Optional, Tuple

from pyrogram import Client, filters
from pyrogram.types import Message

from pytgcalls import PyTgCalls
from pytgcalls.types.input_stream import AudioPiped
from pytgcalls.exceptions import AlreadyJoinedError, NoActiveGroupCall, NotInGroupCallError

# =========================
# ENV (WAJIB)
# =========================
API_ID = int(os.getenv("API_ID", "29655477"))
API_HASH = os.getenv("API_HASH", "2de42d28d36e637c54d5571df5679b7d").strip()

BOT_TOKEN = os.getenv("BOT_TOKEN", "7714224903:AAEI8X6z_A34C5mDlOQcCaTXBkKnp5Q0uTs").strip()

# Assistant account (user account) pakai string session
ASSISTANT_SESSION = os.getenv("ASSISTANT_SESSION", "BQF-4awAOJEHp5bqDsdHGatIW-x-EGmq6moHeaE0QHyVHSWqP2z1TyoL_BvWbKTS4egfzBKbH3wbG9gOhWR350_OodFk5Ya5p1wFxBFjyTr2vPo547zazMa-Z1-Y9r0B-CkGG4iIBDME8GAYXZ0OHOaVv1HyTJ8gNJt3eUo2OQapEckXLmN9t2pIvZh2Af8IeZAts0vvpn2RJkmS93dokPFoGUKJ9LyHQ7E6fbrdGvq7TA-OFt45S-E5uk7coZE-fOVM3q-rK6S838QRXzjq_tKrcyyaRRmYD1xt98KZSb8YxyAapP2OlmQOaga2wvVyqmwRsWpaPSUycbBp85OWQqfOpWQwWwAAAAHn-8vxAA").strip()

# Opsional: batasi siapa yang boleh kontrol
# IS_OWNER_ONLY=1 dan OWNER_IDS="123,456"
IS_OWNER_ONLY = os.getenv("IS_OWNER_ONLY", "5504473114").strip() == "1"
OWNER_IDS = set()
if os.getenv("OWNER_IDS"):
    OWNER_IDS = {int(x.strip()) for x in os.getenv("OWNER_IDS", "5504473114").split(",") if x.strip().isdigit()}

# Folder cache
CACHE_DIR = os.getenv("CACHE_DIR", "cache").strip()
os.makedirs(CACHE_DIR, exist_ok=True)

YTDLP_BIN = shutil.which("yt-dlp") or "yt-dlp"
FFMPEG_BIN = shutil.which("ffmpeg") or "ffmpeg"

YOUTUBE_URL_RE = re.compile(r"(https?://)?(www\.)?(youtube\.com|youtu\.be)/", re.IGNORECASE)

# =========================
# Data structures
# =========================
@dataclass
class Track:
    title: str
    source: str  # URL / query
    file_path: str
    duration: Optional[int] = None

@dataclass
class ChatPlayer:
    queue: List[Track] = field(default_factory=list)
    now_playing: Optional[Track] = None
    playing_task: Optional[asyncio.Task] = None
    lock: asyncio.Lock = field(default_factory=asyncio.Lock)

PLAYERS: Dict[int, ChatPlayer] = {}

# =========================
# Clients
# =========================
if not API_ID or not API_HASH:
    raise RuntimeError("API_ID/API_HASH belum di-set. Set ENV: API_ID, API_HASH.")

if not BOT_TOKEN:
    raise RuntimeError("BOT_TOKEN belum di-set. Set ENV: BOT_TOKEN.")

if not ASSISTANT_SESSION:
    raise RuntimeError("ASSISTANT_SESSION belum di-set. Wajib pakai assistant user session untuk VC streaming.")

bot = Client("musicbot", api_id=API_ID, api_hash=API_HASH, bot_token=BOT_TOKEN)
assistant = Client("assistant", api_id=API_ID, api_hash=API_HASH, session_string=ASSISTANT_SESSION)
calls = PyTgCalls(assistant)

# =========================
# Helpers
# =========================
def is_allowed(m: Message) -> bool:
    if not IS_OWNER_ONLY:
        return True
    u = m.from_user
    return bool(u and u.id in OWNER_IDS)

def ensure_tools():
    if not shutil.which("ffmpeg"):
        raise RuntimeError("ffmpeg tidak ditemukan di PATH. Install ffmpeg dulu.")
    # yt-dlp bisa dari python package atau binary, kita pakai pemanggilan command saja.
    # Kalau command gagal, error akan ditangkap.

def get_player(chat_id: int) -> ChatPlayer:
    if chat_id not in PLAYERS:
        PLAYERS[chat_id] = ChatPlayer()
    return PLAYERS[chat_id]

async def run_cmd(cmd: List[str]) -> Tuple[int, str, str]:
    proc = await asyncio.create_subprocess_exec(
        *cmd,
        stdout=asyncio.subprocess.PIPE,
        stderr=asyncio.subprocess.PIPE,
    )
    out, err = await proc.communicate()
    return proc.returncode, out.decode(errors="ignore"), err.decode(errors="ignore")

async def ytdlp_download_audio(query_or_url: str, chat_id: int) -> Track:
    """
    Download bestaudio ke file .mp3 (via ffmpeg convert) supaya stream stabil.
    """
    # File unik per request
    outtmpl = os.path.join(CACHE_DIR, f"{chat_id}_%(id)s.%(ext)s")
    cmd = [
        YTDLP_BIN,
        "--no-warnings",
        "--geo-bypass",
        "-f", "bestaudio/best",
        "--extract-audio",
        "--audio-format", "mp3",
        "--audio-quality", "0",
        "--no-playlist",
        "-o", outtmpl,
        query_or_url if YOUTUBE_URL_RE.search(query_or_url) else f"ytsearch1:{query_or_url}",
        "--print", "%(title)s",
        "--print", "%(id)s",
    ]
    code, out, err = await run_cmd(cmd)
    if code != 0:
        raise RuntimeError(f"yt-dlp gagal (code={code}): {err.strip() or out.strip()}")

    # Output print: title baris 1, id baris 2 (biasanya)
    lines = [x.strip() for x in out.splitlines() if x.strip()]
    if len(lines) < 2:
        raise RuntimeError("yt-dlp output tidak sesuai (title/id tidak kebaca).")

    title = lines[-2]
    vid = lines[-1]

    # Setelah extract, hasilnya jadi .mp3 dengan template: {chat_id}_{id}.mp3
    file_path = os.path.join(CACHE_DIR, f"{chat_id}_{vid}.mp3")
    if not os.path.exists(file_path):
        # Fallback: cari file yang matching
        for fn in os.listdir(CACHE_DIR):
            if fn.startswith(f"{chat_id}_{vid}.") and fn.endswith(".mp3"):
                file_path = os.path.join(CACHE_DIR, fn)
                break

    if not os.path.exists(file_path):
        raise RuntimeError("File hasil download tidak ditemukan. Cek permission/storage.")

    return Track(title=title, source=query_or_url, file_path=file_path)

async def join_vc(chat_id: int):
    try:
        await calls.join_group_call(chat_id, AudioPiped("silence.mp3"))
    except AlreadyJoinedError:
        return
    except NoActiveGroupCall:
        raise RuntimeError("Belum ada voice chat yang aktif di grup ini. Nyalakan VC dulu.")
    except Exception as e:
        raise RuntimeError(f"Gagal join VC: {e}")

async def change_stream(chat_id: int, file_path: str):
    await calls.change_stream(chat_id, AudioPiped(file_path))

async def leave_vc(chat_id: int):
    try:
        await calls.leave_group_call(chat_id)
    except (NotInGroupCallError,):
        return

async def play_loop(chat_id: int):
    player = get_player(chat_id)
    async with player.lock:
        if player.playing_task and not player.playing_task.done():
            return  # sudah ada loop

        async def _runner():
            while True:
                async with player.lock:
                    if not player.queue:
                        player.now_playing = None
                        break
                    track = player.queue.pop(0)
                    player.now_playing = track

                # Join & stream
                try:
                    # Join dulu kalau belum join
                    try:
                        await calls.join_group_call(chat_id, AudioPiped(track.file_path))
                    except AlreadyJoinedError:
                        await change_stream(chat_id, track.file_path)
                except NoActiveGroupCall:
                    async with player.lock:
                        player.queue.insert(0, track)
                        player.now_playing = None
                    break
                except Exception:
                    # skip track kalau rusak
                    continue

                # Tunggu track selesai: kita pakai durasi "kira-kira" dari ffprobe kalau ada,
                # tapi supaya simpel & tahan banting, kita polling status:
                # Kalau user skip/stop, now_playing akan berubah.
                for _ in range(999999):
                    await asyncio.sleep(1)
                    async with player.lock:
                        if player.now_playing != track:
                            break
                        # kalau queue kosong & tetap track yang sama, lanjut tunggu
                # lanjut loop

            # kalau selesai semua, keluar VC biar gak nangkring
            await leave_vc(chat_id)

        player.playing_task = asyncio.create_task(_runner())

# =========================
# Commands
# =========================
@bot.on_message(filters.command(["start", "help"]))
async def cmd_start(_, m: Message):
    await m.reply(
        "Music Bot v2 siap gas.\n\n"
        "Commands:\n"
        "/play <judul/link>\n"
        "/pause | /resume\n"
        "/skip | /stop\n"
        "/queue | /now\n"
        "/join | /leave\n"
    )

@bot.on_message(filters.command("join"))
async def cmd_join(_, m: Message):
    if not is_allowed(m):
        return await m.reply("Akses ditolak. Ini mode owner-only.")
    try:
        await join_vc(m.chat.id)
        await m.reply("OK, assistant join VC.")
    except Exception as e:
        await m.reply(f"Gagal join: {e}")

@bot.on_message(filters.command("leave"))
async def cmd_leave(_, m: Message):
    if not is_allowed(m):
        return await m.reply("Akses ditolak. Ini mode owner-only.")
    await leave_vc(m.chat.id)
    await m.reply("Keluar dari VC.")

@bot.on_message(filters.command("play"))
async def cmd_play(_, m: Message):
    if not is_allowed(m):
        return await m.reply("Akses ditolak. Ini mode owner-only.")
    if len(m.command) < 2:
        return await m.reply("Pakai: /play <judul atau link youtube>")
    query = m.text.split(None, 1)[1].strip()

    msg = await m.reply("Download dulu ya, jangan panik...")
    try:
        track = await ytdlp_download_audio(query, m.chat.id)
    except Exception as e:
        return await msg.edit(f"Download gagal: {e}")

    player = get_player(m.chat.id)
    async with player.lock:
        player.queue.append(track)
        qpos = len(player.queue)

    await msg.edit(f"Masuk antrian #{qpos}: **{track.title}**")
    await play_loop(m.chat.id)

@bot.on_message(filters.command("pause"))
async def cmd_pause(_, m: Message):
    if not is_allowed(m):
        return await m.reply("Akses ditolak. Ini mode owner-only.")
    try:
        await calls.pause_stream(m.chat.id)
        await m.reply("Paused.")
    except Exception as e:
        await m.reply(f"Gagal pause: {e}")

@bot.on_message(filters.command("resume"))
async def cmd_resume(_, m: Message):
    if not is_allowed(m):
        return await m.reply("Akses ditolak. Ini mode owner-only.")
    try:
        await calls.resume_stream(m.chat.id)
        await m.reply("Resumed.")
    except Exception as e:
        await m.reply(f"Gagal resume: {e}")

@bot.on_message(filters.command("skip"))
async def cmd_skip(_, m: Message):
    if not is_allowed(m):
        return await m.reply("Akses ditolak. Ini mode owner-only.")
    player = get_player(m.chat.id)
    async with player.lock:
        if not player.queue:
            player.now_playing = None
            await leave_vc(m.chat.id)
            return await m.reply("Queue kosong. Keluar VC.")
        # set now_playing beda supaya loop lanjut
        player.now_playing = None
    await m.reply("Skipped. Lanjut lagu berikutnya.")
    await play_loop(m.chat.id)

@bot.on_message(filters.command("stop"))
async def cmd_stop(_, m: Message):
    if not is_allowed(m):
        return await m.reply("Akses ditolak. Ini mode owner-only.")
    player = get_player(m.chat.id)
    async with player.lock:
        player.queue.clear()
        player.now_playing = None
    await leave_vc(m.chat.id)
    await m.reply("Stopped. Queue dibersihin, keluar VC.")

@bot.on_message(filters.command("queue"))
async def cmd_queue(_, m: Message):
    player = get_player(m.chat.id)
    async with player.lock:
        if not player.queue:
            return await m.reply("Queue kosong.")
        txt = "\n".join([f"{i+1}. {t.title}" for i, t in enumerate(player.queue[:20])])
    await m.reply(f"Queue (top 20):\n{txt}")

@bot.on_message(filters.command("now"))
async def cmd_now(_, m: Message):
    player = get_player(m.chat.id)
    async with player.lock:
        if not player.now_playing:
            return await m.reply("Lagi gak muter apa-apa.")
        t = player.now_playing
    await m.reply(f"Now Playing: **{t.title}**")

# =========================
# Main
# =========================
async def main():
    ensure_tools()

    # Buat file silent dummy kalau join butuh stream awal
    silent = "silence.mp3"
    if not os.path.exists(silent):
        # generate 1 detik silent mp3
        subprocess.run(
            [FFMPEG_BIN, "-f", "lavfi", "-i", "anullsrc=r=48000:cl=stereo", "-t", "1", "-q:a", "9", "-acodec", "libmp3lame", silent],
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
            check=False,
        )

    await assistant.start()
    await calls.start()
    await bot.start()
    print("Music Bot v2: ON")

    await asyncio.Event().wait()

if __name__ == "__main__":
    asyncio.run(main())
