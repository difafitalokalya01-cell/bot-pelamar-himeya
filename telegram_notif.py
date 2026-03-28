"""
telegram_notif.py — Modul notifikasi Telegram untuk Bot Pelamar
================================================================
Cara pakai:
  1. Letakkan file ini di folder yang sama dengan bot_pelamar.py
  2. Di bot_pelamar.py dan bot_pelamar2.py, tambahkan di bagian atas:
       from telegram_notif import kirim_telegram, pasang_telegram_ke_bot
  3. Panggil pasang_telegram_ke_bot() di awal fungsi main()

Environment variables yang dibutuhkan (set di Railway):
  TELEGRAM_BOT_TOKEN  = token dari @BotFather
  TELEGRAM_CHAT_ID    = chat ID kamu (dapatkan dari @userinfobot)
"""

import os
import urllib.request
import urllib.error
import json
import time
import logging

log = logging.getLogger(__name__)

# ─── Ambil dari environment variable Railway ───────────────────
TELEGRAM_TOKEN   = os.environ.get("TELEGRAM_BOT_TOKEN", "")
TELEGRAM_CHAT_ID = os.environ.get("TELEGRAM_CHAT_ID", "")

# Throttle: jangan kirim notifikasi sama lebih dari 1x per jam
_tg_notif_cache = {}

def kirim_telegram(judul: str, pesan: str, bot_nama: str = "Bot Pelamar") -> bool:
    """
    Kirim pesan notifikasi ke Telegram.
    Return True jika berhasil, False jika gagal atau tidak dikonfigurasi.
    """
    if not TELEGRAM_TOKEN or not TELEGRAM_CHAT_ID:
        return False  # Belum dikonfigurasi, diam saja

    # Throttle: skip jika judul sama dikirim < 1 jam lalu
    now = time.time()
    if judul in _tg_notif_cache and now - _tg_notif_cache[judul] < 3600:
        return False
    _tg_notif_cache[judul] = now

    teks = (
        f"⚠️ *{bot_nama} — {judul}*\n"
        f"─────────────────────\n"
        f"{pesan}\n"
        f"─────────────────────\n"
        f"🕐 {time.strftime('%Y-%m-%d %H:%M:%S')}"
    )

    # Potong jika terlalu panjang (Telegram max 4096 karakter)
    teks = teks[:4000]

    url     = f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendMessage"
    payload = json.dumps({
        "chat_id":    TELEGRAM_CHAT_ID,
        "text":       teks,
        "parse_mode": "Markdown"
    }).encode("utf-8")

    req = urllib.request.Request(
        url, data=payload,
        headers={"Content-Type": "application/json"},
        method="POST"
    )
    try:
        with urllib.request.urlopen(req, timeout=10) as resp:
            result = json.loads(resp.read().decode("utf-8"))
            if result.get("ok"):
                log.info(f"  📲 Telegram terkirim: {judul}")
                return True
            else:
                log.warning(f"  Telegram gagal: {result}")
                return False
    except urllib.error.HTTPError as e:
        log.warning(f"  Telegram HTTP error {e.code}: {e.read().decode()[:200]}")
    except Exception as e:
        log.warning(f"  Telegram error: {e}")
    return False


def kirim_telegram_startup(bot_nama: str = "Bot Pelamar"):
    """Kirim notifikasi saat bot pertama kali nyala."""
    kirim_telegram(
        judul="✅ Bot Nyala",
        pesan=f"*{bot_nama}* berhasil dijalankan di Railway.\nBot mulai bekerja sekarang.",
        bot_nama=bot_nama
    )


def kirim_telegram_crash(bot_nama: str, error: str):
    """Kirim notifikasi saat bot crash."""
    # Bypass throttle untuk crash
    _tg_notif_cache.pop(f"💥 Bot Crash", None)
    kirim_telegram(
        judul="💥 Bot Crash / Berhenti",
        pesan=f"*{bot_nama}* berhenti karena error:\n`{error[:300]}`\n\nRailway akan mencoba restart otomatis.",
        bot_nama=bot_nama
    )


def pasang_telegram_ke_bot(bot_nama: str = "Bot Pelamar"):
    """
    Panggil fungsi ini di awal main() setiap bot.
    Ini akan:
    1. Kirim notifikasi startup ke Telegram
    2. Menampilkan status konfigurasi di log
    """
    if TELEGRAM_TOKEN and TELEGRAM_CHAT_ID:
        log.info(f"  📲 Telegram aktif. Chat ID: {TELEGRAM_CHAT_ID}")
        kirim_telegram_startup(bot_nama)
    else:
        log.warning("  ⚠ Telegram BELUM dikonfigurasi.")
        log.warning("    Set env variable: TELEGRAM_BOT_TOKEN dan TELEGRAM_CHAT_ID")
        log.warning("    di Railway → Settings → Variables")
