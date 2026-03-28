"""
terapkan_patch.py — Jalankan sekali untuk tambah notifikasi Telegram ke kedua bot
==================================================================================
Cara pakai:
  python terapkan_patch.py

Script ini akan memodifikasi bot_pelamar.py dan bot_pelamar2.py secara otomatis.
Backup asli disimpan sebagai bot_pelamar.py.bak dan bot_pelamar2.py.bak
"""

import shutil
import os
import sys

def patch_bot(filepath, bot_label):
    backup = filepath + ".bak"

    if not os.path.exists(filepath):
        print(f"  ❌ File tidak ditemukan: {filepath}")
        return False

    # Baca isi file
    with open(filepath, "r", encoding="utf-8") as f:
        isi = f.read()

    # Cek apakah sudah di-patch sebelumnya
    if "from telegram_notif import" in isi:
        print(f"  ⚠ {filepath} sudah di-patch sebelumnya. Skip.")
        return True

    # Buat backup
    shutil.copy2(filepath, backup)
    print(f"  ✓ Backup disimpan: {backup}")

    # ─── PATCH 1: Import Telegram di bagian atas ───────────────────
    kode_import = f"""
# === NOTIFIKASI TELEGRAM (auto-patch) ===
try:
    from telegram_notif import kirim_telegram, pasang_telegram_ke_bot, kirim_telegram_crash
except ImportError:
    def kirim_telegram(j, p, **kw): pass
    def pasang_telegram_ke_bot(n): pass
    def kirim_telegram_crash(n, e): pass
# =========================================
"""

    # Tempel setelah "import random"
    if "import random" in isi:
        isi = isi.replace("import random", "import random" + kode_import, 1)
        print(f"  ✓ Patch 1 (import Telegram) berhasil")
    else:
        print(f"  ⚠ Patch 1: 'import random' tidak ditemukan, coba tempel di atas")
        isi = kode_import + isi

    # ─── PATCH 2: Tambah telegram di fungsi kirim_notifikasi ───────
    # Cari akhir fungsi kirim_notifikasi, tepat setelah log.warning gagal
    lama = 'log.warning(f"  Gagal kirim notifikasi: {e}")\n'
    baru = (
        'log.warning(f"  Gagal kirim notifikasi: {e}")\n'
        f'    kirim_telegram(judul, pesan, bot_nama="{bot_label}")\n'
    )
    if lama in isi:
        isi = isi.replace(lama, baru, 1)
        print(f"  ✓ Patch 2 (Telegram di kirim_notifikasi) berhasil")
    else:
        print(f"  ⚠ Patch 2: pola tidak ditemukan (mungkin sudah diubah)")

    # ─── PATCH 3: Startup Telegram di main() ────────────────────────
    lama3 = "_bot_start_time[0] = get_wib_now()\n"
    baru3 = (
        "_bot_start_time[0] = get_wib_now()\n"
        f"    pasang_telegram_ke_bot(\"{bot_label}\")\n"
    )
    if lama3 in isi:
        isi = isi.replace(lama3, baru3, 1)
        print(f"  ✓ Patch 3 (startup Telegram) berhasil")
    else:
        print(f"  ⚠ Patch 3: pola tidak ditemukan")

    # ─── PATCH 4: Crash notification ────────────────────────────────
    # Cari baris di except paling bawah
    if "Bot berhenti:" in isi:
        lama4_options = [
            'log.critical(f"Bot berhenti: {e}")\n',
            'log.critical(f"Bot 2 berhenti: {e}")\n',
        ]
        for lama4 in lama4_options:
            if lama4 in isi:
                baru4 = lama4 + f'        kirim_telegram_crash("{bot_label}", str(e))\n'
                isi = isi.replace(lama4, baru4, 1)
                print(f"  ✓ Patch 4 (crash Telegram) berhasil")
                break
    else:
        print(f"  ⚠ Patch 4: pola crash tidak ditemukan")

    # Tulis kembali file
    with open(filepath, "w", encoding="utf-8") as f:
        f.write(isi)

    print(f"  ✅ {filepath} berhasil di-patch!\n")
    return True


if __name__ == "__main__":
    print("=" * 60)
    print("  Patcher Notifikasi Telegram — Bot Pelamar")
    print("=" * 60)
    print()

    print("📝 Memproses bot_pelamar.py ...")
    patch_bot("bot_pelamar.py", "Bot 1 - Himeya")

    print("📝 Memproses bot_pelamar2.py ...")
    patch_bot("bot_pelamar2.py", "Bot 2 - Fashion Me")

    print("=" * 60)
    print("✅ Selesai!")
    print()
    print("Langkah selanjutnya:")
    print("  1. Pastikan telegram_notif.py ada di folder yang sama")
    print("  2. Set environment variable di Railway:")
    print("       TELEGRAM_BOT_TOKEN = <token dari @BotFather>")
    print("       TELEGRAM_CHAT_ID   = <ID dari @userinfobot>")
    print("  3. Deploy ke Railway")
    print("=" * 60)
