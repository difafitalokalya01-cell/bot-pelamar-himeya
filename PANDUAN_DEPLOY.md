# 🤖 Bot Pelamar — Panduan Deploy ke Railway

## File yang Ada di Folder Ini

| File | Fungsi |
|------|--------|
| `Procfile` | Memberitahu Railway cara menjalankan semua proses |
| `requirements.txt` | Daftar library Python yang dibutuhkan |
| `railway.json` | Konfigurasi Railway (auto restart jika crash) |
| `telegram_notif.py` | Modul notifikasi Telegram |
| `terapkan_patch.py` | Script otomatis untuk tambah Telegram ke bot |
| `web_monitor.py` | Halaman web status bot (Railway butuh ini) |
| `.gitignore` | Daftar file yang TIDAK boleh diupload ke GitHub |

---

## LANGKAH 1 — Siapkan Folder di Komputer Kamu

Kumpulkan semua file ini dalam SATU folder:

```
📁 bot-pelamar/
  ├── bot_pelamar.py          ← file bot kamu
  ├── bot_pelamar2.py         ← file bot kamu
  ├── credentials.json        ← file Google (JANGAN diupload ke GitHub)
  ├── telegram_notif.py       ← dari paket ini
  ├── terapkan_patch.py       ← dari paket ini
  ├── web_monitor.py          ← dari paket ini
  ├── Procfile                ← dari paket ini
  ├── requirements.txt        ← dari paket ini (GANTI yang lama)
  ├── railway.json            ← dari paket ini
  └── .gitignore              ← dari paket ini
```

---

## LANGKAH 2 — Siapkan Bot Telegram (Gratis, 5 menit)

### A. Buat Bot Telegram
1. Buka Telegram, cari `@BotFather`
2. Ketik `/newbot`
3. Ikuti instruksi (isi nama bot dan username)
4. BotFather akan memberi **TOKEN** — salin, simpan

### B. Dapatkan Chat ID kamu
1. Cari `@userinfobot` di Telegram
2. Klik Start
3. Bot akan balas dengan **ID** kamu — salin angkanya

---

## LANGKAH 3 — Jalankan Patcher (Sekali Saja)

Buka terminal/Command Prompt di folder bot, jalankan:

```
python terapkan_patch.py
```

Script ini akan otomatis menambahkan kode Telegram ke kedua bot.
Backup asli tersimpan sebagai `.bak`.

---

## LANGKAH 4 — Upload ke GitHub

> Jika belum punya akun GitHub: daftar gratis di https://github.com

1. Buat repository baru di GitHub (klik tombol **+** → New repository)
2. Nama bebas, misal: `bot-pelamar-himeya`
3. Set ke **Private** (penting!)
4. Upload semua file kecuali yang ada di `.gitignore`
   - **JANGAN upload `credentials.json`** — ini file sensitif!

---

## LANGKAH 5 — Deploy ke Railway

1. Buka https://railway.app dan login
2. Klik **New Project** → **Deploy from GitHub repo**
3. Pilih repository `bot-pelamar-himeya` yang baru dibuat
4. Railway akan otomatis deteksi dan mulai build

---

## LANGKAH 6 — Set Environment Variables di Railway

Ini WAJIB — tanpa ini bot tidak bisa jalan.

Di Railway → klik project kamu → tab **Variables** → tambahkan:

| Nama Variable | Isi |
|--------------|-----|
| `GOOGLE_CREDENTIALS_JSON` | Salin SELURUH isi file `credentials.json` |
| `SPREADSHEET_ID` | `1I93Kw0QFTj1yAda3nJHLmCUUjM-7bi2E6uTLflp3m2Q` |
| `TELEGRAM_BOT_TOKEN` | Token dari @BotFather |
| `TELEGRAM_CHAT_ID` | ID dari @userinfobot |

> **Cara isi GOOGLE_CREDENTIALS_JSON:**
> Buka `credentials.json` dengan Notepad, pilih semua (Ctrl+A),
> salin (Ctrl+C), lalu tempel ke kolom value di Railway.

---

## LANGKAH 7 — Cek di Railway

Setelah deploy:
- Tab **Deployments**: lihat status build (harus hijau ✓)
- Tab **Logs**: lihat output bot secara real-time
- Cek Telegram kamu — harusnya dapat pesan "Bot Nyala ✅"

---

## ❓ Pertanyaan Umum

**Q: Apakah data sent_ids.json akan hilang saat redeploy?**
A: Ya, file lokal hilang. Ini berarti bot mungkin kirim email ulang ke orang yang sama.
   Solusi jangka panjang: migrasi sent_ids ke Google Sheets (bisa dibantu nanti).

**Q: Bot bisa jalan dua sekaligus di Railway?**
A: Ya! Procfile sudah mengatur itu. Bot 1, Bot 2, dan halaman web jalan bersamaan.

**Q: Berapa biaya Railway?**
A: Ada free tier $5/bulan. Untuk 2 bot + web, biasanya cukup untuk ~500 jam/bulan.
   Pantau usage di Railway → Usage.

**Q: Bagaimana kalau bot crash?**
A: railway.json sudah set `restartPolicyType: ON_FAILURE` — Railway akan restart otomatis.
   Kamu juga dapat notifikasi Telegram.

---

## 🆘 Jika Ada Masalah

Kirim isi **Logs** dari Railway ke yang membantu kamu.
Tab Logs ada di Railway → project → Deployments → klik deployment terbaru.
