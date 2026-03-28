"""
╔══════════════════════════════════════════════════════════════════════╗
║   BOT PELAMAR 2 — Himeya Agency v1.0                                ║
║                                                                      ║
║   ARSITEKTUR:                                                        ║
║   Sheet "Input Manual2"  = email yang diinput manual (langsung kirim)║
║   Sheet "Konfigurasi2"   = semua pengaturan bot 2                   ║
║   Sheet "Log Pengiriman2"= rekam jejak semua pengiriman bot 2       ║
║   Sheet "Template Email2"= template email bot 2                     ║
║   Sheet "Daftar HR2"     = daftar HR untuk bot 2                    ║
║                                                                      ║
║   ALUR PER SIKLUS (interval dari Konfigurasi2):                     ║
║     1. Baca Konfigurasi2 dari sheet                                  ║
║     2. Kirim langsung dari Input Manual2 (kolom B kosong)           ║
║     3. Tunggu sesuai "Interval Siklus (menit)", ulang               ║
║                                                                      ║
║   STRUKTUR KOLOM "Input Manual2":                                   ║
║     A=Email  B=Status  C=Nama  D=Posisi  E=Platform                 ║
║     F=Nama HR  G=No WA HR                                           ║
║     B kosong = belum dikirim → bot kirim otomatis                   ║
║     B="Terkirim ✓" = sudah dikirim                                  ║
║                                                                      ║
║   PERBEDAAN DARI BOT 1:                                             ║
║     - Tidak ada fungsi monitor inbox (IMAP)                         ║
║     - Tidak ada Data Pelamar2 / Data Bersih2                        ║
║     - Interval siklus bisa diset dari sheet Konfigurasi2            ║
║     - File lokal semua pakai suffix "2"                             ║
╚══════════════════════════════════════════════════════════════════════╝
"""

import smtplib
import email
import email.utils
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
import gspread
from google.oauth2.service_account import Credentials
import time
import logging
import logging.handlers
import json
import re
import urllib.request
import urllib.error
import os
import socket
import sys
import shutil
from datetime import datetime, timedelta
import random

# ═══════════════════════════════════════════════════════════════
# KONFIGURASI TETAP
# ═══════════════════════════════════════════════════════════════

SHEETS_CONFIG = {
    "credentials_file": os.environ.get("GOOGLE_CREDENTIALS_FILE", "credentials.json"),
    "spreadsheet_id":   os.environ.get("SPREADSHEET_ID", "1I93Kw0QFTj1yAda3nJHLmCUUjM-7bi2E6uTLflp3m2Q"),
    "sheet_manual":     "Input Manual2",
    "sheet_config":     "Konfigurasi2",
    "sheet_log":        "Log Pengiriman2",
    "sheet_template":   "Template Email2",
    "sheet_hr":         "Daftar HR2",
    "sheet_perusahaan": "Daftar Perusahaan",   # ← shared dengan Bot 1
}

GEMINI_API_KEYS = [
    "AIzaSyCd1f39mOOvrXWTcnU__niVKItu_fHLqkY",
    "AIzaSyCR9vEb3MISk3qgkGUYhzNzwnkao7eLq2Q",
    "AIzaSyAiiZ8fsWVAp2FdwlwlccrLpi0v2wujBR4",
    "AIzaSyA_NQazILwDtge2FoW-skweHpn3tkfIqxg",
    "AIzaSyA0Tcn9p4FIYZmKLDnmMkIg6ZBLeubmD3Q",
    "AIzaSyBJ9xKqhcdstw_dhzQCfbiu6ilG_ZVxgRc",
    "AIzaSyDOyf2NV4B2c3sQpdwfocxbNmIt9kmFy-A",
    "AIzaSyD5_GpyfaAtrJxtjhqN5ccIMOfGvtr65Zs",
    "AIzaSyD4sBuOse_ZeOn7MhEfq1A9o3Psi7YiJaw",
    "AIzaSyAr5tWEaxJfYGLq6_LRMJKee6CKBvb0AaI",
    "AIzaSyCBSC5tn4VZJepPziACvhVXirZdi_kZV2k",
    "AIzaSyAMkXgLe_HsvJVXFdTQiUfMfRAHlovuzmg",
    "AIzaSyDSoJIn9Pv890GGM4MafLuJhb5pmLClRso",
    "AIzaSyAQGBZBMPUQytjbTbD9t41dp9_cnwQtZpA",
    "AIzaSyCbjgcHrjmA0zxz7XYGDHysZTh11v0rTJw",
    "AIzaSyDYJvzrY00lWNdgVn99pQOcki1xnGdZA0A",
    "AIzaSyDXiZi3UuRP7k-FFSnZQF7qCsm9vQT2gsQ",
    "AIzaSyDO7JRY8_iNjEZx30qj2booLNDMPIDI59Y",
]
GEMINI_BASE_URL = "https://generativelanguage.googleapis.com/v1beta/models/gemini-2.0-flash:generateContent?key="

# Parameter operasional tetap
DEFAULT_INTERVAL_MENIT  = 5      # fallback jika tidak ada di sheet
MAX_SHEET_RETRIES       = 5
GEMINI_TIMEOUT          = 25
MAX_FIELD_LENGTH        = 200
DELAY_BETWEEN_EMAILS    = 5
MAX_SENT_IDS_MONTHS     = 12
RESTART_INTERVAL_HOURS  = 24

# Kolom Input Manual (1-based)
# A=Email  B=Status  C=Nama  D=Posisi  E=Platform  F=Nama HR  G=No WA HR  H=Nama Perusahaan
MAN_EMAIL      = 1
MAN_STATUS     = 2
MAN_NAMA       = 3
MAN_POSISI     = 4
MAN_PLATFORM   = 5
MAN_NAMA_HR    = 6
MAN_NO_WA      = 7
MAN_PERUSAHAAN = 8   # ← BARU: nama perusahaan pengirim email

# Kolom Log Pengiriman (1-based)
LOG_TIMESTAMP   = 1
LOG_EMAIL       = 2
LOG_NAMA        = 3
LOG_POSISI      = 4
LOG_PLATFORM    = 5
LOG_NAMA_HR     = 6
LOG_NO_WA       = 7
LOG_PENGIRIM    = 8
LOG_PERUSAHAAN  = 9
LOG_KE          = 10
LOG_STATUS      = 11

# File lokal — semua pakai suffix "2" agar tidak bentrok dengan Bot 1
SENT_IDS_FILE          = "sent_ids2.json"
SENT_IDS_BACKUP_FILE   = "sent_ids2.backup.json"
CONFIG_CACHE_FILE      = "config_cache2.json"
STATS_FILE             = "daily_stats2.json"
UPDATE_FLAG_FILE       = "update.flag"

# ═══════════════════════════════════════════════════════════════
# LOGGING
# ═══════════════════════════════════════════════════════════════

log_handler = logging.handlers.RotatingFileHandler(
    "bot_pelamar2.log", maxBytes=5*1024*1024, backupCount=3, encoding="utf-8"
)
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
    handlers=[log_handler, logging.StreamHandler()]
)
log = logging.getLogger(__name__)

# ═══════════════════════════════════════════════════════════════
# STATE GLOBAL
# ═══════════════════════════════════════════════════════════════

_sheet_state      = {"config": None, "log": None, "manual": None,
                     "template": None, "hr": None, "state": None,
                     "perusahaan": None, "_sh": None, "last_connected": 0}
_gemini_key_index = [0]
_gemini_limited   = set()
_total_undangan   = [0]
_notif_terakhir   = {}
_config_cache     = {}
_bot_start_time   = [None]
_kirim_hari_ini   = [0]
_notif_harian_terkirim = [None]

SHEET_REFRESH_MINUTES = 45

# ═══════════════════════════════════════════════════════════════
# LOCK FILE — timestamp-based, aman untuk Railway
# ═══════════════════════════════════════════════════════════════

_lock_path       = os.path.join(os.path.dirname(os.path.abspath(__file__)), "bot2.lock")
LOCK_STALE_MENIT = 10

def buat_lock():
    if os.path.exists(_lock_path):
        try:
            data     = json.loads(open(_lock_path).read())
            pid      = data.get("pid", "?")
            ts       = data.get("ts", 0)
            umur_mnt = (time.time() - ts) / 60
            if umur_mnt > LOCK_STALE_MENIT:
                log.warning(f"⚠ Lock lama ditemukan (PID {pid}, {umur_mnt:.1f} menit). Dianggap stale, dihapus.")
                os.remove(_lock_path)
            else:
                log.warning(f"⚠ Bot 2 lain mungkin masih jalan (PID {pid}, {umur_mnt:.1f} menit lalu).")
                log.warning("  Melanjutkan dalam 10 detik...")
                time.sleep(10)
        except:
            os.remove(_lock_path)
    with open(_lock_path, "w") as f:
        json.dump({"pid": os.getpid(), "ts": time.time()}, f)
    import atexit
    atexit.register(hapus_lock)

def hapus_lock():
    if os.path.exists(_lock_path):
        try: os.remove(_lock_path)
        except: pass

def perbarui_lock():
    try:
        with open(_lock_path, "w") as f:
            json.dump({"pid": os.getpid(), "ts": time.time()}, f)
    except: pass

# ═══════════════════════════════════════════════════════════════
# GEMINI AI
# ═══════════════════════════════════════════════════════════════

def gemini_get_url():
    keys_ok = [k for k in GEMINI_API_KEYS if k not in _gemini_limited]
    if not keys_ok:
        _gemini_limited.clear()
        keys_ok = GEMINI_API_KEYS
    key = keys_ok[_gemini_key_index[0] % len(keys_ok)]
    return GEMINI_BASE_URL + key, key

def gemini_limit(key):
    _gemini_limited.add(key)
    _gemini_key_index[0] += 1
    sisa = len([k for k in GEMINI_API_KEYS if k not in _gemini_limited])
    log.warning(f"  Gemini key limit. {sisa} key tersisa.")

def gemini_call(prompt, is_json=False):
    payload = json.dumps({"contents": [{"parts": [{"text": prompt}]}]}).encode("utf-8")
    for _ in range(len(GEMINI_API_KEYS) * 2):
        url, key = gemini_get_url()
        req = urllib.request.Request(
            url, data=payload,
            headers={"Content-Type": "application/json"}, method="POST"
        )
        try:
            with urllib.request.urlopen(req, timeout=GEMINI_TIMEOUT) as resp:
                result = json.loads(resp.read().decode("utf-8"))
                text   = result["candidates"][0]["content"]["parts"][0]["text"].strip()
                if is_json:
                    text = re.sub(r"```json|```", "", text).strip()
                    return json.loads(text)
                return text
        except socket.timeout:
            time.sleep(5); continue
        except urllib.error.HTTPError as e:
            if e.code == 429:
                gemini_limit(key)
                if not [k for k in GEMINI_API_KEYS if k not in _gemini_limited]:
                    time.sleep(60); _gemini_limited.clear()
                continue
            elif e.code in (401, 403):
                gemini_limit(key); continue
            break
        except: break
    return None

# ═══════════════════════════════════════════════════════════════
# PENYIMPANAN STATE KE GOOGLE SHEETS (bukan file lokal)
# Berbagi sheet "Bot State" dengan Bot 1 — key pakai suffix "2"
# ═══════════════════════════════════════════════════════════════

_state_cache = {}

def state_load(ws_state, key, default):
    if key in _state_cache:
        return _state_cache[key]
    try:
        data = ws_state.get_all_values()
        for row in data[1:]:
            if len(row) >= 2 and row[0].strip() == key:
                val = json.loads(row[1])
                _state_cache[key] = val
                return val
    except Exception as e:
        log.warning(f"  Gagal baca state '{key}': {e}")
    _state_cache[key] = default
    return default

def state_save(ws_state, key, value):
    _state_cache[key] = value
    try:
        data = ws_state.get_all_values()
        for i, row in enumerate(data):
            if len(row) >= 1 and row[0].strip() == key:
                from functools import partial
                sheet_retry(ws_state.update_cell, i + 1, 2, json.dumps(value, ensure_ascii=False))
                return
        sheet_retry(ws_state.append_row, [key, json.dumps(value, ensure_ascii=False)])
    except Exception as e:
        log.warning(f"  Gagal tulis state '{key}': {e}")
        try:
            with open(f"state_{key}.json", "w", encoding="utf-8") as f:
                json.dump(value, f, ensure_ascii=False)
        except: pass

def load_sent_ids(ws_state):
    data = state_load(ws_state, "sent_ids2", {})
    sent = data if isinstance(data, dict) else {}
    batas = datetime.now() - timedelta(days=MAX_SENT_IDS_MONTHS * 30)
    sent_bersih = {}
    for em, info in sent.items():
        if isinstance(info, dict):
            try:
                tgl = datetime.strptime(info.get("terakhir", ""), "%Y-%m-%d %H:%M:%S")
                if tgl > batas:
                    sent_bersih[em] = info
            except: sent_bersih[em] = info
        else:
            sent_bersih[em] = {"jumlah": info, "terakhir": ""}
    log.info(f"  Sent IDs Bot 2: {len(sent_bersih)} undangan terkirim.")
    return sent_bersih

def save_sent_ids(ws_state, sent):
    state_save(ws_state, "sent_ids2", sent)

def load_stats(ws_state=None):
    today = get_wib_now().strftime("%Y-%m-%d")
    if ws_state:
        data = state_load(ws_state, "daily_stats2", {})
    else:
        try:
            with open(STATS_FILE, "r", encoding="utf-8") as f:
                data = json.load(f)
        except: data = {}
    if data.get("date") == today: return data
    return {"date": today, "undangan": 0, "gagal": 0}

def save_stats(ws_state, stats):
    state_save(ws_state, "daily_stats2", stats)
    try:
        with open(STATS_FILE, "w", encoding="utf-8") as f:
            json.dump(stats, f, ensure_ascii=False, indent=2)
    except: pass

# ═══════════════════════════════════════════════════════════════
# TIMEZONE WIB
# ═══════════════════════════════════════════════════════════════

def get_wib_now():
    from datetime import timezone
    utc_now = datetime.now(timezone.utc)
    wib     = timezone(timedelta(hours=7))
    return utc_now.astimezone(wib).replace(tzinfo=None)

def format_wib(dt=None):
    if dt is None: dt = get_wib_now()
    return dt.strftime("%Y-%m-%d %H:%M:%S")

# ═══════════════════════════════════════════════════════════════
# GOOGLE SHEETS
# ═══════════════════════════════════════════════════════════════

def connect_sheets(force=False):
    now     = time.time()
    elapsed = (now - _sheet_state["last_connected"]) / 60
    if not force and _sheet_state["config"] and elapsed < SHEET_REFRESH_MINUTES:
        return (_sheet_state["config"], _sheet_state["log"],
                _sheet_state["manual"], _sheet_state["template"],
                _sheet_state["hr"], _sheet_state["state"],
                _sheet_state.get("perusahaan"))

    scopes = ["https://www.googleapis.com/auth/spreadsheets",
              "https://www.googleapis.com/auth/drive"]

    creds_json = os.environ.get("GOOGLE_CREDENTIALS_JSON", "")
    if creds_json:
        from google.oauth2.service_account import Credentials as _C
        creds = _C.from_service_account_info(json.loads(creds_json), scopes=scopes)
    else:
        cred_file = SHEETS_CONFIG["credentials_file"]
        if not os.path.exists(cred_file):
            raise FileNotFoundError(f"File '{cred_file}' tidak ditemukan!")
        creds = Credentials.from_service_account_file(cred_file, scopes=scopes)

    gc = gspread.authorize(creds)
    sh = gc.open_by_key(SHEETS_CONFIG["spreadsheet_id"])

    # Konfigurasi2
    try:
        ws_config = sh.worksheet(SHEETS_CONFIG["sheet_config"])
    except gspread.WorksheetNotFound:
        ws_config = sh.add_worksheet(SHEETS_CONFIG["sheet_config"], 50, 2)
        buat_sheet_konfigurasi(ws_config)

    # Log Pengiriman2
    try:
        ws_log = sh.worksheet(SHEETS_CONFIG["sheet_log"])
    except gspread.WorksheetNotFound:
        ws_log = sh.add_worksheet(SHEETS_CONFIG["sheet_log"], 5000, 11)
        ws_log.insert_row(
            ["Timestamp","Email Tujuan","Nama","Posisi","Platform",
             "Nama HR","No WA","Email Pengirim","Nama Perusahaan","Ke-","Status"], 1)

    # Input Manual2
    try:
        ws_manual = sh.worksheet(SHEETS_CONFIG["sheet_manual"])
    except gspread.WorksheetNotFound:
        ws_manual = sh.add_worksheet(SHEETS_CONFIG["sheet_manual"], 10000, 8)
        ws_manual.insert_row(
            ["Email","Status","Nama","Posisi","Platform","Nama HR","No WA HR","Nama Perusahaan"], 1)

    # Template Email2
    try:
        ws_template = sh.worksheet(SHEETS_CONFIG["sheet_template"])
    except gspread.WorksheetNotFound:
        ws_template = sh.add_worksheet(SHEETS_CONFIG["sheet_template"], 30, 2)
        buat_sheet_template(ws_template)

    # Daftar HR2
    try:
        ws_hr = sh.worksheet(SHEETS_CONFIG["sheet_hr"])
    except gspread.WorksheetNotFound:
        ws_hr = sh.add_worksheet(SHEETS_CONFIG["sheet_hr"], 50, 5)
        buat_sheet_hr(ws_hr)

    _sheet_state.update({
        "config": ws_config, "log": ws_log,
        "manual": ws_manual, "template": ws_template,
        "hr": ws_hr,
        "_sh": sh,
        "last_connected": now
    })
    if force: log.info("  ✓ Koneksi Google Sheets diperbarui.")

    # Bot State — berbagi sheet yang sama dengan Bot 1
    try:
        ws_state = sh.worksheet("Bot State")
    except gspread.WorksheetNotFound:
        ws_state = sh.add_worksheet("Bot State", 20, 2)
        ws_state.update("A1", [["Kunci", "Nilai"]])
        log.info("  ✓ Sheet 'Bot State' dibuat.")
    _sheet_state["state"] = ws_state

    # Daftar Perusahaan — sheet bersama dengan Bot 1
    try:
        ws_perusahaan = sh.worksheet(SHEETS_CONFIG["sheet_perusahaan"])
    except gspread.WorksheetNotFound:
        ws_perusahaan = None
        log.warning("  ⚠ Sheet 'Daftar Perusahaan' belum ada. Jalankan Bot 1 dulu untuk membuatnya.")
    _sheet_state["perusahaan"] = ws_perusahaan

    return ws_config, ws_log, ws_manual, ws_template, ws_hr, ws_state, ws_perusahaan

def buat_sheet_konfigurasi(ws):
    rows = [
        ["— Identitas Perusahaan —", ""],
        ["Nama Perusahaan", "Himeya Agency"],
        ["Divisi", "Human Resources"],
        ["Email Resmi", "talent2@himeyaagency.com"],
        ["", ""],
        ["— Email Pengirim (SMTP) —", ""],
        ["Email Pengirim", "talent2@himeyaagency.com"],
        ["Password Email", ""],
        ["SMTP Host", "smtp.hostinger.com"],
        ["SMTP Port", "465"],
        ["", ""],
        ["— Notifikasi —", ""],
        ["Email Notifikasi", "propro04040404@gmail.com"],
        ["", ""],
        ["— Default HR —", ""],
        ["Nama HR Default", ""],
        ["No WA Default", ""],
        ["Posisi Default", "Admin"],
        ["Platform Default", "Instagram"],
        ["", ""],
        ["— Rate Limiting —", ""],
        ["Batas Kirim Per Hari", "1000"],
        ["Batas Kirim Per Jam", "200"],
        ["Batas Kirim Per Siklus", "100"],
        ["Jeda Min Antar Email (detik)", "5"],
        ["Jeda Max Antar Email (detik)", "10"],
        ["Jam Mulai Kirim", "05:00"],
        ["Jam Selesai Kirim", "19:00"],
        ["Interval Siklus (menit)", "5"],
        ["", ""],
        ["— Status Bot (diisi otomatis) —", ""],
        ["Heartbeat", ""],
        ["Total Kirim Hari Ini", "0"],
        ["Tanggal Counter", ""],
    ]
    ws.update("A1", rows)
    log.info("  ✓ Sheet Konfigurasi2 dibuat dengan nilai default.")

def buat_sheet_template(ws):
    rows = [
        ["— PETUNJUK —", "Ubah nilai di kolom B. Jangan ubah kolom A."],
        ["", "Variabel: {nama} {posisi} {platform} {nama_hr} {no_wa} {nama_perusahaan} {divisi} {email_resmi} {sapaan}"],
        ["", ""],
        ["— Subject Email —", ""],
        ["Subject", "Undangan Wawancara {posisi} (Segera) — {nama_perusahaan}"],
        ["", ""],
        ["— Isi Email —", ""],
        ["Salam Pembuka", "{sapaan} Kepada {nama},"],
        ["Paragraf 1", "Terima kasih telah melamar posisi {posisi} di {nama_perusahaan} melalui platform {platform}."],
        ["Paragraf 2", "Setelah meninjau lamaran Anda, kami dengan senang hati mengundang Anda untuk mengikuti wawancara online sebagai tahap seleksi berikutnya."],
        ["Paragraf 3", "Silakan konfirmasi kehadiran Anda dengan menghubungi tim kami:"],
        ["Teks Tombol WA", "💬 Konfirmasi via WhatsApp"],
        ["Paragraf 4", "Sertakan saat konfirmasi:\n• CV terbaru (format PDF)\n• Tangkapan layar email undangan ini\n• Data diri singkat (Nama Lengkap & Alamat Email)"],
        ["Paragraf 5", "Apabila Anda memiliki pertanyaan lebih lanjut, jangan ragu untuk menghubungi kami melalui alamat email di bawah ini."],
        ["Salam Penutup", "Hormat kami,"],
        ["", ""],
        ["— Warna & Tampilan —", ""],
        ["Warna Header Atas", "#7b4397"],
        ["Warna Header Bawah", "#dc2430"],
        ["Warna Aksen", "#7b4397"],
        ["Warna Tombol WA", "#25d366"],
    ]
    ws.update("A1", rows)
    ws.format("A1:B1", {"textFormat": {"bold": True}})
    ws.format("A:A", {"textFormat": {"bold": True}})
    log.info("  ✓ Sheet Template Email2 dibuat.")

def buat_sheet_hr(ws):
    rows = [
        ["Nama HR", "No WA", "Kuota Per Hari", "Terkirim Hari Ini", "Status"],
        ["Contoh HR 1", "08xx-xxxx-xxxx", "50", "0", "Aktif"],
        ["Contoh HR 2", "08xx-xxxx-xxxx", "50", "0", "Aktif"],
    ]
    ws.update("A1", rows)
    ws.format("A1:E1", {"textFormat": {"bold": True},
                        "backgroundColor": {"red": 0.28, "green": 0.08, "blue": 0.55}})
    log.info("  ✓ Sheet Daftar HR2 dibuat.")

def baca_template(ws_template):
    tmpl = {}
    try:
        data = ws_template.get_all_values()
        for row in data:
            if len(row) >= 2 and row[0].strip() and not row[0].startswith("—"):
                tmpl[row[0].strip()] = row[1].strip()
    except Exception as e:
        log.warning(f"  Gagal baca Template Email2: {e}")
    return tmpl

def baca_daftar_hr(ws_hr):
    daftar = []
    try:
        data = ws_hr.get_all_values()
        if len(data) <= 1:
            return daftar
        for i, row in enumerate(data[1:], start=2):
            if len(row) < 5:
                continue
            nama     = str(row[0]).strip()
            no_wa    = str(row[1]).strip()
            kuota    = int(row[2]) if str(row[2]).strip().isdigit() else 0
            terkirim = int(row[3]) if str(row[3]).strip().isdigit() else 0
            status   = str(row[4]).strip().lower()
            if not nama or not no_wa or status != "aktif":
                continue
            daftar.append({
                "nama": nama, "no_wa": no_wa,
                "kuota": kuota, "terkirim": terkirim,
                "sisa": max(0, kuota - terkirim),
                "baris": i
            })
    except Exception as e:
        log.warning(f"  Gagal baca Daftar HR2: {e}")
    return daftar

def ambil_hr(ws_hr):
    daftar = baca_daftar_hr(ws_hr)
    for hr in daftar:
        if hr["sisa"] > 0:
            return hr
    return None

def tambah_terkirim_hr(ws_hr, baris_hr):
    try:
        val_lama = ws_hr.cell(baris_hr, 4).value
        val_baru = int(val_lama) + 1 if str(val_lama).strip().isdigit() else 1
        sheet_retry(ws_hr.update_cell, baris_hr, 4, str(val_baru))
    except Exception as e:
        log.warning(f"  Gagal update terkirim HR2: {e}")

def sheet_retry(func, *args, **kwargs):
    for attempt in range(MAX_SHEET_RETRIES):
        try:
            return func(*args, **kwargs)
        except gspread.exceptions.APIError as e:
            wait = 20 * (attempt + 1)
            log.warning(f"  Sheet API error, tunggu {wait}s: {e}")
            connect_sheets(force=True)
            time.sleep(wait)
        except Exception as e:
            log.error(f"  Sheet error: {e}")
            time.sleep(10)
            break
    return None

# ═══════════════════════════════════════════════════════════════
# BACA KONFIGURASI DARI SHEET
# ═══════════════════════════════════════════════════════════════

def baca_konfigurasi(ws_config):
    global _config_cache
    try:
        data = ws_config.get_all_values()
        cfg  = {}
        for row in data:
            if len(row) >= 2 and row[0].strip() and not row[0].startswith("—"):
                cfg[row[0].strip()] = row[1].strip()

        # Validasi nilai wajib — Bot 2 tidak butuh IMAP
        wajib = ["Email Pengirim", "Password Email", "SMTP Host", "SMTP Port",
                 "Nama Perusahaan"]
        kosong = [k for k in wajib if not cfg.get(k)]
        if kosong:
            log.error(f"  ❌ Konfigurasi2 tidak lengkap: {kosong}")
            if _config_cache:
                log.warning("  ⚠ Menggunakan konfigurasi cache terakhir.")
                return _config_cache
            return None

        _config_cache = cfg
        try:
            with open(CONFIG_CACHE_FILE, "w", encoding="utf-8") as f:
                json.dump(cfg, f, ensure_ascii=False, indent=2)
        except: pass

        return cfg

    except Exception as e:
        log.error(f"  Gagal baca Konfigurasi2: {e}")
        if os.path.exists(CONFIG_CACHE_FILE):
            try:
                with open(CONFIG_CACHE_FILE, "r", encoding="utf-8") as f:
                    _config_cache = json.load(f)
                log.warning("  ⚠ Menggunakan konfigurasi cache lokal.")
                return _config_cache
            except: pass
        return None

def ambil_interval_siklus(cfg):
    """Baca interval siklus dari konfigurasi, default 5 menit."""
    try:
        return max(1, int(cfg.get("Interval Siklus (menit)", str(DEFAULT_INTERVAL_MENIT))))
    except:
        return DEFAULT_INTERVAL_MENIT

def interruptible_sleep(detik):
    """Sleep yang bisa langsung dihentikan dengan Ctrl+C (dibagi per 1 detik)."""
    for _ in range(int(detik)):
        time.sleep(1)

def validasi_jam_kirim(cfg):
    try:
        now       = get_wib_now()
        jam_mulai = datetime.strptime(cfg.get("Jam Mulai Kirim", "05:00"), "%H:%M").time()
        jam_akhir = datetime.strptime(cfg.get("Jam Selesai Kirim", "19:00"), "%H:%M").time()
        return jam_mulai <= now.time() <= jam_akhir
    except: return True

def cek_batas_harian(cfg, ws_config):
    global _kirim_hari_ini
    batas = int(cfg.get("Batas Kirim Per Hari", "1000"))
    try:
        data = ws_config.get_all_values()
        for i, row in enumerate(data):
            if row[0].strip() == "Total Kirim Hari Ini":
                jumlah  = int(row[1]) if str(row[1]).strip().isdigit() else 0
                tgl_row = data[i+1][1] if i+1 < len(data) else ""
                today   = get_wib_now().strftime("%Y-%m-%d")
                if tgl_row != today:
                    sheet_retry(ws_config.update_cell, i+2, 2, "0")
                    sheet_retry(ws_config.update_cell, i+3, 2, today)
                    _kirim_hari_ini[0] = 0
                    return True, batas, 0
                _kirim_hari_ini[0] = jumlah
                return jumlah < batas, batas, jumlah
    except: pass
    return True, batas, _kirim_hari_ini[0]

def tambah_counter_harian(ws_config):
    _kirim_hari_ini[0] += 1
    try:
        data = ws_config.get_all_values()
        for i, row in enumerate(data):
            if row[0].strip() == "Total Kirim Hari Ini":
                sheet_retry(ws_config.update_cell, i+2, 2, str(_kirim_hari_ini[0]))
                break
    except: pass

def update_heartbeat(ws_config):
    try:
        data = ws_config.get_all_values()
        for i, row in enumerate(data):
            if row[0].strip() == "Heartbeat":
                sheet_retry(ws_config.update_cell, i+2, 2, format_wib())
                break
    except: pass

# ═══════════════════════════════════════════════════════════════
# HELPER
# ═══════════════════════════════════════════════════════════════

def check_internet():
    try:
        socket.setdefaulttimeout(5)
        socket.create_connection(("8.8.8.8", 53))
        return True
    except: return False

def sanitize(value, max_len=MAX_FIELD_LENGTH):
    if not value or value == "-": return "-"
    value = re.sub(r'[\x00-\x1f\x7f]', '', str(value)).strip()
    if value and value[0] in ('=', '+', '-', '@'):
        value = "'" + value
    return (value[:max_len] + "...") if len(value) > max_len else (value or "-")

def ambil_nilai(row, col, default=""):
    try:
        val = row[col - 1].strip()
        return val if val else default
    except: return default

def validasi_email(e):
    if not re.match(r'^[\w\.\+\-]+@[\w\.\-]+\.\w{2,}$', e.strip()):
        return False
    return True

def format_no_wa(no_wa):
    no = re.sub(r'[\s\-\(\)]', '', no_wa)
    if no.startswith("0"): no = "62" + no[1:]
    elif not no.startswith("62"): no = "62" + no
    return no

def get_sapaan():
    jam = get_wib_now().hour
    if 5 <= jam < 11:   return "Selamat Pagi,"
    elif 11 <= jam < 15: return "Selamat Siang,"
    elif 15 <= jam < 19: return "Selamat Sore,"
    else:                return "Selamat Malam,"

def nama_sudah_valid(nama):
    if not nama or len(nama.strip()) < 2: return False
    nama = nama.strip()
    if re.match(r'(?i)^(admin_|lamaran_|re:\s*|fwd:\s*|lamaran\s+|apply\s+)', nama): return False
    if not any(c.isalpha() for c in nama): return False
    if re.match(r'^[a-z0-9_.]+$', nama.lower()) and ' ' not in nama and any(c.isdigit() for c in nama): return False
    if len(nama) > 60: return False
    return True

def bersihkan_nama_manual(nama_kotor):
    nama = re.sub(r'(?i)(admin_?|lamaran_?|re:\s*|fwd:\s*|lamaran\s+)', '', nama_kotor)
    nama = nama.replace('_', ' ').replace('.', ' ').strip().title()
    return nama if nama and any(c.isalpha() for c in nama) else "Kandidat"

def bersihkan_nama(nama_kotor):
    if not nama_kotor or nama_kotor.strip() in ("-", ""): return "Kandidat"
    nama_kotor = nama_kotor.strip()
    if nama_sudah_valid(nama_kotor): return nama_kotor.title()
    nama_manual = bersihkan_nama_manual(nama_kotor)
    if nama_manual != "Kandidat" and nama_sudah_valid(nama_manual):
        return nama_manual
    nama_safe = re.sub(r'[\x00-\x1f\x7f]', '', nama_kotor).strip()[:200]
    prompt = f"""Tugas: bersihkan nama orang dari teks berikut.
- Jika nama valid, kembalikan Title Case
- Jika username/kode, kembalikan: Kandidat
- Hapus prefix: Admin_, Re:, Lamaran_, Fwd:
- Balas HANYA nama bersih, tanpa tanda kutip
Teks: {nama_safe}"""
    result = gemini_call(prompt, is_json=False)
    if result:
        result = re.sub(r'["\']', '', result).strip()
        if 2 <= len(result) <= 60: return result
    return nama_manual if nama_manual != "Kandidat" else "Kandidat"

def kirim_notifikasi(cfg, judul, pesan):
    if not cfg: return
    now = time.time()
    if judul in _notif_terakhir and now - _notif_terakhir[judul] < 3600: return
    _notif_terakhir[judul] = now
    notif_email = cfg.get("Email Notifikasi", "")
    if not notif_email: return
    try:
        msg            = MIMEMultipart("alternative")
        msg["Subject"] = f"⚠️ BOT PELAMAR 2 — {judul}"
        msg["From"]    = f"Bot Pelamar 2 <{cfg['Email Pengirim']}>"
        msg["To"]      = notif_email
        msg["Date"]    = email.utils.formatdate(localtime=True)
        waktu          = format_wib()
        html = f"""<div style="font-family:Arial,sans-serif;max-width:500px;margin:20px auto;
                    border:2px solid #e74c3c;border-radius:8px;overflow:hidden;">
            <div style="background:#e74c3c;color:#fff;padding:15px 20px;">
                <h2 style="margin:0;">⚠️ Bot Pelamar 2 — Notifikasi</h2></div>
            <div style="padding:20px;">
                <p><strong>Waktu:</strong> {waktu} WIB</p>
                <p><strong>Masalah:</strong> {judul}</p>
                <p><strong>Detail:</strong></p>
                <pre style="background:#f8f8f8;padding:10px;border-radius:4px;
                white-space:pre-wrap;">{pesan}</pre>
            </div></div>"""
        msg.attach(MIMEText(html, "html", "utf-8"))
        with smtplib.SMTP_SSL(cfg["SMTP Host"], int(cfg.get("SMTP Port", "465"))) as s:
            s.login(cfg["Email Pengirim"], cfg["Password Email"])
            s.sendmail(cfg["Email Pengirim"], notif_email, msg.as_bytes())
        log.info(f"  📧 Notifikasi dikirim ke {notif_email}")
    except Exception as e:
        log.warning(f"  Gagal kirim notifikasi: {e}")

def kirim_notifikasi_harian(cfg, ws_manual, stats):
    global _notif_harian_terkirim
    try:
        now  = get_wib_now()
        hari = now.strftime("%Y-%m-%d")
        if _notif_harian_terkirim[0] == hari:
            return
        if not (18 <= now.hour < 19 and now.minute <= 5):
            return
        em_notif = cfg.get("Email Notifikasi", "")
        if not em_notif:
            return

        sisa_manual = 0
        try:
            data_manual = ws_manual.get_all_values()
            sisa_manual = sum(1 for row in data_manual[1:]
                             if len(row) >= 2 and not str(row[1]).strip())
        except: pass

        total_terkirim = stats.get("undangan", 0)

        pesan = f"""RINGKASAN HARIAN BOT PELAMAR 2
Tanggal : {now.strftime("%d %B %Y")}
Jam     : {now.strftime("%H:%M")} WIB
{'='*40}

📨 Total undangan terkirim hari ini : {total_terkirim} email
📋 Sisa kandidat belum dikirim      : {sisa_manual} kandidat

{'='*40}
Bot Pelamar 2 — Himeya Agency"""

        kirim_notifikasi(cfg, f"Ringkasan Harian Bot 2 {now.strftime('%d/%m/%Y')}", pesan)
        _notif_harian_terkirim[0] = hari
        log.info(f"  📊 Notifikasi ringkasan harian Bot 2 dikirim ke {em_notif}")

    except Exception as e:
        log.warning(f"  Gagal kirim notifikasi harian Bot 2: {e}")

# ═══════════════════════════════════════════════════════════════
# MULTI-PERUSAHAAN — shared dengan Bot 1
# ═══════════════════════════════════════════════════════════════

P_NAMA=1; P_KATA_KUNCI=2; P_EMAIL=3; P_PASSWORD=4
P_SMTP_HOST=5; P_SMTP_PORT=6; P_IMAP_HOST=7; P_IMAP_PORT=8
P_AKTIF=9; P_DIVISI=10; P_EMAIL_RESMI=11; P_LOGO=12
P_WARNA_ATAS=13; P_WARNA_BAWAH=14; P_WARNA_AKSEN=15; P_WARNA_TOMBOL=16
P_SUBJECT=17; P_SALAM=18; P_P1=19; P_P2=20; P_P3=21
P_P4=22; P_P5=23; P_PENUTUP=24; P_TOMBOL_WA=25
P_JADWAL=26; P_JAM=27; P_TIPE=28; P_LINK_MEET=29; P_ALAMAT=30
P_DOKUMEN=31; P_CATATAN=32
P_FOOTER_ALAMAT=33; P_FOOTER_TELP=34; P_FOOTER_SOSMED=35

_cache_perusahaan    = []
_cache_perusahaan_ts = [0]
CACHE_PERUSAHAAN_MENIT = 10

def _ambil_col(row, col, default=""):
    try:
        v = row[col - 1].strip()
        return v if v else default
    except: return default

def baca_daftar_perusahaan(ws_perusahaan, force=False):
    global _cache_perusahaan
    now = time.time()
    if not force and _cache_perusahaan and (now - _cache_perusahaan_ts[0]) < CACHE_PERUSAHAAN_MENIT * 60:
        return _cache_perusahaan
    hasil = []
    if not ws_perusahaan:
        return hasil
    try:
        data = ws_perusahaan.get_all_values()
        if len(data) <= 1: return hasil
        for i, row in enumerate(data[1:], start=2):
            if len(row) < P_AKTIF: continue
            aktif = _ambil_col(row, P_AKTIF, "Ya").lower()
            if aktif not in ("ya","yes","true","1"): continue
            nama = _ambil_col(row, P_NAMA)
            if not nama: continue
            kw_raw = _ambil_col(row, P_KATA_KUNCI, nama)
            hasil.append({
                "baris": i, "nama": nama,
                "kata_kunci": [k.strip().lower() for k in kw_raw.split(",") if k.strip()],
                "email":      _ambil_col(row, P_EMAIL),
                "password":   _ambil_col(row, P_PASSWORD),
                "smtp_host":  _ambil_col(row, P_SMTP_HOST, "smtp.hostinger.com"),
                "smtp_port":  _ambil_col(row, P_SMTP_PORT, "465"),
                "divisi":     _ambil_col(row, P_DIVISI, "Human Resources"),
                "email_resmi":_ambil_col(row, P_EMAIL_RESMI),
                "logo":       _ambil_col(row, P_LOGO),
                "warna_atas": _ambil_col(row, P_WARNA_ATAS,  "#7b4397"),
                "warna_bawah":_ambil_col(row, P_WARNA_BAWAH, "#dc2430"),
                "warna_aksen":_ambil_col(row, P_WARNA_AKSEN, "#7b4397"),
                "warna_tombol":_ambil_col(row, P_WARNA_TOMBOL,"#25d366"),
                "subject":    _ambil_col(row, P_SUBJECT,
                                "Undangan Wawancara {posisi} (Segera) — {nama_perusahaan}"),
                "salam":      _ambil_col(row, P_SALAM),
                "p1":         _ambil_col(row, P_P1),
                "p2":         _ambil_col(row, P_P2),
                "p3":         _ambil_col(row, P_P3),
                "p4":         _ambil_col(row, P_P4),
                "p5":         _ambil_col(row, P_P5),
                "penutup":    _ambil_col(row, P_PENUTUP, "Hormat kami,"),
                "tombol_wa":  _ambil_col(row, P_TOMBOL_WA, "💬 Konfirmasi via WhatsApp"),
                "jadwal":     _ambil_col(row, P_JADWAL),
                "jam":        _ambil_col(row, P_JAM),
                "tipe":       _ambil_col(row, P_TIPE, "Online"),
                "link_meet":  _ambil_col(row, P_LINK_MEET),
                "alamat_intv":_ambil_col(row, P_ALAMAT),
                "dokumen":    _ambil_col(row, P_DOKUMEN),
                "catatan":    _ambil_col(row, P_CATATAN),
                "footer_alamat":_ambil_col(row, P_FOOTER_ALAMAT),
                "footer_telp":  _ambil_col(row, P_FOOTER_TELP),
                "footer_sosmed":_ambil_col(row, P_FOOTER_SOSMED),
            })
        _cache_perusahaan    = hasil
        _cache_perusahaan_ts[0] = now
        log.info(f"  ✓ {len(hasil)} perusahaan aktif dimuat.")
    except Exception as e:
        log.warning(f"  Gagal baca Daftar Perusahaan: {e}")
    return hasil

def cocokkan_perusahaan(platform, daftar):
    if not platform or not daftar: return None
    pl = platform.lower()
    for p in daftar:
        for k in p["kata_kunci"]:
            if k in pl: return p
    return None

def cfg_dari_perusahaan(perusahaan, cfg_default):
    return {
        **cfg_default,
        "Nama Perusahaan": perusahaan["nama"],
        "Divisi":          perusahaan["divisi"] or cfg_default.get("Divisi","Human Resources"),
        "Email Pengirim":  perusahaan["email"],
        "Password Email":  perusahaan["password"],
        "SMTP Host":       perusahaan["smtp_host"],
        "SMTP Port":       perusahaan["smtp_port"],
        "Email Resmi":     perusahaan["email_resmi"] or perusahaan["email"],
    }

def tmpl_dari_perusahaan(perusahaan):
    tmpl = {}
    peta = {
        "Subject": perusahaan["subject"], "Salam Pembuka": perusahaan["salam"],
        "Paragraf 1": perusahaan["p1"],   "Paragraf 2": perusahaan["p2"],
        "Paragraf 3": perusahaan["p3"],   "Paragraf 4": perusahaan["p4"],
        "Paragraf 5": perusahaan["p5"],   "Salam Penutup": perusahaan["penutup"],
        "Teks Tombol WA": perusahaan["tombol_wa"],
        "Warna Header Atas": perusahaan["warna_atas"],
        "Warna Header Bawah": perusahaan["warna_bawah"],
        "Warna Aksen": perusahaan["warna_aksen"],
        "Warna Tombol WA": perusahaan["warna_tombol"],
        "_logo": perusahaan["logo"],           "_jadwal": perusahaan["jadwal"],
        "_jam": perusahaan["jam"],             "_tipe": perusahaan["tipe"],
        "_link_meet": perusahaan["link_meet"], "_alamat_intv": perusahaan["alamat_intv"],
        "_dokumen": perusahaan["dokumen"],     "_catatan": perusahaan["catatan"],
        "_footer_alamat": perusahaan["footer_alamat"],
        "_footer_telp": perusahaan["footer_telp"],
        "_footer_sosmed": perusahaan["footer_sosmed"],
    }
    for k, v in peta.items():
        if v: tmpl[k] = v
    return tmpl

# ═══════════════════════════════════════════════════════════════
# EMAIL HTML & SMTP
# ═══════════════════════════════════════════════════════════════

def buat_email_html(cfg, nama, posisi, platform, nama_hr, no_wa, tmpl=None):
    nama_perusahaan = cfg.get("Nama Perusahaan", "Perusahaan")
    email_resmi     = cfg.get("Email Resmi", cfg.get("Email Pengirim", ""))
    divisi          = cfg.get("Divisi", "Human Resources")
    no_wa_link      = format_no_wa(no_wa)
    sapaan          = get_sapaan()
    pesan_wa        = f"Undangan+Wawancara+{posisi.replace(' ', '+')}."
    link_wa         = f"https://api.whatsapp.com/send/?phone={no_wa_link}&text={pesan_wa}&type=phone_number&app_absent=0"

    var = {
        "nama": nama, "posisi": posisi, "platform": platform,
        "nama_hr": nama_hr, "no_wa": no_wa,
        "nama_perusahaan": nama_perusahaan, "divisi": divisi,
        "email_resmi": email_resmi, "sapaan": sapaan,
    }

    def render(teks):
        for k, v in var.items():
            teks = teks.replace("{" + k + "}", v)
        teks = teks.replace("\n", "<br>")
        return teks

    t = tmpl or {}
    salam_pembuka = render(t.get("Salam Pembuka", f"{sapaan} Kepada Bapak/Ibu {nama},"))
    paragraf1     = render(t.get("Paragraf 1", f"Terima kasih telah melamar posisi {posisi} di {nama_perusahaan} melalui platform {platform}."))
    paragraf2     = render(t.get("Paragraf 2", "Setelah meninjau lamaran Anda, kami mengundang Anda untuk wawancara online."))
    paragraf3     = render(t.get("Paragraf 3", "Silakan konfirmasi kehadiran Anda dengan menghubungi tim kami:"))
    teks_tombol   = render(t.get("Teks Tombol WA", "💬 Konfirmasi via WhatsApp"))
    paragraf4     = render(t.get("Paragraf 4",
        "Sertakan saat konfirmasi:<br>• CV terbaru (format PDF)<br>• Tangkapan layar email undangan ini<br>• Data diri singkat"))
    paragraf5     = render(t.get("Paragraf 5",
        "Apabila ada pertanyaan, jangan ragu menghubungi kami."))
    salam_penutup = render(t.get("Salam Penutup", "Hormat kami,"))

    warna_atas   = t.get("Warna Header Atas",  "#7b4397")
    warna_bawah  = t.get("Warna Header Bawah", "#dc2430")
    warna_aksen  = t.get("Warna Aksen",        "#7b4397")
    warna_tombol = t.get("Warna Tombol WA",    "#25d366")

    logo_url  = t.get("_logo", "")
    logo_html = (f'<img src="{logo_url}" alt="{nama_perusahaan}" '
                 f'style="max-height:60px;margin-bottom:10px;"><br>') if logo_url else ""

    jadwal = t.get("_jadwal",""); jam = t.get("_jam","")
    tipe   = t.get("_tipe","");  link_meet = t.get("_link_meet","")
    alamat_intv = t.get("_alamat_intv","")
    blok_jadwal = ""
    if jadwal or jam or link_meet or alamat_intv:
        baris = []
        if jadwal:      baris.append(f"📅 <strong>Tanggal:</strong> {jadwal}")
        if jam:         baris.append(f"🕐 <strong>Jam:</strong> {jam} WIB")
        if tipe:        baris.append(f"🖥️ <strong>Tipe:</strong> {tipe}")
        if link_meet:   baris.append(
            f"🔗 <strong>Link:</strong> <a href='{link_meet}' style='color:{warna_aksen};'>{link_meet}</a>")
        if alamat_intv: baris.append(f"📍 <strong>Lokasi:</strong> {alamat_intv}")
        blok_jadwal = f"""
    <div style="background:#f0f7ff;border-left:4px solid #2196f3;
                border-radius:4px;padding:15px 20px;margin:15px 0;">
      <strong style="color:#2196f3;">📋 Detail Wawancara</strong><br><br>
      {"<br>".join(baris)}
    </div>"""

    dokumen = t.get("_dokumen","")
    blok_dokumen = ""
    if dokumen:
        items = [d.strip() for d in dokumen.replace("<br>","\n").split("\n") if d.strip()]
        li    = "".join(f"<li>{item}</li>" for item in items)
        blok_dokumen = f"""
    <p><strong>Dokumen yang perlu disiapkan:</strong></p>
    <ul style="margin:5px 0;padding-left:20px;line-height:1.8;">{li}</ul>"""

    catatan = t.get("_catatan","")
    blok_catatan = ""
    if catatan:
        blok_catatan = f"""
    <div style="background:#fff8e1;border-left:4px solid #ffc107;
                border-radius:4px;padding:12px 18px;margin:15px 0;font-size:14px;">
      ⚠️ <strong>Catatan:</strong> {catatan}
    </div>"""

    footer_alamat = t.get("_footer_alamat","")
    footer_telp   = t.get("_footer_telp","")
    footer_sosmed = t.get("_footer_sosmed","")
    baris_footer  = []
    if footer_alamat: baris_footer.append(f"📍 {footer_alamat}")
    if footer_telp:   baris_footer.append(f"📞 {footer_telp}")
    if footer_sosmed: baris_footer.append(f"🌐 {footer_sosmed}")
    footer_extra  = (" &nbsp;|&nbsp; ".join(baris_footer) + "<br>") if baris_footer else ""

    return f"""<!DOCTYPE html>
<html><head><meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0"></head>
<body style="margin:0;padding:0;background:#f4f4f4;font-family:Verdana,Geneva,sans-serif;">
<table style="max-width:600px;margin:40px auto;background:#fff;border-radius:12px;
      overflow:hidden;border:1px solid #e0e0e0;box-shadow:0 4px 8px rgba(0,0,0,0.1);"
       border="0" width="600" cellspacing="0" cellpadding="0" align="center">
  <tr><td style="background:linear-gradient(135deg,{warna_atas} 0%,{warna_bawah} 100%);
                 text-align:center;color:#fff;padding:30px 20px;">
    {logo_html}
    <h1 style="margin:0;font-size:24px;font-weight:600;">{nama_perusahaan}</h1>
    <p style="margin:8px 0 0;font-size:14px;color:#f0f0f0;">Divisi {divisi}</p>
  </td></tr>
  <tr><td style="padding:30px 35px;font-size:16px;color:#333;line-height:1.7;">
    <p><strong>{salam_pembuka}</strong></p>
    <p>{paragraf1}</p>
    <p>{paragraf2}</p>
    {blok_jadwal}
    <p>{paragraf3}</p>
    <p style="padding:15px 20px;background:#f9f0ff;border-left:4px solid {warna_aksen};border-radius:4px;">
      <strong>Nama&nbsp;&nbsp;&nbsp;: {nama_hr}</strong><br>
      <strong>WhatsApp: {no_wa}</strong>
    </p>
    <p style="text-align:center;margin:25px 0;">
      <a href="{link_wa}" style="display:inline-block;background:{warna_tombol};color:#fff;
         text-decoration:none;padding:12px 28px;border-radius:8px;font-size:16px;font-weight:600;"
         target="_blank">{teks_tombol}</a>
    </p>
    {blok_dokumen}
    <p>{paragraf4}</p>
    {blok_catatan}
    <p>{paragraf5}</p>
    <p>{salam_penutup}<br>
    <strong>Divisi {divisi}<br>{nama_perusahaan}</strong></p>
    <p style="font-size:12px;color:#999;margin-top:20px;">
    Harap abaikan email ini jika Anda tidak pernah melamar posisi di {nama_perusahaan}.</p>
  </td></tr>
  <tr><td style="background:#f8f8f8;text-align:center;font-size:13px;color:#777;
                 padding:20px 15px;border-top:1px solid #eee;">
    {footer_extra}
    <p style="margin:4px 0;">Email resmi:
    <a href="mailto:{email_resmi}" style="color:#1a73e8;">{email_resmi}</a></p>
  </td></tr>
</table></body></html>"""

def kirim_smtp(cfg, email_tujuan, nama, posisi, platform, nama_hr, no_wa, tmpl=None):
    nama_perusahaan = cfg.get("Nama Perusahaan", "Perusahaan")
    t = tmpl or {}
    subject_raw = t.get("Subject", "Undangan Wawancara {posisi} (Segera) — {nama_perusahaan}")
    subject     = subject_raw.replace("{posisi}", posisi).replace("{nama_perusahaan}", nama_perusahaan)
    try:
        msg               = MIMEMultipart("alternative")
        msg["Subject"]    = subject
        msg["From"]       = f"{nama_perusahaan} HR <{cfg['Email Pengirim']}>"
        msg["To"]         = email_tujuan
        msg["Date"]       = email.utils.formatdate(localtime=True)
        msg["Message-ID"] = email.utils.make_msgid()
        msg.attach(MIMEText(
            buat_email_html(cfg, nama, posisi, platform, nama_hr, no_wa, tmpl),
            "html", "utf-8"))
        with smtplib.SMTP_SSL(cfg["SMTP Host"], int(cfg.get("SMTP Port", "465"))) as s:
            s.login(cfg["Email Pengirim"], cfg["Password Email"])
            s.sendmail(cfg["Email Pengirim"], email_tujuan, msg.as_bytes())
        return True, ""
    except smtplib.SMTPAuthenticationError:
        kirim_notifikasi(cfg, "Login SMTP Bot 2 Gagal",
            f"Bot 2 tidak bisa login SMTP.\nEmail: {cfg['Email Pengirim']}")
        return False, "LOGIN_GAGAL"
    except smtplib.SMTPRecipientsRefused:
        return False, "Email tujuan ditolak"
    except Exception as e:
        return False, str(e)

def catat_log_pengiriman(ws_log, cfg, email_tujuan, nama, posisi, platform,
                          nama_hr, no_wa, ke, status):
    try:
        row = [
            format_wib(), email_tujuan, nama, posisi, platform,
            nama_hr, no_wa,
            cfg.get("Email Pengirim", ""),
            cfg.get("Nama Perusahaan", ""),
            f"ke-{ke}", status,
        ]
        sheet_retry(ws_log.append_row, row)
    except Exception as e:
        log.warning(f"  Gagal catat log: {e}")

# ═══════════════════════════════════════════════════════════════
# KIRIM DARI INPUT MANUAL2
# ═══════════════════════════════════════════════════════════════

def kirim_input_manual(cfg, ws_manual, ws_log, ws_config, sent_ids, stats, tmpl=None, ws_hr=None, ws_state=None, ws_perusahaan=None):
    log.info("── Kirim dari Input Manual2 ─────────────────────────────")

    # Validasi jam operasional
    if not validasi_jam_kirim(cfg):
        log.info(f"  Di luar jam operasional ({cfg.get('Jam Mulai Kirim','05:00')}-{cfg.get('Jam Selesai Kirim','19:00')}). Skip.")
        return sent_ids

    # Cek batas harian
    boleh, batas, sudah = cek_batas_harian(cfg, ws_config)
    if not boleh:
        log.warning(f"  ⛔ Batas harian tercapai ({sudah}/{batas}). Skip.")
        return sent_ids

    if sudah >= batas * 0.85:
        log.warning(f"  ⚠ Sudah kirim {sudah}/{batas} hari ini. Mendekati batas!")
        kirim_notifikasi(cfg, "Mendekati Batas Harian Bot 2",
            f"Sudah kirim {sudah} dari {batas} email hari ini.")

    # Batas per siklus
    batas_siklus = int(cfg.get("Batas Kirim Per Siklus",
                    str(max(1, int(cfg.get("Batas Kirim Per Jam", "200")) // 6))))

    # Baca semua data Input Manual2
    try:
        semua = ws_manual.get_all_values()
    except Exception as e:
        log.error(f"  Gagal baca Input Manual2: {e}")
        return sent_ids

    if len(semua) <= 1:
        log.info("  Input Manual2 kosong.")
        return sent_ids

    # Kumpulkan baris yang belum dikirim (kolom B kosong)
    antrian = []
    for i, row in enumerate(semua[1:], start=2):
        if len(antrian) >= batas_siklus:
            break

        em_raw     = row[MAN_EMAIL - 1].strip()       if len(row) >= MAN_EMAIL       else ""
        status     = row[MAN_STATUS - 1].strip()      if len(row) >= MAN_STATUS      else ""
        nama       = row[MAN_NAMA - 1].strip()        if len(row) >= MAN_NAMA        else ""
        posisi     = row[MAN_POSISI - 1].strip()      if len(row) >= MAN_POSISI      else ""
        platform   = row[MAN_PLATFORM - 1].strip()    if len(row) >= MAN_PLATFORM    else ""
        nama_hr    = row[MAN_NAMA_HR - 1].strip()     if len(row) >= MAN_NAMA_HR     else ""
        no_wa      = row[MAN_NO_WA - 1].strip()       if len(row) >= MAN_NO_WA       else ""
        perusahaan = row[MAN_PERUSAHAAN - 1].strip()  if len(row) >= MAN_PERUSAHAAN  else ""

        if not em_raw: continue
        if status:     continue

        # Skip jika kolom Nama Perusahaan kosong
        if not perusahaan:
            log.info(f"  ⏭ Baris {i}: kolom Nama Perusahaan kosong. Skip.")
            sheet_retry(ws_manual.update_cell, i, MAN_STATUS, "Perusahaan kosong ✗")
            continue

        if not validasi_email(em_raw):
            sheet_retry(ws_manual.update_cell, i, MAN_STATUS, "Email tidak valid ✗")
            continue

        antrian.append({
            "baris":      i,
            "email":      em_raw,
            "nama":       nama       or "Kandidat",
            "posisi":     posisi     or cfg.get("Posisi Default",   "Admin"),
            "platform":   platform   or cfg.get("Platform Default", "Instagram"),
            "nama_hr":    nama_hr    or "",
            "no_wa":      no_wa      or "",
            "perusahaan": perusahaan,
        })

    if not antrian:
        log.info("  Tidak ada antrian baru di Input Manual2.")
        return sent_ids

    log.info(f"  {len(antrian)} antrian dari Input Manual2.")

    for idx, item in enumerate(antrian):
        log.info(f"  → [{idx+1}/{len(antrian)}] {item['email']}")

        info_sent    = sent_ids.get(item["email"], {})
        jumlah_kirim = info_sent.get("jumlah", 0) if isinstance(info_sent, dict) else info_sent

        # Tentukan HR
        nama_hr  = item["nama_hr"]
        no_wa    = item["no_wa"]
        baris_hr = None

        if (not nama_hr or not no_wa) and ws_hr:
            hr = ambil_hr(ws_hr)
            if not hr:
                log.warning("  ⛔ Semua HR2 sudah penuh kuota. Stop kirim.")
                kirim_notifikasi(cfg, "Kuota HR2 Habis",
                    "Semua HR di Daftar HR2 sudah mencapai kuota hari ini.\n"
                    "Silakan reset kolom 'Terkirim Hari Ini' di sheet Daftar HR2.")
                break
            nama_hr  = hr["nama"]
            no_wa    = hr["no_wa"]
            baris_hr = hr["baris"]
            log.info(f"    HR: {nama_hr} (sisa kuota: {hr['sisa']})")

        nama_bersih = bersihkan_nama(item["nama"])
        kali_kirim  = jumlah_kirim + 1

        # ── Cocokkan perusahaan dari kolom H (Nama Perusahaan) ──────
        daftar_p   = baca_daftar_perusahaan(ws_perusahaan) if ws_perusahaan else []
        perusahaan = None
        if daftar_p:
            nama_p = item["perusahaan"].lower()
            for p in daftar_p:
                if p["nama"].lower() == nama_p or any(k in nama_p for k in p["kata_kunci"]):
                    perusahaan = p
                    break
        if perusahaan:
            cfg_kirim  = cfg_dari_perusahaan(perusahaan, cfg)
            tmpl_kirim = tmpl_dari_perusahaan(perusahaan)
            log.info(f"    🏢 Perusahaan: {perusahaan['nama']}")
        else:
            log.warning(f"    ⚠ Perusahaan '{item['perusahaan']}' tidak ditemukan di Daftar Perusahaan. Skip.")
            sheet_retry(ws_manual.update_cell, item["baris"], MAN_STATUS, "Perusahaan tidak dikenal ✗")
            continue

        sukses, err = kirim_smtp(
            cfg_kirim, item["email"], nama_bersih,
            item["posisi"], item["platform"],
            nama_hr, no_wa, tmpl_kirim
        )
        waktu = format_wib()

        if sukses:
            sent_ids[item["email"]] = {"jumlah": kali_kirim, "terakhir": waktu}
            save_sent_ids(ws_state, sent_ids)

            if baris_hr and ws_hr:
                tambah_terkirim_hr(ws_hr, baris_hr)

            status_baru = "Terkirim ✓" if kali_kirim == 1 else f"Terkirim ✓ ({kali_kirim}x)"
            sheet_retry(ws_manual.update_cell, item["baris"], MAN_STATUS, status_baru)

            catat_log_pengiriman(
                ws_log, cfg_kirim, item["email"], nama_bersih,
                item["posisi"], item["platform"], nama_hr, no_wa,
                kali_kirim, "Berhasil ✓"
            )
            tambah_counter_harian(ws_config)
            stats["undangan"] += 1
            save_stats(ws_state, stats)
            _total_undangan[0] += 1
            log.info(f"    ✓ {status_baru}")

        else:
            sheet_retry(ws_manual.update_cell, item["baris"], MAN_STATUS, "Gagal ✗")
            catat_log_pengiriman(
                ws_log, cfg_kirim, item["email"], nama_bersih,
                item["posisi"], item["platform"], nama_hr, no_wa,
                kali_kirim, f"Gagal ✗ ({err})"
            )
            stats["gagal"] += 1
            save_stats(ws_state, stats)
            log.error(f"    ✗ Gagal: {err}")
            if err == "LOGIN_GAGAL":
                log.critical("  Login SMTP gagal! Bot 2 dihentikan.")
                sys.exit(1)

        # Jeda acak antar email
        if idx < len(antrian) - 1:
            jeda_min = int(cfg.get("Jeda Min Antar Email (detik)", "5"))
            jeda_max = int(cfg.get("Jeda Max Antar Email (detik)", "10"))
            jeda     = random.randint(jeda_min, jeda_max)
            log.info(f"    Jeda {jeda} detik...")
            time.sleep(jeda)

    return sent_ids

# ═══════════════════════════════════════════════════════════════
# MAIN LOOP
# ═══════════════════════════════════════════════════════════════

def main():
    buat_lock()
    _bot_start_time[0] = get_wib_now()

    log.info("=" * 68)
    log.info("   BOT PELAMAR 2 v1.0 — Input Manual Only + Interval Dinamis")
    log.info(f"   File   : {os.path.abspath(__file__)}")
    log.info(f"   Mulai  : {format_wib(_bot_start_time[0])} WIB")
    log.info(f"   Python : {sys.version.split()[0]}")
    log.info(f"   Gemini : {len(GEMINI_API_KEYS)} API key")
    log.info("=" * 68)

    if os.path.exists(UPDATE_FLAG_FILE):
        log.warning("  ⚠ File update.flag ditemukan. Bot 2 menunggu update selesai...")
        log.warning("  Hapus file update.flag untuk menjalankan bot.")
        sys.exit(0)

    if not check_internet():
        log.error("Tidak ada internet. Bot 2 dihentikan.")
        sys.exit(1)

    try:
        ip = urllib.request.urlopen("https://api.ipify.org", timeout=5).read().decode()
        log.info(f"  IP Server: {ip}")
    except: pass

    log.info("  Menghubungkan ke Google Sheets...")
    ws_config, ws_log, ws_manual, ws_template, ws_hr, ws_state, ws_perusahaan = connect_sheets()
    log.info("  ✓ Google Sheets terhubung.")

    log.info("  Membaca Konfigurasi2 dari sheet...")
    cfg = baca_konfigurasi(ws_config)
    if not cfg:
        log.critical("  ❌ Konfigurasi2 tidak valid! Bot 2 dihentikan.")
        log.critical("  Pastikan sheet 'Konfigurasi2' sudah diisi dengan benar.")
        sys.exit(1)

    interval = ambil_interval_siklus(cfg)
    log.info(f"  ✓ Konfigurasi valid. Perusahaan: {cfg.get('Nama Perusahaan','?')}")
    log.info(f"  ✓ Email: {cfg.get('Email Pengirim','?')}")
    log.info(f"  ✓ Jam operasional: {cfg.get('Jam Mulai Kirim','05:00')}-{cfg.get('Jam Selesai Kirim','19:00')} WIB")
    log.info(f"  ✓ Batas harian: {cfg.get('Batas Kirim Per Hari','1000')} email")
    log.info(f"  ✓ Interval siklus: {interval} menit")

    sent_ids = load_sent_ids(ws_state)
    stats    = load_stats(ws_state)
    log.info(f"  Statistik hari ini: {stats}")
    log.info("  Bot 2 siap! Mulai kirim...\n")

    consecutive_errors = 0
    last_stat_log      = time.time()

    while True:
        if os.path.exists(UPDATE_FLAG_FILE):
            log.info("  ⚠ Update flag terdeteksi. Bot 2 berhenti untuk update...")
            hapus_lock()
            sys.exit(0)

        selisih_jam = (get_wib_now() - _bot_start_time[0]).total_seconds() / 3600
        if selisih_jam >= RESTART_INTERVAL_HOURS:
            log.info(f"  ℹ️ Bot 2 sudah berjalan {RESTART_INTERVAL_HOURS} jam. Railway mengelola uptime.")
            _bot_start_time[0] = get_wib_now()

        if time.time() - last_stat_log > 3600:
            log.info(f"📊 Statistik Bot 2: {stats}")
            last_stat_log = time.time()

        try:
            ws_config, ws_log, ws_manual, ws_template, ws_hr, ws_state, ws_perusahaan = connect_sheets()

            perbarui_lock()

            cfg_baru = baca_konfigurasi(ws_config)
            if cfg_baru:
                cfg = cfg_baru
            else:
                log.warning("  ⚠ Gagal baca konfigurasi, pakai cache.")

            update_heartbeat(ws_config)

            tmpl = baca_template(ws_template) if ws_template else {}

            interval = ambil_interval_siklus(cfg)

            # Kirim dari Input Manual2 — pakai email perusahaan yang cocok
            sent_ids = kirim_input_manual(
                cfg, ws_manual, ws_log, ws_config, sent_ids, stats,
                tmpl, ws_hr, ws_state, ws_perusahaan)

            kirim_notifikasi_harian(cfg, ws_manual, stats)

            consecutive_errors = 0

        except Exception as e:
            consecutive_errors += 1
            log.error(f"Error di main loop Bot 2 (ke-{consecutive_errors}): {e}")
            try: connect_sheets(force=True)
            except: pass
            wait = interval * 3
            log.warning(f"⚠ {consecutive_errors} error. Tunggu {wait} menit...")
            kirim_notifikasi(cfg, "Error Berturut-turut Bot 2",
                f"Bot 2 error {consecutive_errors}x. Istirahat {wait} menit.")
            interruptible_sleep(wait * 60)
            consecutive_errors = 0
        else:
            log.info(f"  Menunggu {interval} menit...\n")
            interruptible_sleep(interval * 60)


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        log.info("\n⛔ Bot 2 dihentikan oleh pengguna.")
        hapus_lock()
    except Exception as e:
        log.critical(f"Bot 2 berhenti: {e}")
        try:
            cfg = _config_cache if _config_cache else {}
            kirim_notifikasi(cfg, "Bot 2 Berhenti/Crash",
                f"Bot 2 berhenti karena error.\nError: {e}\nSilakan restart.")
        except: pass
        hapus_lock()
        sys.exit(1)
