"""
╔══════════════════════════════════════════════════════════════════════╗
║   BOT PELAMAR — Himeya Agency v3.1                                  ║
║                                                                      ║
║   ARSITEKTUR:                                                        ║
║   Sheet "Data Pelamar"  = email masuk otomatis dari inbox           ║
║   Sheet "Input Manual"  = email yang diinput manual (langsung kirim)║
║   Sheet "Data Bersih"   = email dari inbox, siap diproses undangan  ║
║   Sheet "Konfigurasi"   = semua pengaturan bot                      ║
║   Sheet "Log Pengiriman"= rekam jejak semua pengiriman              ║
║                                                                      ║
║   ALUR PER SIKLUS (setiap 5 menit):                                 ║
║     1. Baca Konfigurasi dari sheet                                   ║
║     2. Tahap 1 → Kirim undangan dari Data Bersih (kolom K="kirim")  ║
║     3. Tahap 2 → Kirim langsung dari Input Manual (kolom B kosong)  ║
║     4. Tahap 3 → Monitor email masuk → simpan ke Data Pelamar       ║
║                                                                      ║
║   STRUKTUR KOLOM "Input Manual":                                     ║
║     A=Email  B=Status  C=Nama  D=Posisi  E=Platform                 ║
║     F=Nama HR  G=No WA HR                                           ║
║     B kosong = belum dikirim → bot kirim otomatis                   ║
║     B="Terkirim ✓" = sudah dikirim                                  ║
║                                                                      ║
║   JAMINAN KEAMANAN DATA:                                            ║
║     - Input Manual kolom B saja yang diubah bot                     ║
║     - Kolom G-L Data Bersih TIDAK PERNAH diubah kecuali K dan L    ║
║     - Data Pelamar hanya di-append, tidak pernah dihapus            ║
║     - Log Pengiriman hanya di-append, tidak pernah dihapus          ║
╚══════════════════════════════════════════════════════════════════════╝
"""

import imaplib
import email
import email.utils
import smtplib
from email.header import decode_header
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from email.utils import parsedate_to_datetime
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
import hashlib
import sys
import shutil
from datetime import datetime, timedelta
import random

# ═══════════════════════════════════════════════════════════════
# KONFIGURASI TETAP (tidak berubah)
# ═══════════════════════════════════════════════════════════════

SHEETS_CONFIG = {
    "credentials_file": os.environ.get("GOOGLE_CREDENTIALS_FILE", "credentials.json"),
    "spreadsheet_id":   os.environ.get("SPREADSHEET_ID", "1I93Kw0QFTj1yAda3nJHLmCUUjM-7bi2E6uTLflp3m2Q"),
    "sheet_pelamar":    "Data Pelamar",
    "sheet_manual":     "Input Manual",
    "sheet_bersih":     "Data Bersih",
    "sheet_config":     "Konfigurasi",
    "sheet_log":        "Log Pengiriman",
    "sheet_template":   "Template Email",
    "sheet_hr":         "Daftar HR",
    "sheet_perusahaan": "Daftar Perusahaan",   # ← BARU: multi-perusahaan
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
CHECK_INTERVAL_MINUTES  = 5
MAX_SHEET_RETRIES       = 5
IMAP_TIMEOUT            = 30
GEMINI_TIMEOUT          = 25
MAX_EMAIL_PER_SIKLUS    = 10
SHEET_REFRESH_MINUTES   = 45
MAX_FIELD_LENGTH        = 200
DELAY_BETWEEN_EMAILS    = 5
MAX_SENT_IDS_MONTHS     = 12
RESTART_INTERVAL_HOURS  = 24
HEARTBEAT_TIMEOUT_MENIT = 15

# Kolom Data Bersih (1-based)
COL_NO          = 1
COL_NAMA        = 2
COL_EMAIL       = 3
COL_TANGGAL     = 4
COL_SUBJECT     = 5
COL_SOURCE      = 6
COL_POSISI      = 7
COL_PLATFORM    = 8
COL_NAMA_HR     = 9
COL_NO_WA       = 10
COL_STATUS      = 11
COL_WAKTU_KIRIM = 12

# Kolom Input Manual (1-based)
# A=Email  B=Status  C=Nama  D=Posisi  E=Platform  F=Nama HR  G=No WA HR  H=Nama Perusahaan
MAN_EMAIL       = 1
MAN_STATUS      = 2
MAN_NAMA        = 3
MAN_POSISI      = 4
MAN_PLATFORM    = 5
MAN_NAMA_HR     = 6
MAN_NO_WA       = 7
MAN_PERUSAHAAN  = 8   # ← BARU: nama perusahaan pengirim email

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

# File lokal
CHECKPOINT_FILE        = "processed_ids.json"
CHECKPOINT_BACKUP_FILE = "processed_ids.backup.json"
SENT_IDS_FILE          = "sent_ids.json"
SENT_IDS_BACKUP_FILE   = "sent_ids.backup.json"
EMAIL_INDEX_FILE       = "email_index.json"
EMAIL_INDEX_BACKUP     = "email_index.backup.json"
CONFIG_CACHE_FILE      = "config_cache.json"
STATS_FILE             = "daily_stats.json"
UPDATE_FLAG_FILE       = "update.flag"

BOUNCE_KEYWORDS = []  # Tidak ada filter bounce

SPAM_KEYWORDS = []    # Tidak ada filter spam

VALID_EMAIL_DOMAINS = [
    r'gmail\.com', r'yahoo\.com', r'hotmail\.com', r'outlook\.com',
    r'.*\.co\.id', r'.*\.ac\.id', r'.*\.sch\.id', r'.*\.go\.id',
    r'.*\.or\.id', r'.*\.net\.id', r'.*\.web\.id',
    r'.*\.[a-z]{2,}',
]

# ═══════════════════════════════════════════════════════════════
# LOGGING
# ═══════════════════════════════════════════════════════════════

log_handler = logging.handlers.RotatingFileHandler(
    "bot_pelamar.log", maxBytes=5*1024*1024, backupCount=3, encoding="utf-8"
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

_sheet_state      = {"pelamar": None, "bersih": None, "config": None, "log": None, "last_connected": 0, "_sh": None}
_gemini_key_index = [0]
_gemini_limited   = set()
_total_undangan   = [0]
_notif_terakhir   = {}
_config_cache     = {}
_bot_start_time   = [None]
_kirim_hari_ini   = [0]
_kirim_jam_ini    = [0]
_jam_reset        = [None]
_notif_harian_terkirim = [None]  # tanggal terakhir notifikasi harian dikirim
read_queue        = []

# ═══════════════════════════════════════════════════════════════
# LOCK FILE — timestamp-based, aman untuk Railway
# ═══════════════════════════════════════════════════════════════

_lock_path      = os.path.join(os.path.dirname(os.path.abspath(__file__)), "bot.lock")
LOCK_STALE_MENIT = 10  # lock dianggap stale setelah 10 menit

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
                log.warning(f"⚠ Bot lain mungkin masih jalan (PID {pid}, {umur_mnt:.1f} menit lalu).")
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
    """Perbarui timestamp lock setiap siklus agar tidak dianggap stale."""
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
# Sheet "Bot State" kolom: A=Kunci  B=Nilai
# Baris: processed_ids | sent_ids | email_index | stats
# ═══════════════════════════════════════════════════════════════

_state_cache = {}   # cache in-memory agar tidak baca sheet terus-menerus
_state_dirty = {}   # tandai key mana yang berubah, perlu ditulis ke sheet

def _get_ws_state(force=False):
    """Ambil worksheet Bot State, buat jika belum ada."""
    try:
        sh = _sheet_state.get("_sh")
        if not sh:
            return None
        try:
            ws = sh.worksheet("Bot State")
        except gspread.WorksheetNotFound:
            ws = sh.add_worksheet("Bot State", 20, 2)
            ws.update("A1", [["Kunci", "Nilai"]])
            log.info("  ✓ Sheet 'Bot State' dibuat.")
        return ws
    except Exception as e:
        log.warning(f"  Gagal akses Bot State: {e}")
        return None

def state_load(ws_state, key, default):
    """Baca satu key dari sheet Bot State. Return parsed JSON atau default."""
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
    """Tulis satu key ke sheet Bot State."""
    _state_cache[key] = value
    try:
        data = ws_state.get_all_values()
        for i, row in enumerate(data):
            if len(row) >= 1 and row[0].strip() == key:
                sheet_retry(ws_state.update_cell, i + 1, 2, json.dumps(value, ensure_ascii=False))
                return
        # Key belum ada — tambahkan baris baru
        sheet_retry(ws_state.append_row, [key, json.dumps(value, ensure_ascii=False)])
    except Exception as e:
        log.warning(f"  Gagal tulis state '{key}': {e}")
        # Fallback: tulis ke file lokal sementara
        try:
            with open(f"state_{key}.json", "w", encoding="utf-8") as f:
                json.dump(value, f, ensure_ascii=False)
        except: pass

def load_checkpoint(ws_state):
    data = state_load(ws_state, "processed_ids", [])
    ids  = set(data) if isinstance(data, list) else set()
    log.info(f"  Checkpoint: {len(ids)} email sudah diproses.")
    return ids

def save_checkpoint(ws_state, ids):
    # Simpan maksimal 50.000 ID terbaru
    state_save(ws_state, "processed_ids", list(ids)[-50000:])

def load_sent_ids(ws_state):
    data = state_load(ws_state, "sent_ids", {})
    sent = data if isinstance(data, dict) else {}
    # Bersihkan entry lebih dari MAX_SENT_IDS_MONTHS bulan
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
    log.info(f"  Sent IDs: {len(sent_bersih)} undangan terkirim.")
    return sent_bersih

def save_sent_ids(ws_state, sent):
    state_save(ws_state, "sent_ids", sent)

def load_email_index(ws_state):
    data = state_load(ws_state, "email_index", [])
    idx  = set(data) if isinstance(data, list) else set()
    log.info(f"  Email index: {len(idx)} email unik.")
    return idx

def save_email_index(ws_state, idx):
    state_save(ws_state, "email_index", list(idx))

def load_stats(ws_state=None):
    today = get_wib_now().strftime("%Y-%m-%d")
    if ws_state:
        data = state_load(ws_state, "daily_stats", {})
    else:
        # Fallback file lokal
        try:
            with open(STATS_FILE, "r", encoding="utf-8") as f:
                data = json.load(f)
        except: data = {}
    if data.get("date") == today: return data
    return {"date": today, "masuk": 0, "undangan": 0, "gagal": 0, "bounce": 0}

def save_stats(ws_state, stats):
    state_save(ws_state, "daily_stats", stats)
    # Tetap tulis file lokal untuk web_monitor.py
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
    if not force and _sheet_state["pelamar"] and elapsed < SHEET_REFRESH_MINUTES:
        return (_sheet_state["pelamar"], _sheet_state["bersih"],
                _sheet_state["config"], _sheet_state["log"],
                _sheet_state.get("manual"), _sheet_state.get("template"),
                _sheet_state.get("hr"), _sheet_state.get("state"),
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

    # Data Pelamar
    try:
        ws_pelamar = sh.worksheet(SHEETS_CONFIG["sheet_pelamar"])
    except gspread.WorksheetNotFound:
        ws_pelamar = sh.add_worksheet(SHEETS_CONFIG["sheet_pelamar"], 5000, 6)
        ws_pelamar.insert_row(["No","Nama","Email","Tanggal","Subject","Message-ID"], 1)

    # Data Bersih
    try:
        ws_bersih = sh.worksheet(SHEETS_CONFIG["sheet_bersih"])
    except gspread.WorksheetNotFound:
        ws_bersih = sh.add_worksheet(SHEETS_CONFIG["sheet_bersih"], 5000, 12)
        ws_bersih.insert_row(
            ["No","Nama","Email","Tanggal","Subject","Source",
             "Posisi","Platform","Nama HR","No WA HR","Status","Waktu Kirim"], 1)

    # Konfigurasi
    try:
        ws_config = sh.worksheet(SHEETS_CONFIG["sheet_config"])
    except gspread.WorksheetNotFound:
        ws_config = sh.add_worksheet(SHEETS_CONFIG["sheet_config"], 50, 2)
        buat_sheet_konfigurasi(ws_config)

    # Log Pengiriman
    try:
        ws_log = sh.worksheet(SHEETS_CONFIG["sheet_log"])
    except gspread.WorksheetNotFound:
        ws_log = sh.add_worksheet(SHEETS_CONFIG["sheet_log"], 5000, 11)
        ws_log.insert_row(
            ["Timestamp","Email Tujuan","Nama","Posisi","Platform",
             "Nama HR","No WA","Email Pengirim","Nama Perusahaan","Ke-","Status"], 1)

    # Input Manual
    try:
        ws_manual = sh.worksheet(SHEETS_CONFIG["sheet_manual"])
    except gspread.WorksheetNotFound:
        ws_manual = sh.add_worksheet(SHEETS_CONFIG["sheet_manual"], 10000, 8)
        ws_manual.insert_row(
            ["Email","Status","Nama","Posisi","Platform","Nama HR","No WA HR","Nama Perusahaan"], 1)

    # Template Email
    try:
        ws_template = sh.worksheet(SHEETS_CONFIG["sheet_template"])
    except gspread.WorksheetNotFound:
        ws_template = sh.add_worksheet(SHEETS_CONFIG["sheet_template"], 30, 2)
        buat_sheet_template(ws_template)

    # Daftar HR
    try:
        ws_hr = sh.worksheet(SHEETS_CONFIG["sheet_hr"])
    except gspread.WorksheetNotFound:
        ws_hr = sh.add_worksheet(SHEETS_CONFIG["sheet_hr"], 50, 5)
        buat_sheet_hr(ws_hr)

    _sheet_state.update({
        "pelamar": ws_pelamar, "bersih": ws_bersih,
        "config": ws_config, "log": ws_log,
        "manual": ws_manual, "template": ws_template,
        "hr": ws_hr,
        "_sh": sh,
        "last_connected": now
    })
    if force: log.info("  ✓ Koneksi Google Sheets diperbarui.")

    # Bot State sheet
    try:
        ws_state = sh.worksheet("Bot State")
    except gspread.WorksheetNotFound:
        ws_state = sh.add_worksheet("Bot State", 20, 2)
        ws_state.update("A1", [["Kunci", "Nilai"]])
        log.info("  ✓ Sheet 'Bot State' dibuat.")
    _sheet_state["state"] = ws_state

    # Daftar Perusahaan — sheet bersama Bot 1 & Bot 2
    try:
        ws_perusahaan = sh.worksheet(SHEETS_CONFIG["sheet_perusahaan"])
    except gspread.WorksheetNotFound:
        ws_perusahaan = sh.add_worksheet(SHEETS_CONFIG["sheet_perusahaan"], 50, 35)
        buat_sheet_perusahaan(ws_perusahaan)
        log.info("  ✓ Sheet 'Daftar Perusahaan' dibuat. Silakan isi data perusahaan.")
    _sheet_state["perusahaan"] = ws_perusahaan

    return ws_pelamar, ws_bersih, ws_config, ws_log, ws_manual, ws_template, ws_hr, ws_state, ws_perusahaan

def buat_sheet_konfigurasi(ws):
    rows = [
        ["— Identitas Perusahaan —", ""],
        ["Nama Perusahaan", "Himeya Agency"],
        ["Divisi", "Human Resources"],
        ["Email Resmi", "talent@himeyaagency.com"],
        ["", ""],
        ["— Email Pengirim (SMTP) —", ""],
        ["Email Pengirim", "talent@himeyaagency.com"],
        ["Password Email", "Minggu@00"],
        ["SMTP Host", "smtp.hostinger.com"],
        ["SMTP Port", "465"],
        ["", ""],
        ["— Email Penerima (IMAP) —", ""],
        ["Email Inbox", "talent@himeyaagency.com"],
        ["IMAP Host", "imap.hostinger.com"],
        ["IMAP Port", "993"],
        ["Folder Inbox", "INBOX"],
        ["", ""],
        ["— Notifikasi —", ""],
        ["Email Notifikasi", "propro04040404@gmail.com"],
        ["", ""],
        ["— Default HR —", ""],
        ["Nama HR Default", "Andini Adelia Puspita"],
        ["No WA Default", "0878-7716-4527"],
        ["Posisi Default", "Admin"],
        ["Platform Default", "Instagram"],
        ["", ""],
        ["— Rate Limiting —", ""],
        ["Batas Kirim Per Hari", "400"],
        ["Batas Kirim Per Jam", "120"],
        ["Batas Kirim Per Siklus", "20"],
        ["Jeda Min Antar Email (detik)", "15"],
        ["Jeda Max Antar Email (detik)", "30"],
        ["Jam Mulai Kirim", "08:00"],
        ["Jam Selesai Kirim", "21:00"],
        ["Interval Siklus (menit)", "5"],
        ["", ""],
        ["— Status Bot (diisi otomatis) —", ""],
        ["Heartbeat", ""],
        ["Total Kirim Hari Ini", "0"],
        ["Tanggal Counter", ""],
    ]
    ws.update("A1", rows)
    log.info("  ✓ Sheet Konfigurasi dibuat dengan nilai default.")

def buat_sheet_template(ws):
    """Buat sheet Template Email dengan nilai default."""
    rows = [
        ["— PETUNJUK —", "Ubah nilai di kolom B. Jangan ubah kolom A."],
        ["", "Variabel yang tersedia: {nama} {posisi} {platform} {nama_hr} {no_wa} {nama_perusahaan} {divisi} {email_resmi} {sapaan}"],
        ["", ""],
        ["— Subject Email —", ""],
        ["Subject", "Undangan Wawancara {posisi} (Segera) — {nama_perusahaan}"],
        ["", ""],
        ["— Isi Email —", ""],
        ["Salam Pembuka", "{sapaan} Kepada Bapak/Ibu {nama},"],
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
    # Format header
    ws.format("A1:B1", {"textFormat": {"bold": True}, "backgroundColor": {"red": 0.18, "green": 0.18, "blue": 0.18}})
    ws.format("A:A", {"textFormat": {"bold": True}})
    log.info("  ✓ Sheet Template Email dibuat.")

def baca_template(ws_template):
    """Baca template email dari sheet. Return dict key→value."""
    tmpl = {}
    try:
        data = ws_template.get_all_values()
        for row in data:
            if len(row) >= 2 and row[0].strip() and not row[0].startswith("—"):
                tmpl[row[0].strip()] = row[1].strip()
    except Exception as e:
        log.warning(f"  Gagal baca Template Email: {e}")
    return tmpl

def get_sapaan():
    """Kembalikan sapaan berdasarkan jam WIB sekarang."""
    jam = get_wib_now().hour
    if 5 <= jam < 11:
        return "Selamat Pagi,"
    elif 11 <= jam < 15:
        return "Selamat Siang,"
    elif 15 <= jam < 19:
        return "Selamat Sore,"
    else:
        return "Selamat Malam,"

def buat_sheet_hr(ws):
    """Buat sheet Daftar HR dengan contoh data."""
    rows = [
        ["Nama HR", "No WA", "Kuota Per Hari", "Terkirim Hari Ini", "Status"],
        ["Contoh HR 1", "08xx-xxxx-xxxx", "50", "0", "Aktif"],
        ["Contoh HR 2", "08xx-xxxx-xxxx", "50", "0", "Aktif"],
    ]
    ws.update("A1", rows)
    ws.format("A1:E1", {"textFormat": {"bold": True},
                        "backgroundColor": {"red": 0.28, "green": 0.08, "blue": 0.55}})
    log.info("  ✓ Sheet Daftar HR dibuat. Silakan isi data HR di sheet tersebut.")

def baca_daftar_hr(ws_hr):
    """
    Baca sheet Daftar HR.
    Return list of dict: [{nama, no_wa, kuota, terkirim, status, baris}, ...]
    Hanya yang Status = Aktif dan kuota > terkirim.
    """
    daftar = []
    try:
        data = ws_hr.get_all_values()
        if len(data) <= 1:
            return daftar
        for i, row in enumerate(data[1:], start=2):
            if len(row) < 5:
                continue
            nama      = str(row[0]).strip()
            no_wa     = str(row[1]).strip()
            kuota     = int(row[2]) if str(row[2]).strip().isdigit() else 0
            terkirim  = int(row[3]) if str(row[3]).strip().isdigit() else 0
            status    = str(row[4]).strip().lower()
            if not nama or not no_wa or status != "aktif":
                continue
            daftar.append({
                "nama": nama, "no_wa": no_wa,
                "kuota": kuota, "terkirim": terkirim,
                "sisa": max(0, kuota - terkirim),
                "baris": i
            })
    except Exception as e:
        log.warning(f"  Gagal baca Daftar HR: {e}")
    return daftar

def ambil_hr(ws_hr):
    """
    Ambil HR pertama di sheet yang masih ada kuota.
    Return dict HR atau None jika semua penuh.
    """
    daftar = baca_daftar_hr(ws_hr)
    for hr in daftar:
        if hr["sisa"] > 0:
            return hr
    return None

def tambah_terkirim_hr(ws_hr, baris_hr):
    """Tambah 1 ke kolom Terkirim Hari Ini untuk HR di baris tertentu."""
    try:
        val_lama = ws_hr.cell(baris_hr, 4).value
        val_baru = int(val_lama) + 1 if str(val_lama).strip().isdigit() else 1
        sheet_retry(ws_hr.update_cell, baris_hr, 4, str(val_baru))
    except Exception as e:
        log.warning(f"  Gagal update terkirim HR: {e}")

def kirim_notifikasi_harian(cfg, ws_bersih, ws_manual, stats):
    """Kirim ringkasan harian ke email notifikasi jam 18:00."""
    global _notif_harian_terkirim
    try:
        now  = get_wib_now()
        hari = now.strftime("%Y-%m-%d")

        # Cek sudah kirim hari ini belum
        if _notif_harian_terkirim[0] == hari:
            return

        # Cek jam — kirim hanya jam 18:00-18:05
        if not (18 <= now.hour < 19 and now.minute <= 5):
            return

        em_notif = cfg.get("Email Notifikasi", "")
        if not em_notif:
            return

        # Hitung sisa kandidat belum dikirim di Data Bersih
        sisa_bersih = 0
        try:
            kolom_k = ws_bersih.col_values(11)  # kolom K = Status
            sisa_bersih = sum(1 for s in kolom_k[1:]
                             if s.strip().lower() == "kirim")
        except: pass

        # Hitung sisa kandidat belum dikirim di Input Manual
        sisa_manual = 0
        try:
            data_manual = ws_manual.get_all_values()
            sisa_manual = sum(1 for row in data_manual[1:]
                             if len(row) >= 2 and not str(row[1]).strip())
        except: pass

        total_sisa    = sisa_bersih + sisa_manual
        total_masuk   = stats.get("masuk", 0)
        total_terkirim = stats.get("undangan", 0)

        pesan = f"""RINGKASAN HARIAN BOT PELAMAR
Tanggal : {now.strftime("%d %B %Y")}
Jam     : {now.strftime("%H:%M")} WIB
{'='*40}

📨 Total undangan terkirim hari ini : {total_terkirim} email
📥 Total kandidat masuk dari inbox  : {total_masuk} kandidat
📋 Sisa kandidat belum dikirim      : {total_sisa} kandidat
   - Dari Data Bersih  : {sisa_bersih}
   - Dari Input Manual : {sisa_manual}

{'='*40}
Bot Pelamar — Himeya Agency"""

        kirim_notifikasi(cfg, f"Ringkasan Harian {now.strftime('%d/%m/%Y')}", pesan)
        _notif_harian_terkirim[0] = hari
        log.info(f"  📊 Notifikasi ringkasan harian dikirim ke {em_notif}")

    except Exception as e:
        log.warning(f"  Gagal kirim notifikasi harian: {e}")

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

        # Validasi nilai wajib
        wajib = ["Email Pengirim", "Password Email", "SMTP Host", "SMTP Port",
                 "Email Inbox", "IMAP Host", "IMAP Port", "Nama Perusahaan"]
        kosong = [k for k in wajib if not cfg.get(k)]
        if kosong:
            log.error(f"  ❌ Konfigurasi tidak lengkap: {kosong}")
            if _config_cache:
                log.warning("  ⚠ Menggunakan konfigurasi cache terakhir.")
                return _config_cache
            return None

        # Simpan ke cache lokal sebagai fallback
        _config_cache = cfg
        try:
            with open(CONFIG_CACHE_FILE, "w", encoding="utf-8") as f:
                json.dump(cfg, f, ensure_ascii=False, indent=2)
        except: pass

        return cfg

    except Exception as e:
        log.error(f"  Gagal baca Konfigurasi: {e}")
        # Coba load dari cache lokal
        if os.path.exists(CONFIG_CACHE_FILE):
            try:
                with open(CONFIG_CACHE_FILE, "r", encoding="utf-8") as f:
                    _config_cache = json.load(f)
                log.warning("  ⚠ Menggunakan konfigurasi cache lokal.")
                return _config_cache
            except: pass
        return None

def validasi_jam_kirim(cfg):
    try:
        now       = get_wib_now()
        jam_mulai = datetime.strptime(cfg.get("Jam Mulai Kirim", "08:00"), "%H:%M").time()
        jam_akhir = datetime.strptime(cfg.get("Jam Selesai Kirim", "17:00"), "%H:%M").time()
        return jam_mulai <= now.time() <= jam_akhir
    except: return True

def cek_batas_harian(cfg, ws_config):
    global _kirim_hari_ini
    batas = int(cfg.get("Batas Kirim Per Hari", "400"))
    # Ambil counter dari sheet
    try:
        data = ws_config.get_all_values()
        for i, row in enumerate(data):
            if row[0].strip() == "Total Kirim Hari Ini":
                jumlah  = int(row[1]) if row[1].strip().isdigit() else 0
                tgl_row = data[i+1][1] if i+1 < len(data) else ""
                today   = get_wib_now().strftime("%Y-%m-%d")
                if tgl_row != today:
                    # Reset counter hari baru
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

def ambil_interval_siklus(cfg):
    """Baca interval siklus dari konfigurasi, default CHECK_INTERVAL_MINUTES."""
    try:
        return max(1, int(cfg.get("Interval Siklus (menit)", str(CHECK_INTERVAL_MINUTES))))
    except:
        return CHECK_INTERVAL_MINUTES

def interruptible_sleep(detik):
    """Sleep yang bisa langsung dihentikan dengan Ctrl+C (dibagi per 1 detik)."""
    for _ in range(int(detik)):
        time.sleep(1)

# ═══════════════════════════════════════════════════════════════
# HELPER
# ═══════════════════════════════════════════════════════════════

def check_internet():
    try:
        socket.setdefaulttimeout(5)
        socket.create_connection(("8.8.8.8", 53))
        return True
    except: return False

def decode_str(s):
    if not s: return ""
    parts  = decode_header(s)
    result = []
    for part, enc in parts:
        if isinstance(part, bytes):
            result.append(part.decode(enc or "utf-8", errors="replace"))
        else:
            result.append(str(part))
    return "".join(result)

def get_email_body(msg):
    body = ""
    if msg.is_multipart():
        for part in msg.walk():
            ct = part.get_content_type()
            cd = str(part.get("Content-Disposition", ""))
            if ct == "text/plain" and "attachment" not in cd:
                try:
                    body = part.get_payload(decode=True).decode(
                        part.get_content_charset() or "utf-8", errors="replace")
                    break
                except: pass
    else:
        try:
            body = msg.get_payload(decode=True).decode(
                msg.get_content_charset() or "utf-8", errors="replace")
        except: pass
    return body[:500]

def make_unique_id(msg_id, sender, subject):
    if msg_id and msg_id.strip():
        return msg_id.strip()
    raw = f"{sender.strip()}|{subject.strip()}".encode("utf-8")
    return "HASH-" + hashlib.sha256(raw).hexdigest()[:24]

def normalize_date(date_str):
    if not date_str:
        return format_wib()
    try:
        dt  = parsedate_to_datetime(date_str)
        wib = dt.astimezone(__import__('datetime').timezone(
            __import__('datetime').timedelta(hours=7)))
        return wib.strftime("%Y-%m-%d %H:%M:%S")
    except:
        return format_wib()

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

def is_bounce(subject, sender):
    txt = (subject + " " + sender).lower()
    for kw in BOUNCE_KEYWORDS:
        if kw in txt: return True, kw
    return False, ""

def is_spam(subject, sender, body=""):
    txt = (subject + " " + sender + " " + body).lower()
    for kw in SPAM_KEYWORDS:
        if kw in txt: return True, kw
    return False, ""

def format_no_wa(no_wa):
    no = re.sub(r'[\s\-\(\)]', '', no_wa)
    if no.startswith("0"): no = "62" + no[1:]
    elif not no.startswith("62"): no = "62" + no
    return no

def connect_imap(cfg):
    mail = imaplib.IMAP4_SSL(cfg["IMAP Host"], int(cfg.get("IMAP Port", "993")))
    mail.sock.settimeout(IMAP_TIMEOUT)
    mail.login(cfg["Email Inbox"], cfg["Password Email"])
    mail.select(cfg.get("Folder Inbox", "INBOX"))
    return mail

def safe_logout(mail):
    try: mail.close()
    except: pass
    try: mail.logout()
    except: pass

def flush_read_queue(mail=None, cfg=None):
    if not read_queue: return
    _mail       = mail
    close_after = False
    if _mail is None and cfg:
        try:
            _mail       = connect_imap(cfg)
            close_after = True
        except: return
    for eid in read_queue:
        try: _mail.store(eid, '+FLAGS', '\\Seen')
        except: pass
    read_queue.clear()
    if close_after:
        safe_logout(_mail)

def kirim_notifikasi(cfg, judul, pesan):
    if not cfg: return
    now = time.time()
    if judul in _notif_terakhir and now - _notif_terakhir[judul] < 3600: return
    _notif_terakhir[judul] = now
    notif_email = cfg.get("Email Notifikasi", "")
    if not notif_email: return
    try:
        msg            = MIMEMultipart("alternative")
        msg["Subject"] = f"⚠️ BOT PELAMAR — {judul}"
        msg["From"]    = f"Bot Pelamar <{cfg['Email Pengirim']}>"
        msg["To"]      = notif_email
        msg["Date"]    = email.utils.formatdate(localtime=True)
        waktu          = format_wib()
        html = f"""<div style="font-family:Arial,sans-serif;max-width:500px;margin:20px auto;
                    border:2px solid #e74c3c;border-radius:8px;overflow:hidden;">
            <div style="background:#e74c3c;color:#fff;padding:15px 20px;">
                <h2 style="margin:0;">⚠️ Bot Pelamar — Notifikasi</h2></div>
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

# ═══════════════════════════════════════════════════════════════
# EKSTRAK NAMA VIA GEMINI
# ═══════════════════════════════════════════════════════════════

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

def ekstrak_nama_email(subject, sender, body):
    email_match  = re.search(r'[\w\.\+\-]+@[\w\.\-]+\.\w+', sender)
    sender_email = email_match.group(0) if email_match else "-"
    name_match   = re.match(r'^"?([^"<\n]+?)"?\s*<', sender)
    sender_name  = name_match.group(1).strip() if name_match else ""
    sender_name  = re.sub(r'[\x00-\x1f]', '', sender_name).strip()
    if sender_name.lower() == sender_email.lower(): sender_name = ""
    if sender_name and len(sender_name) >= 2:
        return {"nama": sender_name, "email": sender_email}
    sender_safe  = re.sub(r'[\x00-\x1f\x7f]', '', sender).strip()[:200]
    subject_safe = re.sub(r'[\x00-\x1f\x7f]', '', subject).strip()[:200]
    body_safe    = re.sub(r'[\x00-\x1f\x7f]', '', body[:300]).strip()
    prompt = f"""Dari data email berikut, ekstrak nama pengirim dan alamat email.
Jika tidak ditemukan, isi "-".
Balas HANYA JSON tanpa penjelasan: {{"nama":"...","email":"..."}}
Dari: {sender_safe}
Subject: {subject_safe}
Isi: {body_safe}"""
    result = gemini_call(prompt, is_json=True)
    if result: return result
    fallback = sender_name if sender_name else (sender_email if sender_email != "-" else "-")
    return {"nama": fallback, "email": sender_email}

# ═══════════════════════════════════════════════════════════════
# MULTI-PERUSAHAAN — Daftar Perusahaan & Template per Perusahaan
# ═══════════════════════════════════════════════════════════════

# Kolom sheet "Daftar Perusahaan" (1-based)
P_NAMA        = 1   # Nama Perusahaan
P_KATA_KUNCI  = 2   # Kata Kunci Platform (pisah koma)
P_EMAIL       = 3   # Email Pengirim
P_PASSWORD    = 4   # Password Email
P_SMTP_HOST   = 5   # SMTP Host
P_SMTP_PORT   = 6   # SMTP Port
P_IMAP_HOST   = 7   # IMAP Host
P_IMAP_PORT   = 8   # IMAP Port
P_AKTIF       = 9   # Aktif (Ya/Tidak)
P_DIVISI      = 10  # Divisi
P_EMAIL_RESMI = 11  # Email Resmi (tampil di footer)
P_LOGO        = 12  # Logo URL
P_WARNA_ATAS  = 13  # Warna Header Atas
P_WARNA_BAWAH = 14  # Warna Header Bawah
P_WARNA_AKSEN = 15  # Warna Aksen
P_WARNA_TOMBOL= 16  # Warna Tombol WA
P_SUBJECT     = 17  # Subject Email
P_SALAM       = 18  # Salam Pembuka
P_P1          = 19  # Paragraf 1
P_P2          = 20  # Paragraf 2
P_P3          = 21  # Paragraf 3
P_P4          = 22  # Paragraf 4
P_P5          = 23  # Paragraf 5
P_PENUTUP     = 24  # Salam Penutup
P_TOMBOL_WA   = 25  # Teks Tombol WA
P_JADWAL      = 26  # Jadwal Wawancara
P_JAM         = 27  # Jam Wawancara
P_TIPE        = 28  # Tipe Interview (Online/Offline)
P_LINK_MEET   = 29  # Link Meet/Zoom
P_ALAMAT      = 30  # Alamat Interview
P_DOKUMEN     = 31  # Daftar Dokumen
P_CATATAN     = 32  # Catatan Batch
P_FOOTER_ALAMAT= 33 # Alamat Kantor (footer)
P_FOOTER_TELP = 34  # No Telp (footer)
P_FOOTER_SOSMED= 35 # Media Sosial (footer, pisah koma)

_cache_perusahaan     = []       # cache daftar perusahaan
_cache_perusahaan_ts  = [0]      # timestamp cache terakhir
CACHE_PERUSAHAAN_MENIT = 10      # refresh cache tiap 10 menit

def buat_sheet_perusahaan(ws):
    """Buat sheet Daftar Perusahaan dengan header dan 1 baris contoh."""
    header = [
        "Nama Perusahaan", "Kata Kunci Platform", "Email Pengirim", "Password Email",
        "SMTP Host", "SMTP Port", "IMAP Host", "IMAP Port", "Aktif", "Divisi",
        "Email Resmi", "Logo URL", "Warna Header Atas", "Warna Header Bawah",
        "Warna Aksen", "Warna Tombol WA", "Subject Email", "Salam Pembuka",
        "Paragraf 1", "Paragraf 2", "Paragraf 3", "Paragraf 4", "Paragraf 5",
        "Salam Penutup", "Teks Tombol WA", "Jadwal Wawancara", "Jam Wawancara",
        "Tipe Interview", "Link Meet/Zoom", "Alamat Interview", "Daftar Dokumen",
        "Catatan Batch", "Alamat Kantor", "No Telepon", "Media Sosial",
    ]
    contoh = [
        "Himeya Agency", "Himeya, Himeya Agency", "talent@himeyaagency.com", "IsiPassword",
        "smtp.hostinger.com", "465", "imap.hostinger.com", "993", "Ya", "Human Resources",
        "talent@himeyaagency.com", "", "#7b4397", "#dc2430",
        "#7b4397", "#25d366",
        "Undangan Wawancara {posisi} — {nama_perusahaan}",
        "{sapaan} Kepada Bapak/Ibu {nama},",
        "Terima kasih telah melamar posisi {posisi} di {nama_perusahaan} melalui {platform}.",
        "Setelah meninjau lamaran Anda, kami dengan senang hati mengundang Anda untuk wawancara.",
        "Silakan konfirmasi kehadiran Anda dengan menghubungi tim kami:",
        "Sertakan saat konfirmasi:\n• CV terbaru (PDF)\n• Screenshot email undangan ini",
        "Jika ada pertanyaan, hubungi kami melalui email di bawah.",
        "Hormat kami,", "💬 Konfirmasi via WhatsApp",
        "", "", "Online", "", "",
        "CV terbaru (PDF)\nFoto 3x4", "",
        "Jl. Sudirman No. 1, Jakarta", "021-12345678", "@himeyaagency",
    ]
    ws.update("A1", [header, contoh])
    ws.format("A1:AI1", {
        "textFormat": {"bold": True},
        "backgroundColor": {"red": 0.18, "green": 0.18, "blue": 0.18},
        "horizontalAlignment": "CENTER",
    })
    log.info("  ✓ Sheet 'Daftar Perusahaan' dibuat dengan contoh data.")

def _ambil_col(row, col, default=""):
    """Ambil nilai kolom dari row (1-based), return default jika kosong."""
    try:
        v = row[col - 1].strip()
        return v if v else default
    except: return default

def baca_daftar_perusahaan(ws_perusahaan, force=False):
    """
    Baca sheet Daftar Perusahaan, return list of dict.
    Di-cache selama CACHE_PERUSAHAAN_MENIT menit.
    """
    global _cache_perusahaan
    now = time.time()
    if not force and _cache_perusahaan and (now - _cache_perusahaan_ts[0]) < CACHE_PERUSAHAAN_MENIT * 60:
        return _cache_perusahaan

    hasil = []
    try:
        data = ws_perusahaan.get_all_values()
        if len(data) <= 1:
            log.warning("  ⚠ Sheet 'Daftar Perusahaan' kosong atau hanya header.")
            return hasil
        for i, row in enumerate(data[1:], start=2):
            if len(row) < P_AKTIF: continue
            aktif = _ambil_col(row, P_AKTIF, "Ya").lower()
            if aktif not in ("ya", "yes", "true", "1"): continue
            nama = _ambil_col(row, P_NAMA)
            if not nama: continue
            kata_kunci_raw = _ambil_col(row, P_KATA_KUNCI, nama)
            kata_kunci = [k.strip().lower() for k in kata_kunci_raw.split(",") if k.strip()]
            hasil.append({
                "baris":         i,
                "nama":          nama,
                "kata_kunci":    kata_kunci,
                "email":         _ambil_col(row, P_EMAIL),
                "password":      _ambil_col(row, P_PASSWORD),
                "smtp_host":     _ambil_col(row, P_SMTP_HOST, "smtp.hostinger.com"),
                "smtp_port":     _ambil_col(row, P_SMTP_PORT, "465"),
                "imap_host":     _ambil_col(row, P_IMAP_HOST, "imap.hostinger.com"),
                "imap_port":     _ambil_col(row, P_IMAP_PORT, "993"),
                "divisi":        _ambil_col(row, P_DIVISI, "Human Resources"),
                "email_resmi":   _ambil_col(row, P_EMAIL_RESMI),
                "logo":          _ambil_col(row, P_LOGO),
                "warna_atas":    _ambil_col(row, P_WARNA_ATAS,  "#7b4397"),
                "warna_bawah":   _ambil_col(row, P_WARNA_BAWAH, "#dc2430"),
                "warna_aksen":   _ambil_col(row, P_WARNA_AKSEN, "#7b4397"),
                "warna_tombol":  _ambil_col(row, P_WARNA_TOMBOL,"#25d366"),
                "subject":       _ambil_col(row, P_SUBJECT,
                                    "Undangan Wawancara {posisi} (Segera) — {nama_perusahaan}"),
                "salam":         _ambil_col(row, P_SALAM),
                "p1":            _ambil_col(row, P_P1),
                "p2":            _ambil_col(row, P_P2),
                "p3":            _ambil_col(row, P_P3),
                "p4":            _ambil_col(row, P_P4),
                "p5":            _ambil_col(row, P_P5),
                "penutup":       _ambil_col(row, P_PENUTUP, "Hormat kami,"),
                "tombol_wa":     _ambil_col(row, P_TOMBOL_WA, "💬 Konfirmasi via WhatsApp"),
                "jadwal":        _ambil_col(row, P_JADWAL),
                "jam":           _ambil_col(row, P_JAM),
                "tipe":          _ambil_col(row, P_TIPE, "Online"),
                "link_meet":     _ambil_col(row, P_LINK_MEET),
                "alamat_intv":   _ambil_col(row, P_ALAMAT),
                "dokumen":       _ambil_col(row, P_DOKUMEN),
                "catatan":       _ambil_col(row, P_CATATAN),
                "footer_alamat": _ambil_col(row, P_FOOTER_ALAMAT),
                "footer_telp":   _ambil_col(row, P_FOOTER_TELP),
                "footer_sosmed": _ambil_col(row, P_FOOTER_SOSMED),
            })
        _cache_perusahaan    = hasil
        _cache_perusahaan_ts[0] = now
        log.info(f"  ✓ {len(hasil)} perusahaan aktif dimuat dari sheet.")
    except Exception as e:
        log.warning(f"  Gagal baca Daftar Perusahaan: {e}")
    return hasil

def cocokkan_perusahaan(platform, daftar_perusahaan):
    """
    Cocokkan platform pelamar ke daftar perusahaan berdasarkan kata kunci.
    Return dict perusahaan yang cocok, atau None jika tidak ada.
    """
    if not platform or not daftar_perusahaan:
        return None
    platform_lower = platform.lower()
    for p in daftar_perusahaan:
        for kunci in p["kata_kunci"]:
            if kunci in platform_lower:
                return p
    return None

def cfg_dari_perusahaan(perusahaan, cfg_default):
    """
    Buat dict cfg override dari data perusahaan.
    Fallback ke cfg_default untuk key yang tidak ada di perusahaan.
    """
    return {
        **cfg_default,
        "Nama Perusahaan":  perusahaan["nama"],
        "Divisi":           perusahaan["divisi"] or cfg_default.get("Divisi", "Human Resources"),
        "Email Pengirim":   perusahaan["email"],
        "Password Email":   perusahaan["password"],
        "SMTP Host":        perusahaan["smtp_host"],
        "SMTP Port":        perusahaan["smtp_port"],
        "IMAP Host":        perusahaan["imap_host"],
        "IMAP Port":        perusahaan["imap_port"],
        "Email Resmi":      perusahaan["email_resmi"] or perusahaan["email"],
    }

def tmpl_dari_perusahaan(perusahaan):
    """
    Buat dict template dari data perusahaan.
    Hanya isi key yang tidak kosong agar fungsi render bisa fallback ke default.
    """
    tmpl = {}
    peta = {
        "Subject":          perusahaan["subject"],
        "Salam Pembuka":    perusahaan["salam"],
        "Paragraf 1":       perusahaan["p1"],
        "Paragraf 2":       perusahaan["p2"],
        "Paragraf 3":       perusahaan["p3"],
        "Paragraf 4":       perusahaan["p4"],
        "Paragraf 5":       perusahaan["p5"],
        "Salam Penutup":    perusahaan["penutup"],
        "Teks Tombol WA":   perusahaan["tombol_wa"],
        "Warna Header Atas":  perusahaan["warna_atas"],
        "Warna Header Bawah": perusahaan["warna_bawah"],
        "Warna Aksen":        perusahaan["warna_aksen"],
        "Warna Tombol WA":    perusahaan["warna_tombol"],
        # Fitur tambahan
        "_logo":            perusahaan["logo"],
        "_jadwal":          perusahaan["jadwal"],
        "_jam":             perusahaan["jam"],
        "_tipe":            perusahaan["tipe"],
        "_link_meet":       perusahaan["link_meet"],
        "_alamat_intv":     perusahaan["alamat_intv"],
        "_dokumen":         perusahaan["dokumen"],
        "_catatan":         perusahaan["catatan"],
        "_footer_alamat":   perusahaan["footer_alamat"],
        "_footer_telp":     perusahaan["footer_telp"],
        "_footer_sosmed":   perusahaan["footer_sosmed"],
    }
    for k, v in peta.items():
        if v:
            tmpl[k] = v
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

    # ── Konten utama ──────────────────────────────────────────
    salam_pembuka = render(t.get("Salam Pembuka",
        f"{sapaan} Kepada Bapak/Ibu {nama},"))
    paragraf1     = render(t.get("Paragraf 1",
        f"Terima kasih telah melamar posisi {posisi} di {nama_perusahaan} melalui platform {platform}."))
    paragraf2     = render(t.get("Paragraf 2",
        "Setelah meninjau lamaran Anda, kami dengan senang hati mengundang Anda untuk mengikuti wawancara online sebagai tahap seleksi berikutnya."))
    paragraf3     = render(t.get("Paragraf 3",
        "Silakan konfirmasi kehadiran Anda dengan menghubungi tim kami:"))
    teks_tombol   = render(t.get("Teks Tombol WA", "💬 Konfirmasi via WhatsApp"))
    paragraf4     = render(t.get("Paragraf 4",
        "Sertakan saat konfirmasi:<br>• CV terbaru (format PDF)<br>• Tangkapan layar email undangan ini<br>• Data diri singkat (Nama Lengkap & Alamat Email)"))
    paragraf5     = render(t.get("Paragraf 5",
        "Apabila Anda memiliki pertanyaan lebih lanjut, jangan ragu untuk menghubungi kami melalui alamat email di bawah ini."))
    salam_penutup = render(t.get("Salam Penutup", "Hormat kami,"))

    # ── Warna & branding ─────────────────────────────────────
    warna_atas    = t.get("Warna Header Atas",  "#7b4397")
    warna_bawah   = t.get("Warna Header Bawah", "#dc2430")
    warna_aksen   = t.get("Warna Aksen",        "#7b4397")
    warna_tombol  = t.get("Warna Tombol WA",    "#25d366")

    # ── Logo (opsional) ───────────────────────────────────────
    logo_url  = t.get("_logo", "")
    logo_html = (f'<img src="{logo_url}" alt="{nama_perusahaan}" '
                 f'style="max-height:60px;margin-bottom:10px;"><br>') if logo_url else ""

    # ── Blok jadwal wawancara (opsional) ─────────────────────
    jadwal    = t.get("_jadwal", "")
    jam       = t.get("_jam", "")
    tipe      = t.get("_tipe", "")
    link_meet = t.get("_link_meet", "")
    alamat_intv = t.get("_alamat_intv", "")

    blok_jadwal = ""
    if jadwal or jam or link_meet or alamat_intv:
        baris_jadwal = []
        if jadwal:      baris_jadwal.append(f"📅 <strong>Tanggal:</strong> {jadwal}")
        if jam:         baris_jadwal.append(f"🕐 <strong>Jam:</strong> {jam} WIB")
        if tipe:        baris_jadwal.append(f"🖥️ <strong>Tipe:</strong> {tipe}")
        if link_meet:   baris_jadwal.append(
            f"🔗 <strong>Link:</strong> <a href='{link_meet}' style='color:{warna_aksen};'>{link_meet}</a>")
        if alamat_intv: baris_jadwal.append(f"📍 <strong>Lokasi:</strong> {alamat_intv}")
        blok_jadwal = f"""
    <div style="background:#f0f7ff;border-left:4px solid #2196f3;
                border-radius:4px;padding:15px 20px;margin:15px 0;">
      <strong style="color:#2196f3;">📋 Detail Wawancara</strong><br><br>
      {"<br>".join(baris_jadwal)}
    </div>"""

    # ── Daftar dokumen (opsional) ────────────────────────────
    dokumen     = t.get("_dokumen", "")
    blok_dokumen = ""
    if dokumen:
        items = [d.strip() for d in dokumen.replace("<br>", "\n").split("\n") if d.strip()]
        li    = "".join(f"<li>{item}</li>" for item in items)
        blok_dokumen = f"""
    <p><strong>Dokumen yang perlu disiapkan:</strong></p>
    <ul style="margin:5px 0;padding-left:20px;line-height:1.8;">{li}</ul>"""

    # ── Catatan batch (opsional) ─────────────────────────────
    catatan     = t.get("_catatan", "")
    blok_catatan = ""
    if catatan:
        blok_catatan = f"""
    <div style="background:#fff8e1;border-left:4px solid #ffc107;
                border-radius:4px;padding:12px 18px;margin:15px 0;font-size:14px;">
      ⚠️ <strong>Catatan:</strong> {catatan}
    </div>"""

    # ── Footer info kantor (opsional) ────────────────────────
    footer_alamat = t.get("_footer_alamat", "")
    footer_telp   = t.get("_footer_telp", "")
    footer_sosmed = t.get("_footer_sosmed", "")
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

def simpan_ke_sent_folder(cfg, msg_bytes):
    try:
        mail = imaplib.IMAP4_SSL(cfg["IMAP Host"], int(cfg.get("IMAP Port", "993")))
        mail.login(cfg["Email Pengirim"], cfg["Password Email"])
        for folder in ["Sent", "INBOX.Sent", "Sent Items", "Sent Messages"]:
            try:
                status, _ = mail.select(f'"{folder}"')
                if status == "OK":
                    mail.append(f'"{folder}"', "\\Seen",
                                imaplib.Time2Internaldate(time.time()), msg_bytes)
                    break
            except: continue
        mail.logout()
    except Exception as e:
        log.warning(f"    ⚠ Gagal simpan ke Sent: {e}")

def kirim_smtp(cfg, email_tujuan, nama, posisi, platform, nama_hr, no_wa, tmpl=None):
    nama_perusahaan = cfg.get("Nama Perusahaan", "Perusahaan")
    t = tmpl or {}

    # Subject dari template, fallback ke default
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
        msg_bytes = msg.as_bytes()
        with smtplib.SMTP_SSL(cfg["SMTP Host"], int(cfg.get("SMTP Port", "465"))) as s:
            s.login(cfg["Email Pengirim"], cfg["Password Email"])
            s.sendmail(cfg["Email Pengirim"], email_tujuan, msg_bytes)
        simpan_ke_sent_folder(cfg, msg_bytes)
        return True, ""
    except smtplib.SMTPAuthenticationError:
        kirim_notifikasi(cfg, "Login SMTP Gagal",
            f"Bot tidak bisa login SMTP.\nEmail: {cfg['Email Pengirim']}")
        return False, "LOGIN_GAGAL"
    except smtplib.SMTPRecipientsRefused:
        return False, "Email tujuan ditolak"
    except Exception as e:
        return False, str(e)

def catat_log_pengiriman(ws_log, cfg, email_tujuan, nama, posisi, platform,
                          nama_hr, no_wa, ke, status):
    try:
        row = [
            format_wib(),
            email_tujuan,
            nama,
            posisi,
            platform,
            nama_hr,
            no_wa,
            cfg.get("Email Pengirim", ""),
            cfg.get("Nama Perusahaan", ""),
            f"ke-{ke}",
            status,
        ]
        sheet_retry(ws_log.append_row, row)
    except Exception as e:
        log.warning(f"  Gagal catat log: {e}")

# ═══════════════════════════════════════════════════════════════
# TAHAP 1 — KIRIM UNDANGAN
# ═══════════════════════════════════════════════════════════════

def tahap1_kirim_undangan(cfg, ws_bersih, ws_log, ws_config, sent_ids, stats, tmpl=None, ws_hr=None, ws_state=None, ws_perusahaan=None):
    log.info("── TAHAP 1: Cek undangan wawancara ──────────────────────")

    # Validasi jam operasional
    if not validasi_jam_kirim(cfg):
        log.info(f"  Di luar jam operasional ({cfg.get('Jam Mulai Kirim','08:00')}-{cfg.get('Jam Selesai Kirim','17:00')}). Skip kirim.")
        return sent_ids

    # Cek batas harian
    boleh, batas, sudah = cek_batas_harian(cfg, ws_config)
    if not boleh:
        log.warning(f"  ⛔ Batas harian tercapai ({sudah}/{batas}). Skip kirim.")
        return sent_ids

    # Peringatan mendekati batas
    if sudah >= batas * 0.85:
        log.warning(f"  ⚠ Sudah kirim {sudah}/{batas} hari ini. Mendekati batas!")
        kirim_notifikasi(cfg, "Mendekati Batas Harian",
            f"Sudah kirim {sudah} dari {batas} email hari ini.")

    # Baca kolom K saja (lebih efisien)
    try:
        kolom_k = ws_bersih.col_values(COL_STATUS)
    except Exception as e:
        log.error(f"  Gagal baca Data Bersih: {e}")
        return sent_ids

    if len(kolom_k) <= 1:
        log.info("  Data Bersih kosong.")
        return sent_ids

    # Kumpulkan baris yang perlu dikirim
    baris_kirim = []
    for i, status in enumerate(kolom_k[1:], start=2):
        if status.strip().lower() == "kirim":
            baris_kirim.append(i)

    if not baris_kirim:
        log.info("  Tidak ada undangan yang perlu dikirim.")
        return sent_ids

    # Batasi per siklus — baca dari Konfigurasi
    batas_siklus = int(cfg.get("Batas Kirim Per Siklus",
                    str(max(1, int(cfg.get("Batas Kirim Per Jam", "120")) // 6))))
    if len(baris_kirim) > batas_siklus:
        log.info(f"  {len(baris_kirim)} antrian, batasi {batas_siklus} per siklus.")
        baris_kirim = baris_kirim[:batas_siklus]

    # Baca data baris yang dibutuhkan saja
    semua = ws_bersih.get_all_values()
    antrian = []
    for baris in baris_kirim:
        if baris - 1 >= len(semua): continue
        row = semua[baris - 1]
        em  = ambil_nilai(row, COL_EMAIL, "")
        if not em or not validasi_email(em):
            sheet_retry(ws_bersih.update_cell, baris, COL_STATUS, "Email tidak valid ✗")
            continue
        antrian.append({
            "baris":    baris,
            "nama":     ambil_nilai(row, COL_NAMA,    "Kandidat"),
            "email":    em,
            "posisi":   ambil_nilai(row, COL_POSISI,   cfg.get("Posisi Default", "Admin")),
            "platform": ambil_nilai(row, COL_PLATFORM, cfg.get("Platform Default", "Instagram")),
            "nama_hr":  ambil_nilai(row, COL_NAMA_HR,  ""),
            "no_wa":    ambil_nilai(row, COL_NO_WA,    ""),
        })

    if not antrian:
        return sent_ids

    log.info(f"  {len(antrian)} undangan akan dikirim.")

    for idx, item in enumerate(antrian):
        log.info(f"  → [{idx+1}/{len(antrian)}] {item['email']}")

        info_sent    = sent_ids.get(item["email"], {})
        jumlah_kirim = info_sent.get("jumlah", 0) if isinstance(info_sent, dict) else info_sent

        # Tentukan HR — pakai dari sheet jika kolom kosong
        nama_hr = item["nama_hr"]
        no_wa   = item["no_wa"]
        baris_hr = None

        if (not nama_hr or not no_wa) and ws_hr:
            hr = ambil_hr(ws_hr)
            if not hr:
                log.warning("  ⛔ Semua HR sudah penuh kuota. Stop kirim.")
                kirim_notifikasi(cfg, "Kuota HR Habis",
                    "Semua HR sudah mencapai kuota hari ini.\n"
                    "Silakan reset kolom 'Terkirim Hari Ini' di sheet Daftar HR.")
                break
            nama_hr  = hr["nama"]
            no_wa    = hr["no_wa"]
            baris_hr = hr["baris"]
            log.info(f"    HR: {nama_hr} (sisa kuota: {hr['sisa']})")

        nama_bersih  = bersihkan_nama(item["nama"])
        kali_kirim   = jumlah_kirim + 1

        # ── Tentukan cfg & template per perusahaan ──────────
        daftar_p = baca_daftar_perusahaan(ws_perusahaan) if ws_perusahaan else []
        perusahaan = cocokkan_perusahaan(item["platform"], daftar_p)
        if perusahaan:
            cfg_kirim  = cfg_dari_perusahaan(perusahaan, cfg)
            tmpl_kirim = tmpl_dari_perusahaan(perusahaan)
            log.info(f"    🏢 Perusahaan: {perusahaan['nama']}")
        else:
            if daftar_p:
                log.warning(f"    ⚠ Platform '{item['platform']}' tidak cocok dg perusahaan manapun. Skip.")
                sheet_retry(ws_bersih.update_cell, item["baris"], COL_STATUS, "Platform tidak dikenal ✗")
                continue
            # Fallback: tidak ada Daftar Perusahaan, pakai cfg global
            cfg_kirim  = cfg
            tmpl_kirim = tmpl or {}

        sukses, err  = kirim_smtp(
            cfg_kirim, item["email"], nama_bersih,
            item["posisi"], item["platform"],
            nama_hr, no_wa, tmpl_kirim
        )
        waktu = format_wib()

        if sukses:
            # Update sent_ids
            sent_ids[item["email"]] = {"jumlah": kali_kirim, "terakhir": waktu}
            save_sent_ids(ws_state, sent_ids)

            # Update kolom K dan L saja
            status_baru = "Terkirim ✓" if kali_kirim == 1 else f"Terkirim ✓ ({kali_kirim}x)"
            sheet_retry(ws_bersih.update_cell, item["baris"], COL_STATUS,      status_baru)
            sheet_retry(ws_bersih.update_cell, item["baris"], COL_WAKTU_KIRIM, waktu)

            # Update kuota HR
            if baris_hr and ws_hr:
                tambah_terkirim_hr(ws_hr, baris_hr)

            # Catat ke Log Pengiriman
            catat_log_pengiriman(
                ws_log, cfg_kirim, item["email"], nama_bersih,
                item["posisi"], item["platform"], nama_hr, no_wa,
                kali_kirim, "Berhasil ✓"
            )

            # Update counter harian
            tambah_counter_harian(ws_config)
            stats["undangan"] += 1
            save_stats(ws_state, stats)
            _total_undangan[0] += 1
            log.info(f"    ✓ {status_baru}")

        else:
            sheet_retry(ws_bersih.update_cell, item["baris"], COL_STATUS,      "Gagal ✗")
            sheet_retry(ws_bersih.update_cell, item["baris"], COL_WAKTU_KIRIM, waktu)
            catat_log_pengiriman(
                ws_log, cfg, item["email"], nama_bersih,
                item["posisi"], item["platform"], item["nama_hr"], item["no_wa"],
                kali_kirim, f"Gagal ✗ ({err})"
            )
            stats["gagal"] += 1
            save_stats(ws_state, stats)
            log.error(f"    ✗ Gagal: {err}")
            if err == "LOGIN_GAGAL":
                log.critical("  Login SMTP gagal! Bot dihentikan.")
                sys.exit(1)

        # Jeda acak antar email (anti-spam)
        if idx < len(antrian) - 1:
            jeda_min = int(cfg.get("Jeda Min Antar Email (detik)", "45"))
            jeda_max = int(cfg.get("Jeda Max Antar Email (detik)", "90"))
            jeda     = random.randint(jeda_min, jeda_max)
            log.info(f"    Jeda {jeda} detik...")
            time.sleep(jeda)

    return sent_ids

# ═══════════════════════════════════════════════════════════════
# TAHAP 2 — KIRIM DARI INPUT MANUAL
# ═══════════════════════════════════════════════════════════════

def tahap2_kirim_input_manual(cfg, ws_manual, ws_log, ws_config, sent_ids, stats, kuota_sisa, tmpl=None, ws_hr=None, ws_state=None, ws_perusahaan=None):
    """
    Baca sheet Input Manual, kirim undangan ke email yang kolom B-nya kosong.
    kuota_sisa = sisa kuota per siklus setelah Tahap 1 (maks 5 total gabungan)
    """
    log.info("── TAHAP 2: Kirim dari Input Manual ─────────────────────")

    if kuota_sisa <= 0:
        log.info("  Kuota siklus habis di Tahap 1. Skip Input Manual.")
        return sent_ids

    # Validasi jam operasional
    if not validasi_jam_kirim(cfg):
        log.info("  Di luar jam operasional. Skip.")
        return sent_ids

    # Cek batas harian
    boleh, batas, sudah = cek_batas_harian(cfg, ws_config)
    if not boleh:
        log.warning(f"  ⛔ Batas harian tercapai ({sudah}/{batas}). Skip.")
        return sent_ids

    # Baca kolom A (email) dan B (status) sekaligus
    try:
        if ws_manual.getlastrow() <= 1 if hasattr(ws_manual, 'getlastrow') else False:
            log.info("  Input Manual kosong.")
            return sent_ids
        semua = ws_manual.get_all_values()
    except Exception as e:
        log.error(f"  Gagal baca Input Manual: {e}")
        return sent_ids

    if len(semua) <= 1:
        log.info("  Input Manual kosong.")
        return sent_ids

    # Kumpulkan baris yang belum dikirim (kolom B kosong)
    antrian = []
    for i, row in enumerate(semua[1:], start=2):
        if len(antrian) >= kuota_sisa:
            break

        em_raw     = row[MAN_EMAIL - 1].strip()       if len(row) >= MAN_EMAIL       else ""
        status     = row[MAN_STATUS - 1].strip()      if len(row) >= MAN_STATUS      else ""
        nama       = row[MAN_NAMA - 1].strip()        if len(row) >= MAN_NAMA        else ""
        posisi     = row[MAN_POSISI - 1].strip()      if len(row) >= MAN_POSISI      else ""
        platform   = row[MAN_PLATFORM - 1].strip()    if len(row) >= MAN_PLATFORM    else ""
        nama_hr    = row[MAN_NAMA_HR - 1].strip()     if len(row) >= MAN_NAMA_HR     else ""
        no_wa      = row[MAN_NO_WA - 1].strip()       if len(row) >= MAN_NO_WA       else ""
        perusahaan = row[MAN_PERUSAHAAN - 1].strip()  if len(row) >= MAN_PERUSAHAAN  else ""

        # Skip jika email kosong
        if not em_raw:
            continue

        # Skip jika status sudah terisi
        if status:
            continue

        # Skip jika kolom Nama Perusahaan kosong
        if not perusahaan:
            log.info(f"  ⏭ Baris {i}: kolom Nama Perusahaan kosong. Skip.")
            sheet_retry(ws_manual.update_cell, i, MAN_STATUS, "Perusahaan kosong ✗")
            continue

        # Validasi format email
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
        log.info("  Tidak ada antrian baru di Input Manual.")
        return sent_ids

    log.info(f"  {len(antrian)} antrian dari Input Manual.")

    for idx, item in enumerate(antrian):
        log.info(f"  → [{idx+1}/{len(antrian)}] {item['email']}")

        info_sent    = sent_ids.get(item["email"], {})
        jumlah_kirim = info_sent.get("jumlah", 0) if isinstance(info_sent, dict) else info_sent

        # Tentukan HR — pakai dari sheet jika kolom kosong
        nama_hr  = item["nama_hr"]
        no_wa    = item["no_wa"]
        baris_hr = None

        if (not nama_hr or not no_wa) and ws_hr:
            hr = ambil_hr(ws_hr)
            if not hr:
                log.warning("  ⛔ Semua HR sudah penuh kuota. Stop kirim Input Manual.")
                kirim_notifikasi(cfg, "Kuota HR Habis",
                    "Semua HR sudah mencapai kuota hari ini.\n"
                    "Silakan reset kolom 'Terkirim Hari Ini' di sheet Daftar HR.")
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

            # Update kuota HR
            if baris_hr and ws_hr:
                tambah_terkirim_hr(ws_hr, baris_hr)

            status_baru = "Terkirim ✓" if kali_kirim == 1 else f"Terkirim ✓ ({kali_kirim}x)"
            sheet_retry(ws_manual.update_cell, item["baris"], MAN_STATUS, status_baru)

            catat_log_pengiriman(
                ws_log, cfg_kirim, item["email"], nama_bersih,
                item["posisi"], item["platform"], item["nama_hr"], item["no_wa"],
                kali_kirim, "Berhasil ✓"
            )
            tambah_counter_harian(ws_config)
            stats["undangan"] += 1
            save_stats(ws_state, stats)
            _total_undangan[0] += 1
            log.info(f"    ✓ {status_baru}")

        else:
            sheet_retry(ws_manual.update_cell, item["baris"], MAN_STATUS, f"Gagal ✗")
            catat_log_pengiriman(
                ws_log, cfg_kirim, item["email"], nama_bersih,
                item["posisi"], item["platform"], item["nama_hr"], item["no_wa"],
                kali_kirim, f"Gagal ✗ ({err})"
            )
            stats["gagal"] += 1
            save_stats(ws_state, stats)
            log.error(f"    ✗ Gagal: {err}")
            if err == "LOGIN_GAGAL":
                log.critical("  Login SMTP gagal! Bot dihentikan.")
                sys.exit(1)

        # Jeda acak antar email
        if idx < len(antrian) - 1:
            jeda_min = int(cfg.get("Jeda Min Antar Email (detik)", "45"))
            jeda_max = int(cfg.get("Jeda Max Antar Email (detik)", "90"))
            jeda     = random.randint(jeda_min, jeda_max)
            log.info(f"    Jeda {jeda} detik...")
            time.sleep(jeda)

    return sent_ids

# ═══════════════════════════════════════════════════════════════
# TAHAP 3 — MONITOR EMAIL MASUK (MULTI-INBOX)
# ═══════════════════════════════════════════════════════════════

def tahap3_fetch_email(cfg, processed_ids, email_index, ws_pelamar, stats, ws_state, ws_perusahaan=None):
    log.info("── TAHAP 3: Monitor email masuk ─────────────────────────")
    if not check_internet():
        log.warning("  Tidak ada internet, skip.")
        return

    daftar_p     = baca_daftar_perusahaan(ws_perusahaan) if ws_perusahaan else []
    daftar_inbox = []

    if daftar_p:
        for p in daftar_p:
            if p.get("imap_host") and p.get("email") and p.get("password"):
                daftar_inbox.append({
                    "nama":     p["nama"],
                    "email":    p["email"],
                    "password": p["password"],
                    "host":     p["imap_host"],
                    "port":     p["imap_port"],
                    "folder":   cfg.get("Folder Inbox", "INBOX"),
                })
    else:
        daftar_inbox.append({
            "nama":     cfg.get("Nama Perusahaan", ""),
            "email":    cfg.get("Email Inbox", cfg.get("Email Pengirim", "")),
            "password": cfg.get("Password Email", ""),
            "host":     cfg.get("IMAP Host", ""),
            "port":     cfg.get("IMAP Port", "993"),
            "folder":   cfg.get("Folder Inbox", "INBOX"),
        })

    try:
        col_a  = ws_pelamar.col_values(1)
        row_no = sum(1 for v in col_a if v.strip().isdigit())
    except:
        row_no = 0

    for inbox in daftar_inbox:
        label = inbox["nama"] or inbox["email"]
        log.info(f"  📬 Cek inbox: {label}")
        if not inbox["email"] or not inbox["password"] or not inbox["host"]:
            log.warning(f"  ⚠ IMAP tidak lengkap untuk {label}. Skip.")
            continue
        mail = None
        try:
            mail = imaplib.IMAP4_SSL(inbox["host"], int(inbox["port"]))
            mail.sock.settimeout(IMAP_TIMEOUT)
            mail.login(inbox["email"], inbox["password"])
            mail.select(inbox["folder"])
            _, data   = mail.search(None, "UNSEEN")
            email_ids = data[0].split()
            if not email_ids:
                log.info(f"    Tidak ada email baru.")
                safe_logout(mail); continue
            if len(email_ids) > 100:
                log.warning(f"  ⚠ Inbox flood {label}: {len(email_ids)} email!")
                kirim_notifikasi(cfg, f"Inbox Flood: {label}",
                    f"{len(email_ids)} email belum dibaca. Bot proses 10 dulu.")
            batch     = email_ids[:MAX_EMAIL_PER_SIKLUS]
            baca_list = []
            log.info(f"    {len(email_ids)} UNREAD, proses {len(batch)}.")
            for eid in batch:
                try:
                    _, msg_data = mail.fetch(eid, "(BODY.PEEK[])")
                    raw    = msg_data[0][1]
                    msg    = email.message_from_bytes(raw)
                    subj   = decode_str(msg.get("Subject", ""))
                    sender = decode_str(msg.get("From", ""))
                    msg_id = make_unique_id(msg.get("Message-ID",""), sender, subj)
                    if msg_id in processed_ids:
                        baca_list.append(eid); continue
                    bounce, kw = is_bounce(subj, sender)
                    if bounce:
                        baca_list.append(eid)
                        stats["bounce"] += 1
                        save_stats(ws_state, stats); continue
                    body      = get_email_body(msg)
                    spam, kw  = is_spam(subj, sender, body)
                    if spam:
                        baca_list.append(eid); continue
                    extracted = ekstrak_nama_email(subj, sender, body)
                    nama      = sanitize(extracted.get("nama", "-"))
                    em        = sanitize(extracted.get("email", sender))
                    tanggal   = normalize_date(msg.get("Date", ""))
                    subj_cl   = sanitize(subj, max_len=300)
                    if not validasi_email(em):
                        baca_list.append(eid); continue
                    if em.lower() in email_index:
                        processed_ids.add(msg_id)
                        save_checkpoint(ws_state, processed_ids)
                        baca_list.append(eid); continue
                    row_no += 1
                    # Kolom Source = nama perusahaan pemilik inbox (otomatis tandai)
                    row    = [row_no, nama, em, tanggal, subj_cl, inbox["nama"]]
                    saved  = False
                    for attempt in range(MAX_SHEET_RETRIES):
                        try:
                            ws_pelamar.append_row(row)
                            saved = True; break
                        except Exception as e:
                            log.warning(f"  Sheet retry {attempt+1}: {e}")
                            time.sleep(20 * (attempt + 1))
                    if saved:
                        processed_ids.add(msg_id)
                        email_index.add(em.lower())
                        save_checkpoint(ws_state, processed_ids)
                        save_email_index(ws_state, email_index)
                        baca_list.append(eid)
                        stats["masuk"] += 1
                        save_stats(ws_state, stats)
                        log.info(f"  ✓ [{row_no}] {nama} | {em} → {label}")
                    else:
                        row_no -= 1
                        log.error(f"  ✗ Gagal simpan: {em}")
                    time.sleep(DELAY_BETWEEN_EMAILS)
                except Exception as e:
                    log.error(f"  Error proses email {eid}: {e}"); continue
            for eid in baca_list:
                try: mail.store(eid, '+FLAGS', '\\Seen')
                except: pass
        except imaplib.IMAP4.error as e:
            err = str(e)
            if "AUTHENTICATIONFAILED" in err or "LOGIN" in err:
                log.error(f"  ❌ Login IMAP gagal: {label}")
                kirim_notifikasi(cfg, f"Login IMAP Gagal: {label}",
                    f"Email: {inbox['email']}\nError: {err}")
            else:
                log.error(f"  IMAP error {label}: {e}")
        except socket.timeout:
            log.error(f"  Timeout IMAP: {label}")
        except Exception as e:
            log.error(f"  Error fetch {label}: {e}")
        finally:
            if mail: safe_logout(mail)

# ═══════════════════════════════════════════════════════════════
# MAIN LOOP
# ═══════════════════════════════════════════════════════════════

def main():
    buat_lock()
    _bot_start_time[0] = get_wib_now()

    log.info("=" * 68)
    log.info("   BOT PELAMAR v3.0 — Multi Sheet + Rate Limiting")
    log.info(f"   File   : {os.path.abspath(__file__)}")
    log.info(f"   Mulai  : {format_wib(_bot_start_time[0])} WIB")
    log.info(f"   Python : {sys.version.split()[0]}")
    log.info(f"   Gemini : {len(GEMINI_API_KEYS)} API key")
    log.info("=" * 68)

    # Cek update flag
    if os.path.exists(UPDATE_FLAG_FILE):
        log.warning("  ⚠ File update.flag ditemukan. Bot menunggu update selesai...")
        log.warning("  Hapus file update.flag untuk menjalankan bot.")
        sys.exit(0)

    if not check_internet():
        log.error("Tidak ada internet. Bot dihentikan.")
        sys.exit(1)

    # IP publik
    try:
        ip = urllib.request.urlopen("https://api.ipify.org", timeout=5).read().decode()
        log.info(f"  IP Server: {ip}")
    except: pass

    log.info("  Menghubungkan ke Google Sheets...")
    ws_pelamar, ws_bersih, ws_config, ws_log, ws_manual, ws_template, ws_hr, ws_state, ws_perusahaan = connect_sheets()
    log.info("  ✓ Google Sheets terhubung.")

    # Baca & validasi konfigurasi
    log.info("  Membaca Konfigurasi dari sheet...")
    cfg = baca_konfigurasi(ws_config)
    if not cfg:
        log.critical("  ❌ Konfigurasi tidak valid! Bot dihentikan.")
        log.critical("  Pastikan sheet 'Konfigurasi' sudah diisi dengan benar.")
        sys.exit(1)
    log.info(f"  ✓ Konfigurasi valid. Perusahaan: {cfg.get('Nama Perusahaan','?')}")
    log.info(f"  ✓ Email: {cfg.get('Email Pengirim','?')}")
    log.info(f"  ✓ Jam operasional: {cfg.get('Jam Mulai Kirim','08:00')}-{cfg.get('Jam Selesai Kirim','17:00')} WIB")
    log.info(f"  ✓ Batas harian: {cfg.get('Batas Kirim Per Hari','400')} email")

    # Load state dari Google Sheets (bukan file lokal)
    processed_ids = load_checkpoint(ws_state)
    sent_ids      = load_sent_ids(ws_state)
    email_index   = load_email_index(ws_state)
    stats         = load_stats(ws_state)
    log.info(f"  Statistik hari ini: {stats}")
    log.info("  Bot siap! Mulai monitoring...\n")

    consecutive_errors = 0
    last_stat_log      = time.time()

    while True:
        if os.path.exists(UPDATE_FLAG_FILE):
            log.info("  ⚠ Update flag terdeteksi. Bot berhenti untuk update...")
            hapus_lock()
            sys.exit(0)

        selisih_jam = (get_wib_now() - _bot_start_time[0]).total_seconds() / 3600
        if selisih_jam >= RESTART_INTERVAL_HOURS:
            log.info(f"  ℹ️ Bot sudah berjalan {RESTART_INTERVAL_HOURS} jam. Railway mengelola uptime.")
            _bot_start_time[0] = get_wib_now()

        if time.time() - last_stat_log > 3600:
            log.info(f"📊 Statistik: {stats}")
            last_stat_log = time.time()

        try:
            ws_pelamar, ws_bersih, ws_config, ws_log, ws_manual, ws_template, ws_hr, ws_state, ws_perusahaan = connect_sheets()

            perbarui_lock()

            cfg_baru = baca_konfigurasi(ws_config)
            if cfg_baru:
                cfg = cfg_baru
            else:
                log.warning("  ⚠ Gagal baca konfigurasi, pakai cache.")

            update_heartbeat(ws_config)

            tmpl = baca_template(ws_template) if ws_template else {}

            batas_siklus = int(cfg.get("Batas Kirim Per Siklus",
                            str(max(1, int(cfg.get("Batas Kirim Per Jam", "120")) // 6))))

            # TAHAP 1: Kirim undangan dari Data Bersih
            sent_ids_sebelum = len(sent_ids)
            sent_ids = tahap1_kirim_undangan(
                cfg, ws_bersih, ws_log, ws_config, sent_ids, stats,
                tmpl, ws_hr, ws_state, ws_perusahaan)
            terkirim_tahap1 = len(sent_ids) - sent_ids_sebelum
            kuota_sisa      = max(0, batas_siklus - terkirim_tahap1)

            # TAHAP 2: Kirim langsung dari Input Manual
            if ws_manual:
                sent_ids = tahap2_kirim_input_manual(
                    cfg, ws_manual, ws_log, ws_config, sent_ids, stats,
                    kuota_sisa, tmpl, ws_hr, ws_state, ws_perusahaan)

            # TAHAP 3: Monitor semua inbox perusahaan
            tahap3_fetch_email(
                cfg, processed_ids, email_index, ws_pelamar, stats,
                ws_state, ws_perusahaan)

            kirim_notifikasi_harian(cfg, ws_bersih, ws_manual, stats)

            consecutive_errors = 0

        except Exception as e:
            consecutive_errors += 1
            log.error(f"Error di main loop (ke-{consecutive_errors}): {e}")
            try: connect_sheets(force=True)
            except: pass
            interval = ambil_interval_siklus(cfg)
            wait = interval * 3
            log.warning(f"⚠ {consecutive_errors} error. Tunggu {wait} menit...")
            kirim_notifikasi(cfg, "Error Berturut-turut",
                f"Bot error {consecutive_errors}x. Istirahat {wait} menit.")
            interruptible_sleep(wait * 60)
            consecutive_errors = 0
        else:
            interval = ambil_interval_siklus(cfg)
            log.info(f"  Menunggu {interval} menit...\n")
            interruptible_sleep(interval * 60)


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        log.info("\n⛔ Bot dihentikan oleh pengguna.")
        hapus_lock()
    except Exception as e:
        log.critical(f"Bot berhenti: {e}")
        try:
            cfg = _config_cache if _config_cache else {}
            kirim_notifikasi(cfg, "Bot Berhenti/Crash",
                f"Bot berhenti karena error.\nError: {e}\nSilakan restart.")
        except: pass
        hapus_lock()
        sys.exit(1)