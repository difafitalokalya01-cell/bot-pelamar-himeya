"""
╔══════════════════════════════════════════════════════════════════╗
║   BOT PELAMAR KOMPLEKS — Himeya Agency                          ║
║   v1.0 — Semua fungsi dalam satu bot                            ║
║                                                                  ║
║   FITUR:                                                         ║
║   ✓ Monitor email masuk otomatis setiap 5 menit                 ║
║   ✓ Simpan data pelamar ke Google Sheets                        ║
║   ✓ Deduplikasi otomatis ke sheet "Data Bersih"                 ║
║   ✓ Kirim undangan wawancara otomatis (jika kolom K = "kirim")  ║
║   ✓ Kirim ulang (ganti kolom K ke "kirim" lagi)                 ║
║   ✓ Rotasi 18 Gemini API key otomatis                           ║
║   ✓ Anti-spam: jeda 15 detik, istirahat per 100 email           ║
║   ✓ Checkpoint & backup — data aman dari mati lampu             ║
║   ✓ Filter bounce & auto-reply                                  ║
║   ✓ Catat jumlah pengiriman di kolom K                          ║
╚══════════════════════════════════════════════════════════════════╝

STRUKTUR KOLOM SHEET "Data Bersih":
  A = No           — otomatis
  B = Nama         — otomatis
  C = Email        — otomatis
  D = Tanggal      — otomatis
  E = Subject      — otomatis
  F = Message-ID   — otomatis
  G = Posisi       — isi manual (kosong = default: Admin)
  H = Platform     — isi manual (kosong = default: Instagram)
  I = Nama HR      — isi manual (kosong = default: Andini Adelia Puspita)
  J = No WA HR     — isi manual (kosong = default: 0878-7716-4527)
  K = Status       — isi "kirim" untuk undang, "kirim" lagi untuk kirim ulang
  L = Waktu Kirim  — otomatis

CARA PAKAI:
  1. Isi password email di bagian KONFIGURASI di bawah
  2. Jalankan: python bot_pelamar.py
  3. Untuk kirim undangan: isi kolom K = "kirim" di sheet Data Bersih
  4. Untuk kirim ulang: ganti kolom K ke "kirim" lagi
"""

import imaplib
import email
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
from collections import deque
from datetime import datetime

# ═══════════════════════════════════════════════════════════════
# KONFIGURASI — EDIT BAGIAN INI
# ═══════════════════════════════════════════════════════════════

# ── Email Hostinger ───────────────────────────────────────────
# Nilai diambil dari Environment Variable Railway
# Kalau tidak ada, pakai nilai default di bawah
EMAIL_CONFIG = {
    "imap_host":  "imap.hostinger.com",
    "imap_port":  993,
    "smtp_host":  "smtp.hostinger.com",
    "smtp_port":  465,
    "email":      os.environ.get("EMAIL_ADDRESS", "person@herseoul.com"),
    "password":   os.environ.get("EMAIL_PASSWORD", "ISI_PASSWORD_EMAIL_DI_SINI"),
    "folder":     "INBOX",
}

# ── Google Sheets ─────────────────────────────────────────────
SHEETS_CONFIG = {
    "credentials_file": os.environ.get("GOOGLE_CREDENTIALS_FILE", "credentials.json"),
    "spreadsheet_id":   os.environ.get("SPREADSHEET_ID", "1I93Kw0QFTj1yAda3nJHLmCUUjM-7bi2E6uTLflp3m2Q"),
    "sheet_mentah":     "Data Pelamar",
    "sheet_bersih":     "Data Bersih",
}

# ── Gemini AI — 18 API key rotasi otomatis ────────────────────
GEMINI_API_KEYS = [
    "AIzaSyCd1f39mOOvrXWTcnU__niVKItu_fHLqkY",  # key 1
    "AIzaSyCR9vEb3MISk3qgkGUYhzNzwnkao7eLq2Q",  # key 2
    "AIzaSyAiiZ8fsWVAp2FdwlwlccrLpi0v2wujBR4",  # key 3
    "AIzaSyA_NQazILwDtge2FoW-skweHpn3tkfIqxg",  # key 4
    "AIzaSyA0Tcn9p4FIYZmKLDnmMkIg6ZBLeubmD3Q",  # key 5
    "AIzaSyBJ9xKqhcdstw_dhzQCfbiu6ilG_ZVxgRc",  # key 6
    "AIzaSyDOyf2NV4B2c3sQpdwfocxbNmIt9kmFy-A",  # key 7
    "AIzaSyD5_GpyfaAtrJxtjhqN5ccIMOfGvtr65Zs",  # key 8
    "AIzaSyD4sBuOse_ZeOn7MhEfq1A9o3Psi7YiJaw",  # key 9
    "AIzaSyAr5tWEaxJfYGLq6_LRMJKee6CKBvb0AaI",  # key 10
    "AIzaSyCBSC5tn4VZJepPziACvhVXirZdi_kZV2k",  # key 11
    "AIzaSyAMkXgLe_HsvJVXFdTQiUfMfRAHlovuzmg",  # key 12
    "AIzaSyDSoJIn9Pv890GGM4MafLuJhb5pmLClRso",  # key 13
    "AIzaSyAQGBZBMPUQytjbTbD9t41dp9_cnwQtZpA",  # key 14
    "AIzaSyCbjgcHrjmA0zxz7XYGDHysZTh11v0rTJw",  # key 15
    "AIzaSyDYJvzrY00lWNdgVn99pQOcki1xnGdZA0A",  # key 16
    "AIzaSyDXiZi3UuRP7k-FFSnZQF7qCsm9vQT2gsQ",  # key 17
    "AIzaSyDO7JRY8_iNjEZx30qj2booLNDMPIDI59Y",  # key 18
]
GEMINI_BASE_URL = "https://generativelanguage.googleapis.com/v1beta/models/gemini-2.0-flash:generateContent?key="

# ── Default nilai undangan jika kolom sheet kosong ────────────
DEFAULT_UNDANGAN = {
    "posisi":   "Admin",
    "platform": "Instagram",
    "nama_hr":  "Andini Adelia Puspita",
    "no_wa":    "0878-7716-4527",
}

# ── Info perusahaan ───────────────────────────────────────────
NAMA_PERUSAHAAN = "Himeya Agency"
EMAIL_RESMI     = "talent@himeyaagency.com"

# ── Parameter operasional ─────────────────────────────────────
CHECK_INTERVAL_MINUTES  = 5
DELAY_BETWEEN_EMAILS    = 5
JEDA_ANTAR_UNDANGAN     = 15
BATAS_UNDANGAN_SIKLUS   = 10
BATAS_KIRIM_100         = 100
JEDA_PER_100_UNDANGAN   = 3600
MAX_SHEET_RETRIES       = 5
MAX_IMAP_RETRIES        = 3
IMAP_TIMEOUT            = 30
GEMINI_TIMEOUT          = 25
MAX_QUEUE_SIZE          = 10
MAX_RETRY_PER_EMAIL     = 3
SHEET_REFRESH_MINUTES   = 45
MAX_FIELD_LENGTH        = 200

# ── Indeks kolom sheet "Data Bersih" (1-based) ───────────────
COL_NO          = 1
COL_NAMA        = 2
COL_EMAIL       = 3
COL_TANGGAL     = 4
COL_SUBJECT     = 5
COL_MSGID       = 6
COL_POSISI      = 7
COL_PLATFORM    = 8
COL_NAMA_HR     = 9
COL_NO_WA       = 10
COL_STATUS      = 11
COL_WAKTU_KIRIM = 12
COL_STATISTIK   = 14  # Kolom N — info statistik

# ── File lokal ────────────────────────────────────────────────
CHECKPOINT_FILE        = "processed_ids.json"
CHECKPOINT_BACKUP_FILE = "processed_ids.backup.json"
SENT_IDS_FILE          = "sent_ids.json"
SENT_IDS_BACKUP_FILE   = "sent_ids.backup.json"
STATS_FILE             = "daily_stats.json"

# ── Keyword bounce / auto-reply ───────────────────────────────
BOUNCE_KEYWORDS = [
    "mail delivery failed", "delivery status notification",
    "undeliverable", "delivery failure", "failed delivery",
    "mailer-daemon", "postmaster", "auto-reply", "out of office",
    "automatic reply", "autoreply", "vacation reply",
    "tidak di kantor", "sedang tidak tersedia",
]

# ═══════════════════════════════════════════════════════════════
# SETUP LOGGING
# ═══════════════════════════════════════════════════════════════

log_handler = logging.handlers.RotatingFileHandler(
    "bot_pelamar.log",
    maxBytes=5 * 1024 * 1024,
    backupCount=3,
    encoding="utf-8"
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

email_queue: deque = deque()
read_queue:  list  = []
_sheet_state       = {"mentah": None, "bersih": None, "last_connected": 0}
_gemini_key_index  = [0]
_gemini_limited    = set()
_total_undangan    = [0]

# ═══════════════════════════════════════════════════════════════
# GEMINI AI — ROTASI KEY
# ═══════════════════════════════════════════════════════════════

def gemini_get_url():
    keys_ok = [k for k in GEMINI_API_KEYS if k not in _gemini_limited]
    if not keys_ok:
        log.warning("  Semua Gemini key limit, reset...")
        _gemini_limited.clear()
        keys_ok = GEMINI_API_KEYS
    key = keys_ok[_gemini_key_index[0] % len(keys_ok)]
    return GEMINI_BASE_URL + key, key


def gemini_limit(key):
    _gemini_limited.add(key)
    _gemini_key_index[0] += 1
    sisa = len([k for k in GEMINI_API_KEYS if k not in _gemini_limited])
    if sisa > 0:
        log.warning(f"  Gemini key limit! Rotasi → {sisa} key tersisa.")
    else:
        log.warning("  Semua Gemini key limit hari ini.")


def gemini_call(prompt, is_json=False):
    """Panggil Gemini AI dengan rotasi key otomatis."""
    payload = json.dumps({
        "contents": [{"parts": [{"text": prompt}]}]
    }).encode("utf-8")

    for attempt in range(len(GEMINI_API_KEYS) * 2):
        url, key = gemini_get_url()
        req = urllib.request.Request(
            url, data=payload,
            headers={"Content-Type": "application/json"},
            method="POST"
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
            log.warning(f"  Gemini timeout ({attempt+1}), coba lagi...")
            time.sleep(5)
            continue
        except urllib.error.HTTPError as e:
            if e.code == 429:
                gemini_limit(key)
                keys_ok = [k for k in GEMINI_API_KEYS if k not in _gemini_limited]
                if not keys_ok:
                    log.warning("  Semua key limit, tunggu 60 detik...")
                    time.sleep(60)
                    _gemini_limited.clear()
                continue
            elif e.code in (401, 403):
                log.error(f"  Gemini key tidak valid (HTTP {e.code}), skip.")
                gemini_limit(key)
                continue
            log.error(f"  Gemini HTTP {e.code}")
            break
        except (json.JSONDecodeError, KeyError):
            log.error("  Gemini respons tidak valid")
            break
        except Exception as e:
            log.error(f"  Gemini error: {e}")
            break
    return None

# ═══════════════════════════════════════════════════════════════
# CHECKPOINT & SENT IDS
# ═══════════════════════════════════════════════════════════════

def load_json_file(filepath, backup_path, default):
    for path, label in [(filepath, "utama"), (backup_path, "backup")]:
        if not os.path.exists(path):
            continue
        try:
            with open(path, "r", encoding="utf-8") as f:
                return json.load(f)
        except Exception as e:
            log.warning(f"  File {label} rusak: {e}")
    return default


def save_json_file(filepath, backup_path, data):
    tmp = filepath + ".tmp"
    try:
        with open(tmp, "w", encoding="utf-8") as f:
            json.dump(data, f, ensure_ascii=False, indent=2)
        if os.path.exists(filepath):
            shutil.copy2(filepath, backup_path)
        os.replace(tmp, filepath)
    except Exception as e:
        log.error(f"  Gagal tulis {filepath}: {e}")
    finally:
        if os.path.exists(tmp):
            try: os.remove(tmp)
            except: pass


def load_checkpoint():
    data = load_json_file(CHECKPOINT_FILE, CHECKPOINT_BACKUP_FILE, {"processed_ids": []})
    ids  = set(data.get("processed_ids", []))
    log.info(f"  Checkpoint: {len(ids)} email sudah diproses.")
    return ids


def save_checkpoint(ids):
    lst = list(ids)[-50000:]
    save_json_file(CHECKPOINT_FILE, CHECKPOINT_BACKUP_FILE, {"processed_ids": lst})


def load_sent_ids():
    data = load_json_file(SENT_IDS_FILE, SENT_IDS_BACKUP_FILE, {"sent": {}})
    sent = data.get("sent", {})
    log.info(f"  Sent IDs: {len(sent)} email sudah pernah dikirimi undangan.")
    return sent


def save_sent_ids(sent):
    save_json_file(SENT_IDS_FILE, SENT_IDS_BACKUP_FILE, {"sent": sent})

# ═══════════════════════════════════════════════════════════════
# STATISTIK HARIAN
# ═══════════════════════════════════════════════════════════════

def load_stats():
    today = datetime.now().strftime("%Y-%m-%d")
    data  = load_json_file(STATS_FILE, STATS_FILE, {})
    if data.get("date") == today:
        return data
    return {"date": today, "masuk": 0, "undangan": 0, "gagal": 0, "bounce": 0}


def save_stats(stats):
    try:
        with open(STATS_FILE, "w", encoding="utf-8") as f:
            json.dump(stats, f, ensure_ascii=False, indent=2)
    except Exception as e:
        log.warning(f"  Gagal simpan statistik: {e}")


def log_stats(stats):
    log.info(
        f"📊 Statistik ({stats['date']}): "
        f"{stats['masuk']} email masuk | "
        f"{stats['undangan']} undangan terkirim | "
        f"{stats['gagal']} gagal | "
        f"{stats['bounce']} bounce"
    )

# ═══════════════════════════════════════════════════════════════
# KONEKSI GOOGLE SHEETS
# ═══════════════════════════════════════════════════════════════

def connect_sheets(force=False):
    now     = time.time()
    elapsed = (now - _sheet_state["last_connected"]) / 60

    if not force and _sheet_state["mentah"] and elapsed < SHEET_REFRESH_MINUTES:
        return _sheet_state["mentah"], _sheet_state["bersih"]

    scopes = [
        "https://www.googleapis.com/auth/spreadsheets",
        "https://www.googleapis.com/auth/drive",
    ]

    # Coba baca credentials dari Environment Variable dulu (Railway)
    # Kalau tidak ada, baca dari file credentials.json (PC lokal)
    creds_json = os.environ.get("GOOGLE_CREDENTIALS_JSON", "")
    if creds_json:
        import json as _json
        from google.oauth2.service_account import Credentials as _Creds
        creds_info = _json.loads(creds_json)
        creds = _Creds.from_service_account_info(creds_info, scopes=scopes)
        log.info("  Credentials dari Environment Variable.")
    else:
        cred_file = SHEETS_CONFIG["credentials_file"]
        if not os.path.exists(cred_file):
            raise FileNotFoundError(
                f"File '{cred_file}' tidak ditemukan!\n"
                f"Solusi Railway: set env var GOOGLE_CREDENTIALS_JSON\n"
                f"Solusi lokal  : letakkan credentials.json di folder yang sama"
            )
        creds = Credentials.from_service_account_file(cred_file, scopes=scopes)
        log.info("  Credentials dari file credentials.json.")

    gc = gspread.authorize(creds)
    sh    = gc.open_by_key(SHEETS_CONFIG["spreadsheet_id"])

    # Sheet mentah
    try:
        ws_mentah = sh.worksheet(SHEETS_CONFIG["sheet_mentah"])
    except gspread.WorksheetNotFound:
        ws_mentah = sh.add_worksheet(SHEETS_CONFIG["sheet_mentah"], 2000, 10)

    # Sheet bersih
    try:
        ws_bersih = sh.worksheet(SHEETS_CONFIG["sheet_bersih"])
    except gspread.WorksheetNotFound:
        ws_bersih = sh.add_worksheet(SHEETS_CONFIG["sheet_bersih"], 2000, 15)

    _sheet_state["mentah"]         = ws_mentah
    _sheet_state["bersih"]         = ws_bersih
    _sheet_state["last_connected"] = now
    if force:
        log.info("  ✓ Koneksi Google Sheets diperbarui.")
    return ws_mentah, ws_bersih


def sheet_retry(func, *args, **kwargs):
    """Jalankan fungsi sheet dengan retry otomatis."""
    for attempt in range(MAX_SHEET_RETRIES):
        try:
            return func(*args, **kwargs)
        except gspread.exceptions.APIError as e:
            wait = 20 * (attempt + 1)
            log.warning(f"  Sheet API error, tunggu {wait}s ({attempt+1}/{MAX_SHEET_RETRIES}): {e}")
            connect_sheets(force=True)
            time.sleep(wait)
        except Exception as e:
            log.error(f"  Sheet error: {e}")
            time.sleep(10)
            break
    return None

# ═══════════════════════════════════════════════════════════════
# HELPER UMUM
# ═══════════════════════════════════════════════════════════════

def check_internet():
    try:
        socket.setdefaulttimeout(5)
        socket.create_connection(("8.8.8.8", 53))
        return True
    except OSError:
        return False


def wait_internet(max_attempts=10):
    for i in range(max_attempts):
        if check_internet():
            return True
        wait = 15 * (i + 1)
        log.warning(f"  Tidak ada internet, tunggu {wait}s... ({i+1}/{max_attempts})")
        time.sleep(wait)
    return False


def decode_str(s):
    if s is None: return ""
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
                        part.get_content_charset() or "utf-8", errors="replace"
                    )
                    break
                except: pass
    else:
        try:
            body = msg.get_payload(decode=True).decode(
                msg.get_content_charset() or "utf-8", errors="replace"
            )
        except: pass
    return body[:1000]


def make_unique_id(msg_id, sender, subject):
    if msg_id and msg_id.strip():
        return msg_id.strip()
    raw = f"{sender.strip()}|{subject.strip()}".encode("utf-8")
    return "HASH-" + hashlib.sha256(raw).hexdigest()[:24]


def normalize_date(date_str):
    if not date_str:
        return datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    try:
        return parsedate_to_datetime(date_str).strftime("%Y-%m-%d %H:%M:%S")
    except:
        return date_str


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
    except (IndexError, AttributeError):
        return default


def validasi_email(email_str):
    return bool(re.match(r'^[\w\.\+\-]+@[\w\.\-]+\.\w{2,}$', email_str.strip()))


def is_bounce(subject, sender):
    txt = (subject + " " + sender).lower()
    for kw in BOUNCE_KEYWORDS:
        if kw in txt:
            return True, kw
    return False, ""


def format_no_wa(no_wa):
    no = re.sub(r'[\s\-\(\)]', '', no_wa)
    if no.startswith("0"):
        no = "62" + no[1:]
    elif not no.startswith("62"):
        no = "62" + no
    return no

# ═══════════════════════════════════════════════════════════════
# EKSTRAK NAMA VIA GEMINI
# ═══════════════════════════════════════════════════════════════

def ekstrak_nama_email(subject, sender, body):
    """Ekstrak nama & email dari header, fallback ke Gemini AI."""
    email_match  = re.search(r'[\w\.\+\-]+@[\w\.\-]+\.\w+', sender)
    sender_email = email_match.group(0) if email_match else "-"
    name_match   = re.match(r'^"?([^"<\n]+?)"?\s*<', sender)
    sender_name  = name_match.group(1).strip() if name_match else ""
    sender_name  = re.sub(r'[\x00-\x1f]', '', sender_name).strip()
    if sender_name.lower() == sender_email.lower():
        sender_name = ""

    if sender_name and len(sender_name) >= 2:
        return {"nama": sender_name, "email": sender_email}

    # Bersihkan karakter aneh sebelum kirim ke Gemini (cegah HTTP 400)
    sender_safe  = re.sub(r'[\x00-\x1f\x7f]', '', sender).strip()[:200]
    subject_safe = re.sub(r'[\x00-\x1f\x7f]', '', subject).strip()[:200]
    body_safe    = re.sub(r'[\x00-\x1f\x7f]', '', body[:300]).strip()

    prompt = f"""Dari data email berikut, ekstrak nama pengirim dan alamat email.
Jika tidak ditemukan, isi dengan tanda "-".
Balas HANYA dengan JSON, tanpa penjelasan, tanpa markdown.
Format: {{"nama": "...", "email": "..."}}
Dari: {sender_safe}
Subject: {subject_safe}
Isi: {body_safe}"""

    result = gemini_call(prompt, is_json=True)
    if result:
        return result

    fallback = sender_name if sender_name else (sender_email if sender_email != "-" else "-")
    return {"nama": fallback, "email": sender_email}


def nama_sudah_valid(nama):
    """
    Cek apakah nama sudah valid tanpa perlu Gemini AI.
    Valid = mengandung huruf, tidak ada prefix aneh, tidak semua angka.
    Contoh valid   : 'Zahav Creative', 'Difa Fitalokalya', 'Arum'
    Contoh tidak valid: 'smqtmuda040', 'Admin_Arum', 'Lamaran_Admin'
    """
    if not nama or len(nama.strip()) < 2:
        return False
    nama = nama.strip()
    # Ada prefix yang perlu dibersihkan
    PREFIX_KOTOR = r'(?i)^(admin_|lamaran_|re:\s*|fwd:\s*|lamaran\s+|apply\s+|aplikasi\s+)' 
    if re.match(PREFIX_KOTOR, nama):
        return False
    # Semua karakter adalah angka/simbol (bukan nama)
    if not any(c.isalpha() for c in nama):
        return False
    # Username style: huruf+angka tanpa spasi, misal smqtmuda040
    if re.match(r'^[a-z0-9_.]+$', nama.lower()) and not ' ' in nama and any(c.isdigit() for c in nama):
        return False
    # Terlalu panjang kemungkinan bukan nama
    if len(nama) > 60:
        return False
    return True


def bersihkan_nama_manual(nama_kotor):
    """Bersihkan nama secara lokal tanpa Gemini AI."""
    nama = re.sub(r'(?i)(admin_?|lamaran_?|re:\s*|fwd:\s*|lamaran\s+)', '', nama_kotor)
    nama = nama.replace('_', ' ').replace('.', ' ').strip().title()
    return nama if nama and any(c.isalpha() for c in nama) else "Kandidat"


def bersihkan_nama(nama_kotor):
    """
    Bersihkan nama pelamar.
    - Jika nama sudah valid → langsung pakai, TANPA panggil Gemini (hemat kuota)
    - Jika nama perlu dibersihkan prefix → bersihkan manual, TANPA Gemini
    - Hanya panggil Gemini jika nama benar-benar tidak bisa dibersihkan manual
    """
    if not nama_kotor or nama_kotor.strip() in ("-", ""):
        return "Kandidat"

    nama_kotor = nama_kotor.strip()

    # ── Cek 1: Nama sudah valid, langsung pakai ───────────────
    if nama_sudah_valid(nama_kotor):
        return nama_kotor.title()

    # ── Cek 2: Ada prefix kotor, coba bersihkan manual ───────
    nama_manual = bersihkan_nama_manual(nama_kotor)
    if nama_manual != "Kandidat" and nama_sudah_valid(nama_manual):
        log.info(f"    Nama dibersihkan manual: '{nama_kotor}' → '{nama_manual}'")
        return nama_manual

    # ── Cek 3: Baru panggil Gemini jika benar-benar perlu ────
    log.info(f"    Nama tidak dikenali, tanya Gemini AI...")
    nama_safe = re.sub(r'[\x00-\x1f\x7f]', '', nama_kotor).strip()[:200]

    prompt = f"""Tugas: bersihkan nama orang dari teks berikut.
Aturan:
- Jika teks adalah nama orang yang valid, kembalikan dalam format Title Case
- Jika teks adalah username/email/kode (contoh: smqtmuda040), kembalikan: Kandidat
- Hapus prefix tidak relevan: Admin_, Re:, Lamaran_, Fwd:
- Balas HANYA nama bersih saja, tanpa tanda kutip, tanpa penjelasan
Teks: {nama_safe}"""

    result = gemini_call(prompt, is_json=False)
    if result:
        result = re.sub(r'["\']', '', result).strip()
        if 2 <= len(result) <= 60:
            return result

    # Fallback akhir
    return nama_manual if nama_manual != "Kandidat" else "Kandidat"

# ═══════════════════════════════════════════════════════════════
# TAHAP 1 — KIRIM UNDANGAN WAWANCARA
# ═══════════════════════════════════════════════════════════════

def buat_email_html(nama, posisi, platform, nama_hr, no_wa, no_wa_link):
    pesan_wa = f"Undangan+Wawancara+{posisi.replace(' ', '+')}."
    link_wa  = f"https://api.whatsapp.com/send/?phone={no_wa_link}&text={pesan_wa}&type=phone_number&app_absent=0"
    return f"""<!DOCTYPE html>
<html><head><meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0"></head>
<body style="margin:0;padding:0;background:#f4f4f4;font-family:Verdana,Geneva,sans-serif;">
<table style="max-width:600px;margin:40px auto;background:#fff;border-radius:12px;
              overflow:hidden;border:1px solid #e0e0e0;box-shadow:0 4px 8px rgba(0,0,0,0.1);"
       border="0" width="600" cellspacing="0" cellpadding="0" align="center">
  <tr><td style="background:linear-gradient(135deg,#7b4397 0%,#dc2430 100%);
                 text-align:center;color:#fff;padding:30px 20px;
                 border-bottom:4px solid rgba(255,255,255,0.2);">
    <h1 style="margin:0;font-size:24px;font-weight:600;letter-spacing:1px;">{NAMA_PERUSAHAAN}</h1>
    <p style="margin:8px 0 0;font-size:14px;color:#f0f0f0;font-weight:300;">Divisi Human Resources</p>
  </td></tr>
  <tr><td style="padding:30px 35px;font-size:16px;color:#333;line-height:1.7;">
    <p style="margin:0 0 20px 0;"><strong>Kepada {nama},</strong></p>
    <p style="margin:0 0 20px 0;">Terima kasih telah melamar posisi <strong>{posisi}</strong> di
    <strong>{NAMA_PERUSAHAAN}</strong> melalui platform <strong>{platform}</strong>.
    Kami menghargai waktu dan usaha Anda dalam proses lamaran ini.</p>
    <p style="margin:0 0 20px 0;">Setelah meninjau dokumen dan informasi yang Anda kirimkan, kami dengan
    senang hati mengundang Anda untuk mengikuti <strong>wawancara online</strong> sebagai
    tahap berikutnya dari proses seleksi.</p>
    <p style="margin:0 0 15px 0;">Silakan konfirmasi kehadiran Anda dengan menghubungi kami segera:</p>
    <p style="margin:0 0 20px 0;padding:15px 20px;background:#f9f0ff;
              border-left:4px solid #7b4397;border-radius:4px;">
      <strong>Nama&nbsp;&nbsp;&nbsp;: {nama_hr}</strong><br>
      <strong>WhatsApp: {no_wa}</strong>
    </p>
    <p style="text-align:center;margin:25px 0;">
      <a href="{link_wa}" style="display:inline-block;background:#25d366;color:#fff;
         text-decoration:none;padding:12px 28px;border-radius:8px;font-size:16px;font-weight:600;"
         target="_blank">💬 Kirim Pesan WhatsApp</a>
    </p>
    <p style="margin:0 0 12px 0;">Mohon sertakan dokumen berikut saat konfirmasi:</p>
    <ul style="margin:0 0 20px 0;padding-left:25px;">
      <li style="margin-bottom:8px;">CV terbaru dalam format PDF</li>
      <li style="margin-bottom:8px;">Tangkapan layar email undangan ini</li>
      <li style="margin-bottom:8px;">Data diri singkat (Nama Lengkap &amp; Alamat Email)</li>
    </ul>
    <p style="margin:0 0 20px 0;">Apabila ada pertanyaan, jangan ragu menghubungi kami melalui email di bawah.</p>
    <p style="margin:0;">Hormat kami,<br>
    <strong>Divisi Human Resources<br>{NAMA_PERUSAHAAN}</strong></p>
  </td></tr>
  <tr><td style="background:#f8f8f8;text-align:center;font-size:14px;color:#777;
                 padding:20px 15px;border-top:1px solid #eee;">
    <p style="margin:0;">Email resmi:
    <a href="mailto:{EMAIL_RESMI}" style="color:#1a73e8;text-decoration:none;">{EMAIL_RESMI}</a></p>
    <small style="display:block;color:#999;margin-top:10px;font-size:12px;">
      Harap abaikan email ini jika Anda tidak pernah melamar posisi di {NAMA_PERUSAHAAN}.
    </small>
  </td></tr>
</table></body></html>"""


def kirim_smtp(email_tujuan, nama, posisi, platform, nama_hr, no_wa):
    """Kirim email via SMTP Hostinger."""
    try:
        no_wa_link = format_no_wa(no_wa)
        msg = MIMEMultipart("alternative")
        msg["Subject"] = f"Undangan Wawancara {posisi} — {NAMA_PERUSAHAAN}"
        msg["From"]    = f"{NAMA_PERUSAHAAN} HR <{EMAIL_CONFIG['email']}>"
        msg["To"]      = email_tujuan
        msg.attach(MIMEText(buat_email_html(nama, posisi, platform, nama_hr, no_wa, no_wa_link), "html", "utf-8"))

        with smtplib.SMTP_SSL(EMAIL_CONFIG["smtp_host"], EMAIL_CONFIG["smtp_port"]) as server:
            server.login(EMAIL_CONFIG["email"], EMAIL_CONFIG["password"])
            server.sendmail(EMAIL_CONFIG["email"], email_tujuan, msg.as_string())
        return True, ""
    except smtplib.SMTPAuthenticationError:
        return False, "LOGIN_GAGAL"
    except smtplib.SMTPRecipientsRefused:
        return False, "Email tujuan ditolak"
    except Exception as e:
        return False, str(e)


def proses_kirim_undangan(ws_bersih, sent_ids, stats):
    """Tahap 1: Scan sheet Data Bersih, kirim undangan untuk status 'kirim'."""
    log.info("── TAHAP 1: Cek undangan wawancara ──────────────────────")

    try:
        semua = ws_bersih.get_all_values()
    except Exception as e:
        log.error(f"  Gagal baca sheet: {e}")
        return sent_ids

    if len(semua) <= 1:
        log.info("  Sheet kosong.")
        return sent_ids

    # Baris 1 = header milik user (skip)
    # Baris 2 ke bawah = data pelamar
    rows   = semua[1:]
    antrian = []

    for i, row in enumerate(rows, start=2):
        status = ambil_nilai(row, COL_STATUS).lower().strip()
        if status != "kirim":
            continue
        em = ambil_nilai(row, COL_EMAIL)
        if not em or not validasi_email(em):
            sheet_retry(ws_bersih.update_cell, i, COL_STATUS, "Email tidak valid ✗")
            continue
        antrian.append({
            "baris":    i,
            "nama":     ambil_nilai(row, COL_NAMA, "Kandidat"),
            "email":    em,
            "posisi":   ambil_nilai(row, COL_POSISI,   DEFAULT_UNDANGAN["posisi"]),
            "platform": ambil_nilai(row, COL_PLATFORM, DEFAULT_UNDANGAN["platform"]),
            "nama_hr":  ambil_nilai(row, COL_NAMA_HR,  DEFAULT_UNDANGAN["nama_hr"]),
            "no_wa":    ambil_nilai(row, COL_NO_WA,    DEFAULT_UNDANGAN["no_wa"]),
        })

    if not antrian:
        log.info("  Tidak ada undangan yang perlu dikirim.")
        return sent_ids

    # Batasi per siklus
    if len(antrian) > BATAS_UNDANGAN_SIKLUS:
        log.info(f"  {len(antrian)} undangan, batasi {BATAS_UNDANGAN_SIKLUS} per siklus.")
        antrian = antrian[:BATAS_UNDANGAN_SIKLUS]

    log.info(f"  Ditemukan {len(antrian)} undangan akan dikirim.")

    for idx, item in enumerate(antrian):
        log.info(f"  → [{idx+1}/{len(antrian)}] {item['email']}")

        # Hitung berapa kali sudah dikirim
        kali_kirim = sent_ids.get(item["email"], 0) + 1

        # Hapus dari sent_ids agar bisa kirim ulang
        if item["email"] in sent_ids:
            log.info(f"    Kirim ulang (ke-{kali_kirim}x)")

        # Bersihkan nama
        nama_bersih = bersihkan_nama(item["nama"])
        log.info(f"    Nama: '{item['nama']}' → '{nama_bersih}'")

        # Kirim email
        sukses, err = kirim_smtp(
            item["email"], nama_bersih,
            item["posisi"], item["platform"],
            item["nama_hr"], item["no_wa"]
        )

        waktu = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

        if sukses:
            sent_ids[item["email"]] = kali_kirim
            save_sent_ids(sent_ids)
            status_baru = f"Terkirim ✓" if kali_kirim == 1 else f"Terkirim ✓ ({kali_kirim}x)"
            sheet_retry(ws_bersih.update_cell, item["baris"], COL_STATUS,      status_baru)
            sheet_retry(ws_bersih.update_cell, item["baris"], COL_WAKTU_KIRIM, waktu)
            stats["undangan"] += 1
            save_stats(stats)
            log.info(f"    ✓ {status_baru}")
            _total_undangan[0] += 1
        else:
            sheet_retry(ws_bersih.update_cell, item["baris"], COL_STATUS,      "Gagal ✗")
            sheet_retry(ws_bersih.update_cell, item["baris"], COL_WAKTU_KIRIM, waktu)
            stats["gagal"] += 1
            save_stats(stats)
            log.error(f"    ✗ Gagal: {err}")
            if err == "LOGIN_GAGAL":
                log.critical("  Login SMTP gagal! Cek password. Bot dihentikan.")
                sys.exit(1)

        # Istirahat anti-spam per 100 email
        if _total_undangan[0] > 0 and _total_undangan[0] % BATAS_KIRIM_100 == 0:
            log.warning(f"  ⏸ {_total_undangan[0]} undangan terkirim. Istirahat 1 jam...")
            time.sleep(JEDA_PER_100_UNDANGAN)

        if idx < len(antrian) - 1:
            time.sleep(JEDA_ANTAR_UNDANGAN)

    return sent_ids

# ═══════════════════════════════════════════════════════════════
# TAHAP 2 — MONITOR EMAIL MASUK
# ═══════════════════════════════════════════════════════════════

def connect_imap():
    socket.setdefaulttimeout(IMAP_TIMEOUT)
    mail = imaplib.IMAP4_SSL(EMAIL_CONFIG["imap_host"], EMAIL_CONFIG["imap_port"])
    mail.login(EMAIL_CONFIG["email"], EMAIL_CONFIG["password"])
    mail.select(EMAIL_CONFIG["folder"])
    return mail


def safe_logout(mail):
    try: mail.logout()
    except: pass


def flush_read_queue():
    if not read_queue:
        return
    for attempt in range(MAX_IMAP_RETRIES):
        mail = None
        try:
            mail = connect_imap()
            for eid in read_queue:
                try: mail.store(eid, "+FLAGS", "\\Seen")
                except: pass
            log.info(f"  ✓ {len(read_queue)} email ditandai DIBACA.")
            read_queue.clear()
            return
        except Exception as e:
            log.warning(f"  Gagal flush read_queue ({attempt+1}): {e}")
            time.sleep(10 * (attempt + 1))
        finally:
            if mail: safe_logout(mail)


def fetch_emails(processed_ids, ws_mentah, stats):
    """Tahap 2: Ambil email baru dari IMAP dan simpan ke sheet."""
    log.info("── TAHAP 2: Monitor email masuk ─────────────────────────")

    if not check_internet():
        log.warning("  Tidak ada internet, skip.")
        return

    mail = None
    try:
        mail = connect_imap()
        _, data  = mail.search(None, "UNSEEN")
        email_ids = data[0].split()

        if not email_ids:
            log.info("  Tidak ada email baru.")
            return

        batch = email_ids[:MAX_QUEUE_SIZE]
        log.info(f"  {len(email_ids)} email UNREAD, proses {len(batch)}.")

        # Ambil nomor urut terakhir dari sheet
        try:
            col_a  = ws_mentah.col_values(1)
            row_no = sum(1 for v in col_a if v.strip().isdigit())
        except:
            row_no = 0

        for eid in batch:
            try:
                _, msg_data = mail.fetch(eid, "(BODY.PEEK[])")
                raw     = msg_data[0][1]
                msg     = email.message_from_bytes(raw)
                subject = decode_str(msg.get("Subject", ""))
                sender  = decode_str(msg.get("From",    ""))
                raw_id  = msg.get("Message-ID", "")
                msg_id  = make_unique_id(raw_id, sender, subject)

                if msg_id in processed_ids:
                    read_queue.append(eid)
                    continue

                # Filter bounce
                bounce, kw = is_bounce(subject, sender)
                if bounce:
                    log.info(f"  ⏭ Bounce/auto-reply: '{kw}' — dilewati")
                    read_queue.append(eid)
                    stats["bounce"] += 1
                    save_stats(stats)
                    continue

                # Ekstrak nama & email
                body      = get_email_body(msg)
                extracted = ekstrak_nama_email(subject, sender, body)
                nama      = sanitize(extracted.get("nama",  "-"))
                em        = sanitize(extracted.get("email", sender))
                tanggal   = normalize_date(msg.get("Date", ""))
                subj_clean = sanitize(subject, max_len=300)

                row_no += 1
                row = [row_no, nama, em, tanggal, subj_clean, msg_id]

                # Simpan ke sheet
                saved = False
                for attempt in range(MAX_SHEET_RETRIES):
                    try:
                        ws_mentah.append_row(row)
                        saved = True
                        break
                    except Exception as e:
                        log.warning(f"  Sheet retry {attempt+1}: {e}")
                        connect_sheets(force=True)
                        _, ws_mentah = connect_sheets()
                        time.sleep(20 * (attempt + 1))

                if saved:
                    processed_ids.add(msg_id)
                    save_checkpoint(processed_ids)
                    read_queue.append(eid)
                    stats["masuk"] += 1
                    save_stats(stats)
                    log.info(f"  ✓ [{row_no}] {nama} | {em}")
                else:
                    row_no -= 1
                    log.error(f"  ✗ Gagal simpan: {nama} | {em}")

                time.sleep(DELAY_BETWEEN_EMAILS)

            except Exception as e:
                log.error(f"  Gagal proses email {eid}: {e}")
                continue

        if read_queue:
            flush_read_queue()

    except imaplib.IMAP4.error as e:
        err = str(e)
        if "AUTHENTICATIONFAILED" in err or "LOGIN" in err:
            log.error("  ❌ Login IMAP gagal! Kemungkinan penyebab:")
            log.error("     1. Password salah di Environment Variable EMAIL_PASSWORD")
            log.error("     2. Hostinger memblokir login dari IP server Railway")
            log.error("     3. Perlu aktifkan akses IMAP di pengaturan Hostinger")
        else:
            log.error(f"  IMAP error: {e}")
    except socket.timeout:
        log.error("  Timeout IMAP")
    except Exception as e:
        log.error(f"  Error fetch email: {e}")
    finally:
        if mail: safe_logout(mail)

# ═══════════════════════════════════════════════════════════════
# TAHAP 3 — DEDUPLIKASI
# ═══════════════════════════════════════════════════════════════

def proses_deduplikasi(ws_mentah, ws_bersih):
    """Tahap 3: Deduplikasi Data Pelamar → tulis ke Data Bersih."""
    log.info("── TAHAP 3: Deduplikasi ─────────────────────────────────")

    try:
        data_mentah = ws_mentah.getDataRange().get_all_values() if hasattr(ws_mentah, 'getDataRange') else ws_mentah.get_all_values()
    except:
        try:
            data_mentah = ws_mentah.get_all_values()
        except Exception as e:
            log.error(f"  Gagal baca sheet mentah: {e}")
            return

    if len(data_mentah) <= 1:
        log.info("  Tidak ada data di sheet mentah.")
        return

    header = data_mentah[0]
    rows   = data_mentah[1:]

    # Deduplikasi berdasarkan email (kolom C)
    seen   = {}
    for row in rows:
        em = str(row[COL_EMAIL - 1] if len(row) >= COL_EMAIL else "").strip().lower()
        if not em or em in ("-", "email"): continue
        if em not in seen:
            seen[em] = row

    data_bersih = list(seen.values())

    # Urutkan terbaru di atas
    def parse_tgl(row):
        try:
            return datetime.strptime(str(row[COL_TANGGAL - 1])[:19], "%Y-%m-%d %H:%M:%S")
        except:
            return datetime.min

    data_bersih.sort(key=parse_tgl, reverse=True)

    # Nomor ulang
    for i, row in enumerate(data_bersih):
        while len(row) < COL_MSGID:
            row.append("")
        row[COL_NO - 1] = i + 1

    # Simpan data kolom G-L yang sudah diisi sebelumnya
    # Baris 1 = header milik user — TIDAK disentuh bot
    # Baris 2 ke bawah = data pelamar
    # Kolom P ke kanan = statistik (jauh dari data, tidak menimpa)
    try:
        semua_bersih = ws_bersih.get_all_values()
        email_ke_bot = {}
        # Skip baris 1 (milik user), baca dari baris 2
        for row in semua_bersih[1:]:
            em = str(row[COL_EMAIL - 1]).strip().lower() if len(row) >= COL_EMAIL else ""
            if em and em not in ("-", "email"):
                bot_data = list(row[COL_POSISI-1:COL_WAKTU_KIRIM]) if len(row) >= COL_WAKTU_KIRIM else list(row[COL_POSISI-1:])
                while len(bot_data) < 6:
                    bot_data.append("")
                email_ke_bot[em] = bot_data
    except Exception as e:
        log.warning(f"  Gagal simpan data kolom G-L: {e}")
        email_ke_bot = {}

    # Tulis ulang sheet bersih
    # Baris 1 = TIDAK DISENTUH (milik user)
    # Baris 2 ke bawah = data pelamar (A-L)
    # Kolom P baris 2-6 = statistik (tidak ganggu data)
    try:
        # Hitung baris terakhir yang ada datanya
        last_row = max(len(semua_bersih) + 5, len(data_bersih) + 10)

        # Bersihkan baris 2 ke bawah kolom A-L saja (bukan baris 1)
        ws_bersih.batch_clear([f"A2:L{last_row}"])

        # Tulis data bersih mulai baris 2
        if data_bersih:
            ws_bersih.update(data_bersih, "A2")

        # Kembalikan data kolom G-L dengan batch update
        if email_ke_bot:
            updates = []
            for j, row in enumerate(data_bersih, start=2):
                em_lower = str(row[COL_EMAIL - 1]).strip().lower()
                if em_lower in email_ke_bot:
                    bot_data = email_ke_bot[em_lower]
                    while len(bot_data) < 6:
                        bot_data.append("")
                    updates.append({
                        "range": f"G{j}:L{j}",
                        "values": [bot_data]
                    })
            if updates:
                ws_bersih.batch_update(updates)

        # Statistik di kolom P (kolom 16) — jauh dari data, tidak menimpa apapun
        now = datetime.now().strftime("%d/%m/%Y %H:%M")
        ws_bersih.update([
            ["📊 Statistik"],
            [f"Diperbarui: {now}"],
            [f"Total unik: {len(data_bersih)} pelamar"],
            [f"Data mentah: {len(rows)} baris"],
            [f"Duplikat: {len(rows) - len(data_bersih)}"],
        ], "P2:P6")

        log.info(f"  ✓ {len(data_bersih)} unik dari {len(rows)} baris. Duplikat: {len(rows) - len(data_bersih)}")
        log.info(f"  ✓ Baris 1 tidak diubah (milik user).")

    except Exception as e:
        log.error(f"  Gagal tulis sheet bersih: {e}")
        connect_sheets(force=True)

# ═══════════════════════════════════════════════════════════════
# MAIN LOOP
# ═══════════════════════════════════════════════════════════════

def main():
    log.info("=" * 66)
    log.info("   BOT PELAMAR KOMPLEKS — Himeya Agency v1.0")
    log.info(f"   Mulai  : {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    log.info(f"   Python : {sys.version.split()[0]}")
    log.info(f"   Gemini : {len(GEMINI_API_KEYS)} API key | {len(GEMINI_API_KEYS) * 1500} request/hari")
    log.info("=" * 66)

    # Validasi password
    if "ISI_PASSWORD" in EMAIL_CONFIG["password"]:
        log.critical("❌ Password email belum diisi! Edit bagian EMAIL_CONFIG.")
        sys.exit(1)

    # Cek internet
    log.info("Memeriksa koneksi internet...")
    if not wait_internet(max_attempts=5):
        log.critical("FATAL: Tidak ada koneksi internet.")
        sys.exit(1)
    log.info("✓ Internet tersambung.")

    # Tampilkan IP server (untuk whitelist di Hostinger)
    try:
        req = urllib.request.Request("https://api.ipify.org?format=json")
        with urllib.request.urlopen(req, timeout=10) as resp:
            ip_data = json.loads(resp.read().decode())
            server_ip = ip_data.get("ip", "tidak diketahui")
            log.info(f"🌐 IP Server Railway: {server_ip}")
            log.info(f"   → Tambahkan IP ini ke whitelist Hostinger!")
    except Exception as e:
        log.warning(f"  Tidak bisa cek IP server: {e}")
    log.info("")

    # Koneksi Google Sheets
    log.info("Menghubungkan ke Google Sheets...")
    try:
        ws_mentah, ws_bersih = connect_sheets()
        log.info("✓ Google Sheets terhubung.\n")
    except Exception as e:
        log.critical(f"FATAL: Tidak bisa konek ke Google Sheets: {e}")
        sys.exit(1)

    # Pastikan header sheet mentah
    try:
        header_mentah = ws_mentah.row_values(1)
        if not header_mentah:
            ws_mentah.insert_row(["No", "Nama", "Email", "Tanggal", "Subject", "Message-ID"], 1)
    except: pass

    # Load state
    processed_ids = load_checkpoint()
    sent_ids      = load_sent_ids()
    stats         = load_stats()
    log.info(f"  Checkpoint   : {len(processed_ids)} email diproses")
    log.info(f"  Sent IDs     : {len(sent_ids)} undangan terkirim")
    log.info(f"  Statistik    : {stats}")
    log.info("  Bot siap! Mulai monitoring...\n")

    consecutive_errors = 0
    last_stat_log      = time.time()

    while True:
        # Log statistik tiap 1 jam
        if time.time() - last_stat_log > 3600:
            log_stats(stats)
            last_stat_log = time.time()

        try:
            ws_mentah, ws_bersih = connect_sheets()

            # ── TAHAP 1: Kirim undangan ───────────────────────
            sent_ids = proses_kirim_undangan(ws_bersih, sent_ids, stats)

            # ── TAHAP 2: Monitor email masuk ──────────────────
            fetch_emails(processed_ids, ws_mentah, stats)

            # ── TAHAP 3: Deduplikasi ──────────────────────────
            proses_deduplikasi(ws_mentah, ws_bersih)

            consecutive_errors = 0

        except Exception as e:
            consecutive_errors += 1
            log.error(f"Error di main loop (ke-{consecutive_errors}): {e}")
            try:
                connect_sheets(force=True)
            except: pass

        if consecutive_errors >= 5:
            wait = CHECK_INTERVAL_MINUTES * 3
            log.warning(f"⚠ {consecutive_errors} error berturut-turut. Tunggu {wait} menit...")
            time.sleep(wait * 60)
            consecutive_errors = 0
        else:
            log.info(f"  Menunggu {CHECK_INTERVAL_MINUTES} menit...\n")
            time.sleep(CHECK_INTERVAL_MINUTES * 60)


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        log.info("\n⛔ Bot dihentikan oleh pengguna.")
    except Exception as e:
        log.critical(f"Bot berhenti karena error: {e}")
        sys.exit(1)
