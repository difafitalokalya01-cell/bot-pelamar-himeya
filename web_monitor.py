"""
web_monitor.py — Server HTTP ringan untuk Railway
Railway butuh port terbuka agar tidak mematikan service.
File ini juga menampilkan status bot secara sederhana.
"""

import os
import json
from flask import Flask, jsonify
from datetime import datetime

app = Flask(__name__)

def baca_stats(filepath):
    try:
        with open(filepath, "r", encoding="utf-8") as f:
            return json.load(f)
    except:
        return {}

def baca_config_cache(filepath):
    try:
        with open(filepath, "r", encoding="utf-8") as f:
            data = json.load(f)
            return {
                "perusahaan": data.get("Nama Perusahaan", "-"),
                "email": data.get("Email Pengirim", "-"),
                "heartbeat": data.get("Heartbeat", "-"),
            }
    except:
        return {}

@app.route("/")
def index():
    stats1  = baca_stats("daily_stats.json")
    stats2  = baca_stats("daily_stats2.json")
    cfg1    = baca_config_cache("config_cache.json")
    cfg2    = baca_config_cache("config_cache2.json")
    now_str = datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S UTC")

    html = f"""<!DOCTYPE html>
<html lang="id">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>Bot Pelamar — Status</title>
<style>
  body {{ font-family: Arial, sans-serif; background: #1a1a2e; color: #eee; margin: 0; padding: 20px; }}
  h1 {{ color: #e94560; }}
  .card {{ background: #16213e; border-radius: 10px; padding: 20px; margin: 15px 0; border-left: 4px solid #e94560; }}
  .card h2 {{ margin: 0 0 10px; color: #0f3460; color: #a8dadc; font-size: 1.1em; }}
  .row {{ display: flex; justify-content: space-between; padding: 5px 0; border-bottom: 1px solid #0f3460; }}
  .label {{ color: #aaa; }}
  .val {{ font-weight: bold; color: #e9c46a; }}
  .ok {{ color: #06d6a0; }}
  .footer {{ text-align: center; color: #555; font-size: 0.8em; margin-top: 30px; }}
</style>
</head>
<body>
<h1>🤖 Bot Pelamar — Himeya Agency</h1>
<p style="color:#aaa">Terakhir cek: {now_str}</p>

<div class="card">
  <h2>📦 Bot 1 — {cfg1.get('perusahaan', 'Bot Pelamar')}</h2>
  <div class="row"><span class="label">Email Pengirim</span><span class="val">{cfg1.get('email', '-')}</span></div>
  <div class="row"><span class="label">Heartbeat Terakhir</span><span class="val ok">{cfg1.get('heartbeat', '-')}</span></div>
  <div class="row"><span class="label">Tanggal Stats</span><span class="val">{stats1.get('date', '-')}</span></div>
  <div class="row"><span class="label">Undangan Terkirim Hari Ini</span><span class="val ok">{stats1.get('undangan', 0)}</span></div>
  <div class="row"><span class="label">Email Masuk</span><span class="val">{stats1.get('masuk', 0)}</span></div>
  <div class="row"><span class="label">Gagal</span><span class="val" style="color:#e94560">{stats1.get('gagal', 0)}</span></div>
</div>

<div class="card">
  <h2>📦 Bot 2 — {cfg2.get('perusahaan', 'Bot Pelamar 2')}</h2>
  <div class="row"><span class="label">Email Pengirim</span><span class="val">{cfg2.get('email', '-')}</span></div>
  <div class="row"><span class="label">Heartbeat Terakhir</span><span class="val ok">{cfg2.get('heartbeat', '-')}</span></div>
  <div class="row"><span class="label">Tanggal Stats</span><span class="val">{stats2.get('date', '-')}</span></div>
  <div class="row"><span class="label">Undangan Terkirim Hari Ini</span><span class="val ok">{stats2.get('undangan', 0)}</span></div>
  <div class="row"><span class="label">Gagal</span><span class="val" style="color:#e94560">{stats2.get('gagal', 0)}</span></div>
</div>

<div class="footer">Bot Pelamar v3.1 — Railway Deployment</div>
</body>
</html>"""
    return html

@app.route("/health")
def health():
    return jsonify({"status": "ok", "time": datetime.utcnow().isoformat()})

if __name__ == "__main__":
    port = int(os.environ.get("PORT", 8080))
    app.run(host="0.0.0.0", port=port)
