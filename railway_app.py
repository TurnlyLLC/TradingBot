#!/usr/bin/env python3
from __future__ import annotations

import atexit
import json
import os
import shutil
import signal
import sqlite3
import subprocess
import sys
import threading
import time
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional

from flask import Flask, jsonify, render_template_string, request, send_file

try:
    import boto3
except Exception:
    boto3 = None

BASE_DIR = Path(__file__).resolve().parent
DATA_DIR = Path(os.getenv("DATA_DIR", "/app/data"))
LOG_DIR = DATA_DIR / "logs"
BACKUP_DIR = DATA_DIR / "backups"
KEY_DIR = DATA_DIR / "secrets"
CONFIG_PATH = BASE_DIR / "config_signal_collector.json"
COLLECTOR_FILE = BASE_DIR / "kalshi_signal_collector.py"

for p in [DATA_DIR, LOG_DIR, BACKUP_DIR, KEY_DIR]:
    p.mkdir(parents=True, exist_ok=True)

COLLECTOR_STDOUT = LOG_DIR / "collector_stdout.log"
COLLECTOR_STDERR = LOG_DIR / "collector_stderr.log"
APP_LOG = LOG_DIR / "railway_app.log"
KEY_PATH = KEY_DIR / "kalshi_private_key.pem"
DB_PATH = DATA_DIR / "signal_collector.db"


def log(msg: str) -> None:
    line = f"[{datetime.now(timezone.utc).isoformat()}] {msg}"
    print(line, flush=True)
    with APP_LOG.open("a", encoding="utf-8") as f:
        f.write(line + "\n")


def ensure_private_key() -> None:
    pem_inline = os.getenv("KALSHI_PRIVATE_KEY_PEM", "").strip()
    pem_path = os.getenv("KALSHI_PRIVATE_KEY_PATH", "").strip()

    if pem_inline:
        text = pem_inline.replace("\\n", "\n")
        KEY_PATH.write_text(text, encoding="utf-8")
        os.environ["KALSHI_PRIVATE_KEY_PATH"] = str(KEY_PATH)
        return

    if pem_path and Path(pem_path).exists():
        os.environ["KALSHI_PRIVATE_KEY_PATH"] = pem_path
        return

    raise RuntimeError("Missing Kalshi private key. Set KALSHI_PRIVATE_KEY_PEM or KALSHI_PRIVATE_KEY_PATH.")


@dataclass
class CollectorManager:
    process: Optional[subprocess.Popen] = None
    stop_event: threading.Event = threading.Event()
    restart_count: int = 0
    last_start_ts: int = 0
    last_exit_code: Optional[int] = None
    monitor_thread: Optional[threading.Thread] = None

    def start(self) -> None:
        if self.process and self.process.poll() is None:
            return
        ensure_private_key()
        env = os.environ.copy()
        env.setdefault("PYTHONUNBUFFERED", "1")
        cmd = [sys.executable, str(COLLECTOR_FILE), "--config", str(CONFIG_PATH)]
        stdout = COLLECTOR_STDOUT.open("ab")
        stderr = COLLECTOR_STDERR.open("ab")
        self.process = subprocess.Popen(cmd, cwd=str(BASE_DIR), env=env, stdout=stdout, stderr=stderr)
        self.last_start_ts = int(time.time())
        log(f"collector_started pid={self.process.pid}")

    def stop(self) -> None:
        self.stop_event.set()
        if self.process and self.process.poll() is None:
            log("collector_stopping")
            self.process.terminate()
            try:
                self.process.wait(timeout=20)
            except subprocess.TimeoutExpired:
                self.process.kill()
        self.process = None

    def status(self) -> Dict[str, Any]:
        running = bool(self.process and self.process.poll() is None)
        return {
            "running": running,
            "pid": self.process.pid if running else None,
            "restart_count": self.restart_count,
            "last_start_ts": self.last_start_ts,
            "last_exit_code": self.last_exit_code,
        }

    def monitor(self) -> None:
        self.start()
        while not self.stop_event.is_set():
            if self.process is None:
                time.sleep(2)
                continue
            code = self.process.poll()
            if code is None:
                time.sleep(5)
                continue
            self.last_exit_code = code
            if self.stop_event.is_set():
                break
            self.restart_count += 1
            log(f"collector_crashed exit_code={code} restarting=true")
            time.sleep(5)
            self.start()


class BackupManager:
    def __init__(self, collector: CollectorManager):
        self.collector = collector
        self.stop_event = threading.Event()
        self.thread: Optional[threading.Thread] = None
        self.interval = int(os.getenv("BACKUP_INTERVAL_SECONDS", "900"))
        self.retention_days = int(os.getenv("RETENTION_DAYS", "3"))
        self.last_backup_file: Optional[str] = None
        self.last_backup_ts: Optional[int] = None
        self.last_backup_error: Optional[str] = None
        self.s3 = None
        self.bucket = os.getenv("BUCKET", "").strip()
        if boto3 and self.bucket and os.getenv("ACCESS_KEY_ID") and os.getenv("SECRET_ACCESS_KEY") and os.getenv("ENDPOINT"):
            self.s3 = boto3.client(
                "s3",
                aws_access_key_id=os.getenv("ACCESS_KEY_ID"),
                aws_secret_access_key=os.getenv("SECRET_ACCESS_KEY"),
                endpoint_url=os.getenv("ENDPOINT"),
                region_name=os.getenv("REGION", "auto"),
            )

    def perform_backup(self) -> None:
        if not DB_PATH.exists():
            return
        ts = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
        backup_db = BACKUP_DIR / f"signal_collector_{ts}.db"
        shutil.copy2(DB_PATH, backup_db)
        if APP_LOG.exists():
            shutil.copy2(APP_LOG, BACKUP_DIR / f"railway_app_{ts}.log")
        if COLLECTOR_STDOUT.exists():
            shutil.copy2(COLLECTOR_STDOUT, BACKUP_DIR / f"collector_stdout_{ts}.log")
        if COLLECTOR_STDERR.exists():
            shutil.copy2(COLLECTOR_STDERR, BACKUP_DIR / f"collector_stderr_{ts}.log")
        self.last_backup_file = str(backup_db)
        self.last_backup_ts = int(time.time())
        self.last_backup_error = None
        self.prune_local_backups()
        if self.s3:
            try:
                self.s3.upload_file(str(backup_db), self.bucket, f"backups/{backup_db.name}")
                if APP_LOG.exists():
                    self.s3.upload_file(str(APP_LOG), self.bucket, f"logs/{APP_LOG.name}")
                if COLLECTOR_STDOUT.exists():
                    self.s3.upload_file(str(COLLECTOR_STDOUT), self.bucket, f"logs/{COLLECTOR_STDOUT.name}")
                if COLLECTOR_STDERR.exists():
                    self.s3.upload_file(str(COLLECTOR_STDERR), self.bucket, f"logs/{COLLECTOR_STDERR.name}")
            except Exception as e:
                self.last_backup_error = str(e)
                log(f"backup_upload_error error={e}")

    def prune_local_backups(self) -> None:
        cutoff = time.time() - self.retention_days * 86400
        for p in BACKUP_DIR.glob("*"):
            try:
                if p.stat().st_mtime < cutoff:
                    p.unlink(missing_ok=True)
            except Exception:
                pass

    def monitor(self) -> None:
        while not self.stop_event.is_set():
            try:
                self.perform_backup()
            except Exception as e:
                self.last_backup_error = str(e)
                log(f"backup_error error={e}")
            self.stop_event.wait(self.interval)

    def start(self) -> None:
        self.thread = threading.Thread(target=self.monitor, daemon=True)
        self.thread.start()

    def stop(self) -> None:
        self.stop_event.set()

    def status(self) -> Dict[str, Any]:
        return {
            "bucket_configured": bool(self.s3),
            "bucket_name": self.bucket or None,
            "last_backup_file": self.last_backup_file,
            "last_backup_ts": self.last_backup_ts,
            "last_backup_error": self.last_backup_error,
            "backup_interval_seconds": self.interval,
        }


collector = CollectorManager()
backup_manager = BackupManager(collector)
app = Flask(__name__)

DASHBOARD_HTML = """
<!doctype html>
<html>
<head>
  <meta name="viewport" content="width=device-width, initial-scale=1" />
  <title>Kalshi Signal Collector Dashboard</title>
  <style>
    body { font-family: Arial, sans-serif; background: #0b1020; color: #ecf2ff; margin: 0; padding: 16px; }
    .grid { display: grid; grid-template-columns: repeat(auto-fit,minmax(180px,1fr)); gap: 12px; }
    .card { background: #141c33; border-radius: 16px; padding: 14px; box-shadow: 0 6px 20px rgba(0,0,0,0.25); }
    h1 { font-size: 24px; margin: 0 0 14px; }
    h2 { font-size: 16px; margin: 0 0 8px; color: #bcd1ff; }
    .big { font-size: 28px; font-weight: bold; }
    table { width: 100%; border-collapse: collapse; font-size: 13px; }
    th, td { text-align: left; padding: 8px; border-bottom: 1px solid #25304f; }
    .row { margin-top: 14px; }
    a, a:visited { color: #9cd0ff; }
    .ok { color: #7dffa0; }
    .bad { color: #ff9aa7; }
    .muted { color: #9fb1d9; }
  </style>
  <script>
    async function refresh() {
      const resp = await fetch('/api/dashboard');
      const data = await resp.json();
      document.getElementById('collector').textContent = data.collector.running ? 'Running' : 'Stopped';
      document.getElementById('collector').className = data.collector.running ? 'big ok' : 'big bad';
      document.getElementById('pending').textContent = data.counts.pending || 0;
      document.getElementById('labeled').textContent = data.counts.labeled || 0;
      document.getElementById('signals24').textContent = data.metrics.signals_last_24h || 0;
      document.getElementById('labeled24').textContent = data.metrics.labeled_last_24h || 0;
      document.getElementById('good24').textContent = data.metrics.good_last_24h || 0;
      document.getElementById('bad24').textContent = data.metrics.bad_last_24h || 0;
      document.getElementById('tracked').textContent = data.metrics.tracked_markets || 0;
      document.getElementById('dbsize').textContent = data.metrics.db_size_mb || 0;
      document.getElementById('restart').textContent = data.collector.restart_count || 0;
      document.getElementById('backup').textContent = data.backup.last_backup_ts ? new Date(data.backup.last_backup_ts*1000).toLocaleString() : 'none';
      const tbody = document.getElementById('recent');
      tbody.innerHTML = '';
      for (const row of data.recent_signals) {
        const tr = document.createElement('tr');
        tr.innerHTML = `<td>${row.created_at}</td><td>${row.market_ticker}</td><td>${row.signal_type}</td><td>${row.hypothetical_entry_price_cents}</td><td>${row.label_result || ''}</td><td>${row.label_quality ?? ''}</td>`;
        tbody.appendChild(tr);
      }
    }
    setInterval(refresh, 10000);
    window.onload = refresh;
  </script>
</head>
<body>
  <h1>Kalshi Signal Collector</h1>
  <div class="grid">
    <div class="card"><h2>Collector</h2><div id="collector" class="big">...</div><div class="muted">Auto-restart enabled</div></div>
    <div class="card"><h2>Pending labels</h2><div id="pending" class="big">0</div></div>
    <div class="card"><h2>Labeled</h2><div id="labeled" class="big">0</div></div>
    <div class="card"><h2>Signals 24h</h2><div id="signals24" class="big">0</div></div>
    <div class="card"><h2>Labeled 24h</h2><div id="labeled24" class="big">0</div></div>
    <div class="card"><h2>Good 24h</h2><div id="good24" class="big">0</div></div>
    <div class="card"><h2>Bad 24h</h2><div id="bad24" class="big">0</div></div>
    <div class="card"><h2>Tracked markets</h2><div id="tracked" class="big">0</div></div>
    <div class="card"><h2>DB size MB</h2><div id="dbsize" class="big">0</div></div>
    <div class="card"><h2>Restarts</h2><div id="restart" class="big">0</div></div>
    <div class="card"><h2>Last backup</h2><div id="backup" class="big" style="font-size:16px">none</div></div>
  </div>
  <div class="row card">
    <h2>Recent labeled signals</h2>
    <table>
      <thead><tr><th>Time</th><th>Ticker</th><th>Type</th><th>Entry</th><th>Result</th><th>Quality</th></tr></thead>
      <tbody id="recent"></tbody>
    </table>
  </div>
  <div class="row muted">
    <a href="/api/dashboard">JSON</a> · <a href="/download/db">Download DB</a> · <a href="/health">Health</a>
  </div>
</body>
</html>
"""


def db_conn() -> sqlite3.Connection:
    conn = sqlite3.connect(str(DB_PATH))
    conn.row_factory = sqlite3.Row
    return conn


def query_one(sql: str, params: tuple = ()) -> Any:
    if not DB_PATH.exists():
        return None
    with db_conn() as conn:
        return conn.execute(sql, params).fetchone()


def query_all(sql: str, params: tuple = ()) -> List[sqlite3.Row]:
    if not DB_PATH.exists():
        return []
    with db_conn() as conn:
        return conn.execute(sql, params).fetchall()


def dashboard_payload() -> Dict[str, Any]:
    counts = {"pending": 0, "labeled": 0}
    if DB_PATH.exists():
        rows = query_all("SELECT status, COUNT(*) AS n FROM signal_events GROUP BY status")
        for r in rows:
            counts[r["status"]] = r["n"]

    since = int(time.time()) - 86400
    signals24 = query_one("SELECT COUNT(*) AS n FROM signal_events WHERE created_ts >= ?", (since,))
    labeled24 = query_one("SELECT COUNT(*) AS n FROM signal_events WHERE label_ts IS NOT NULL AND label_ts >= ?", (since,))
    good24 = query_one("SELECT COUNT(*) AS n FROM signal_events WHERE label_ts >= ? AND label_quality = 1", (since,))
    bad24 = query_one("SELECT COUNT(*) AS n FROM signal_events WHERE label_ts >= ? AND label_quality = 0", (since,))
    tracked = query_one("SELECT value FROM collector_stats WHERE key = 'last_universe_market_count'")
    recent = query_all(
        """
        SELECT market_ticker, signal_type, hypothetical_entry_price_cents, label_result, label_quality, created_ts
        FROM signal_events
        WHERE label_ts IS NOT NULL
        ORDER BY label_ts DESC
        LIMIT 50
        """
    )

    recent_rows = []
    for r in recent:
        recent_rows.append({
            "market_ticker": r["market_ticker"],
            "signal_type": r["signal_type"],
            "hypothetical_entry_price_cents": r["hypothetical_entry_price_cents"],
            "label_result": r["label_result"],
            "label_quality": r["label_quality"],
            "created_at": datetime.fromtimestamp(r["created_ts"], tz=timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC"),
        })

    metrics = {
        "signals_last_24h": int(signals24["n"]) if signals24 else 0,
        "labeled_last_24h": int(labeled24["n"]) if labeled24 else 0,
        "good_last_24h": int(good24["n"]) if good24 else 0,
        "bad_last_24h": int(bad24["n"]) if bad24 else 0,
        "tracked_markets": int(tracked["value"]) if tracked and str(tracked["value"]).isdigit() else (tracked["value"] if tracked else 0),
        "db_size_mb": round(DB_PATH.stat().st_size / (1024 * 1024), 2) if DB_PATH.exists() else 0,
    }
    return {
        "collector": collector.status(),
        "backup": backup_manager.status(),
        "counts": counts,
        "metrics": metrics,
        "recent_signals": recent_rows,
    }


@app.route('/')
def home():
    return render_template_string(DASHBOARD_HTML)


@app.route('/health')
def health():
    payload = dashboard_payload()
    return jsonify({"ok": True, **payload})


@app.route('/api/dashboard')
def api_dashboard():
    return jsonify(dashboard_payload())


@app.route('/download/db')
def download_db():
    if not DB_PATH.exists():
        return jsonify({"error": "database not ready"}), 404
    return send_file(DB_PATH, as_attachment=True, download_name='signal_collector.db')


@app.route('/admin/restart', methods=['POST'])
def admin_restart():
    token = os.getenv('ADMIN_TOKEN', '')
    supplied = request.headers.get('X-Admin-Token', '')
    if token and supplied != token:
        return jsonify({"error": "unauthorized"}), 401
    collector.stop()
    time.sleep(2)
    collector.stop_event = threading.Event()
    collector.monitor_thread = threading.Thread(target=collector.monitor, daemon=True)
    collector.monitor_thread.start()
    return jsonify({"ok": True})


def boot_background_workers() -> None:
    collector.monitor_thread = threading.Thread(target=collector.monitor, daemon=True)
    collector.monitor_thread.start()
    backup_manager.start()


def shutdown(*_args):
    log('shutdown_requested')
    backup_manager.stop()
    collector.stop()


signal.signal(signal.SIGTERM, shutdown)
signal.signal(signal.SIGINT, shutdown)
atexit.register(shutdown)

if __name__ == '__main__':
    log('railway_app_starting')
    boot_background_workers()
    port = int(os.getenv('PORT', '8080'))
    app.run(host='0.0.0.0', port=port, threaded=True)
