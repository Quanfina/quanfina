"""
HTTP wrapper for scanner.py — Cloud Run / Cloud Scheduler integration.

Endpoints:
  POST /scan         — run today's scan (skips if already done)
  POST /scan?force=1 — force rescan (deletes today's data first)
  GET  /health       — health check
"""

import os
import threading
import logging
from datetime import date, timedelta
from flask import Flask, jsonify, request

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger(__name__)

app = Flask(__name__)

_scan_lock = threading.Lock()
_scan_running = False


def _today_scan_date():
    today = date.today()
    if today.weekday() == 5:
        return str(today - timedelta(days=1))
    if today.weekday() == 6:
        return str(today - timedelta(days=2))
    return str(today)


def _existing_count(scan_date):
    from db_connection import get_connection
    conn = get_connection()
    c = conn.cursor()
    try:
        c.execute("SELECT COUNT(*) FROM minervini_scans WHERE scan_date = %s", (scan_date,))
        return c.fetchone()[0]
    except Exception:
        return 0
    finally:
        conn.close()


@app.route("/health", methods=["GET"])
def health():
    return jsonify({"status": "ok"}), 200


@app.route("/scan", methods=["POST"])
def scan():
    global _scan_running

    force = request.args.get("force", "0") == "1"
    if request.is_json:
        force = force or bool(request.json.get("force", False))

    scan_date = _today_scan_date()

    if not force:
        count = _existing_count(scan_date)
        if count > 0:
            log.info("Scan skipped — %d records already exist for %s", count, scan_date)
            return jsonify({
                "status": "skipped",
                "scan_date": scan_date,
                "existing_records": count,
            }), 200

    with _scan_lock:
        if _scan_running:
            return jsonify({"status": "busy", "message": "Scan already in progress"}), 409
        _scan_running = True

    os.environ["QUANFINA_NONINTERACTIVE"] = "force" if force else "skip"

    try:
        log.info("Starting scan for %s (force=%s)", scan_date, force)
        from scanner import run_scan
        run_scan()
        log.info("Scan completed for %s", scan_date)
        return jsonify({"status": "ok", "scan_date": scan_date}), 200
    except SystemExit:
        return jsonify({"status": "skipped", "scan_date": scan_date}), 200
    except Exception as e:
        log.exception("Scan failed: %s", e)
        return jsonify({"status": "error", "message": str(e)}), 500
    finally:
        with _scan_lock:
            _scan_running = False


if __name__ == "__main__":
    port = int(os.getenv("PORT", 8080))
    app.run(host="0.0.0.0", port=port)
