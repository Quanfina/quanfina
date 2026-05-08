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
from scanner import init_db

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger(__name__)

app = Flask(__name__)

_db_initialized = False
_scan_lock = threading.Lock()
_scan_running = False


@app.before_request
def _ensure_db_initialized():
    global _db_initialized
    if _db_initialized:
        return
    try:
        print("=== _ensure_db_initialized: calling init_db() ===", flush=True)
        log.info("Lazy init_db() triggered by first request")
        init_db()
        _db_initialized = True
        print("=== init_db() OK, all tables ensured ===", flush=True)
        log.info("init_db() completed successfully")
    except Exception as e:
        print(f"=== init_db() FAILED: {e} ===", flush=True)
        log.error("init_db() FAILED: %s", e, exc_info=True)
        # NOT crash — let the request continue


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
        count = c.fetchone()[0]
        if count == 0:
            return 0
        for tbl in ["minervini_fundamental_scans", "minervini_52w_high", "minervini_fundamental_only"]:
            c.execute(f"SELECT COUNT(*) FROM {tbl} WHERE scan_date = %s", (scan_date,))
            if c.fetchone()[0] == 0:
                log.info("Partial scan detected for %s — %s is empty, will re-run", scan_date, tbl)
                return 0
        return count
    except Exception:
        return 0
    finally:
        conn.close()


@app.route("/health", methods=["GET"])
def health():
    """Container ve DB sağlık kontrolü."""
    try:
        from db_connection import get_connection
        conn = get_connection()
        cur = conn.cursor()
        cur.execute("""
            SELECT table_name FROM information_schema.tables
            WHERE table_schema = 'public' AND table_name IN ('trades', 'journal_entries')
            ORDER BY table_name
        """)
        tables = [r[0] for r in cur.fetchall()]
        conn.close()
        return jsonify({
            "status": "ok",
            "db_initialized": _db_initialized,
            "tables_found": tables,
            "expected_tables": ["journal_entries", "trades"],
            "all_tables_present": set(tables) >= {"trades", "journal_entries"},
        })
    except Exception as e:
        return jsonify({
            "status": "error",
            "db_initialized": _db_initialized,
            "error": str(e),
        }), 500


@app.route("/scan", methods=["POST"])
def scan():
    global _scan_running

    from scanner import run_scan, scan_sectors

    force = request.args.get("force", "0") == "1"
    date_override = request.args.get("date", None)
    if request.is_json:
        force = force or bool(request.json.get("force", False))
        date_override = date_override or request.json.get("date", None)

    if date_override:
        scan_date = date_override
        log.info("Date override: %s", scan_date)
    else:
        scan_date = _today_scan_date()
    today = date.today()

    # --- 1. Hisse taraması ---
    stock_response = None
    stock_http = 200

    if not force:
        count = _existing_count(scan_date)
        if count > 0:
            log.info("Scan skipped — %d records already exist for %s", count, scan_date)
            stock_response = {"status": "skipped", "scan_date": scan_date, "existing_records": count}

    if stock_response is None:
        with _scan_lock:
            if _scan_running:
                return jsonify({"status": "busy", "message": "Scan already in progress"}), 409
            _scan_running = True

        os.environ["QUANFINA_NONINTERACTIVE"] = "force" if force else "skip"

        try:
            log.info("Starting scan for %s (force=%s)", scan_date, force)
            run_scan(scan_date_override=date_override)
            log.info("Scan completed for %s", scan_date)
            stock_response = {"status": "ok", "scan_date": scan_date}
        except SystemExit:
            stock_response = {"status": "skipped", "scan_date": scan_date}
        except Exception as e:
            log.exception("Scan failed: %s", e)
            stock_response = {"status": "error", "message": str(e)}
            stock_http = 500
        finally:
            with _scan_lock:
                _scan_running = False

    # --- 2. Sektör taraması (her zaman çalışır, hisse sonucundan bağımsız) ---
    try:
        sector_count = scan_sectors(today)
        sector_status = "ok" if sector_count else "failed"
    except Exception as e:
        log.error("Sektör tarama hatası: %s", e)
        sector_count = None
        sector_status = "error"

    stock_response["sectors"] = {"status": sector_status, "count": sector_count}
    return jsonify(stock_response), stock_http


if __name__ == "__main__":
    port = int(os.getenv("PORT", 8080))
    app.run(host="0.0.0.0", port=port)
