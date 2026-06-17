"""scanner SERIAL id sequence resync + scan() "sessiz ok" defekti testleri (17 Haz 2026).

Kok neden (canli kanit): prod hisse taramasi 06-12..06-17 6 gun sessizce 0 kayit
yazdi. minervini_scans SERIAL id sequence'i max(id)'nin gerisinde kalmis (DB'ye
explicit-id ile ~803 satir restore edilmis ama sequence guncellenmemis) -> her INSERT
'duplicate key value violates *_pkey' -> run_scan ilk hatada break -> 0 kayit. Ustelik
scan() endpoint'i bunu "ok" raporluyordu (sektor calistigi icin fark edilmedi).

Bu testler iki fix'i kapsar:
  1. resync_serial_sequences — yazma oncesi sequence'i max(id)'ye senkronlar (self-heal)
  2. scan() — gercek yazim 0 ise "ok" yerine "warning" doner (gorunurluk)
"""
import sys
from pathlib import Path

import pytest

HERE = Path(__file__).resolve().parent
PROJECT_ROOT = HERE.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

try:
    from scanner import resync_serial_sequences, _SERIAL_ID_TABLES, _pvh_nan_safe
except ImportError as e:
    pytest.skip(f"scanner import edilemedi: {e}", allow_module_level=True)


class TestPvhNanSafe:
    """price_volume_history NaN/Inf -> None (JSONB 'Token NaN is invalid' kok neden #2)."""

    def test_nan_becomes_none(self):
        import json
        pvh = [{"date": "2026-02-24", "open": float("nan"), "close": 10.5, "volume": 100}]
        safe = _pvh_nan_safe(pvh)
        assert safe[0]["open"] is None
        assert safe[0]["close"] == 10.5
        # Kritik: artik gecerli JSON (NaN token yok)
        assert "NaN" not in json.dumps(safe)

    def test_inf_becomes_none(self):
        pvh = [{"o": float("inf"), "c": float("-inf"), "v": 5}]
        safe = _pvh_nan_safe(pvh)
        assert safe[0]["o"] is None and safe[0]["c"] is None and safe[0]["v"] == 5

    def test_clean_values_untouched(self):
        pvh = [{"date": "2026-01-01", "open": 1.0, "close": 2.0, "volume": 9}]
        assert _pvh_nan_safe(pvh) == pvh

    def test_empty_and_none(self):
        assert _pvh_nan_safe(None) is None
        assert _pvh_nan_safe([]) == []


class _FakeCursor:
    """resync icin minimal psycopg2 cursor taklidi — execute kaydeder, fetchone kuyruktan doner."""

    def __init__(self, fetch_queue, raise_on_first=False):
        self.calls = []                # (sql, params)
        self._fetch = list(fetch_queue)
        self._raise_on_first = raise_on_first

    def execute(self, sql, params=None):
        self.calls.append((sql, params))
        if self._raise_on_first and len(self.calls) == 1:
            raise RuntimeError("simulated DB error")

    def fetchone(self):
        return self._fetch.pop(0) if self._fetch else None


class TestResyncSerialSequences:
    def test_setval_issued_per_table_with_max_id(self):
        # Her tablo icin once pg_get_serial_sequence (seq adi doner), sonra setval.
        cur = _FakeCursor(fetch_queue=[("minervini_scans_id_seq",)])
        resync_serial_sequences(cur, tables=["minervini_scans"])

        assert len(cur.calls) == 2, "lookup + setval beklenir"
        lookup_sql, lookup_params = cur.calls[0]
        assert "pg_get_serial_sequence" in lookup_sql
        assert lookup_params == ("minervini_scans",)

        setval_sql, setval_params = cur.calls[1]
        assert "setval" in setval_sql
        assert "MAX(id)" in setval_sql
        assert setval_params == ("minervini_scans_id_seq",)

    def test_skips_table_with_no_sequence(self):
        # pg_get_serial_sequence None donerse setval edilmez (skip).
        cur = _FakeCursor(fetch_queue=[(None,)])
        resync_serial_sequences(cur, tables=["nonexistent_table"])
        assert len(cur.calls) == 1, "yalniz lookup, setval YOK"
        assert "setval" not in cur.calls[0][0]

    def test_exception_is_swallowed_not_raised(self):
        # Bir tablo hata verirse digerlerini bozmadan devam (non-fatal).
        cur = _FakeCursor(fetch_queue=[("x_seq",)], raise_on_first=True)
        resync_serial_sequences(cur, tables=["minervini_scans"])  # raise ETMEMELI

    def test_default_table_list_covers_scanner_tables(self):
        assert "minervini_scans" in _SERIAL_ID_TABLES
        assert "sector_rotation" in _SERIAL_ID_TABLES


# --- scan() endpoint "sessiz ok" -> "warning" davranisi ---

scanner_server = pytest.importorskip(
    "scanner_server",
    reason="flask/scanner_server import edilemedi (test venv'inde flask yoksa atla)",
)


@pytest.fixture
def client(monkeypatch):
    import scanner

    # before_request init_db()'yi atla (DB'ye gitmesin)
    monkeypatch.setattr(scanner_server, "_db_initialized", True, raising=False)
    # run_scan + scan_sectors no-op (network/DB yok)
    monkeypatch.setattr(scanner, "run_scan", lambda *a, **k: None, raising=False)
    monkeypatch.setattr(scanner, "scan_sectors", lambda *a, **k: 11, raising=False)
    # bugun zaten veri yok -> run_scan'e gir
    monkeypatch.setattr(scanner_server, "_existing_count", lambda d: 0, raising=False)

    scanner_server.app.config["TESTING"] = True
    return scanner_server.app.test_client()


def test_scan_warns_when_zero_rows_written(client, monkeypatch):
    # run_scan donduu ama minervini_scans'e 0 satir yazildi -> SILENT FAILURE
    monkeypatch.setattr(scanner_server, "_stock_row_count", lambda d: 0, raising=False)
    resp = client.post("/scan")
    data = resp.get_json()
    assert data["status"] == "warning"
    assert data["stock_rows"] == 0
    assert "yazilmadi" in data["warning"]


def test_scan_ok_when_rows_written(client, monkeypatch):
    monkeypatch.setattr(scanner_server, "_stock_row_count", lambda d: 844, raising=False)
    resp = client.post("/scan")
    data = resp.get_json()
    assert data["status"] == "ok"
    assert data["stock_rows"] == 844
