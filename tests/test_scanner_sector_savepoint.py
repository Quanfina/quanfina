"""scan_sectors per-sektor SAVEPOINT izolasyonu + kismi-basari gorunurlugu (B1-02, 05 Tem 2026).

Kok neden (FAZ-1 denetim B1-02): scan_sectors tek commit + rollback kullaniyordu -> bir
sektor INSERT'i patlarsa TUM sector_rotation yazimi 0'a dusuyordu (H#17 Kok#3 all-or-nothing).
Ayrica donusu int/None idi -> kismi basari gorunmezdi (H#17 Kok#4 silent partial).

Fix: _write_sector_rows() per-sektor SAVEPOINT (P490 minervini loop deseninin BIREBIR aynasi)
+ scan_sectors dict {saved, failed, total} doner + scanner_server kismi fail'de scan() status
"warning" yapar. Bu testler 3 yolu sabitler:
  (a) bir sektor exception -> digerleri yazilir (savepoint izolasyonu) [unit, fake cursor]
  (b) kismi fail -> endpoint status "warning" [endpoint]
  (c) tam basari -> "ok" + legacy int donus geriye-uyum [endpoint]
"""
import sys
from pathlib import Path

import pytest

HERE = Path(__file__).resolve().parent
PROJECT_ROOT = HERE.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

try:
    import scanner
    import scanner_server
except ImportError as e:  # pragma: no cover
    pytest.skip(f"scanner/scanner_server import edilemedi: {e}", allow_module_level=True)


def _make_sector(ticker):
    return {"ticker": ticker, "sector_name": "Test", "perf_1w": 1.0, "perf_1m": 1.0,
            "perf_3m": 1.0, "perf_6m": 1.0, "perf_1y": 1.0, "rs_score": 1.0, "rs_rank": 1}


class _FakeCursor:
    """psycopg2 cursor taklidi: SAVEPOINT/RELEASE/ROLLBACK TO no-op; INSERT belirli
    ticker'da RuntimeError firlatir (bozuk sektor simulasyonu). execute log'lanir."""
    def __init__(self, fail_on_ticker=None):
        self.fail_on_ticker = fail_on_ticker
        self.log = []

    def execute(self, sql, params=None):
        s = " ".join(sql.split())
        if s.startswith("SAVEPOINT"):
            self.log.append("SAVEPOINT")
        elif s.startswith("RELEASE"):
            self.log.append("RELEASE")
        elif s.startswith("ROLLBACK TO"):
            self.log.append("ROLLBACK")
        elif "INSERT INTO sector_rotation" in s:
            ticker = params[1]  # (scan_date, ticker, ...)
            self.log.append(("INSERT", ticker))
            if self.fail_on_ticker and ticker == self.fail_on_ticker:
                raise RuntimeError(f"simulated INSERT fail: {ticker}")


class TestWriteSectorRows:
    def test_savepoint_isolation_one_fails_rest_written(self):
        # 2. sektor (XLF) patlar -> XLK + XLE yazilir (savepoint izolasyonu, cascade YOK)
        sectors = [_make_sector("XLK"), _make_sector("XLF"), _make_sector("XLE")]
        cur = _FakeCursor(fail_on_ticker="XLF")
        saved, failed = scanner._write_sector_rows(cur, "2026-07-05", sectors)
        assert (saved, failed) == (2, 1)
        assert cur.log.count("SAVEPOINT") == 3   # her sektor savepoint aldi
        assert cur.log.count("RELEASE") == 2     # XLK + XLE basarili
        assert cur.log.count("ROLLBACK") == 1    # yalniz XLF geri alindi

    def test_all_ok_no_rollback(self):
        cur = _FakeCursor()
        saved, failed = scanner._write_sector_rows(cur, "2026-07-05",
                                                   [_make_sector("XLK"), _make_sector("XLF")])
        assert (saved, failed) == (2, 0)
        assert cur.log.count("ROLLBACK") == 0
        assert cur.log.count("RELEASE") == 2

    def test_empty_sectors(self):
        cur = _FakeCursor()
        assert scanner._write_sector_rows(cur, "2026-07-05", []) == (0, 0)


@pytest.fixture
def client(monkeypatch):
    monkeypatch.setattr(scanner_server, "_db_initialized", True, raising=False)
    monkeypatch.setattr(scanner, "run_scan", lambda *a, **k: None, raising=False)
    monkeypatch.setattr(scanner_server, "_existing_count", lambda d: 0, raising=False)
    # stock 844 -> stock status "ok" (sektor escalation'i izole test edilir)
    monkeypatch.setattr(scanner_server, "_stock_row_count", lambda d: 844, raising=False)
    scanner_server.app.config["TESTING"] = True
    return scanner_server.app.test_client()


class TestScanSectorEndpointStatus:
    def test_partial_sector_fail_escalates_to_warning(self, client, monkeypatch):
        # scan_sectors kismi: 8 yazildi, 3 basarisiz -> sektor "warning" + top-level "warning"
        monkeypatch.setattr(scanner, "scan_sectors",
                            lambda *a, **k: {"saved": 8, "failed": 3, "total": 11}, raising=False)
        data = client.post("/scan").get_json()
        assert data["sectors"]["status"] == "warning"
        assert data["sectors"]["failed"] == 3
        assert data["sectors"]["count"] == 8
        assert data["status"] == "warning"          # H#17 Kok#4: top-level'a yansidi
        assert "Sektör taraması" in data["warning"]

    def test_full_sector_success_ok(self, client, monkeypatch):
        monkeypatch.setattr(scanner, "scan_sectors",
                            lambda *a, **k: {"saved": 11, "failed": 0, "total": 11}, raising=False)
        data = client.post("/scan").get_json()
        assert data["sectors"]["status"] == "ok"
        assert data["sectors"]["count"] == 11
        assert data["status"] == "ok"               # sektor temiz -> escalation yok

    def test_total_sector_fail_status_failed(self, client, monkeypatch):
        monkeypatch.setattr(scanner, "scan_sectors",
                            lambda *a, **k: {"saved": 0, "failed": 11, "total": 11}, raising=False)
        data = client.post("/scan").get_json()
        assert data["sectors"]["status"] == "failed"
        assert data["status"] == "warning"          # hic sektor yazilmadi -> gorunur uyari

    def test_legacy_int_return_backward_compat(self, client, monkeypatch):
        # eski/mock int donusu (11) -> saved=11, failed=0 -> "ok" (isinstance geriye-uyum)
        monkeypatch.setattr(scanner, "scan_sectors", lambda *a, **k: 11, raising=False)
        data = client.post("/scan").get_json()
        assert data["sectors"]["status"] == "ok"
        assert data["sectors"]["count"] == 11
        assert data["status"] == "ok"
