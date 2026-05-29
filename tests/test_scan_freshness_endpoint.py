"""
/api/scan/freshness (P375) — tarama veri tazeligi endpoint testi.

Sn. Ferit "14 gun eski veri" acisinin onlenmesi. is_stale = calendar_days > 4
(hafta sonu Cum->Sali 4 gunu asmaz; >4 = tarama atlandi). DB durumundan bagimsiz
sekil + mantik tutarliligi test edilir.
"""
import sys
from pathlib import Path

import pytest

HERE = Path(__file__).resolve().parent
PROJECT_ROOT = HERE.parent
API_DIR = PROJECT_ROOT / "api"
for d in (str(PROJECT_ROOT), str(API_DIR)):
    if d not in sys.path:
        sys.path.insert(0, d)

try:
    from fastapi.testclient import TestClient
    import main as api_main
except ImportError:
    pytest.skip("fastapi yok", allow_module_level=True)


@pytest.fixture(scope="module")
def client():
    return TestClient(api_main.app)


class TestScanFreshnessEndpoint:
    def test_200_and_shape(self, client):
        r = client.get("/api/scan/freshness")
        assert r.status_code == 200
        d = r.json()
        for key in ("latest_scan_date", "is_stale", "calendar_days_old",
                    "threshold_days", "message"):
            assert key in d
        assert isinstance(d["is_stale"], bool)
        assert isinstance(d["message"], str) and d["message"]

    def test_threshold_is_4(self, client):
        assert client.get("/api/scan/freshness").json()["threshold_days"] == 4

    def test_is_stale_consistent_with_days(self, client):
        # calendar_days_old varsa is_stale == (days > 4) tutarli olmali
        d = client.get("/api/scan/freshness").json()
        if d["calendar_days_old"] is not None:
            assert d["is_stale"] == (d["calendar_days_old"] > 4)

    def test_no_data_is_stale(self, client, monkeypatch):
        # scan_latest_date None -> is_stale=True (veri yok = guvenli taraf)
        monkeypatch.setattr(api_main, "scan_latest_date", lambda: None)
        d = client.get("/api/scan/freshness").json()
        assert d["latest_scan_date"] is None
        assert d["is_stale"] is True

    def test_fresh_date_not_stale(self, client, monkeypatch):
        # Bugunun tarihi -> taze (is_stale False, 0 gun)
        from datetime import date
        monkeypatch.setattr(api_main, "scan_latest_date", lambda: date.today().isoformat())
        d = client.get("/api/scan/freshness").json()
        assert d["is_stale"] is False
        assert d["calendar_days_old"] == 0

    def test_old_date_is_stale(self, client, monkeypatch):
        # 10 gun once -> bayat
        from datetime import date, timedelta
        old = (date.today() - timedelta(days=10)).isoformat()
        monkeypatch.setattr(api_main, "scan_latest_date", lambda: old)
        d = client.get("/api/scan/freshness").json()
        assert d["is_stale"] is True
        assert d["calendar_days_old"] == 10
