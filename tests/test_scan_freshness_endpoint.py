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


class TestMultiTableFreshness:
    """B1-03 (05 Tem 2026): sources (minervini_scans + sector_rotation) + any_stale."""

    def test_sources_shape_two_legs(self, client):
        d = client.get("/api/scan/freshness").json()
        assert "sources" in d and "any_stale" in d
        assert isinstance(d["any_stale"], bool)
        tables = {s["table"] for s in d["sources"]}
        assert tables == {"minervini_scans", "sector_rotation"}
        for s in d["sources"]:
            for k in ("table", "label", "latest_scan_date", "calendar_days_old", "is_stale"):
                assert k in s

    def test_both_fresh_any_stale_false(self, client, monkeypatch):
        from datetime import date
        today = date.today().isoformat()
        monkeypatch.setattr(api_main, "scan_latest_date", lambda: today)
        monkeypatch.setattr(api_main, "latest_scan_date_for", lambda table: today)
        d = client.get("/api/scan/freshness").json()
        assert d["any_stale"] is False
        assert d["is_stale"] is False  # top-level = minervini (degismez)
        assert all(s["is_stale"] is False for s in d["sources"])

    def test_only_sector_stale_any_stale_true_source_named(self, client, monkeypatch):
        # Kritik B1-03: minervini TAZE, sector BAYAT -> top-level is_stale=False (minervini)
        # ama any_stale=True; mesaj SADECE 'Sektör rotasyonu' adlandirir (minervini karismaz)
        from datetime import date, timedelta
        today = date.today().isoformat()
        old = (date.today() - timedelta(days=20)).isoformat()
        monkeypatch.setattr(api_main, "scan_latest_date", lambda: today)             # minervini taze
        monkeypatch.setattr(api_main, "latest_scan_date_for", lambda table: old)     # sector bayat
        d = client.get("/api/scan/freshness").json()
        assert d["is_stale"] is False        # minervini taze -> top-level DEGISMEZ
        assert d["any_stale"] is True         # sector bayat -> yeni aggregate yakalar
        assert "Sektör rotasyonu" in d["message"]
        assert "Hisse taraması BAYAT" not in d["message"]  # minervini taze, adlandirilmaz
        sec = next(s for s in d["sources"] if s["table"] == "sector_rotation")
        assert sec["is_stale"] is True and sec["calendar_days_old"] == 20

    def test_both_stale_combined_message(self, client, monkeypatch):
        from datetime import date, timedelta
        old = (date.today() - timedelta(days=15)).isoformat()
        monkeypatch.setattr(api_main, "scan_latest_date", lambda: old)
        monkeypatch.setattr(api_main, "latest_scan_date_for", lambda table: old)
        d = client.get("/api/scan/freshness").json()
        assert d["any_stale"] is True
        assert "Hisse taraması BAYAT" in d["message"]
        assert "Sektör rotasyonu BAYAT" in d["message"]


class TestFreshnessWhitelist:
    """latest_scan_date_for whitelist: bilinmeyen tablo -> ValueError (SQL injection korumasi)."""

    def test_unknown_table_raises(self):
        from db_helpers import latest_scan_date_for
        with pytest.raises(ValueError):
            latest_scan_date_for("web_trades; DROP TABLE minervini_scans")

    def test_known_tables_no_raise(self):
        # Gercek DB'ye vurur; date VEYA None doner, ASLA raise etmez (DB hata -> None)
        from db_helpers import latest_scan_date_for
        for t in ("minervini_scans", "sector_rotation"):
            latest_scan_date_for(t)  # exception yok = pass

    def test_label_whitelist(self):
        from db_helpers import freshness_source_label
        assert freshness_source_label("sector_rotation") == "Sektör rotasyonu"
        with pytest.raises(ValueError):
            freshness_source_label("bogus")
