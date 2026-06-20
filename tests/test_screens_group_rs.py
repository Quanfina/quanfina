"""
P556 (20 Haz 2026): Bulk tarama Grup RS zenginlestirme — /api/screens/{slug}.

P553 compute_group_rs_confirmation tarama olceginde: her tarama satirina sektor + group_tier
(LEADING/NEUTRAL/LAGGING) eklenir (Minervini s.95 + O'Neil 'L' "leading stock in leading
group"). Tek batch sektor sorgusu (N+1 yok). Veri yoksa None (MOCK YOK — Kural #28).
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

RANK_MAP = {
    "Technology": 1, "Industrials": 2, "Materials": 3, "Financials": 4,
    "Consumer Discretionary": 5, "Real Estate": 6, "Health Care": 7,
    "Utilities": 8, "Energy": 9, "Consumer Staples": 10, "Communication Services": 11,
}


@pytest.fixture(scope="module")
def client():
    return TestClient(api_main.app)


def _valid_slug(client) -> str:
    return client.get("/api/screens").json()[0]["slug"]


class TestBulkGroupRSEnrichment:
    def test_rows_get_group_tier_from_real_sector(self, client, monkeypatch):
        monkeypatch.setattr(api_main, "db_health_check", lambda: True)
        monkeypatch.setattr(
            api_main, "screen_get_results_dispatch",
            lambda slug, limit=500: [
                {"symbol": "NVDA", "grade": "A", "rs_ibd": 99, "price": 100.0, "passed": 1, "scan_date": "2026-06-20"},
                {"symbol": "XOM", "grade": "B", "rs_ibd": 80, "price": 50.0, "passed": 1, "scan_date": "2026-06-20"},
            ],
        )
        monkeypatch.setattr(api_main, "stock_sectors_map_latest",
                            lambda syms: {"NVDA": "Technology", "XOM": "Energy"})
        monkeypatch.setattr(api_main, "sector_rank_map_latest", lambda: RANK_MAP)
        # pivot enrichment ag/network'u test-disi birak
        monkeypatch.setattr(api_main, "_compute_signal_pivot_status", lambda *a, **k: None)

        slug = _valid_slug(client)
        r = client.get(f"/api/screens/{slug}?nocache=1&limit=5")
        assert r.status_code == 200
        rows = {row["symbol"]: row for row in r.json()}
        # Technology #1 -> LEADING
        assert rows["NVDA"]["group_tier"] == "LEADING"
        assert rows["NVDA"]["sector"] == "Technology"
        # Energy #9 -> LAGGING (yalniz kurt riski)
        assert rows["XOM"]["group_tier"] == "LAGGING"
        assert rows["XOM"]["sector"] == "Energy"

    def test_unknown_sector_group_tier_null_not_mock(self, client, monkeypatch):
        monkeypatch.setattr(api_main, "db_health_check", lambda: True)
        monkeypatch.setattr(
            api_main, "screen_get_results_dispatch",
            lambda slug, limit=500: [
                {"symbol": "ZZZZ", "grade": "C", "rs_ibd": 70, "price": 10.0, "passed": 1, "scan_date": "2026-06-20"},
            ],
        )
        # sektor haritasinda yok -> tier None (uydurma YOK — Kural #28)
        monkeypatch.setattr(api_main, "stock_sectors_map_latest", lambda syms: {})
        monkeypatch.setattr(api_main, "sector_rank_map_latest", lambda: RANK_MAP)
        monkeypatch.setattr(api_main, "_compute_signal_pivot_status", lambda *a, **k: None)

        slug = _valid_slug(client)
        r = client.get(f"/api/screens/{slug}?nocache=1&limit=5")
        assert r.status_code == 200
        row = r.json()[0]
        assert row["group_tier"] is None
        assert row["sector"] is None

    def test_field_present_in_schema(self, client, monkeypatch):
        # Gercek cagri (monkeypatch'siz) — field her zaman donmeli (None olsa bile)
        slug = _valid_slug(client)
        r = client.get(f"/api/screens/{slug}?limit=3")
        assert r.status_code == 200
        for row in r.json():
            assert "group_tier" in row
            assert "sector" in row
