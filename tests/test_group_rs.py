"""
Grup/Endustri RS onayi (P553, 20 Haz 2026) — math + endpoint testleri.

2 uzman convergence (Minervini TLSMW s.95 + O'Neil CAN SLIM 'L'): guclu hisse guclu
grupta olmali. compute_group_rs_confirmation sector_rotation RS rank ile teyit eder.
SEKTOR proxy (11 SPDR), gercek IBD endustri grubu degil (Kural #26 durustluk). MOCK YOK
(Kural #28): veri yoksa available=False.
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

from quanfina_math import (  # noqa: E402
    normalize_sector_to_spdr,
    compute_group_rs_confirmation,
)

# Gercek DB-dogrulanmis ornek (P553): 11 SPDR sektor, RS rank 1=en guclu.
RANK_MAP = {
    "Technology": 1, "Industrials": 2, "Materials": 3, "Financials": 4,
    "Consumer Discretionary": 5, "Real Estate": 6, "Health Care": 7,
    "Utilities": 8, "Energy": 9, "Consumer Staples": 10, "Communication Services": 11,
}


class TestNormalizeSector:
    @pytest.mark.parametrize("finviz,spdr", [
        ("Basic Materials", "Materials"),
        ("Consumer Cyclical", "Consumer Discretionary"),
        ("Consumer Defensive", "Consumer Staples"),
        ("Financial", "Financials"),
        ("Healthcare", "Health Care"),
        ("Technology", "Technology"),       # birebir
        ("Energy", "Energy"),
        ("Real Estate", "Real Estate"),
        ("Utilities", "Utilities"),
        ("Industrials", "Industrials"),
        ("Communication Services", "Communication Services"),
    ])
    def test_finviz_maps_to_spdr(self, finviz, spdr):
        assert normalize_sector_to_spdr(finviz) == spdr

    def test_spdr_name_passthrough(self):
        # sector_rotation kaynagindan zaten SPDR adi gelirse korunur
        assert normalize_sector_to_spdr("Health Care") == "Health Care"

    def test_whitespace_trimmed(self):
        assert normalize_sector_to_spdr("  Technology  ") == "Technology"

    def test_unknown_returns_none(self):
        # Eslesme uydurma YASAK (Kural #26)
        assert normalize_sector_to_spdr("Crypto Mining") is None

    def test_none_and_empty(self):
        assert normalize_sector_to_spdr(None) is None
        assert normalize_sector_to_spdr("") is None

    def test_all_11_finviz_sectors_resolve(self):
        # minervini_scans.sector distinct (DB-dogrulanmis) tum 11 deger SPDR'ye cozulur
        finviz_all = [
            "Basic Materials", "Communication Services", "Consumer Cyclical",
            "Consumer Defensive", "Energy", "Financial", "Healthcare",
            "Industrials", "Real Estate", "Technology", "Utilities",
        ]
        resolved = {normalize_sector_to_spdr(s) for s in finviz_all}
        assert None not in resolved
        assert resolved == set(RANK_MAP.keys())


class TestGroupRSConfirmation:
    def test_leading_sector(self):
        # Technology #1 -> LEADING (rank <= 4)
        r = compute_group_rs_confirmation("Technology", RANK_MAP)
        assert r["available"] is True
        assert r["sector_spdr"] == "Technology"
        assert r["rs_rank"] == 1
        assert r["tier"] == "LEADING"
        assert r["is_confirmed"] is True
        assert r["is_lone_wolf"] is False

    def test_leading_boundary_rank4(self):
        # Financials #4 -> hala LEADING (ust tercil siniri)
        r = compute_group_rs_confirmation("Financial", RANK_MAP)
        assert r["rs_rank"] == 4
        assert r["tier"] == "LEADING"

    def test_neutral_sector(self):
        # Real Estate #6 -> NEUTRAL (5-7)
        r = compute_group_rs_confirmation("Real Estate", RANK_MAP)
        assert r["tier"] == "NEUTRAL"
        assert r["is_confirmed"] is False
        assert r["is_lone_wolf"] is False

    def test_lagging_sector_lone_wolf(self):
        # Communication Services #11 -> LAGGING "yalniz kurt"
        r = compute_group_rs_confirmation("Communication Services", RANK_MAP)
        assert r["tier"] == "LAGGING"
        assert r["is_confirmed"] is False
        assert r["is_lone_wolf"] is True

    def test_lagging_boundary_rank8(self):
        # Utilities #8 -> LAGGING (alt tercil baslangici, 11-4+1=8)
        r = compute_group_rs_confirmation("Utilities", RANK_MAP)
        assert r["rs_rank"] == 8
        assert r["tier"] == "LAGGING"

    def test_finviz_name_normalized_in_lookup(self):
        # Finviz "Consumer Cyclical" -> SPDR "Consumer Discretionary" #5 NEUTRAL
        r = compute_group_rs_confirmation("Consumer Cyclical", RANK_MAP)
        assert r["sector_spdr"] == "Consumer Discretionary"
        assert r["rs_rank"] == 5
        assert r["tier"] == "NEUTRAL"

    def test_unknown_sector_unavailable(self):
        r = compute_group_rs_confirmation("Crypto Mining", RANK_MAP)
        assert r["available"] is False
        assert r["tier"] is None

    def test_empty_rank_map_unavailable(self):
        # MOCK YOK (Kural #28): rank map bos -> available False, uydurma sayi yok
        r = compute_group_rs_confirmation("Technology", {})
        assert r["available"] is False
        assert r["rs_rank"] is None

    def test_none_sector_unavailable(self):
        r = compute_group_rs_confirmation(None, RANK_MAP)
        assert r["available"] is False

    def test_mark_says_cites_canon(self):
        r = compute_group_rs_confirmation("Technology", RANK_MAP)
        assert "s.95" in r["mark_says"] or "O'Neil" in r["mark_says"]


# --- Endpoint (TestClient) ---
try:
    from fastapi.testclient import TestClient
    import main as api_main
    _HAS_API = True
except ImportError:
    _HAS_API = False


@pytest.mark.skipif(not _HAS_API, reason="fastapi yok")
class TestGroupRSEndpoint:
    @pytest.fixture(scope="class")
    def client(self):
        return TestClient(api_main.app)

    def test_200_shape(self, client):
        r = client.get("/api/stock/NVDA/group-rs")
        assert r.status_code == 200
        d = r.json()
        for k in ("symbol", "available", "tier", "is_confirmed", "is_lone_wolf", "mark_says"):
            assert k in d
        assert d["symbol"] == "NVDA"

    def test_unavailable_is_honest_not_mock(self, client, monkeypatch):
        # Veri yok -> available False (MOCK satir DONMEZ — Kural #28)
        monkeypatch.setattr(api_main, "stock_sector_latest", lambda s: None)
        monkeypatch.setattr(api_main, "sector_rank_map_latest", lambda: {})
        r = client.get("/api/stock/ZZZZ/group-rs")
        assert r.status_code == 200
        d = r.json()
        assert d["available"] is False
        assert d["rs_rank"] is None
        assert d["tier"] is None

    def test_leading_wired_end_to_end(self, client, monkeypatch):
        monkeypatch.setattr(api_main, "stock_sector_latest", lambda s: "Technology")
        monkeypatch.setattr(api_main, "sector_rank_map_latest", lambda: RANK_MAP)
        r = client.get("/api/stock/NVDA/group-rs")
        d = r.json()
        assert d["available"] is True
        assert d["sector"] == "Technology"
        assert d["rs_rank"] == 1
        assert d["tier"] == "LEADING"
        assert d["is_confirmed"] is True

    def test_lone_wolf_wired_end_to_end(self, client, monkeypatch):
        # Finviz adi -> normalize -> zayif sektor
        monkeypatch.setattr(api_main, "stock_sector_latest", lambda s: "Communication Services")
        monkeypatch.setattr(api_main, "sector_rank_map_latest", lambda: RANK_MAP)
        r = client.get("/api/stock/GOOGL/group-rs")
        d = r.json()
        assert d["tier"] == "LAGGING"
        assert d["is_lone_wolf"] is True
