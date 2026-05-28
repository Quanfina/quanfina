"""/api/sector-rotation endpoint pytest — 11 SPDR sektör ETF RS rank.

DB-bagimli (sector_rotation tablosu). DB erisilemezse skip.
"""
import pytest
import sys
from pathlib import Path

HERE = Path(__file__).resolve().parent
PROJECT_ROOT = HERE.parent
API_DIR = PROJECT_ROOT / "api"
for d in (str(PROJECT_ROOT), str(API_DIR)):
    if d not in sys.path:
        sys.path.insert(0, d)

try:
    from fastapi.testclient import TestClient
    import main as api_main
    from db_helpers import db_health_check
except ImportError:
    pytest.skip("fastapi/db_helpers yok", allow_module_level=True)

if not db_health_check():
    pytest.skip("Cloud SQL erisilemez", allow_module_level=True)


@pytest.fixture(scope="module")
def client():
    return TestClient(api_main.app)


def test_status_200(client):
    r = client.get("/api/sector-rotation")
    assert r.status_code == 200


def test_returns_list(client):
    r = client.get("/api/sector-rotation")
    assert isinstance(r.json(), list)


def test_sector_fields(client):
    r = client.get("/api/sector-rotation")
    data = r.json()
    if data:
        s = data[0]
        for field in ("ticker", "sector_name", "rs_rank", "perf_1y"):
            assert field in s


def test_sorted_by_rank(client):
    """rs_rank artan sira (1=en guclu sektor)."""
    r = client.get("/api/sector-rotation")
    ranks = [s["rs_rank"] for s in r.json() if s["rs_rank"] is not None]
    assert ranks == sorted(ranks)


def test_rank_1_strongest(client):
    """İlk satir rs_rank=1 (lider sektor)."""
    r = client.get("/api/sector-rotation")
    data = r.json()
    if data and data[0]["rs_rank"] is not None:
        assert data[0]["rs_rank"] == 1


def test_spdr_etf_tickers(client):
    """SPDR sektör ETF ticker'ları (XL* prefix)."""
    r = client.get("/api/sector-rotation")
    for s in r.json():
        assert s["ticker"].startswith("XL"), f"{s['ticker']} SPDR degil"
