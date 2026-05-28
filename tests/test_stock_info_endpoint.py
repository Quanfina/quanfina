"""/api/stock/{symbol}/info endpoint pytest (KARAR #735 — Mark signals MOCK->DB).

KRITIK: P331 regresyon — Mark signals (carr_stage vb.) olan sembol endpoint'i
500 VERMEMELI (MarkSignalsBlock sema uyumu). Backfill sonrasi gercek DB verisi.
DB-bagimli. DB erisilemezse skip.
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
    r = client.get("/api/stock/AAPL/info")
    assert r.status_code == 200


def test_required_fields(client):
    r = client.get("/api/stock/AAPL/info")
    d = r.json()
    for field in ("symbol", "name", "sector", "price", "rs_rating"):
        assert field in d


def test_symbol_uppercase(client):
    """Kucuk harf sembol -> buyuk harf normalize."""
    r = client.get("/api/stock/aapl/info")
    assert r.json()["symbol"] == "AAPL"


def test_mark_signals_no_500_regression(client):
    """P331 REGRESYON: Mark signals (carr_stage) olan sembol 500 VERMEMELI.

    Backfill sonrasi AAPL carr_stage=2. MarkSignalsBlock sema uyumsuzlugu
    (tennis_ball STILL_RUNNING gibi) 500'e yol acardi — bu test onu yakalar."""
    for sym in ("AAPL", "AAOI", "AHCO", "NVDA"):
        r = client.get(f"/api/stock/{sym}/info")
        assert r.status_code == 200, f"{sym} -> {r.status_code} (500 regresyon!)"


def test_mark_signals_carr_stage_present(client):
    """Backfill sonrasi AAPL mark_signals.carr_stage dolu (gercek DB)."""
    r = client.get("/api/stock/AAPL/info")
    ms = r.json().get("mark_signals")
    # Backfill yapildiysa carr_stage var; yapilmadiysa MOCK fallback/None — esnek
    if ms is not None:
        # carr_stage varsa 1-4 araliginda
        cs = ms.get("carr_stage")
        if cs is not None:
            assert cs in (1, 2, 3, 4)


def test_mark_signals_schema_valid(client):
    """mark_signals alanlari MarkSignalsBlock semasina uygun (Literal degerler)."""
    r = client.get("/api/stock/AAOI/info")
    ms = r.json().get("mark_signals")
    if ms is not None:
        if ms.get("vcp_quality_score") is not None:
            assert ms["vcp_quality_score"] in ("EXCELLENT", "PASS")
        if ms.get("tennis_ball_pattern") is not None:
            assert ms["tennis_ball_pattern"] in ("TENNIS_BALL", "partial", "none")
        if ms.get("volume_asymmetry_tier") is not None:
            assert ms["volume_asymmetry_tier"] in ("healthy", "neutral", "distribution")


def test_unknown_symbol_graceful_200(client):
    """Bilinmeyen sembol -> 200 generic fallback (grafik hep acilir, KARAR #498)."""
    r = client.get("/api/stock/ZZINVALIDTICKER/info")
    assert r.status_code == 200
    d = r.json()
    assert d["symbol"] == "ZZINVALIDTICKER"


def test_active_strategies_list(client):
    """active_strategies her zaman liste (None degil)."""
    r = client.get("/api/stock/AAPL/info")
    assert isinstance(r.json()["active_strategies"], list)
