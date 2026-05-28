"""/api/patterns endpoint pytest (KARAR ADAY #714 — Pattern Library Migration 010).

7 Mark/O'Neil canon pattern. DB-bagimli (Cloud SQL). DB erisilemezse skip.
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
    pytest.skip("fastapi TestClient / db_helpers yok", allow_module_level=True)

if not db_health_check():
    pytest.skip("Cloud SQL erisilemez", allow_module_level=True)


@pytest.fixture(scope="module")
def client():
    return TestClient(api_main.app)


def test_status_200(client):
    r = client.get("/api/patterns")
    assert r.status_code == 200


def test_returns_list(client):
    r = client.get("/api/patterns")
    assert isinstance(r.json(), list)


def test_seven_canon_patterns(client):
    """Migration 010 ile 7 Mark/O'Neil canon pattern."""
    r = client.get("/api/patterns")
    data = r.json()
    assert len(data) == 7


def test_standard_vcp_present(client):
    """Standard VCP pattern mevcut + Mark kitap referansli."""
    r = client.get("/api/patterns")
    names = [p["pattern_name"] for p in r.json()]
    assert "Standard VCP" in names


def test_all_have_book_ref(client):
    """KALICI ILKE #4: her pattern Mark kitap referansli (mark_book_ref dolu)."""
    r = client.get("/api/patterns")
    for p in r.json():
        assert p["mark_book_ref"], f"{p['pattern_name']} mark_book_ref bos"
        assert "TLSMW" in p["mark_book_ref"] or "O'Neil" in p["mark_book_ref"]


def test_canon_pattern_names(client):
    """7 canon pattern adlari (Migration 010 birebir)."""
    r = client.get("/api/patterns")
    names = {p["pattern_name"] for p in r.json()}
    expected = {
        "Standard VCP", "Cup-with-Handle", "Cup Completion Cheat",
        "Low Cheat", "Power Play (HTF)", "Double Bottom", "Square Box",
    }
    assert names == expected


def test_contraction_params_valid(client):
    """contraction_count_min <= max (mantikli aralik)."""
    r = client.get("/api/patterns")
    for p in r.json():
        cmin = p["contraction_count_min"]
        cmax = p["contraction_count_max"]
        if cmin is not None and cmax is not None:
            assert cmin <= cmax, f"{p['pattern_name']} contraction min>max"


def test_base_weeks_params_valid(client):
    """base_weeks_min <= max."""
    r = client.get("/api/patterns")
    for p in r.json():
        bmin = p["base_weeks_min"]
        bmax = p["base_weeks_max"]
        if bmin is not None and bmax is not None:
            assert bmin <= bmax, f"{p['pattern_name']} base_weeks min>max"


def test_power_play_htf_params(client):
    """Power Play (HTF) base_weeks 3-6 (Mark High Tight Flag canon)."""
    r = client.get("/api/patterns")
    pp = next((p for p in r.json() if p["pattern_name"] == "Power Play (HTF)"), None)
    assert pp is not None
    assert pp["base_weeks_min"] == 3
    assert pp["base_weeks_max"] == 6
