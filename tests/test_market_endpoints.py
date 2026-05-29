"""Market + health endpoint pytest: /api/health + /api/market/calendar/status.

DB-bagimli (health db_connected). DB erisilemezse health yine doner (db false),
ama calendar/market DB gerektirmez. Modul skip yok — health her durumda 200.
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
except ImportError:
    pytest.skip("fastapi yok", allow_module_level=True)


@pytest.fixture(scope="module")
def client():
    return TestClient(api_main.app)


# ── /api/health ──────────────────────────────────────────────────────────────

def test_health_200(client):
    r = client.get("/api/health")
    assert r.status_code == 200


def test_health_fields(client):
    r = client.get("/api/health")
    d = r.json()
    assert d["status"] == "ok"
    assert d["service"] == "quanfina-api"
    assert "db_connected" in d
    assert "timestamp" in d


def test_health_db_connected_bool(client):
    r = client.get("/api/health")
    assert isinstance(r.json()["db_connected"], bool)


# ── /api/market/calendar/status ──────────────────────────────────────────────

def test_calendar_200(client):
    r = client.get("/api/market/calendar/status")
    assert r.status_code == 200


def test_calendar_fields(client):
    r = client.get("/api/market/calendar/status")
    d = r.json()
    for field in ("is_open", "session", "now_et", "now_tr", "next_open_tr", "last_trading_day"):
        assert field in d


def test_calendar_session_valid(client):
    """session 4 gecerli degerden biri."""
    r = client.get("/api/market/calendar/status")
    assert r.json()["session"] in ("regular", "pre_market", "post_market", "closed")


def test_calendar_is_open_bool(client):
    r = client.get("/api/market/calendar/status")
    assert isinstance(r.json()["is_open"], bool)


# ── /api/market/status ───────────────────────────────────────────────────────

def test_market_status_200(client):
    r = client.get("/api/market/status")
    assert r.status_code == 200


def test_market_status_core_fields(client):
    r = client.get("/api/market/status")
    d = r.json()
    for field in ("spy_stage", "vix", "distribution_days", "market_health_score", "suggested_mode"):
        assert field in d


# Paket 383: Backend P382 clean-room kontrat uçtan uca doğrulama
# Frontend P383 hook testleri (use-trading-mode-hook P383 describe) bu kontrata
# bağlı. Backend regresyon olursa frontend mod tetiği sessizce kırılırdı.

def test_market_health_label_clean_room_enum(client):
    """market_health_label sadece P382 clean-room enum dönmeli (legacy YOK)."""
    r = client.get("/api/market/status")
    d = r.json()
    assert d["market_health_label"] in {"HEALTHY", "NEUTRAL", "UNDER_PRESSURE"}, (
        f"Beklenmedik label: {d['market_health_label']!r} — "
        f"Markets360 Bundle sizmasi 'YESIL/SARI/KIRMIZI' donduruyor olabilir."
    )
    # Legacy regresyon koruması
    assert d["market_health_label"] not in {"YEŞİL", "SARI", "KIRMIZI", "YESIL"}


def test_market_status_suggested_mode_clean_room(client):
    """suggested_mode P382 clean-room enum (LONG/CAUTION/DEFENSIVE, eski CASH yok)."""
    r = client.get("/api/market/status")
    d = r.json()
    assert d["suggested_mode"] in {"LONG", "CAUTION", "DEFENSIVE"}
    assert d["suggested_mode"] != "CASH"  # P382 oncesi MOCK degeri


def test_market_health_score_in_p382_kontrat_band(client):
    """market_health_score P382 helper'in 3 kategori degerinden (25/50/75)
    biri olmali. Helper tek mantik kaynak — endpoint MOCK_MARKET_STATUS
    default 75'ini override etmeli (gerçek wire)."""
    r = client.get("/api/market/status")
    d = r.json()
    assert d["market_health_score"] in {25, 50, 75}, (
        f"Beklenmedik score: {d['market_health_score']} — "
        f"_compute_market_health helper 25/50/75 üretir, MOCK 75 override edilmeli."
    )


def test_market_status_health_label_score_consistency(client):
    """Backend kontrat tutarlilik: label ile score eslesmeli (helper invariant)."""
    r = client.get("/api/market/status")
    d = r.json()
    label = d["market_health_label"]
    score = d["market_health_score"]
    expected_score = {"UNDER_PRESSURE": 25, "NEUTRAL": 50, "HEALTHY": 75}[label]
    assert score == expected_score, (
        f"Label='{label}' beklenen score={expected_score}, gercek={score} — "
        f"_compute_market_health icindeki esleme bozuldu."
    )
