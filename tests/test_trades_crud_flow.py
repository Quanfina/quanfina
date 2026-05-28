"""Paper Trading uctan uca akis pytest (D — KARAR #717 + #475).

Trade CRUD: POST (plan zorunlu) -> GET (listele) -> PATCH (kapat + P&L) -> DELETE.
Mark TTLC Sec 1: "Always go in with a plan" — 6 plan alani ZORUNLU (422 testi).

DB-bagimli (web_trades gercek write). Cleanup garantili (fixture DELETE).
DB erisilemezse skip.
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


# Mark canon tam body — plan 6 alan + signal_source ZORUNLU
TRADE_BODY = {
    "symbol": "ZZTEST",
    "strategy": "minervini",
    "setup_type": "VCP",
    "signal_source": "manual_self",
    "entry_date": "2026-05-28",
    "entry_price": 100.0,
    "shares": 100,
    "status": "open",
    "plan_entry_trigger": "Pivot $100.00 breakout (test)",
    "plan_stop": 95.0,
    "plan_target": 115.0,
    "plan_size_pct": 10.0,
    "plan_exit_strategy": "Trail 50DMA veya hedef",
    "plan_time_horizon": "swing",
}


@pytest.fixture
def created_trade(client):
    """POST ile trade olustur, yield id, sonunda DELETE (cleanup garantili)."""
    r = client.post("/api/trades", json=TRADE_BODY)
    assert r.status_code == 201, f"POST basarisiz: {r.status_code} {r.text[:200]}"
    trade = r.json()
    tid = trade["id"]
    yield tid
    # Cleanup — test trade'i sil (DB temiz kalir)
    client.delete(f"/api/trades/{tid}")


def test_create_trade_201(created_trade):
    """POST /api/trades -> 201 + id atandi."""
    assert isinstance(created_trade, int)
    assert created_trade > 0


def test_create_returns_plan_fields(client):
    """Olusturulan trade plan alanlarini dondurur (Mark disiplini)."""
    r = client.post("/api/trades", json=TRADE_BODY)
    assert r.status_code == 201
    t = r.json()
    try:
        assert t["plan_stop"] == 95.0
        assert t["plan_target"] == 115.0
        assert t["status"] == "open"
    finally:
        client.delete(f"/api/trades/{t['id']}")


def test_trade_appears_in_list(client, created_trade):
    """GET /api/trades -> olusturulan trade listede."""
    r = client.get("/api/trades")
    assert r.status_code == 200
    ids = [t["id"] for t in r.json()]
    assert created_trade in ids


def test_close_trade_pl_calc(client, created_trade):
    """PATCH kapat (exit_price=110) -> P&L hesap (entry=100, shares=100 -> +1000 / +10%)."""
    r = client.patch(
        f"/api/trades/{created_trade}",
        json={"status": "closed", "exit_date": "2026-05-29", "exit_price": 110.0},
    )
    assert r.status_code == 200
    t = r.json()
    assert t["status"] == "closed"
    assert t["pl_dollar"] == 1000.0
    assert t["pl_pct"] == 10.0


def test_close_trade_loss_pl(client, created_trade):
    """PATCH kapat zararla (exit=92) -> -800 / -8% (Mark %7 limit asimi ornek)."""
    r = client.patch(
        f"/api/trades/{created_trade}",
        json={"status": "closed", "exit_date": "2026-05-29", "exit_price": 92.0},
    )
    assert r.status_code == 200
    t = r.json()
    assert t["pl_dollar"] == -800.0
    assert t["pl_pct"] == -8.0


def test_delete_trade_204(client):
    """DELETE /api/trades/{id} -> 204."""
    r = client.post("/api/trades", json=TRADE_BODY)
    tid = r.json()["id"]
    rd = client.delete(f"/api/trades/{tid}")
    assert rd.status_code == 204
    # Silindikten sonra listede yok
    ids = [t["id"] for t in client.get("/api/trades").json()]
    assert tid not in ids


def test_plan_required_422(client):
    """Mark TTLC Sec 1: plan'siz trade REDDEDILIR (422 — 'Always go in with a plan')."""
    body_no_plan = {k: v for k, v in TRADE_BODY.items() if not k.startswith("plan_")}
    r = client.post("/api/trades", json=body_no_plan)
    assert r.status_code == 422, "Plan'siz trade kabul edilmemeli (Mark disiplini)"


def test_signal_source_required_422(client):
    """signal_source ZORUNLU (KARAR #477) — eksikse 422."""
    body_no_src = {k: v for k, v in TRADE_BODY.items() if k != "signal_source"}
    r = client.post("/api/trades", json=body_no_src)
    assert r.status_code == 422
