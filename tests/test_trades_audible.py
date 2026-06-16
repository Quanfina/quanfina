"""Migration 011 / KARAR ADAY #960 — Acik pozisyon Audible Lock pytest.

Aktif stop/hedef (stop_loss/target_price) pozisyon acildiktan sonra duzenlenebilir AMA
DEGISIYORSA audible_reason ZORUNLU (Mark "audible" disiplini — sebepsiz stop kaydirma yok).
PATCH sebep yoksa 422; ayni deger (degisiklik yok) serbest.

DB-bagimli (web_trades gercek write). Cleanup garantili (fixture DELETE). DB yoksa skip.
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
    from db_helpers import db_health_check
except ImportError:
    pytest.skip("fastapi/db_helpers yok", allow_module_level=True)

if not db_health_check():
    pytest.skip("Cloud SQL erisilemez", allow_module_level=True)


@pytest.fixture(scope="module")
def client():
    return TestClient(api_main.app)


TRADE_BODY = {
    "symbol": "ZZAUDIBLE",
    "strategy": "minervini",
    "setup_type": "VCP",
    "signal_source": "manual_self",
    "entry_date": "2026-05-28",
    "entry_price": 100.0,
    "shares": 100,
    "status": "open",
    "plan_entry_trigger": "Pivot $100 breakout (test)",
    "plan_stop": 95.0,
    "plan_target": 115.0,
    "plan_size_pct": 10.0,
    "plan_exit_strategy": "Trail 50DMA",
    "plan_time_horizon": "swing",
}


@pytest.fixture
def tid(client):
    r = client.post("/api/trades", json=TRADE_BODY)
    assert r.status_code == 201, f"POST basarisiz: {r.status_code} {r.text[:200]}"
    _id = r.json()["id"]
    yield _id
    client.delete(f"/api/trades/{_id}")


def test_new_trade_stop_defaults_to_plan(client, tid):
    """add_trade: aktif stop/hedef baslangici = plan (Migration 011)."""
    t = next(x for x in client.get("/api/trades").json() if x["id"] == tid)
    assert t["stop_loss"] == 95.0
    assert t["target_price"] == 115.0
    assert t["audible_reason"] is None


def test_stop_change_without_reason_422(client, tid):
    """#960 Audible Lock: aktif stop 95->98 degisiyor + sebep YOK -> 422."""
    r = client.patch(f"/api/trades/{tid}", json={"stop_loss": 98.0})
    assert r.status_code == 422, f"422 bekleniyordu: {r.status_code} {r.text[:200]}"
    assert "audible" in r.text.lower() or "sebep" in r.text.lower()


def test_target_change_without_reason_422(client, tid):
    """#960: aktif hedef 115->130 degisiyor + sebep YOK -> 422."""
    r = client.patch(f"/api/trades/{tid}", json={"target_price": 130.0})
    assert r.status_code == 422, f"422 bekleniyordu: {r.status_code} {r.text[:200]}"


def test_stop_change_with_reason_200(client, tid):
    """Sebep VAR -> 200 + aktif stop guncellendi + audible_reason kayitli."""
    r = client.patch(
        f"/api/trades/{tid}",
        json={"stop_loss": 98.0, "audible_reason": "trailing breakeven ustu"},
    )
    assert r.status_code == 200, f"200 bekleniyordu: {r.status_code} {r.text[:200]}"
    body = r.json()
    assert body["stop_loss"] == 98.0
    assert body["audible_reason"] == "trailing breakeven ustu"
    # plan_stop DEGISMEZ (orijinal plan korunur)
    assert body["plan_stop"] == 95.0


def test_same_stop_no_reason_ok(client, tid):
    """Ayni deger PATCH (degisiklik yok) -> sebep gerekmez (200)."""
    r = client.patch(f"/api/trades/{tid}", json={"stop_loss": 95.0})
    assert r.status_code == 200, f"200 bekleniyordu: {r.status_code} {r.text[:200]}"
