"""Migration 012 / KARAR #477 (P482) — signal_source persistence pytest.

Denetim bulgu #15: TradeCreate signal_source ZORUNLU aliyordu ama web_trades'te
kolon YOKTU + db_helpers insert/select etmiyordu -> deger sessizce dusuyordu
(KARAR #477 "hangi giris tipi daha karli" analizi calismiyordu). Fix: Migration
012 ADD COLUMN signal_source + db_helpers insert/select wire + add_trade pass.

DB-bagimli (web_trades gercek write). Cleanup garantili (DELETE). DB yoksa skip.
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


_BASE = {
    "symbol": "ZZSIGSRC",
    "strategy": "minervini",
    "setup_type": "VCP",
    "entry_date": "2026-06-16",
    "entry_price": 100.0,
    "shares": 10,
    "status": "open",
    "plan_entry_trigger": "Pivot $100 breakout (test)",
    "plan_stop": 95.0,
    "plan_target": 115.0,
    "plan_size_pct": 10.0,
    "plan_exit_strategy": "Trail 50DMA",
    "plan_time_horizon": "swing",
}


@pytest.mark.parametrize("src", ["strategy", "manual_self", "manual_external"])
def test_signal_source_persists_roundtrip(client, src):
    """POST signal_source -> DB'ye yazilir -> POST response + GET roundtrip'te doner."""
    r = client.post("/api/trades", json={**_BASE, "signal_source": src})
    assert r.status_code == 201, f"POST basarisiz: {r.status_code} {r.text[:200]}"
    tid = r.json()["id"]
    try:
        # 1) POST response (trades_get_by_id SELECT)
        assert r.json()["signal_source"] == src, "POST response signal_source dusmus"
        # 2) GET /api/trades roundtrip (trades_get_all SELECT)
        got = next(x for x in client.get("/api/trades").json() if x["id"] == tid)
        assert got["signal_source"] == src, "GET roundtrip signal_source dusmus (KARAR #477 hala kirik)"
    finally:
        client.delete(f"/api/trades/{tid}")
