"""/api/signals endpoint pytest (KARAR #469 — watchlist'ten türetilmiş sinyaller).

Read-only GET. DB-bagimli. DB erisilemezse skip.
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
    r = client.get("/api/signals")
    assert r.status_code == 200


def test_returns_list(client):
    r = client.get("/api/signals")
    assert isinstance(r.json(), list)


def test_signal_shape(client):
    """Sinyal varsa zorunlu alanlar (symbol, strategy, status, price)."""
    r = client.get("/api/signals")
    data = r.json()
    if data:
        s = data[0]
        for field in ("symbol", "strategy", "status", "price"):
            assert field in s


def test_signal_status_valid(client):
    """status watchlist katmanlarından biri (watch/on_deck/focus/buy)."""
    r = client.get("/api/signals")
    for s in r.json():
        assert s["status"] in ("watch", "on_deck", "focus", "buy")


def test_signal_strategy_valid(client):
    """strategy minervini veya carr."""
    r = client.get("/api/signals")
    for s in r.json():
        assert s["strategy"] in ("minervini", "carr")


def test_signal_price_coherent_with_stop_target(client):
    """P493 (Kural #28): price stale web_watchlist snapshot iken stop/hedef ile
    TUTARSIZdi (NVDA price=$875.40 eklendigi tarih, ama hedef $268 canli OHLCV'den
    => price > hedef = sacma sinyal). Fix: price canli son kapanis. Invariant:
    stop/hedef doluysa stop_loss < price < target_price (tutarli AL sinyali).
    """
    r = client.get("/api/signals")
    for s in r.json():
        stop, price, target = s.get("stop_loss"), s.get("price"), s.get("target_price")
        if stop is not None and target is not None and price is not None:
            assert stop < price < target, (
                f"{s['symbol']}/{s['strategy']}: price={price} stop/hedef ile tutarsiz "
                f"(stop={stop}, hedef={target}). Stale-price regresyonu (P493)?"
            )
