"""P536 — /api/screens/{slug} stale-while-revalidate (SWR).

compute-screen'ler (pocket_pivot/mean_reversion/pullback...) cold ~20s. 30 dk cache
dolunca kullanici 20s skeleton bekliyordu. SWR: bayat cache -> eski sonuc ANINDA +
arka planda yenile. Bu test dispatch'i stub'layip 3 yolu dogrular: taze (recompute
yok) / bayat (stale don + bg refresh) / cold (bloklayan).
"""
import sys
import time
from pathlib import Path

import pytest

HERE = Path(__file__).resolve().parent
ROOT = HERE.parent
API = ROOT / "api"
for d in (str(ROOT), str(API)):
    if d not in sys.path:
        sys.path.insert(0, d)

try:
    from fastapi.testclient import TestClient
    import main as api_main
except ImportError:
    pytest.skip("fastapi yok", allow_module_level=True)

SLUG = "pocket_pivot"
CK = (SLUG, 500)


@pytest.fixture
def client():
    return TestClient(api_main.app)


@pytest.fixture(autouse=True)
def _clean():
    api_main._SCREENS_CACHE.pop(CK, None)
    api_main._screen_refreshing.discard(CK)
    # pivot enrichment yfinance cagirmasin
    yield
    api_main._SCREENS_CACHE.pop(CK, None)
    api_main._screen_refreshing.discard(CK)


def _row(sym):
    return api_main.ScreenResultRow(symbol=sym)


def test_fresh_cache_no_recompute(client, monkeypatch):
    def _boom(*a, **k):
        raise AssertionError("taze cache'te dispatch cagrilmamali")
    monkeypatch.setattr(api_main, "screen_get_results_dispatch", _boom)
    api_main._SCREENS_CACHE[CK] = (api_main._now(), [_row("FRESH_CACHED")])
    r = client.get(f"/api/screens/{SLUG}")
    assert r.status_code == 200
    assert r.json()[0]["symbol"] == "FRESH_CACHED"


def test_stale_returns_stale_then_bg_refresh(client, monkeypatch):
    monkeypatch.setattr(api_main, "db_health_check", lambda: True)
    monkeypatch.setattr(api_main, "_compute_signal_pivot_status", lambda *a, **k: None)
    monkeypatch.setattr(api_main, "screen_get_results_dispatch",
                        lambda slug, limit=500: [{"symbol": "REFRESHED"}])
    stale_ts = api_main._now() - api_main._SCREENS_CACHE_TTL - 10
    api_main._SCREENS_CACHE[CK] = (stale_ts, [_row("STALE")])

    r = client.get(f"/api/screens/{SLUG}")
    assert r.status_code == 200
    assert r.json()[0]["symbol"] == "STALE"  # ANINDA stale (20s beklemez)

    for _ in range(60):
        v = api_main._SCREENS_CACHE.get(CK)
        if v and v[1] and v[1][0].symbol == "REFRESHED":
            break
        time.sleep(0.05)
    assert api_main._SCREENS_CACHE[CK][1][0].symbol == "REFRESHED"
    assert CK not in api_main._screen_refreshing  # dedupe guard temizlendi


def test_cold_computes_blocking(client, monkeypatch):
    monkeypatch.setattr(api_main, "db_health_check", lambda: True)
    monkeypatch.setattr(api_main, "_compute_signal_pivot_status", lambda *a, **k: None)
    monkeypatch.setattr(api_main, "screen_get_results_dispatch",
                        lambda slug, limit=500: [{"symbol": "COLD"}])
    r = client.get(f"/api/screens/{SLUG}")
    assert r.status_code == 200
    assert r.json()[0]["symbol"] == "COLD"
    assert api_main._SCREENS_CACHE[CK][1][0].symbol == "COLD"
