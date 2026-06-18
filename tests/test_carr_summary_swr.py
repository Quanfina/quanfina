"""P532 — /api/carr/summary stale-while-revalidate (SWR) davranisi.

screen_carr_summary cold-compute ~23s (800+ hisse x 9 detector). 30 dk cache
dolunca kullanici 23s bekliyordu. SWR: bayat cache -> eski deger ANINDA + arka
planda yenile. Bu test screen_carr_summary'yi stub'layip 3 yolu dogrular:
taze (recompute yok) / bayat (stale don + bg refresh) / cold (bloklayan hesap).
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

CK = ("carr_summary", 6)


@pytest.fixture
def client():
    return TestClient(api_main.app)


@pytest.fixture(autouse=True)
def _clean_cache():
    api_main._SCREENS_CACHE.pop(CK, None)
    api_main._carr_summary_refreshing.discard(6)
    yield
    api_main._SCREENS_CACHE.pop(CK, None)
    api_main._carr_summary_refreshing.discard(6)


def _model(count):
    return api_main.CarrSummarySetup(
        slug="pullback", label="Pullback", direction="LONG", count=count, candidates=[]
    )


def _rows(count):
    return [{"slug": "pullback", "label": "Pullback", "direction": "LONG",
             "count": count, "candidates": []}]


def test_fresh_cache_no_recompute(client, monkeypatch):
    """Taze cache (TTL icinde) -> cached don, screen_carr_summary CAGRILMAZ."""
    monkeypatch.setattr(api_main, "db_health_check", lambda: True)

    def _boom(*a, **k):
        raise AssertionError("taze cache'te recompute olmamali")

    monkeypatch.setattr(api_main, "screen_carr_summary", _boom)
    api_main._SCREENS_CACHE[CK] = (api_main._now(), [_model(7)])
    r = client.get("/api/carr/summary")
    assert r.status_code == 200
    assert r.json()[0]["count"] == 7


def test_stale_returns_stale_then_bg_refresh(client, monkeypatch):
    """Bayat cache -> eski deger (count=11) ANINDA + arka plan cache'i 99'a gunceller."""
    monkeypatch.setattr(api_main, "db_health_check", lambda: True)
    monkeypatch.setattr(api_main, "screen_carr_summary", lambda top_n=6: _rows(99))
    stale_ts = api_main._now() - api_main._SCREENS_CACHE_TTL - 10
    api_main._SCREENS_CACHE[CK] = (stale_ts, [_model(11)])

    r = client.get("/api/carr/summary")
    assert r.status_code == 200
    assert r.json()[0]["count"] == 11  # ANINDA stale (23s beklemez)

    # arka plan refresh cache'i tazeler (stub hizli; poll ~2.5s ust sinir)
    for _ in range(50):
        v = api_main._SCREENS_CACHE.get(CK)
        if v and v[1] and v[1][0].count == 99:
            break
        time.sleep(0.05)
    assert api_main._SCREENS_CACHE[CK][1][0].count == 99
    assert 6 not in api_main._carr_summary_refreshing  # dedupe guard temizlendi


def test_cold_computes_blocking(client, monkeypatch):
    """Cache yok -> bloklayan hesap (skeleton P526 gosterilir), sonuc cache'lenir."""
    monkeypatch.setattr(api_main, "db_health_check", lambda: True)
    monkeypatch.setattr(api_main, "screen_carr_summary", lambda top_n=6: _rows(5))
    r = client.get("/api/carr/summary")
    assert r.status_code == 200
    assert r.json()[0]["count"] == 5
    assert api_main._SCREENS_CACHE[CK][1][0].count == 5


def test_nocache_forces_recompute(client, monkeypatch):
    """nocache=true -> taze cache olsa bile yeniden hesap (olcum/force-refresh yolu)."""
    monkeypatch.setattr(api_main, "db_health_check", lambda: True)
    monkeypatch.setattr(api_main, "screen_carr_summary", lambda top_n=6: _rows(42))
    api_main._SCREENS_CACHE[CK] = (api_main._now(), [_model(7)])  # taze ama eski deger
    r = client.get("/api/carr/summary?nocache=true")
    assert r.status_code == 200
    assert r.json()[0]["count"] == 42  # cache atlandi, yeniden hesaplandi
