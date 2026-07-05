"""B4-01 (None-guard) + B1-04 (watchlist_update rowcount) testleri (05 Tem 2026).

B1-04: watchlist_update artik trades_update aynasi -> rowcount>0 bool doner (eskiden None).
B4-01: write-endpoint'lerde final get_one/get_by_id None -> `**None` opak TypeError→500 yerine
temiz HTTP (PATCH/promote→404, POST→500). None yolu near-imkansiz (tek-kullanici; TOCTOU race)
-> defansif hardening. Race, get_one/get_by_id mock->None ile simule edilir.
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
    import db_helpers
except ImportError:
    pytest.skip("fastapi yok", allow_module_level=True)


# ---- B1-04: watchlist_update rowcount (engine mock, DB'siz) ----

class _FakeResult:
    def __init__(self, rowcount): self.rowcount = rowcount

class _FakeConn:
    def __init__(self, rowcount): self._rc = rowcount
    def execute(self, *a, **k): return _FakeResult(self._rc)

class _FakeBegin:
    def __init__(self, rowcount): self._rc = rowcount
    def __enter__(self): return _FakeConn(self._rc)
    def __exit__(self, *a): return False

class _FakeEngine:
    def __init__(self, rowcount): self._rc = rowcount
    def begin(self): return _FakeBegin(self._rc)


class TestWatchlistUpdateRowcount:
    def test_returns_true_on_rowcount_1(self, monkeypatch):
        monkeypatch.setattr(db_helpers, "engine", _FakeEngine(1))
        assert db_helpers.watchlist_update("AAPL", "minervini", {"status": "focus"}) is True

    def test_returns_false_on_zero_rows(self, monkeypatch):
        # WHERE 0 satir eslesir (yok/race) -> False (trades_update deseni)
        monkeypatch.setattr(db_helpers, "engine", _FakeEngine(0))
        assert db_helpers.watchlist_update("NOPE", "minervini", {"status": "focus"}) is False

    def test_empty_updates_false_no_db(self, monkeypatch):
        # bos updates -> False, DB'ye HIC gitmez (engine cagirilirsa patlar)
        monkeypatch.setattr(db_helpers, "engine", _FakeEngine(999))  # cagirilmamali
        assert db_helpers.watchlist_update("AAPL", "minervini", {}) is False

    def test_only_pk_keys_false(self, monkeypatch):
        monkeypatch.setattr(db_helpers, "engine", _FakeEngine(999))
        assert db_helpers.watchlist_update("AAPL", "minervini", {"symbol": "X", "strategy": "y"}) is False


# ---- B4-01: endpoint None-guard (race simulasyonu) ----

@pytest.fixture
def client():
    return TestClient(api_main.app)


class TestWriteEndpointGuards:
    def test_patch_watchlist_race_delete_returns_404(self, client, monkeypatch):
        # exists→True (gecer), update, sonra get_one→None (race-delete) → 404, opak 500 DEGIL
        monkeypatch.setattr(api_main, "watchlist_exists", lambda s, st: True)
        monkeypatch.setattr(api_main, "watchlist_update", lambda s, st, u: True)
        monkeypatch.setattr(api_main, "watchlist_get_one", lambda s, st: None)
        r = client.patch("/api/watchlist/AAPL/minervini", json={"status": "focus"})
        assert r.status_code == 404

    def test_promote_race_delete_returns_404(self, client, monkeypatch):
        # ilk get_one→row (gecer), update, ikinci get_one→None → 404
        calls = {"n": 0}
        def _get_one(s, st):
            calls["n"] += 1
            return {"status": "watch"} if calls["n"] == 1 else None
        monkeypatch.setattr(api_main, "watchlist_get_one", _get_one)
        monkeypatch.setattr(api_main, "watchlist_update", lambda s, st, u: True)
        r = client.post("/api/watchlist/AAPL/minervini/promote")
        assert r.status_code == 404

    def test_patch_trades_race_delete_returns_404(self, client, monkeypatch):
        # ilk get_by_id→row (gecer), update, ikinci get_by_id→None → 404
        calls = {"n": 0}
        def _get(tid):
            calls["n"] += 1
            if calls["n"] == 1:
                return {"entry_price": 100.0, "shares": 10, "stop_loss": 95.0,
                        "target_price": 110.0, "status": "open", "exit_price": None}
            return None
        monkeypatch.setattr(api_main, "trades_get_by_id", _get)
        monkeypatch.setattr(api_main, "trades_update", lambda tid, u: True)
        r = client.patch("/api/trades/1", json={"note": "x"})
        assert r.status_code == 404

    def test_post_watchlist_insert_unverifiable_returns_500(self, client, monkeypatch):
        # exists→False (ekle yoluna gir), insert no-op, get_one→None → 500 (insert dogrulanamadi)
        monkeypatch.setattr(api_main, "watchlist_exists", lambda s, st: False)
        monkeypatch.setattr(api_main, "watchlist_insert", lambda row: None)
        monkeypatch.setattr(api_main, "watchlist_recompute_consensus", lambda: None)
        monkeypatch.setattr(api_main, "watchlist_get_one", lambda s, st: None)
        monkeypatch.setattr(api_main, "_resolve_price_real", lambda s: 100.0)
        monkeypatch.setattr(api_main, "_resolve_rs_real", lambda s: 80)
        r = client.post("/api/watchlist",
                        json={"symbol": "AAPL", "strategy": "minervini", "status": "watch"})
        assert r.status_code == 500
