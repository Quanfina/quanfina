"""
P564 (20 Haz 2026): Watchlist'e ekleme GERÇEK RS + fiyat saklar — seeded mock KALDIRILDI.

Önceki bug (Kural #28 ihlali): add_watchlist_row _mock_rs (60+seed%31) + _mock_price (seed)
yazıyordu → taranmamış sembol için SAHTE RS/fiyat web_watchlist'e kalıcı + gerçek gösteriliyordu.
Fix: _resolve_rs_real (scan>cross-sectional>None) + _resolve_price_real (scan>watchlist>quote>0).
"""
import sys
from pathlib import Path
from types import SimpleNamespace

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
    _HAS_API = True
except ImportError:
    _HAS_API = False


@pytest.mark.skipif(not _HAS_API, reason="fastapi yok")
class TestResolveRsReal:
    def test_scanned_symbol_returns_scan_rs(self, monkeypatch):
        monkeypatch.setattr(api_main, "_resolve_rs_ibd", lambda s: 85)
        assert api_main._resolve_rs_real("ZZZ") == 85

    def test_untracked_cross_sectional(self, monkeypatch):
        # scan yok → canlı cross-sectional (P561). close 100→150 = +%50; evren → persentil 59
        monkeypatch.setattr(api_main, "_resolve_rs_ibd", lambda s: None)
        bars = [SimpleNamespace(close=100)] + [SimpleNamespace(close=150)] * 199
        monkeypatch.setattr(api_main, "_fetch_ohlcv_real", lambda s, n=252: bars)
        monkeypatch.setattr(api_main, "universe_perf_year_values", lambda: [10, 20, 30, 60, 70])
        assert api_main._resolve_rs_real("ZZZ") == 59

    def test_no_data_returns_none_not_seeded(self, monkeypatch):
        # Hiç veri yok → None (eski seeded 60+seed%31 DEĞİL)
        monkeypatch.setattr(api_main, "_resolve_rs_ibd", lambda s: None)
        monkeypatch.setattr(api_main, "_fetch_ohlcv_real", lambda s, n=252: None)
        monkeypatch.setattr(api_main, "universe_perf_year_values", lambda: [])
        assert api_main._resolve_rs_real("ZZZ") is None

    def test_mock_functions_removed(self):
        # Seeded uydurma fonksiyonlar tamamen kaldırıldı (Kural #18)
        assert not hasattr(api_main, "_mock_rs")
        assert not hasattr(api_main, "_mock_price")


@pytest.mark.skipif(not _HAS_API, reason="fastapi yok")
class TestAddWatchlistRealData:
    @pytest.fixture(scope="class")
    def client(self):
        return TestClient(api_main.app)

    def test_add_stores_real_rs_not_seeded(self, client, monkeypatch):
        captured = {}
        monkeypatch.setattr(api_main, "watchlist_exists", lambda s, st: False)
        monkeypatch.setattr(api_main, "watchlist_insert", lambda row: captured.update(row))
        monkeypatch.setattr(api_main, "watchlist_recompute_consensus", lambda: None)
        monkeypatch.setattr(api_main, "watchlist_get_one", lambda s, st: dict(captured))
        # Gerçek kaynaklar
        monkeypatch.setattr(api_main, "_resolve_rs_ibd", lambda s: 91)
        monkeypatch.setattr(api_main, "_fetch_scan_symbol_data",
                            lambda s: {"price": 212.5, "rs_ibd": 91, "company": "X",
                                       "sector": "Technology", "industry": "SW", "market_cap": None})
        r = client.post("/api/watchlist",
                        json={"symbol": "ZZZ", "strategy": "minervini", "status": "watch"})
        assert r.status_code == 201
        # ZZZ seed → eski mock 60+(sum(ord)%31)=60+(270%31)=82 OLURDU; gerçek 91 saklandı
        assert captured["rs_rating"] == 91
        assert captured["price"] == 212.5

    def test_add_no_data_stores_zero_not_seeded(self, client, monkeypatch):
        captured = {}
        monkeypatch.setattr(api_main, "watchlist_exists", lambda s, st: False)
        monkeypatch.setattr(api_main, "watchlist_insert", lambda row: captured.update(row))
        monkeypatch.setattr(api_main, "watchlist_recompute_consensus", lambda: None)
        monkeypatch.setattr(api_main, "watchlist_get_one", lambda s, st: dict(captured))
        monkeypatch.setattr(api_main, "_resolve_rs_ibd", lambda s: None)
        monkeypatch.setattr(api_main, "_fetch_ohlcv_real", lambda s, n=252: None)
        monkeypatch.setattr(api_main, "universe_perf_year_values", lambda: [])
        monkeypatch.setattr(api_main, "_fetch_scan_symbol_data", lambda s: None)
        monkeypatch.setattr(api_main, "watchlist_get_all", lambda: [])
        monkeypatch.setattr(api_main, "get_stock_quote",
                            lambda s: SimpleNamespace(source="mock", price=0.0))
        r = client.post("/api/watchlist",
                        json={"symbol": "QQXY", "strategy": "carr", "status": "watch"})
        assert r.status_code == 201
        # Veri yok → 0 (dürüst), seeded uydurma DEĞİL
        assert captured["rs_rating"] == 0
        assert captured["price"] == 0.0
