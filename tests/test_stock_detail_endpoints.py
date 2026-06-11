"""
/api/stock/{symbol}/{overhead,atr,relative-volume,breakout-quality} (Paket 378).

Stok detay sayfasi Mark compute kartlari — yfinance OHLCV'den hesaplar (_get_ohlcv).
Endpoint wire + serialize + edge (bilinmeyen sembol -> 500 yok) kapsanir.

NOT (P378): _OHLCV_CACHE manuel seed edilir -> yfinance NETWORK CAGRISI BYPASS
(test_stock_quote_endpoint pateni). Aksi halde her endpoint canli yfinance ceker
(2 saat + flaky). Seed ile fast + deterministik + network-bagimsiz.
"""
import json
import sys
import time
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
except ImportError:
    pytest.skip("fastapi yok", allow_module_level=True)


def _synthetic_bars(n=250, start=100.0):
    """Sentetik OHLCV (hafif uptrend) — compute helper'lar icin yeterli (>=200 bar)."""
    bars = []
    for i in range(n):
        close = start + i * 0.5
        bars.append(api_main.OhlcvBar(
            time=f"2026-01-{(i % 28) + 1:02d}",
            open=close - 0.3, high=close + 0.8, low=close - 0.8,
            close=close, volume=1_000_000 + i * 1000,
        ))
    return bars


@pytest.fixture(scope="module")
def client():
    return TestClient(api_main.app)


@pytest.fixture(autouse=True)
def _seed_cache():
    # yfinance bypass: test sembolleri icin OHLCV cache'i doldur (network yok)
    now = time.time()
    for sym in ("AAPL", "ZZZZ"):
        api_main._OHLCV_CACHE[sym] = (now, _synthetic_bars())
    yield
    for sym in ("AAPL", "ZZZZ"):
        api_main._OHLCV_CACHE.pop(sym, None)


METRICS = ["overhead", "atr", "relative-volume", "breakout-quality"]


class TestStockDetailComputeEndpoints:
    @pytest.mark.parametrize("metric", METRICS)
    def test_known_symbol_200(self, client, metric):
        r = client.get(f"/api/stock/AAPL/{metric}")
        assert r.status_code == 200, f"{metric} -> {r.status_code}: {r.text[:200]}"
        d = r.json()
        assert "mark_says" in d and isinstance(d["mark_says"], str)
        json.dumps(d, allow_nan=False)  # inf/NaN serialize 500 sinifina karsi

    @pytest.mark.parametrize("metric", METRICS)
    def test_seeded_unknown_symbol_no_500(self, client, metric):
        # ZZZZ cache'te seed'li -> compute calisir, 200 + serialize-safe
        r = client.get(f"/api/stock/ZZZZ/{metric}")
        assert r.status_code == 200, f"{metric} ZZZZ -> {r.status_code}: {r.text[:200]}"
        json.dumps(r.json(), allow_nan=False)

    def test_atr_stop_levels_ordering(self, client):
        # ATR 3 stop seviyesi varsa tight >= normal >= loose (stop fiyati uzaklikla duser)
        d = client.get("/api/stock/AAPL/atr").json()
        t, n, l = d.get("suggested_stop_tight"), d.get("suggested_stop_normal"), d.get("suggested_stop_loose")
        if t is not None and n is not None and l is not None:
            assert t >= n >= l

    def test_relative_volume_category_valid(self, client):
        d = client.get("/api/stock/AAPL/relative-volume").json()
        if d.get("category") is not None:
            assert d["category"] in {"DRYING", "QUIET", "NORMAL", "HIGH", "SURGE"}

    def test_breakout_quality_real_components_wired(self, client):
        """P450 (DD-sonrasi #2 derin arastirma): 4/5 hardcoded bilesen GERCEK
        OHLCV'den hesaplandi (gap_up inline + compute_pivot_breakout + compute_
        overhead_supply + compute_vcp_pass). Artik 5/5 gercek → real_components
        5 bilesen, P443 'kismi MOCK' uyarisi kalkar. UI tam-skoru guvenle gosterir."""
        d = client.get("/api/stock/AAPL/breakout-quality").json()
        assert "real_components" in d
        rc = set(d["real_components"])
        assert {
            "volume", "gap_up", "breakout_strength", "prior_contraction", "overhead_clean",
        } == rc
        # Artik hardcoded MOCK yok → "kismi" uyarisi mark_says'te OLMAMALI
        assert "Kısmi" not in d["mark_says"] and "kısmi" not in d["mark_says"]
