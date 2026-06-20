"""
P570 (20 Haz 2026): Carr Pullback/Relief Rally ratio — piyasa rejim göstergesi (s.280).

compute_carr_relief_rally = compute_carr_pullback'in TAM AYNASI (SHORT). Oran = pullback/relief.
Carr s.280: >1+yükseliyor → boğa/oversold (AL); <1+düşüyor → ayı/overbought (SHORT).
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

from quanfina_math import compute_carr_relief_rally, compute_carr_pullback  # noqa: E402


class TestComputeCarrReliefRally:
    def test_insufficient_data(self):
        r = compute_carr_relief_rally([1] * 10, [1] * 10, [1] * 10, [1] * 10)
        assert r["detected"] is False
        assert r["direction"] is None

    def test_uptrend_no_relief_signal(self):
        # Net yükseliş (SMA20>SMA50>SMA200) → relief rally (SHORT) sinyali YOK
        closes = [100 + i * 0.5 for i in range(220)]  # yükselen
        o = closes[:]; h = [c + 1 for c in closes]; lo = [c - 1 for c in closes]
        r = compute_carr_relief_rally(o, h, lo, closes)
        assert r["detected"] is False
        # downtrend kuralları yükselişte sağlanmaz
        assert r["rules"]["sma50_alti_sma200"] is False

    def test_mirror_of_pullback_opposite_direction(self):
        # Aynı yükselen veri: pullback LONG yönlü kurguda, relief SHORT kurguda — ayna
        closes = [100 + i * 0.5 for i in range(220)]
        o = closes[:]; h = [c + 1 for c in closes]; lo = [c - 1 for c in closes]
        pb = compute_carr_pullback(o, h, lo, closes)
        rr = compute_carr_relief_rally(o, h, lo, closes)
        # Yükselişte: pullback'in sma50>sma200 True, relief'in sma50<sma200 False (ayna)
        assert pb["rules"]["sma50_ust_sma200"] is True
        assert rr["rules"]["sma50_alti_sma200"] is False

    def test_downtrend_detection_short(self):
        # Uzun düşüş + son barlarda overbought sıçrama + bearish kapanış → SHORT sinyal
        closes = [300 - i * 0.8 for i in range(195)]          # 300 → ~145 dik düşüş
        closes += [148, 151, 153, 154, 152]                   # küçük sıçrama (stoch>80)
        o = [c for c in closes]
        h = [c + 0.5 for c in closes]
        lo = [c - 0.5 for c in closes]
        o[-1] = 155.0  # final bar bearish: open > close (155 > 152)
        h[-1] = 155.5
        r = compute_carr_relief_rally(o, h, lo, closes)
        # Düşüş trendi kuralları sağlanmalı (en azından SMA dizilimi + düşüş)
        assert r["rules"]["sma50_alti_sma200"] is True
        assert r["rules"]["sma20_alti_sma50"] is True
        if r["detected"]:
            assert r["direction"] == "SHORT"
            assert r["entry"] < r["stop"]      # SHORT: stop entry'nin ÜSTÜNDE
            assert r["target"] < r["entry"]    # hedef entry'nin ALTINDA


# --- Oran rejim mantığı (endpoint) ---
try:
    from fastapi.testclient import TestClient
    import main as api_main
    _HAS_API = True
except ImportError:
    _HAS_API = False


@pytest.mark.skipif(not _HAS_API, reason="fastapi yok")
class TestRatioRegimeEndpoint:
    @pytest.fixture(scope="class")
    def client(self):
        return TestClient(api_main.app)

    def test_bullish_oversold(self, client, monkeypatch):
        # >1 ve yükseliyor → BULLISH_OVERSOLD (AL)
        monkeypatch.setattr(api_main, "pullback_relief_ratio_series", lambda days=5: [
            {"scan_date": "2026-06-20", "pullback": 30, "relief": 20, "ratio": 1.5},
            {"scan_date": "2026-06-19", "pullback": 24, "relief": 20, "ratio": 1.2},
        ])
        d = client.get("/api/market/pullback-relief-ratio").json()
        assert d["available"] is True
        assert d["ratio"] == 1.5
        assert d["direction"] == "rising"
        assert d["regime"] == "BULLISH_OVERSOLD"
        assert "buyable dip" in d["mark_says"] or "dipten alım" in d["mark_says"]

    def test_bearish_overbought(self, client, monkeypatch):
        # <1 ve düşüyor → BEARISH_OVERBOUGHT (SHORT)
        monkeypatch.setattr(api_main, "pullback_relief_ratio_series", lambda days=5: [
            {"scan_date": "2026-06-20", "pullback": 12, "relief": 20, "ratio": 0.6},
            {"scan_date": "2026-06-19", "pullback": 16, "relief": 20, "ratio": 0.8},
        ])
        d = client.get("/api/market/pullback-relief-ratio").json()
        assert d["regime"] == "BEARISH_OVERBOUGHT"
        assert d["direction"] == "falling"

    def test_neutral_high_but_falling(self, client, monkeypatch):
        # >1 ama DÜŞÜYOR → NEUTRAL (net boğa değil)
        monkeypatch.setattr(api_main, "pullback_relief_ratio_series", lambda days=5: [
            {"scan_date": "2026-06-20", "pullback": 30, "relief": 20, "ratio": 1.5},
            {"scan_date": "2026-06-19", "pullback": 36, "relief": 20, "ratio": 1.8},
        ])
        d = client.get("/api/market/pullback-relief-ratio").json()
        assert d["regime"] == "NEUTRAL"
        assert d["direction"] == "falling"

    def test_relief_zero_denominator(self, client, monkeypatch):
        # relief=0 (payda) → ratio None, aşırı boğa notu
        monkeypatch.setattr(api_main, "pullback_relief_ratio_series", lambda days=5: [
            {"scan_date": "2026-06-20", "pullback": 25, "relief": 0, "ratio": None},
        ])
        d = client.get("/api/market/pullback-relief-ratio").json()
        assert d["available"] is True
        assert d["ratio"] is None
        assert "aşırı boğa" in d["mark_says"]

    def test_no_data_unavailable(self, client, monkeypatch):
        monkeypatch.setattr(api_main, "pullback_relief_ratio_series", lambda days=5: [])
        d = client.get("/api/market/pullback-relief-ratio").json()
        assert d["available"] is False
