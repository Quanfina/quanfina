"""Carr Pullback (TLFAL 2.baski s.268 entry screener) — compute_carr_pullback testleri. P504.

ENTRY 6 kural (s.268) tam sourced; CIKIS (stop/target) cift danisma bekliyor -> None (Kural #26).
Sentetik 210-bar: dik uptrend + son 3 bar shallow pullback (Stoch %K<20 ama SMA20 hala rising).
"""
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from quanfina_math import compute_carr_pullback, _sma_at, _stochastic_k  # noqa: E402


def _uptrend_pullback(slope: float = 3.0, n: int = 210):
    """Dik uptrend (SMA20>SMA50>SMA200 + rising) + son 3 bar shallow pullback.
    close[-4]=top, [-3]=-8, [-2]=-5, [-1]=+3 (bullish bar near low -> Stoch %K<20).
    """
    closes = [100.0 + slope * i for i in range(n)]
    closes[-3] = closes[-4] - 8.0
    closes[-2] = closes[-3] - 5.0
    closes[-1] = closes[-2] + 3.0
    opens = [c - 0.5 for c in closes]   # her bar bullish (rule 6 sadece bugun bakar)
    highs = [c + 1.0 for c in closes]
    lows = [c - 1.0 for c in closes]
    return opens, highs, lows, closes


class TestCarrPullbackGuards:
    def test_empty(self):
        r = compute_carr_pullback([], [], [], [])
        assert r["detected"] is False
        assert r["direction"] is None

    def test_length_mismatch(self):
        r = compute_carr_pullback([1.0] * 210, [1.0] * 210, [1.0] * 210, [1.0] * 209)
        assert r["detected"] is False
        assert "uzunluk" in r["mark_says"].lower()

    def test_insufficient_bars(self):
        o = [100.0 + i for i in range(150)]
        r = compute_carr_pullback(o, o, o, o)
        assert r["detected"] is False
        assert "200" in r["mark_says"]


class TestCarrPullbackDetection:
    def test_valid_pullback_detected(self):
        o, h, l, c = _uptrend_pullback()
        r = compute_carr_pullback(o, h, l, c)
        assert r["detected"] is True, r["mark_says"]
        assert r["direction"] == "LONG"
        assert all(r["rules"].values()), r["rules"]
        assert r["entry"] == round(c[-1], 2)

    def test_stoch_oversold_below_20(self):
        o, h, l, c = _uptrend_pullback()
        r = compute_carr_pullback(o, h, l, c)
        assert r["stoch_k"] < 20.0, f"Stoch %K={r['stoch_k']} (oversold <20 bekleniyor)"

    def test_stop_target_none_pending_cift_danisma(self):
        """Kural #26 — Carr cikis kurallari (s.227-242) lokal eksik, uydurma YOK -> None."""
        o, h, l, c = _uptrend_pullback()
        r = compute_carr_pullback(o, h, l, c)
        assert r["detected"] is True
        assert r["stop"] is None
        assert r["target"] is None

    def test_sma_alignment_reported(self):
        o, h, l, c = _uptrend_pullback()
        r = compute_carr_pullback(o, h, l, c)
        assert r["sma20"] > r["sma50"] > r["sma200"]

    def test_not_bullish_candle_rejects(self):
        o, h, l, c = _uptrend_pullback()
        o = list(o)
        o[-1] = c[-1] + 1.0  # bugun bearish (close < open) -> rule 6 fail
        r = compute_carr_pullback(o, h, l, c)
        assert r["detected"] is False
        assert r["rules"]["bugun_bullish"] is False

    def test_stoch_not_oversold_rejects(self):
        o, h, l, c = _uptrend_pullback()
        c = list(c)
        c[-1] = c[-4]  # son bar tepe seviyeye geri -> Stoch yuksek
        o = [x - 0.5 for x in c]
        h = [x + 1.0 for x in c]
        l = [x - 1.0 for x in c]
        r = compute_carr_pullback(o, h, l, c)
        assert r["rules"].get("stoch_k_oversold") is False or r["detected"] is False


class TestCarrPullbackHelpers:
    def test_sma_at(self):
        closes = [float(i) for i in range(1, 21)]  # 1..20
        assert _sma_at(closes, 19, 20) == sum(range(1, 21)) / 20
        assert _sma_at(closes, 5, 20) is None  # yetersiz veri

    def test_stochastic_k_range(self):
        closes = [10.0, 11.0, 12.0, 13.0, 14.0, 15.0, 16.0]
        highs = [c + 1.0 for c in closes]
        lows = [c - 1.0 for c in closes]
        k = _stochastic_k(highs, lows, closes, 6, 5, 3)
        assert 0.0 <= k <= 100.0

    def test_stochastic_k_flat_range_neutral(self):
        """Duz aralik (HH==LL) -> 50 notr (div0 onleme)."""
        flat = [50.0] * 7
        k = _stochastic_k(flat, flat, flat, 6, 5, 3)
        assert k == 50.0
