"""Carr Bullish Divergence (TLFAL 2.baski s.258) — compute_bullish_divergence. P514.

Uptrend-dip LONG ADAYI: fiyat lower low + 2+ gosterge higher low. 6 gosterge (MACD line/hist,
Stoch%K, RSI(5), CCI(20), OBV) -> 2+ diverge. Cift danisma teyitli. Sentetik 230-bar: uptrend
(100->300) + keskin dip (momentum dibi ~i-15) + nazik grind (marjinal lower low + momentum
recovery) + bugun yesil.
"""
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from quanfina_math import (  # noqa: E402
    compute_bullish_divergence,
    _macd_histogram_series,
    _rsi_series,
    _cci_series,
)


def _bullish_div_series(n: int = 230):
    """Uptrend (0..200: ->300) + keskin dip (201..214: momentum dibi) + nazik grind
    (215..228: marjinal lower low ama momentum recovery) + bugun (229) yesil.
    Divergence: i-15=214 momentum dibi, bugun gostergeler daha yuksek (price lower low).
    """
    closes = []
    for i in range(n):
        if i <= 200:
            c = 100.0 + i
        elif i <= 214:
            c = 300.0 - (i - 200) * 1.1   # keskin dip -> 284.6
        elif i <= 228:
            c = 284.6 - (i - 214) * 0.15  # nazik grind -> 282.5 (marjinal lower low)
        else:
            c = 285.0                     # bugun yesil yukari
        closes.append(c)
    opens = [c - 0.2 for c in closes]
    opens[-1] = closes[-2]  # bugun open = dun close -> yesil (285 > 282.5)
    highs = [c + 1.0 for c in closes]
    lows = [c - 1.0 for c in closes]
    volumes = [1000.0] * n
    return opens, highs, lows, closes, volumes


class TestBullishDivergenceGuards:
    def test_empty(self):
        r = compute_bullish_divergence([], [], [], [], [])
        assert r["detected"] is False and r["direction"] is None

    def test_length_mismatch(self):
        x = [1.0] * 201
        r = compute_bullish_divergence(x, x, x, x, [1.0] * 200)
        assert r["detected"] is False and "uzunluk" in r["mark_says"].lower()

    def test_insufficient_bars(self):
        x = [100.0 + i for i in range(150)]
        r = compute_bullish_divergence(x, x, x, x, [1000.0] * 150)
        assert r["detected"] is False and "201" in r["mark_says"]


class TestBullishDivergenceDetection:
    def test_valid_divergence_detected(self):
        o, h, l, c, v = _bullish_div_series()
        r = compute_bullish_divergence(o, h, l, c, v)
        assert r["detected"] is True, f"{r['mark_says']} | {r['rules']}"
        assert r["direction"] == "LONG"
        assert all(r["rules"].values()), r["rules"]

    def test_two_or_more_indicators_diverge(self):
        """Carr s.255 — en az 2 gosterge divergence sart."""
        o, h, l, c, v = _bullish_div_series()
        r = compute_bullish_divergence(o, h, l, c, v)
        assert r["divergence_count"] >= 2, f"{r['divergence_count']}: {r['divergence_indicators']}"
        assert len(r["divergence_indicators"]) == r["divergence_count"]

    def test_entry_is_close(self):
        """entry=close (ertesi gun open proxy, s.262)."""
        o, h, l, c, v = _bullish_div_series()
        r = compute_bullish_divergence(o, h, l, c, v)
        assert r["entry"] == round(c[-1], 2)

    def test_uptrend_context(self):
        """SMA50>SMA200 (uptrend-dip, s.258)."""
        o, h, l, c, v = _bullish_div_series()
        r = compute_bullish_divergence(o, h, l, c, v)
        assert r["sma50"] > r["sma200"]

    def test_exit_stop_below_pivot(self):
        """STOP sell-off son dibi alti (s.262) + %8 cap; target 2R; TIME STOP YOK."""
        o, h, l, c, v = _bullish_div_series()
        r = compute_bullish_divergence(o, h, l, c, v)
        assert r["stop"] < r["entry"] and r["target"] > r["entry"]
        assert r["risk_pct"] <= 8.01
        expected = r["entry"] + 2.0 * (r["entry"] - r["stop"])
        assert abs(r["target"] - expected) < 0.05
        assert r["rr"] == 2.0
        assert "time_stop_days" not in r

    def test_quality_candidate(self):
        o, h, l, c, v = _bullish_div_series()
        r = compute_bullish_divergence(o, h, l, c, v)
        assert r["quality"] == "CANDIDATE"
        assert len(r["eyeball_checks"]) >= 3

    def test_not_green_rejected(self):
        o, h, l, c, v = _bullish_div_series()
        o = list(o)
        o[-1] = c[-1] + 1.0  # bearish bugun
        r = compute_bullish_divergence(o, h, l, c, v)
        assert r["detected"] is False
        assert r["rules"]["bugun_yesil"] is False

    def test_pure_uptrend_no_pivot_rejected(self):
        """Dip yoksa (saf uptrend) pivot lower low kurallari fail."""
        c = [100.0 + i for i in range(230)]
        o = [x - 0.2 for x in c]
        h = [x + 1.0 for x in c]
        l = [x - 1.0 for x in c]
        v = [1000.0] * 230
        r = compute_bullish_divergence(o, h, l, c, v)
        assert r["detected"] is False
        assert r["rules"]["pivot_low_20"] is False


class TestDivergenceHelpers:
    def test_rsi_bounds(self):
        rising = [100.0 + i for i in range(20)]
        rsi = _rsi_series(rising, 5)
        assert rsi[4] is None  # period warmup
        assert rsi[-1] == 100.0  # tamamen yukselen -> RSI 100 (loss=0)

    def test_rsi_falling(self):
        falling = [100.0 - i for i in range(20)]
        rsi = _rsi_series(falling, 5)
        assert rsi[-1] == 0.0  # tamamen dusen -> RSI 0 (gain=0)

    def test_cci_zero_when_flat(self):
        flat = [100.0] * 30
        cci = _cci_series([100.5] * 30, [99.5] * 30, flat, 20)
        assert cci[-1] == 0.0  # sabit TP -> mean dev 0 -> CCI 0

    def test_macd_histogram_basic(self):
        closes = [100.0 + i for i in range(60)]  # yukselen
        mh = _macd_histogram_series(closes, 12, 26, 9)
        assert mh[20] is None  # signal warmup oncesi
        assert mh[-1] is not None
