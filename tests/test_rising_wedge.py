"""Carr Rising Wedge Breakdown (TLFAL 2.baski Bol.19) — compute_rising_wedge_breakdown. P521.

uptrend-sonu kama kirilimi SHORT ADAYI. 6 kural + bearish divergence (MACD/OBV dusuyor). Cift
danisma teyitli. Sentetik 120-bar: guclu uptrend + yavas grind (kama, MACD decel) + dagitim
(down-vol -> OBV duser) + daralan range + bugun kirmizi.
"""
import math
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from quanfina_math import compute_rising_wedge_breakdown  # noqa: E402


def _rising_wedge_series(n: int = 120):
    """Phase1 guclu uptrend (0..69: ->169) + phase2 yavas grind+osilasyon (70..119: higher
    highs ama MACD decel) + daralan range + dagitim (down gun buyuk vol -> OBV duser). Bugun kirmizi.
    """
    opens, highs, lows, closes, volumes = [], [], [], [], []
    for i in range(n):
        if i <= 69:
            c = 100.0 + i * 1.0   # guclu uptrend
            hr = 1.0
        else:
            t = i - 70
            c = 169.0 + t * 0.05 + 1.2 * math.sin(t * 0.85)  # yavas grind + osilasyon
            hr = max(0.3, 2.5 - t * 0.045)  # daralan range (kama)
        closes.append(c)
        highs.append(c + hr)
        lows.append(c - hr)
        opens.append(c - 0.2)
    # dagitim: down gun buyuk hacim (OBV duser), up gun kucuk
    for i in range(n):
        if i == 0 or closes[i] >= closes[i - 1]:
            volumes.append(500.0)
        else:
            volumes.append(3000.0)
    # bugun kirmizi (close < open)
    opens[-1] = closes[-1] + 0.3
    return opens, highs, lows, closes, volumes


class TestRisingWedgeGuards:
    def test_empty(self):
        r = compute_rising_wedge_breakdown([], [], [], [], [])
        assert r["detected"] is False and r["direction"] is None

    def test_length_mismatch(self):
        x = [1.0] * 90
        r = compute_rising_wedge_breakdown(x, x, x, x, [1.0] * 89)
        assert r["detected"] is False and "uzunluk" in r["mark_says"].lower()

    def test_insufficient_bars(self):
        x = [100.0 + i for i in range(70)]
        r = compute_rising_wedge_breakdown(x, x, x, x, [1000.0] * 70)
        assert r["detected"] is False and "90" in r["mark_says"]


class TestRisingWedgeDetection:
    def test_valid_wedge_detected(self):
        o, h, l, c, v = _rising_wedge_series()
        r = compute_rising_wedge_breakdown(o, h, l, c, v)
        assert r["detected"] is True, f"{r['mark_says']} | {r['rules']}"
        assert r["direction"] == "SHORT"
        assert all(r["rules"].values()), r["rules"]

    def test_bearish_divergence(self):
        """SMA50 yukseliyor (uptrend) AMA MACD+OBV dusuyor (bearish divergence)."""
        o, h, l, c, v = _rising_wedge_series()
        r = compute_rising_wedge_breakdown(o, h, l, c, v)
        assert r["rules"]["sma50_yukseliyor"] is True
        assert r["rules"]["macd_dusuyor"] is True
        assert r["rules"]["obv_dusuyor"] is True

    def test_entry_is_close(self):
        o, h, l, c, v = _rising_wedge_series()
        r = compute_rising_wedge_breakdown(o, h, l, c, v)
        assert r["entry"] == round(c[-1], 2)

    def test_short_stop_above_target_below(self):
        o, h, l, c, v = _rising_wedge_series()
        r = compute_rising_wedge_breakdown(o, h, l, c, v)
        assert r["stop"] > r["entry"] and r["target"] < r["entry"]
        assert r["risk_pct"] <= 8.01
        expected = r["entry"] - 2.0 * (r["stop"] - r["entry"])
        assert abs(r["target"] - expected) < 0.05
        assert r["rr"] == 2.0
        assert "time_stop_days" not in r

    def test_quality_candidate(self):
        o, h, l, c, v = _rising_wedge_series()
        r = compute_rising_wedge_breakdown(o, h, l, c, v)
        assert r["quality"] == "CANDIDATE"
        assert any("OBV" in e.upper() for e in r["eyeball_checks"])

    def test_not_red_rejected(self):
        o, h, l, c, v = _rising_wedge_series()
        o = list(o)
        o[-1] = c[-1] - 1.0  # yesil bugun
        r = compute_rising_wedge_breakdown(o, h, l, c, v)
        assert r["detected"] is False
        assert r["rules"]["bugun_kirmizi"] is False

    def test_strong_uptrend_no_divergence_rejected(self):
        """Saf guclu uptrend (MACD yukseliyor) -> macd_dusuyor fail (divergence yok)."""
        c = [100.0 + i * 1.0 for i in range(120)]  # surekli guclu uptrend
        o = [x - 0.2 for x in c]
        h = [x + 1.0 for x in c]
        l = [x - 1.0 for x in c]
        v = [1000.0] * 120
        r = compute_rising_wedge_breakdown(o, h, l, c, v)
        assert r["detected"] is False
        assert r["rules"]["macd_dusuyor"] is False
