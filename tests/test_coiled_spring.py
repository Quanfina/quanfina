"""Carr Coiled Spring (TLFAL 2.baski s.250) — compute_coiled_spring. P509.

8 tarama kosulu (s.250) + GOSTERGE YOK (s.248 pure pattern) + TIER-2 eyeball (quality=CANDIDATE)
+ cikis 50MA-%2/%8/2R. Cift danisma teyitli. Sentetik 80-bar: uptrend(100->159) + daralan
coil (yatay, range kuculen), bugun yesil signal high.
"""
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from quanfina_math import compute_coiled_spring  # noqa: E402


def _coiled_spring_series(n: int = 80):
    """Phase1 uptrend (100->159, 60g zirve recent) + phase2 daralan coil (yatay 159, range
    kuculen). Bugun yesil, high<=3/7g once, low>=3/7g once, close>SMA20, SMA20>SMA50×1.03.
    """
    opens, highs, lows, closes = [], [], [], []
    for i in range(n):
        if i <= 59:
            c = 100.0 + i
            opens.append(c - 0.3)
            highs.append(c + 0.5)
            lows.append(c - 0.5)
            closes.append(c)
        else:
            hr = max(0.1, 1.0 - (i - 60) * 0.045)  # half-range kuculen (1.0 -> 0.145)
            opens.append(159.0)
            highs.append(159.0 + hr)
            lows.append(159.0 - hr)
            closes.append(159.0)  # yatay merkez (drift YOK)
    # bugun: yesil mum, kapanis bugunku high'a yakin (>SMA20)
    closes[-1] = 159.10
    opens[-1] = 159.00
    return opens, highs, lows, closes


class TestCoiledSpringGuards:
    def test_empty(self):
        r = compute_coiled_spring([], [], [], [])
        assert r["detected"] is False and r["direction"] is None

    def test_length_mismatch(self):
        x = [1.0] * 60
        r = compute_coiled_spring(x, x, x, [1.0] * 59)
        assert r["detected"] is False and "uzunluk" in r["mark_says"].lower()

    def test_insufficient_bars(self):
        x = [100.0 + i for i in range(40)]
        r = compute_coiled_spring(x, x, x, x)
        assert r["detected"] is False and "60" in r["mark_says"]


class TestCoiledSpringDetection:
    def test_valid_coil_detected(self):
        o, h, l, c = _coiled_spring_series()
        r = compute_coiled_spring(o, h, l, c)
        assert r["detected"] is True, f"{r['mark_says']} | {r['rules']}"
        assert r["direction"] == "LONG"
        assert all(r["rules"].values()), r["rules"]

    def test_quality_is_candidate(self):
        """TIER-2 eyeball -> quality='CANDIDATE' (GOOD degil — durustluk, Kural #26)."""
        o, h, l, c = _coiled_spring_series()
        r = compute_coiled_spring(o, h, l, c)
        assert r["quality"] == "CANDIDATE"
        assert len(r["eyeball_checks"]) >= 3  # daralma + aci + 50MA temasi

    def test_entry_is_signal_high(self):
        o, h, l, c = _coiled_spring_series()
        r = compute_coiled_spring(o, h, l, c)
        assert r["entry"] == round(h[-1], 2)
        assert r["signal_close"] == round(c[-1], 2)

    def test_exit_stop_target(self):
        """STOP 50MA-%2 / %8 cap (s.252,303), TARGET 2R (s.324), TIME STOP YOK."""
        o, h, l, c = _coiled_spring_series()
        r = compute_coiled_spring(o, h, l, c)
        assert r["stop"] < r["entry"] and r["target"] > r["entry"]
        assert r["risk_pct"] <= 8.01
        expected = r["entry"] + 2.0 * (r["entry"] - r["stop"])
        assert abs(r["target"] - expected) < 0.05
        assert r["rr"] == 2.0
        assert "time_stop_days" not in r

    def test_no_indicators_only_ma(self):
        """GOSTERGE YOK (s.248): cikti sadece SMA20/SMA50 baglami, BB/ATR/Stoch alani yok."""
        o, h, l, c = _coiled_spring_series()
        r = compute_coiled_spring(o, h, l, c)
        assert r["sma20"] is not None and r["sma50"] is not None
        for forbidden in ("lower_bb", "upper_bb", "stoch_k", "macd", "atr"):
            assert forbidden not in r

    def test_range_expanding_rejected(self):
        """Bugun high 3 gun oncesini gecerse (daralma YOK) -> red."""
        o, h, l, c = _coiled_spring_series()
        h = list(h)
        h[-1] = h[-4] + 5.0  # bugun high 3g once + 5 (genisleme)
        r = compute_coiled_spring(o, h, l, c)
        assert r["detected"] is False
        assert r["rules"]["high_3g_alti"] is False

    def test_not_green_rejected(self):
        o, h, l, c = _coiled_spring_series()
        o = list(o)
        o[-1] = c[-1] + 1.0  # bearish bugun
        r = compute_coiled_spring(o, h, l, c)
        assert r["detected"] is False
        assert r["rules"]["bugun_yesil"] is False

    def test_no_uptrend_ma_rejected(self):
        """SMA20 SMA50×1.03 ustunde degilse (trend yok) -> red. Duz seri."""
        flat = [100.0] * 80
        r = compute_coiled_spring(flat, [100.5] * 80, [99.5] * 80, flat)
        assert r["detected"] is False
        assert r["rules"]["sma20_ust_sma50_sep"] is False
