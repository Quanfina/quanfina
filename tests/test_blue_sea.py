"""Carr Blue Sea Breakdown (TLFAL 2.baski s.289) — compute_blue_sea_breakdown. P517.

Blue Sky'in SHORT aynasi. 6 kural (s.289) + ASIMETRI (s.286: close > 0.8×52h zirve). Cift
danisma teyitli. Sentetik 280-bar: yukselis (->300 zirve) + decline (%20 ici, ~250) + bugun
40g yeni dusuk + kirmizi + OBV/MACD yeni dusuk.
"""
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from quanfina_math import compute_blue_sea_breakdown  # noqa: E402


def _blue_sea_series(n: int = 280):
    """Phase1 yukselis (0..160: ->300 zirve=52h high) + phase2 decline (161..279: ~250,
    zirvenin %20 ustunde). Bugun 40g yeni dusuk, 52h dip DEGIL, kirmizi. Tum gunler down->OBV duser.
    """
    opens, highs, lows, closes, volumes = [], [], [], [], []
    for i in range(n):
        if i <= 160:
            c = 100.0 + i * 1.25   # yukselis -> 300
        else:
            c = 300.0 - (i - 160) * 0.42  # decline -> ~250 (zirvenin %20 ustunde)
        closes.append(c)
        opens.append(c + 0.2)   # kirmizi mum (close<open) — sadece bugun kurali onemli
        highs.append(c + 0.5)
        lows.append(c - 0.5)
        volumes.append(1000.0)
    return opens, highs, lows, closes, volumes


class TestBlueSeaGuards:
    def test_empty(self):
        r = compute_blue_sea_breakdown([], [], [], [], [])
        assert r["detected"] is False and r["direction"] is None

    def test_length_mismatch(self):
        x = [1.0] * 261
        r = compute_blue_sea_breakdown(x, x, x, x, [1.0] * 260)
        assert r["detected"] is False and "uzunluk" in r["mark_says"].lower()

    def test_insufficient_bars(self):
        x = [100.0 - i * 0.1 for i in range(200)]
        r = compute_blue_sea_breakdown(x, x, x, x, [1000.0] * 200)
        assert r["detected"] is False and "261" in r["mark_says"]


class TestBlueSeaDetection:
    def test_valid_breakdown_detected(self):
        o, h, l, c, v = _blue_sea_series()
        r = compute_blue_sea_breakdown(o, h, l, c, v)
        assert r["detected"] is True, f"{r['mark_says']} | {r['rules']}"
        assert r["direction"] == "SHORT"
        assert all(r["rules"].values()), r["rules"]

    def test_entry_is_signal_low(self):
        """SHORT — ENTRY = signal low alti sell-stop (s.317), high DEGIL."""
        o, h, l, c, v = _blue_sea_series()
        r = compute_blue_sea_breakdown(o, h, l, c, v)
        assert r["entry"] == round(l[-1], 2)

    def test_short_stop_above_target_below(self):
        """SHORT: stop entry USTUNDE, target ALTINDA; %6/%8 cap; 2R."""
        o, h, l, c, v = _blue_sea_series()
        r = compute_blue_sea_breakdown(o, h, l, c, v)
        assert r["stop"] > r["entry"], "short stop entry ustunde olmali"
        assert r["target"] < r["entry"], "short target entry altinda olmali"
        assert r["risk_pct"] <= 8.01
        expected = r["entry"] - 2.0 * (r["stop"] - r["entry"])
        assert abs(r["target"] - expected) < 0.05
        assert r["rr"] == 2.0
        assert "time_stop_days" not in r

    def test_asymmetric_oversold_filter(self):
        """ASIMETRI (s.286): close > 0.8×52h zirve. Cok dusen (oversold) -> red."""
        o, h, l, c, v = _blue_sea_series()
        c = list(c)
        # bugunu 52h zirvenin (~300) %20'den fazla altina indir -> asiri_satilmamis fail
        c[-1] = 200.0  # 200 < 0.8*300=240
        o = [x + 0.2 for x in c]
        h = [x + 0.5 for x in c]
        l = [x - 0.5 for x in c]
        r = compute_blue_sea_breakdown(o, h, l, c, v)
        assert r["detected"] is False
        assert r["rules"]["asiri_satilmamis"] is False

    def test_not_red_rejected(self):
        o, h, l, c, v = _blue_sea_series()
        o = list(o)
        o[-1] = c[-1] - 1.0  # yesil bugun
        r = compute_blue_sea_breakdown(o, h, l, c, v)
        assert r["detected"] is False
        assert r["rules"]["bugun_kirmizi"] is False
