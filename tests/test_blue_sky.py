"""Carr Blue Sky Breakout (TLFAL 2.baski s.264-265) — compute_blue_sky_breakout. P507.

SINYAL 6 kural (s.264-265) + cikis Ch22 evrensel (%6/%8/2R). Cift danisma teyitli.
Sentetik 280-bar V-recovery: tepe(299) -> dusus -> baz(255) -> recovery -> 40g kirilim(288),
52h zirve(299) ALTI, OBV+MACD yeni 40g yuksek.
"""
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from quanfina_math import (  # noqa: E402
    compute_blue_sky_breakout,
    _ema_series,
    _obv_series,
    _macd_line_series,
)


def _blue_sky_series(n: int = 280):
    """V-recovery: phase1 uptrend->299, phase2 decline->255 baz, phase3 recovery->280,
    bugun 40g kirilim 288 (52h zirve 299 alti). OBV+MACD recovery'de yeni 40g yuksek.
    """
    closes = []
    for i in range(n):
        if i <= 99:
            c = 200.0 + i
        elif i <= 239:
            c = max(255.0, 299.0 - (i - 99) * 0.5)
        else:
            c = 255.0 + (i - 239) * 0.64
        closes.append(c)
    closes[-1] = 288.0  # breakout jump (>40g high ~280, <260g high 299)
    opens = [c - 0.5 for c in closes]
    highs = [c + 0.5 for c in closes]
    lows = [c - 0.5 for c in closes]
    volumes = [1000.0] * n
    volumes[-1] = 3000.0  # breakout hacmi
    return opens, highs, lows, closes, volumes


class TestBlueSkyGuards:
    def test_empty(self):
        r = compute_blue_sky_breakout([], [], [], [], [])
        assert r["detected"] is False
        assert r["direction"] is None

    def test_length_mismatch(self):
        x = [1.0] * 261
        r = compute_blue_sky_breakout(x, x, x, x, [1.0] * 260)
        assert r["detected"] is False
        assert "uzunluk" in r["mark_says"].lower()

    def test_insufficient_bars(self):
        x = [100.0 + i for i in range(200)]
        r = compute_blue_sky_breakout(x, x, x, x, [1000.0] * 200)
        assert r["detected"] is False
        assert "261" in r["mark_says"]


class TestBlueSkyDetection:
    def test_valid_breakout_detected(self):
        o, h, l, c, v = _blue_sky_series()
        r = compute_blue_sky_breakout(o, h, l, c, v)
        assert r["detected"] is True, f"{r['mark_says']} | {r['rules']}"
        assert r["direction"] == "LONG"
        assert all(r["rules"].values()), r["rules"]

    def test_entry_is_signal_high(self):
        """ENTRY = sinyal gunu HIGH'i ustu buy-stop (s.335), close DEGIL."""
        o, h, l, c, v = _blue_sky_series()
        r = compute_blue_sky_breakout(o, h, l, c, v)
        assert r["entry"] == round(h[-1], 2)
        assert r["signal_close"] == round(c[-1], 2)

    def test_below_52w_high(self):
        """52-hafta yuksek DEGIL (s.265): close < 260g prior max."""
        o, h, l, c, v = _blue_sky_series()
        r = compute_blue_sky_breakout(o, h, l, c, v)
        assert c[-1] < r["high_260d"]
        assert c[-1] > r["high_40d"]  # ama 40g yeni yuksek

    def test_exit_universal_ch22(self):
        """Blue Sky-ozel cikis KAYNAK YOK -> Ch22 evrensel: %6 stop, 2R, %8 cap (s.324-325)."""
        o, h, l, c, v = _blue_sky_series()
        r = compute_blue_sky_breakout(o, h, l, c, v)
        assert r["stop"] < r["entry"] and r["target"] > r["entry"]
        assert abs(r["risk_pct"] - 6.0) < 0.01  # %6 (s.325)
        assert r["risk_pct"] <= 8.01            # %8 cap (s.325)
        expected = r["entry"] + 2.0 * (r["entry"] - r["stop"])
        assert abs(r["target"] - expected) < 0.05
        assert r["rr"] == 2.0

    def test_no_time_stop(self):
        o, h, l, c, v = _blue_sky_series()
        r = compute_blue_sky_breakout(o, h, l, c, v)
        assert "time_stop_days" not in r

    def test_new_52w_high_rejected(self):
        """Bugun 260g zirveyi gecerse (52h yuksek) -> Blue Sky DEGIL (s.265)."""
        o, h, l, c, v = _blue_sky_series()
        c = list(c)
        c[-1] = 350.0  # 260g high (299) ustu -> rule 2 fail
        o = [x - 0.5 for x in c]
        h = [x + 0.5 for x in c]
        l = [x - 0.5 for x in c]
        r = compute_blue_sky_breakout(o, h, l, c, v)
        assert r["detected"] is False
        assert r["rules"]["degil_52h_yuksek"] is False

    def test_not_bullish_candle_rejected(self):
        o, h, l, c, v = _blue_sky_series()
        o = list(o)
        o[-1] = c[-1] + 1.0  # bearish bugun
        r = compute_blue_sky_breakout(o, h, l, c, v)
        assert r["detected"] is False
        assert r["rules"]["bugun_bullish"] is False


class TestBlueSkyHelpers:
    def test_ema_series_seed(self):
        vals = [float(i) for i in range(1, 11)]  # 1..10
        ema = _ema_series(vals, 5)
        assert ema[3] is None and ema[4] == sum(range(1, 6)) / 5  # seed = SMA(1..5)=3.0
        assert ema[-1] > ema[4]  # yukselen seri -> EMA artar

    def test_ema_insufficient(self):
        assert _ema_series([1.0, 2.0], 5) == []

    def test_obv_direction(self):
        closes = [10.0, 11.0, 10.5, 10.5, 12.0]
        vols = [100.0, 200.0, 50.0, 30.0, 400.0]
        obv = _obv_series(closes, vols)
        # +200 (up), -50 (down), 0 (esit), +400 (up) = 0,200,150,150,550
        assert obv == [0.0, 200.0, 150.0, 150.0, 550.0]

    def test_macd_line_basic(self):
        closes = [100.0 + i for i in range(40)]  # yukselen -> MACD pozitif
        macd = _macd_line_series(closes, 12, 26)
        assert macd[10] is None       # slow(26) seed oncesi
        assert macd[-1] is not None and macd[-1] > 0
