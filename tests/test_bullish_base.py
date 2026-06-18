"""Carr Bullish Base Breakout (TLFAL 2.baski s.291) — compute_bullish_base_breakout. P512.

CONTRARIAN downtrend-sonu LONG ADAYI. 7 tarama + yesil tetik (s.291). KRITIK: entry=close
(kirilim beklenmez, s.284,289). OBV+MACD yukseliyor. Cikis Ch22 (%6/%8/2R). TIER-2 eyeball.
Sentetik 220-bar: steep downtrend (300->141) + 60-bar daralan baz (OBV/MACD recovery + osc).
"""
import math
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from quanfina_math import compute_bullish_base_breakout  # noqa: E402


def _bullish_base_series(n: int = 220):
    """Phase1 steep downtrend (0..159: 300->141) + phase2 daralan baz (160..219): hafif
    dusus + osilasyon (up gunler OBV icin), range kuculen, volume asimetri (up=3000/down=800).
    """
    opens, highs, lows, closes, volumes = [], [], [], [], []
    for i in range(n):
        if i <= 159:
            c = 300.0 - i  # steep downtrend
            hr = 1.0
        else:
            t = i - 160
            c = 140.0 - t * 0.03 + 1.2 * math.sin(t * 0.8)  # hafif dusus + osilasyon
            hr = max(0.2, 2.5 - t * 0.04)  # daralan gunluk range
        closes.append(c)
        opens.append(c - 0.2)  # her bar yesil (sadece bugun kurali onemli)
        highs.append(c + hr)
        lows.append(c - hr)
    # volume asimetri: up gun (close>prev) buyuk, down gun kucuk -> baz'da OBV yukselir
    for i in range(n):
        if i == 0 or closes[i] > closes[i - 1]:
            volumes.append(3000.0)
        else:
            volumes.append(800.0)
    return opens, highs, lows, closes, volumes


class TestBullishBaseGuards:
    def test_empty(self):
        r = compute_bullish_base_breakout([], [], [], [], [])
        assert r["detected"] is False and r["direction"] is None

    def test_length_mismatch(self):
        x = [1.0] * 200
        r = compute_bullish_base_breakout(x, x, x, x, [1.0] * 199)
        assert r["detected"] is False and "uzunluk" in r["mark_says"].lower()

    def test_insufficient_bars(self):
        x = [100.0 + i for i in range(150)]
        r = compute_bullish_base_breakout(x, x, x, x, [1000.0] * 150)
        assert r["detected"] is False and "200" in r["mark_says"]


class TestBullishBaseDetection:
    def test_valid_base_detected(self):
        o, h, l, c, v = _bullish_base_series()
        r = compute_bullish_base_breakout(o, h, l, c, v)
        assert r["detected"] is True, f"{r['mark_says']} | {r['rules']}"
        assert r["direction"] == "LONG"
        assert all(r["rules"].values()), r["rules"]

    def test_entry_is_close_not_signal_high(self):
        """KRITIK FARK (s.284): kirilim beklenmez -> entry = CLOSE (signal high DEGIL)."""
        o, h, l, c, v = _bullish_base_series()
        r = compute_bullish_base_breakout(o, h, l, c, v)
        assert r["entry"] == round(c[-1], 2)
        assert r["entry"] != round(h[-1], 2)  # signal high DEGIL

    def test_quality_candidate(self):
        o, h, l, c, v = _bullish_base_series()
        r = compute_bullish_base_breakout(o, h, l, c, v)
        assert r["quality"] == "CANDIDATE"
        assert any("RISING WEDGE" in e.upper() for e in r["eyeball_checks"])

    def test_downtrend_context(self):
        """CONTRARIAN: SMA50>SMA20 + SMA50<SMA200 (downtrend bazi)."""
        o, h, l, c, v = _bullish_base_series()
        r = compute_bullish_base_breakout(o, h, l, c, v)
        assert r["sma50"] > r["sma20"], "downtrend: 50 SMA 20 SMA ustunde olmali"
        assert r["sma50"] < r["sma200"], "downtrend: 50 SMA 200 SMA altinda olmali"

    def test_exit_universal(self):
        o, h, l, c, v = _bullish_base_series()
        r = compute_bullish_base_breakout(o, h, l, c, v)
        assert r["stop"] < r["entry"] and r["target"] > r["entry"]
        assert abs(r["risk_pct"] - 6.0) < 0.01
        expected = r["entry"] + 2.0 * (r["entry"] - r["stop"])
        assert abs(r["target"] - expected) < 0.05
        assert r["rr"] == 2.0
        assert "time_stop_days" not in r

    def test_not_green_rejected(self):
        o, h, l, c, v = _bullish_base_series()
        o = list(o)
        o[-1] = c[-1] + 1.0  # bearish bugun
        r = compute_bullish_base_breakout(o, h, l, c, v)
        assert r["detected"] is False
        assert r["rules"]["bugun_yesil"] is False

    def test_uptrend_rejected(self):
        """Uptrend (SMA20>SMA50) -> rule 1 fail (bu setup downtrend bazi arar)."""
        c = [100.0 + i for i in range(220)]  # uptrend
        o = [x - 0.2 for x in c]
        h = [x + 1.0 for x in c]
        l = [x - 1.0 for x in c]
        v = [1000.0] * 220
        r = compute_bullish_base_breakout(o, h, l, c, v)
        assert r["detected"] is False
        assert r["rules"]["sma50_ust_sma20"] is False
