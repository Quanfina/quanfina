"""Carr Gap Down (TLFAL 2.baski s.273-274) — compute_gap_down. P519.

Uzun-ralli-sonu reversal SHORT ADAYI. 8 kural (s.273-274) + GOSTERGE YOK (s.275 pure price,
sadece 50MA). Gap: high[bugun]<low[dun]×0.99 (s.270). entry=close (s.270). Cift danisma teyitli.
Sentetik 130-bar: steady uptrend (50MA 3 ay yukseliyor) + bugun gap-down kirmizi mum.
"""
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from quanfina_math import compute_gap_down  # noqa: E402


def _gap_down_series(n: int = 130):
    """Steady uptrend (100->164.5, SMA50 yukseliyor, fiyat 50MA ustunde) + bugun gap-down
    kirmizi mum (high < dun low × 0.99).
    """
    opens, highs, lows, closes = [], [], [], []
    for i in range(n):
        c = 100.0 + i * 0.5  # steady uptrend
        opens.append(c - 0.3)
        highs.append(c + 0.5)
        lows.append(c - 0.5)
        closes.append(c)
    # bugun: gap down kirmizi mum (high < dun low × 0.99)
    yest_low = lows[-2]
    gap_level = yest_low * 0.99
    closes[-1] = gap_level - 1.5
    opens[-1] = gap_level - 0.5   # close < open -> kirmizi
    highs[-1] = gap_level - 0.05  # high < gap_level -> gap
    lows[-1] = gap_level - 2.0
    return opens, highs, lows, closes


class TestGapDownGuards:
    def test_empty(self):
        r = compute_gap_down([], [], [], [])
        assert r["detected"] is False and r["direction"] is None

    def test_length_mismatch(self):
        x = [1.0] * 130
        r = compute_gap_down(x, x, x, [1.0] * 129)
        assert r["detected"] is False and "uzunluk" in r["mark_says"].lower()

    def test_insufficient_bars(self):
        x = [100.0 + i for i in range(90)]
        r = compute_gap_down(x, x, x, x)
        assert r["detected"] is False and "111" in r["mark_says"]


class TestGapDownDetection:
    def test_valid_gap_down_detected(self):
        o, h, l, c = _gap_down_series()
        r = compute_gap_down(o, h, l, c)
        assert r["detected"] is True, f"{r['mark_says']} | {r['rules']}"
        assert r["direction"] == "SHORT"
        assert all(r["rules"].values()), r["rules"]

    def test_entry_is_close(self):
        """ENTRY = gap gunu close (market emri, s.270 — signal low BEKLEMEZ)."""
        o, h, l, c = _gap_down_series()
        r = compute_gap_down(o, h, l, c)
        assert r["entry"] == round(c[-1], 2)

    def test_quality_candidate_news(self):
        """~%100 mekanik AMA haber teyidi sart -> CANDIDATE + eyeball haber notu (s.272)."""
        o, h, l, c = _gap_down_series()
        r = compute_gap_down(o, h, l, c)
        assert r["quality"] == "CANDIDATE"
        assert any("HABER" in e.upper() for e in r["eyeball_checks"])

    def test_short_stop_above_target_below(self):
        o, h, l, c = _gap_down_series()
        r = compute_gap_down(o, h, l, c)
        assert r["stop"] > r["entry"] and r["target"] < r["entry"]
        assert r["risk_pct"] <= 8.01
        expected = r["entry"] - 2.0 * (r["stop"] - r["entry"])
        assert abs(r["target"] - expected) < 0.05
        assert r["rr"] == 2.0
        assert "time_stop_days" not in r

    def test_no_indicators(self):
        """GOSTERGE YOK (s.275): cikti sadece sma50, OBV/MACD/Stoch yok."""
        o, h, l, c = _gap_down_series()
        r = compute_gap_down(o, h, l, c)
        assert r["sma50"] is not None
        for forbidden in ("obv", "macd", "stoch_k", "rsi"):
            assert forbidden not in r

    def test_no_gap_rejected(self):
        """Gap yoksa (bugun high dun low ustunde) -> red."""
        o, h, l, c = _gap_down_series()
        h = list(h)
        h[-1] = l[-2] + 5.0  # bugun high dun low ustunde -> gap yok
        r = compute_gap_down(o, h, l, c)
        assert r["detected"] is False
        assert r["rules"]["gap_down"] is False

    def test_no_uptrend_rejected(self):
        """50MA yukselmiyor (downtrend) -> red (Gap Down uzun ralli SONU arar)."""
        c = [200.0 - i * 0.5 for i in range(130)]  # downtrend
        o = [x + 0.3 for x in c]
        h = [x + 0.5 for x in c]
        l = [x - 0.5 for x in c]
        r = compute_gap_down(o, h, l, c)
        assert r["detected"] is False
        assert r["rules"]["sma50_rising_20"] is False
