"""Carr Mean Reversion (quanfina_math.compute_mean_reversion) pytest — P500.

Canon: Carr 2.baski "Bonus System I" — s.356 (5 LONG kurali) + s.357-358 (SHORT simetrik)
+ s.410-411 (%8 hard cap) + s.400/404 (7-gun time stop) + s.340 (SMA20 × 0.9 esigi).
Kural #24 Asama 5 (pytest) + Kural #26 (sayfa-atifli esik guard, uydurma degil).
"""
import sys
from pathlib import Path

import pytest

HERE = Path(__file__).resolve().parent
ROOT = HERE.parent
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

try:
    from quanfina_math import (
        compute_mean_reversion,
        MEAN_REV_SMA_THRESHOLD, MEAN_REV_HARD_CAP_PCT, MEAN_REV_TIME_STOP_DAYS,
    )
except ImportError as e:
    pytest.skip(f"quanfina_math import edilemedi: {e}", allow_module_level=True)


def _ohlc(closes, opens):
    """closes+opens -> (opens, highs, lows, closes) sentetik OHLC (high/low cikinti)."""
    highs = [max(o, c) + 2 for o, c in zip(opens, closes)]
    lows = [min(o, c) - 2 for o, c in zip(opens, closes)]
    return opens, highs, lows, closes


# Yuksek-vol osilasyon (std>5 ki alt BB < SMA20×0.9 mumkun olsun — rule 2+3 birlikte)
_BASE = [100 + 6 * ((-1) ** i) for i in range(19)]


class TestMeanReversionCanonGuard:
    """Kural #26: esikler Carr 2.baski sayfa-atifli, UYDURMA DEGIL."""

    def test_sma_threshold_is_0_9(self):
        assert MEAN_REV_SMA_THRESHOLD == 0.9      # Carr s.340 (%10 esik)

    def test_hard_cap_is_8(self):
        assert MEAN_REV_HARD_CAP_PCT == 8.0       # Carr s.410-411

    def test_time_stop_is_7(self):
        assert MEAN_REV_TIME_STOP_DAYS == 7       # Carr s.400, 404


class TestMeanReversionDetection:
    def test_insufficient_data(self):
        c = [100.0] * 10
        assert compute_mean_reversion(c, c, c, c)["detected"] is False

    def test_length_mismatch(self):
        r = compute_mean_reversion([100.0] * 21, [100.0] * 21, [100.0] * 20, [100.0] * 21)
        assert r["detected"] is False

    def test_flat_no_signal(self):
        """Hafif osilasyon, dip yok -> sinyal yok (rule 1/3 saglanmaz)."""
        c = [100.0 + ((-1) ** i) for i in range(25)]
        r = compute_mean_reversion(*_ohlc(c, list(c)))
        assert r["detected"] is False

    def test_long_signal(self):
        """5 LONG kosulu (Carr s.356) -> LONG sinyal + hedef SMA20 + stop entry alti."""
        r = compute_mean_reversion(*_ohlc(_BASE + [60.0, 74.0], _BASE + [75.0, 77.0]))
        assert r["detected"] is True
        assert r["direction"] == "LONG"
        assert r["entry"] == 74.0
        assert r["target"] == r["sma20"]          # hedef SMA20 dinamik (Carr s.339)
        assert r["stop"] < r["entry"]             # long: stop entry altinda
        assert all(r["rules"].values())           # 5 kosul da True

    def test_long_hard_cap_8pct(self):
        """Yapisal stop >%8 ise %8 cap'e clamp (Carr s.410-411)."""
        r = compute_mean_reversion(*_ohlc(_BASE + [60.0, 74.0], _BASE + [75.0, 77.0]))
        risk_pct = (r["entry"] - r["stop"]) / r["entry"] * 100
        assert risk_pct <= MEAN_REV_HARD_CAP_PCT + 0.01
        assert round(r["stop"], 2) == round(r["entry"] * 0.92, 2)   # tam %8 cap

    def test_short_signal(self):
        """SHORT simetrik (Carr s.357-358) -> stop entry ustunde."""
        r = compute_mean_reversion(*_ohlc(_BASE + [116.0, 115.0], _BASE + [101.0, 112.0]))
        assert r["detected"] is True
        assert r["direction"] == "SHORT"
        assert r["stop"] > r["entry"]
        assert r["target"] == r["sma20"]
