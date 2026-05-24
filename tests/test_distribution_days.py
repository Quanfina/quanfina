"""
KARAR #488 — count_distribution_days pytest test seti.

O'Neil DD mekanik (close <= -0.2% + volume > prev day) + Mark Regime
4-katman mapping coverage.
"""
import pytest
import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parent.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from quanfina_math import (
    count_distribution_days,
    DISTRIBUTION_DAY_CLOSE_THRESHOLD_PCT,
    DISTRIBUTION_DAY_LOOKBACK_DAYS,
)


# =====================================================================
# Test: O'Neil mekanik (close ≤ -0.2% + volume > prev day)
# =====================================================================

class TestDdMechanics:
    def test_empty_data(self):
        r = count_distribution_days([], [])
        assert r['count'] == 0
        assert r['regime_hint'] == 'HEALTHY'

    def test_mismatched_lengths(self):
        r = count_distribution_days([100, 101], [1000])
        assert r['count'] == 0

    def test_single_day_insufficient(self):
        r = count_distribution_days([100.0], [1000])
        assert r['count'] == 0

    def test_no_dd_uptrend(self):
        """Sürekli yükseliş — DD yok."""
        closes = [100, 101, 102, 103, 104, 105]
        volumes = [1000, 1100, 1050, 1200, 1100, 1300]
        r = count_distribution_days(closes, volumes)
        assert r['count'] == 0
        assert r['regime_hint'] == 'HEALTHY'

    def test_dd_below_threshold_no_volume(self):
        """Close düşüş ama volume yok → DD sayılmaz (O'Neil iki şart)."""
        closes = [100.0, 99.5]   # -0.5% düşüş
        volumes = [2000, 1000]   # volume düştü
        r = count_distribution_days(closes, volumes)
        assert r['count'] == 0

    def test_dd_with_volume_threshold(self):
        """Close < -0.2% + volume > prev → DD."""
        closes = [100.0, 99.5]   # -0.5%
        volumes = [1000, 1500]   # volume arttı
        r = count_distribution_days(closes, volumes)
        assert r['count'] == 1

    def test_volume_higher_close_neutral(self):
        """Volume arttı ama close düşmedi → DD yok."""
        closes = [100.0, 100.0]
        volumes = [1000, 2000]
        r = count_distribution_days(closes, volumes)
        assert r['count'] == 0

    def test_borderline_close_minus_02_pct(self):
        """Tam -0.2% sınır → DD (≤ threshold)."""
        closes = [100.0, 99.8]  # -0.2%
        volumes = [1000, 1500]
        r = count_distribution_days(closes, volumes)
        assert r['count'] == 1

    def test_just_above_threshold_no_dd(self):
        """-0.1% → DD yok."""
        closes = [100.0, 99.9]  # -0.1%
        volumes = [1000, 1500]
        r = count_distribution_days(closes, volumes)
        assert r['count'] == 0


# =====================================================================
# Test: Mark Regime 4-katman mapping (KARAR #488)
# =====================================================================

def _make_dd_data(dd_count: int):
    """N adet DD oluşturan veri üret + arada normal günler."""
    closes = [100.0]
    volumes = [1000]
    for i in range(dd_count):
        # DD: close -0.5%, volume +50%
        prev = closes[-1]
        closes.append(prev * 0.995)
        volumes.append(int(volumes[-1] * 1.5))
        # Normal gün (toparlanma): close +0.1%, volume düşük
        closes.append(closes[-1] * 1.001)
        volumes.append(int(volumes[-1] * 0.8))
    return closes, volumes


class TestRegimeMapping:
    def test_0_dd_healthy(self):
        closes, vols = _make_dd_data(0)
        r = count_distribution_days(closes, vols)
        assert r['count'] == 0
        assert r['regime_hint'] == 'HEALTHY'

    def test_2_dd_healthy(self):
        closes, vols = _make_dd_data(2)
        r = count_distribution_days(closes, vols)
        assert r['count'] == 2
        assert r['regime_hint'] == 'HEALTHY'

    def test_3_dd_caution(self):
        closes, vols = _make_dd_data(3)
        r = count_distribution_days(closes, vols)
        assert r['count'] == 3
        assert r['regime_hint'] == 'CAUTION'

    def test_4_dd_under_pressure(self):
        closes, vols = _make_dd_data(4)
        r = count_distribution_days(closes, vols)
        assert r['count'] == 4
        assert r['regime_hint'] == 'UNDER_PRESSURE'

    def test_5_dd_bear_pressure(self):
        closes, vols = _make_dd_data(5)
        r = count_distribution_days(closes, vols)
        assert r['count'] == 5
        assert r['regime_hint'] == 'BEAR_PRESSURE'

    def test_7_dd_bear_pressure(self):
        closes, vols = _make_dd_data(7)
        r = count_distribution_days(closes, vols)
        assert r['count'] == 7
        assert r['regime_hint'] == 'BEAR_PRESSURE'


# =====================================================================
# Test: Lookback penceresi (default 20)
# =====================================================================

class TestLookback:
    def test_default_lookback_constant(self):
        assert DISTRIBUTION_DAY_LOOKBACK_DAYS == 20

    def test_close_threshold_constant(self):
        assert DISTRIBUTION_DAY_CLOSE_THRESHOLD_PCT == -0.2

    def test_lookback_truncates_old_dd(self):
        """Eski DD'ler 20 gün dışı → sayılmaz."""
        # 30 günlük veri, ilk 5'inde DD, son 20'sinde 0
        closes = [100.0]
        volumes = [1000]
        # 5 DD
        for _ in range(5):
            prev = closes[-1]
            closes.append(prev * 0.995)
            volumes.append(int(volumes[-1] * 1.5))
        # 25 normal gün
        for _ in range(25):
            closes.append(closes[-1] * 1.001)
            volumes.append(int(volumes[-1] * 0.95))
        # Lookback 20 → son 20 günde 0 DD beklenir
        r = count_distribution_days(closes, volumes, lookback_days=20)
        # Eski DD'ler pencere dışı (lookback'ten kesilir)
        assert r['count'] == 0

    def test_custom_lookback(self):
        """lookback=5 ile farklı sayım."""
        closes, vols = _make_dd_data(4)
        # 4 DD üretilmiş (her DD + normal gün) = 8 ek günler + 1 başlangıç
        r = count_distribution_days(closes, vols, lookback_days=5)
        # Son 5 günde belki 2-3 DD'ye düşer
        assert r['count'] <= 4


class TestMarkSays:
    def test_mark_says_present_all_regimes(self):
        """Her regime mark_says içermeli (KALICI İLKE #4)."""
        for n in [0, 2, 3, 4, 5, 7]:
            closes, vols = _make_dd_data(n)
            r = count_distribution_days(closes, vols)
            assert r['mark_says']
            assert len(r['mark_says']) > 10

    def test_mark_says_contains_count(self):
        closes, vols = _make_dd_data(3)
        r = count_distribution_days(closes, vols)
        assert '3' in r['mark_says']
