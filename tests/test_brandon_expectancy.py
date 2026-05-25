"""
KARAR #733 alt-paket (Paket 73): compute_brandon_expectancy pytest.
Mark Brandon Video — Avg gain %20 / loss %4 / win 50% / E ≥ 10%.
"""
import pytest
import sys
from pathlib import Path

HERE = Path(__file__).resolve().parent
PROJECT_ROOT = HERE.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from quanfina_math import (
    compute_brandon_expectancy,
    BRANDON_AVG_GAIN_TARGET_PCT,
    BRANDON_AVG_LOSS_TARGET_PCT,
    BRANDON_WIN_RATE_TARGET,
    BRANDON_EXPECTANCY_EXCELLENT_PCT,
    BRANDON_EXPECTANCY_GOOD_PCT,
    BRANDON_EXPECTANCY_ACCEPTABLE_PCT,
)


class TestExpectancyCategories:
    def test_excellent(self):
        """Brandon canon zirvesi: %30 gain, %3 loss, %60 win → E ≈ 16.8."""
        r = compute_brandon_expectancy(30.0, 3.0, 0.60)
        assert r['category'] == 'EXCELLENT'
        assert r['expectancy_pct'] > BRANDON_EXPECTANCY_EXCELLENT_PCT

    def test_good(self):
        """Brandon eşiği: %20 gain, %4 loss, %60 win → E = 12 - 1.6 = 10.4."""
        r = compute_brandon_expectancy(20.0, 4.0, 0.60)
        assert r['category'] == 'GOOD'
        assert r['expectancy_pct'] >= BRANDON_EXPECTANCY_GOOD_PCT

    def test_acceptable(self):
        """Asgari: %15 gain, %5 loss, %55 win → E ≈ 6."""
        r = compute_brandon_expectancy(15.0, 5.0, 0.55)
        assert r['category'] == 'ACCEPTABLE'
        assert r['expectancy_pct'] >= BRANDON_EXPECTANCY_ACCEPTABLE_PCT
        assert r['expectancy_pct'] < BRANDON_EXPECTANCY_GOOD_PCT

    def test_poor(self):
        """Disiplinsiz: %10 gain, %8 loss, %40 win → E ≈ -0.8."""
        r = compute_brandon_expectancy(10.0, 8.0, 0.40)
        assert r['category'] == 'POOR'
        assert r['expectancy_pct'] < BRANDON_EXPECTANCY_ACCEPTABLE_PCT


class TestBrandonTargets:
    def test_meets_all_targets(self):
        """Tüm hedefler tutuyor."""
        r = compute_brandon_expectancy(25.0, 3.0, 0.55)
        assert r['meets_brandon_targets'] is True

    def test_misses_avg_gain(self):
        """avg_gain < 20 → False."""
        r = compute_brandon_expectancy(15.0, 3.0, 0.60)
        assert r['meets_brandon_targets'] is False

    def test_misses_avg_loss(self):
        """avg_loss > 4 → False."""
        r = compute_brandon_expectancy(25.0, 6.0, 0.60)
        assert r['meets_brandon_targets'] is False

    def test_misses_win_rate(self):
        """win < 50% → False."""
        r = compute_brandon_expectancy(25.0, 3.0, 0.40)
        assert r['meets_brandon_targets'] is False


class TestEdgeCases:
    def test_invalid_win_rate(self):
        r = compute_brandon_expectancy(20.0, 4.0, 1.5)
        assert r['category'] is None

    def test_negative_loss(self):
        r = compute_brandon_expectancy(20.0, -3.0, 0.50)
        assert r['category'] is None

    def test_zero_win_rate(self):
        """%0 win, sadece kayıp → expectancy negatif."""
        r = compute_brandon_expectancy(20.0, 5.0, 0.0)
        assert r['expectancy_pct'] == -5.0  # 0×20 - 1×5


class TestMarkCanon:
    def test_excellent_says_brandon(self):
        r = compute_brandon_expectancy(30.0, 3.0, 0.60)
        if r['category'] == 'EXCELLENT':
            assert 'Brandon' in r['mark_says']

    def test_poor_says_rba_drop(self):
        r = compute_brandon_expectancy(8.0, 10.0, 0.30)
        if r['category'] == 'POOR':
            says = r['mark_says']
            assert 'RBA' in says or 'TTLC' in says


class TestConstants:
    def test_avg_gain_20(self):
        assert BRANDON_AVG_GAIN_TARGET_PCT == 20.0

    def test_avg_loss_4(self):
        assert BRANDON_AVG_LOSS_TARGET_PCT == 4.0

    def test_win_rate_50(self):
        assert BRANDON_WIN_RATE_TARGET == 0.50
