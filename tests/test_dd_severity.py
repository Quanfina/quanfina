"""compute_distribution_day_severity testleri — O'Neil canon (P105)."""
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from quanfina_math import (
    compute_distribution_day_severity,
    DD_SEVERITY_CAUTION_MIN,
    DD_SEVERITY_HEAVY_MIN,
    DD_SEVERITY_EXTREME_MIN,
)


def test_clean_zero():
    res = compute_distribution_day_severity(0)
    assert res['severity'] == 'CLEAN'
    assert res['allocation_factor'] == 1.0


def test_clean_two():
    res = compute_distribution_day_severity(2)
    assert res['severity'] == 'CLEAN'
    assert res['allocation_factor'] == 1.0


def test_caution_three():
    res = compute_distribution_day_severity(3)
    assert res['severity'] == 'CAUTION'
    assert res['allocation_factor'] == 0.5


def test_caution_four():
    res = compute_distribution_day_severity(4)
    assert res['severity'] == 'CAUTION'


def test_heavy_five():
    res = compute_distribution_day_severity(5)
    assert res['severity'] == 'HEAVY'
    assert res['allocation_factor'] == 0.25


def test_heavy_six():
    res = compute_distribution_day_severity(6)
    assert res['severity'] == 'HEAVY'


def test_extreme_seven():
    res = compute_distribution_day_severity(7)
    assert res['severity'] == 'EXTREME'
    assert res['allocation_factor'] == 0.0


def test_extreme_ten():
    res = compute_distribution_day_severity(10)
    assert res['severity'] == 'EXTREME'


def test_negative_dd_invalid():
    res = compute_distribution_day_severity(-1)
    assert res['severity'] is None


def test_mark_says_present():
    for dd in [0, 3, 5, 7]:
        res = compute_distribution_day_severity(dd)
        assert res['mark_says'] is not None
        assert len(res['mark_says']) > 10


def test_thresholds_match_constants():
    """Sabit eşikler doğru kategorize ediyor."""
    assert compute_distribution_day_severity(DD_SEVERITY_CAUTION_MIN - 1)['severity'] == 'CLEAN'
    assert compute_distribution_day_severity(DD_SEVERITY_CAUTION_MIN)['severity'] == 'CAUTION'
    assert compute_distribution_day_severity(DD_SEVERITY_HEAVY_MIN)['severity'] == 'HEAVY'
    assert compute_distribution_day_severity(DD_SEVERITY_EXTREME_MIN)['severity'] == 'EXTREME'
