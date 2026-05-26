"""compute_breakout_quality testleri — Mark TLSMW Ch 10 puan kompoziti (P107)."""
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from quanfina_math import compute_breakout_quality


def test_excellent_all_criteria():
    res = compute_breakout_quality(
        volume_confirmed=True, gap_up=True, breakout_pct=2.5,
        prior_contraction=True, overhead_clean=True,
    )
    assert res['category'] == 'EXCELLENT'
    assert res['score'] == 100


def test_good_volume_breakout():
    res = compute_breakout_quality(
        volume_confirmed=True, gap_up=False, breakout_pct=2.0,
        prior_contraction=True, overhead_clean=False,
    )
    assert res['category'] == 'GOOD'
    assert res['score'] == 70


def test_marginal_partial():
    res = compute_breakout_quality(
        volume_confirmed=True, gap_up=False, breakout_pct=1.0,
        prior_contraction=False, overhead_clean=True,
    )
    # 30 + 0 + 15 + 0 + 10 = 55 MARGINAL
    assert res['category'] == 'MARGINAL'


def test_poor_no_volume():
    res = compute_breakout_quality(
        volume_confirmed=False, gap_up=False, breakout_pct=0.3,
        prior_contraction=False, overhead_clean=False,
    )
    assert res['category'] == 'POOR'
    assert res['score'] == 0


def test_breakdown_returned():
    res = compute_breakout_quality(
        volume_confirmed=True, gap_up=True, breakout_pct=2.0,
    )
    assert 'volume' in res['breakdown']
    assert res['breakdown']['volume'] == 30
    assert res['breakdown']['gap_up'] == 20


def test_breakout_strength_tiers():
    # Strong (>=1.7)
    r1 = compute_breakout_quality(False, False, 2.0)
    assert r1['breakdown']['breakout_strength'] == 25
    # Mid (>=0.5)
    r2 = compute_breakout_quality(False, False, 0.8)
    assert r2['breakdown']['breakout_strength'] == 15
    # Weak
    r3 = compute_breakout_quality(False, False, 0.1)
    assert r3['breakdown']['breakout_strength'] == 0


def test_mark_says_present():
    res = compute_breakout_quality(True, True, 2.0, True, True)
    assert len(res['mark_says']) > 10
