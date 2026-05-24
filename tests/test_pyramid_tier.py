"""
KARAR #732 — compute_pyramid_tier pytest test seti.

Mark KARAR #487 v20.98 birebir 3-tier (Pilot/Standart/Full) +
Mark X "Trades Working" güvenlik kilidi.

quanfina_math.py compute_pyramid_tier her tier eşik + Mark X kilit
coverage tam.
"""
import pytest
from pathlib import Path
import sys

PROJECT_ROOT = Path(__file__).resolve().parent.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from quanfina_math import (
    compute_pyramid_tier,
    MARK_PYRAMID_PILOT_PCT_RANGE,
    MARK_PYRAMID_STANDARD_PCT_RANGE,
    MARK_PYRAMID_FULL_PCT_RANGE,
)


# Sabit portfolio for senaryolarda
PV = 100_000.0


# =====================================================================
# Test: Tier sınır eşikleri
# =====================================================================

class TestTierBoundaries:
    """KARAR #487 birebir: Pilot %1-3, Standart %6.25-12.5, Full %15-25."""

    def test_zero_position_below_pilot(self):
        r = compute_pyramid_tier(0, PV)
        assert r['tier'] == 'BELOW_PILOT'
        assert r['position_pct'] == 0.0
        assert r['next_tier'] == 'PILOT'

    def test_below_pilot_min(self):
        # %0.5 → BELOW_PILOT
        r = compute_pyramid_tier(500, PV)
        assert r['tier'] == 'BELOW_PILOT'
        assert r['next_tier'] == 'PILOT'

    def test_pilot_min_boundary(self):
        # %1.0 sınır → PILOT
        r = compute_pyramid_tier(1000, PV)
        assert r['tier'] == 'PILOT'
        assert r['severity'] == 'ok'

    def test_pilot_mid(self):
        # %2 → PILOT (KARAR #487 zayıf piyasa)
        r = compute_pyramid_tier(2000, PV)
        assert r['tier'] == 'PILOT'

    def test_pilot_max_boundary(self):
        # %3.0 sınır → PILOT
        r = compute_pyramid_tier(3000, PV)
        assert r['tier'] == 'PILOT'

    def test_between_pilot_and_standard(self):
        # %4 → PILOT (Mark X kilit testi, prev_tier_profitable=False default)
        r = compute_pyramid_tier(4000, PV)
        assert r['tier'] == 'PILOT'  # Standart altı, hala pilot
        assert r['next_tier'] == 'STANDARD'
        # Mark X kilit: prev_tier_profitable=False → warn
        assert r['severity'] == 'warn'

    def test_between_pilot_and_standard_with_lock_unlocked(self):
        # %4 + prev_tier_profitable=True → info (pilot kâra geçti)
        r = compute_pyramid_tier(4000, PV, prev_tier_profitable=True)
        assert r['severity'] == 'info'

    def test_standard_min_boundary(self):
        # %6.25 sınır → STANDARD
        r = compute_pyramid_tier(6250, PV)
        assert r['tier'] == 'STANDARD'

    def test_standard_mid_locked(self):
        # %10 + prev=False → STANDARD warn (Mark X kilit)
        r = compute_pyramid_tier(10_000, PV)
        assert r['tier'] == 'STANDARD'
        assert r['severity'] == 'warn'

    def test_standard_mid_unlocked(self):
        # %10 + prev=True → STANDARD ok
        r = compute_pyramid_tier(10_000, PV, prev_tier_profitable=True)
        assert r['tier'] == 'STANDARD'
        assert r['severity'] == 'ok'

    def test_standard_max_boundary(self):
        # %12.5 sınır → STANDARD
        r = compute_pyramid_tier(12_500, PV)
        assert r['tier'] == 'STANDARD'

    def test_between_standard_and_full(self):
        # %13.5 → STANDARD (Full altı), Mark X warn
        r = compute_pyramid_tier(13_500, PV)
        assert r['tier'] == 'STANDARD'
        assert r['next_tier'] == 'FULL'

    def test_full_min_boundary(self):
        # %15 sınır → FULL
        r = compute_pyramid_tier(15_000, PV)
        assert r['tier'] == 'FULL'

    def test_full_max_boundary(self):
        # %25 sınır → FULL
        r = compute_pyramid_tier(25_000, PV)
        assert r['tier'] == 'FULL'

    def test_over_max_violation(self):
        # %30 → OVER_MAX (Mark MAX %25+ aşırı)
        r = compute_pyramid_tier(30_000, PV)
        assert r['tier'] == 'OVER_MAX'
        assert r['severity'] == 'violation'
        assert r['next_tier'] is None

    def test_extreme_position(self):
        # %50+ → OVER_MAX
        r = compute_pyramid_tier(60_000, PV)
        assert r['tier'] == 'OVER_MAX'


# =====================================================================
# Test: Mark X "Trades Working" kilit
# =====================================================================

class TestMarkXLock:
    """Mark birebir: 'Trades not working = no size increase'.
    Pilot kâra geçmeden Standart, Standart kâra geçmeden Full YASAK."""

    def test_standard_locked_default(self):
        """Default prev_tier_profitable=False → Standart pozisyon warn."""
        r = compute_pyramid_tier(10_000, PV)  # %10
        assert r['tier'] == 'STANDARD'
        assert r['severity'] == 'warn'
        assert 'Mark X' in r['mark_says'] or 'kara gecmedi' in r['mark_says']

    def test_full_locked_default(self):
        """Full pozisyon kilit kapalı → warn."""
        r = compute_pyramid_tier(20_000, PV)  # %20
        assert r['tier'] == 'FULL'
        assert r['severity'] == 'warn'

    def test_full_unlocked(self):
        """Önceki tier kâra geçtiyse Full ok."""
        r = compute_pyramid_tier(20_000, PV, prev_tier_profitable=True)
        assert r['tier'] == 'FULL'
        assert r['severity'] == 'ok'

    def test_pilot_no_lock_needed(self):
        """Pilot tier kilit gerektirmez (ilk tier)."""
        r = compute_pyramid_tier(2000, PV)  # %2
        assert r['tier'] == 'PILOT'
        assert r['severity'] == 'ok'  # Kilit kontrolü yok, pilot kendi tier


# =====================================================================
# Test: Edge cases
# =====================================================================

class TestEdgeCases:
    def test_negative_position(self):
        r = compute_pyramid_tier(-100, PV)
        assert r['tier'] == 'BELOW_PILOT'

    def test_zero_portfolio(self):
        r = compute_pyramid_tier(1000, 0)
        assert r['tier'] == 'BELOW_PILOT'

    def test_position_equals_portfolio(self):
        # %100 → OVER_MAX
        r = compute_pyramid_tier(PV, PV)
        assert r['tier'] == 'OVER_MAX'

    def test_mark_says_present(self):
        """Her tier mark_says içermeli (KALICI İLKE #4)."""
        for pos in [500, 2000, 4000, 10_000, 20_000, 30_000]:
            r = compute_pyramid_tier(pos, PV)
            assert r['mark_says']
            assert len(r['mark_says']) > 10


# =====================================================================
# Test: Sabitler Mark KARAR #487 birebir
# =====================================================================

class TestPyramidConstants:
    """Sabitler Mark canon birebir doğrulama (KALICI İLKE #4)."""

    def test_pilot_range(self):
        assert MARK_PYRAMID_PILOT_PCT_RANGE == (1.0, 3.0)

    def test_standard_range(self):
        assert MARK_PYRAMID_STANDARD_PCT_RANGE == (6.25, 12.5)

    def test_full_range(self):
        assert MARK_PYRAMID_FULL_PCT_RANGE == (15.0, 25.0)
