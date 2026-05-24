"""
KARAR #733 alt-paket (Paket 51): compute_market_breadth helper pytest.

Mark+O'Neil A/D Line canon baseline test:
- A/D ratio threshold (STRONG/NEUTRAL/WEAK)
- 20-gun birikimli A/D Line
- Edge cases (empty, mismatched, all advances/declines)
- KALICI İLKE #4 Mark birebir alinti koruma
"""
import pytest
import sys
from pathlib import Path

HERE = Path(__file__).resolve().parent
PROJECT_ROOT = HERE.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from quanfina_math import (
    compute_market_breadth,
    BREADTH_STRONG_THRESHOLD,
    BREADTH_WEAK_THRESHOLD,
    BREADTH_LOOKBACK_DAYS,
)


class TestBreadthThresholds:
    """A/D ratio threshold mapping: STRONG / NEUTRAL / WEAK."""

    def test_strong_breadth(self):
        """A/D ratio >= 1.5 → STRONG."""
        adv = [300, 320, 340, 350, 360]
        dec = [200, 210, 200, 210, 200]  # 360/200 = 1.8 → STRONG
        result = compute_market_breadth(adv, dec)
        assert result['breadth_health'] == 'STRONG'
        assert result['ad_ratio'] >= BREADTH_STRONG_THRESHOLD

    def test_neutral_breadth(self):
        """0.8 <= A/D ratio < 1.5 → NEUTRAL."""
        adv = [200, 210, 220, 230, 240]
        dec = [200, 210, 220, 230, 230]  # 240/230 = 1.04 → NEUTRAL
        result = compute_market_breadth(adv, dec)
        assert result['breadth_health'] == 'NEUTRAL'

    def test_weak_breadth(self):
        """A/D ratio < 0.8 → WEAK."""
        adv = [100, 90, 80, 70, 60]
        dec = [400, 420, 430, 440, 450]  # 60/450 = 0.13 → WEAK
        result = compute_market_breadth(adv, dec)
        assert result['breadth_health'] == 'WEAK'
        assert result['ad_ratio'] < BREADTH_WEAK_THRESHOLD

    def test_exact_strong_threshold(self):
        """Tam STRONG sınırı (A/D = 1.5)."""
        adv = [150]
        dec = [100]  # 1.5 → STRONG (>= threshold)
        result = compute_market_breadth(adv, dec)
        assert result['breadth_health'] == 'STRONG'

    def test_exact_weak_threshold(self):
        """A/D = 0.8 → NEUTRAL (sınır dahil)."""
        adv = [80]
        dec = [100]  # 0.8 → NEUTRAL (>= 0.8)
        result = compute_market_breadth(adv, dec)
        assert result['breadth_health'] == 'NEUTRAL'


class TestADLineCumulative:
    """20-gun birikimli A/D Line."""

    def test_positive_cumulative(self):
        """Tum gunler advance > decline → pozitif birikim."""
        adv = [300] * 5
        dec = [200] * 5
        result = compute_market_breadth(adv, dec)
        assert result['ad_line_cumulative'] == 500  # (300-200)*5

    def test_negative_cumulative(self):
        """Tum gunler decline > advance → negatif birikim."""
        adv = [100] * 5
        dec = [300] * 5
        result = compute_market_breadth(adv, dec)
        assert result['ad_line_cumulative'] == -1000  # (100-300)*5

    def test_lookback_window(self):
        """Lookback 20 gun — daha kisa veri ile mevcut tum gunler."""
        adv = [250] * 10  # sadece 10 gun
        dec = [150] * 10
        result = compute_market_breadth(adv, dec, lookback_days=20)
        # 20 istense de sadece 10 gun var
        assert result['ad_line_cumulative'] == 1000  # (250-150)*10

    def test_lookback_truncate(self):
        """30 gun veri + 20 gun lookback → son 20 gun."""
        adv = [100] * 10 + [300] * 20  # ilk 10 zayıf, son 20 güçlü
        dec = [200] * 10 + [100] * 20
        result = compute_market_breadth(adv, dec, lookback_days=20)
        # Son 20 gün: (300-100)*20 = 4000
        assert result['ad_line_cumulative'] == 4000


class TestEdgeCases:
    """Geçersiz veya boş input."""

    def test_empty_input(self):
        result = compute_market_breadth([], [])
        assert result['ad_ratio'] is None
        assert result['ad_line_cumulative'] is None
        assert result['breadth_health'] is None
        assert 'verisi yok' in result['mark_says']

    def test_mismatched_lengths(self):
        result = compute_market_breadth([100, 200], [50])
        assert result['ad_ratio'] is None
        assert result['breadth_health'] is None
        assert 'uzunlugu farkli' in result['mark_says']

    def test_zero_declines(self):
        """Decline = 0 → A/D ratio inf (tum hisseler yükselmis)."""
        result = compute_market_breadth([300], [0])
        # inf STRONG threshold üstü olduğu için STRONG
        assert result['breadth_health'] == 'STRONG'


class TestMarkCanonGuard:
    """KALICI İLKE #4: Mark birebir alinti koruma."""

    def test_strong_mark_says(self):
        adv = [400]
        dec = [200]  # 2.0 → STRONG
        result = compute_market_breadth(adv, dec)
        says = result['mark_says'].lower()
        # Mark TLSMW Ch 5: "advances exceed declines"
        assert 'advances' in says and 'exceed' in says

    def test_weak_mark_says(self):
        adv = [50]
        dec = [400]  # 0.125 → WEAK
        result = compute_market_breadth(adv, dec)
        says = result['mark_says'].lower()
        # O'Neil: "A/D index oncesi zayifliyorsa erken uyari"
        assert "o'neil" in says or 'erken uyari' in says or 'index' in says


class TestConstants:
    """Mark+O'Neil canon sabit doğrulama."""

    def test_strong_threshold_15(self):
        assert BREADTH_STRONG_THRESHOLD == 1.5

    def test_weak_threshold_08(self):
        assert BREADTH_WEAK_THRESHOLD == 0.8

    def test_lookback_20_days(self):
        assert BREADTH_LOOKBACK_DAYS == 20
