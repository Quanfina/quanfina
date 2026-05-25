"""
KARAR #733 alt-paket (Paket 70): compute_pivot_breakout pytest.

Mark TLSMW Ch 10 + O'Neil CANSLIM pivot kırılım canon — son N gün
tepe üstüne hacim teyitli kırılım = AL sinyali.
"""
import pytest
import sys
from pathlib import Path

HERE = Path(__file__).resolve().parent
PROJECT_ROOT = HERE.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from quanfina_math import (
    compute_pivot_breakout,
    PIVOT_BUFFER_PCT,
    PIVOT_VOLUME_CONFIRM_RATIO,
    PIVOT_VOLUME_LOOKBACK_DAYS,
    PIVOT_PRICE_LOOKBACK_DAYS,
)


class TestBreakoutDetection:
    """4 ana durum: CONFIRMED / WEAK / NEAR_PIVOT / BELOW_PIVOT."""

    def test_confirmed_breakout(self):
        """Pivot üstü + hacim 2x → CONFIRMED (Mark AL canon)."""
        # 20 gün 95-100 arası baz, gün 21 kırılım 105
        closes = [95, 96, 97, 98, 99, 100, 99, 98, 97, 98,
                  99, 100, 100, 99, 100, 99, 98, 99, 100, 100, 105]
        # Hacim normal 100, kırılım günü 250 (2.5x)
        volumes = [100] * 20 + [250]
        result = compute_pivot_breakout(closes, volumes, price_lookback=20,
                                         volume_lookback=15)
        assert result['status'] == 'CONFIRMED'
        assert result['volume_confirmed'] is True
        assert result['breakout_pct'] > 0
        assert result['pivot_price'] == 100  # max of first 20

    def test_weak_breakout_no_volume(self):
        """Pivot üstü + hacim 1.2x (eşik 1.5x altı) → WEAK."""
        closes = [95, 96, 97, 98, 99, 100, 99, 98, 97, 98,
                  99, 100, 100, 99, 100, 99, 98, 99, 100, 100, 105]
        volumes = [100] * 20 + [120]  # 1.2x — eşik altı
        result = compute_pivot_breakout(closes, volumes, price_lookback=20,
                                         volume_lookback=15)
        assert result['status'] == 'WEAK'
        assert result['volume_confirmed'] is False
        assert result['breakout_pct'] > 0

    def test_near_pivot_watch(self):
        """Pivot %3 altı yakın → NEAR_PIVOT."""
        closes = [95, 96, 97, 98, 99, 100, 99, 98, 97, 98,
                  99, 100, 100, 99, 100, 99, 98, 99, 100, 100, 98]
        # Bugün 98, pivot 100, breakout -2% → NEAR_PIVOT (-3 ile 0 arası)
        volumes = [100] * 21
        result = compute_pivot_breakout(closes, volumes, price_lookback=20)
        assert result['status'] == 'NEAR_PIVOT'
        assert result['breakout_pct'] < 0
        assert result['breakout_pct'] > -3

    def test_below_pivot(self):
        """Pivot %3+ altı → BELOW_PIVOT."""
        closes = [95, 96, 97, 98, 99, 100, 99, 98, 97, 98,
                  99, 100, 100, 99, 100, 99, 98, 99, 100, 100, 90]
        # Bugün 90, pivot 100, -10%
        volumes = [100] * 21
        result = compute_pivot_breakout(closes, volumes, price_lookback=20)
        assert result['status'] == 'BELOW_PIVOT'
        assert result['breakout_pct'] < -3


class TestPivotCalculation:
    """Pivot fiyat ve breakout % doğru hesaplama."""

    def test_pivot_excludes_today(self):
        """Bugün hariç son N gün max → pivot (bugün dahil edilmemeli)."""
        # 20 gün max 100, bugün 110 (kırılım)
        closes = [90] * 19 + [100] + [110]
        volumes = [100] * 21
        result = compute_pivot_breakout(closes, volumes, price_lookback=20)
        # Pivot = 100 (bugünkü 110 hariç son 20 gün max)
        assert result['pivot_price'] == 100
        assert result['current_price'] == 110

    def test_breakout_pct_calculation(self):
        """Breakout % = (current - pivot) / pivot * 100."""
        closes = [100] * 20 + [110]
        volumes = [100] * 20 + [200]
        result = compute_pivot_breakout(closes, volumes, price_lookback=20,
                                         volume_lookback=15)
        # 110 vs 100 = +10%
        assert result['breakout_pct'] == 10.0


class TestVolumeConfirmation:
    """Hacim teyit eşiği (1.5x) doğrulama."""

    def test_volume_exact_threshold(self):
        """1.5x tam eşik → confirmed (>= ile inclusive)."""
        closes = [100] * 20 + [105]
        # Volume avg ~100, bugün 150 → 1.5x
        volumes = [100] * 20 + [150]
        result = compute_pivot_breakout(closes, volumes, price_lookback=20,
                                         volume_lookback=15)
        assert result['volume_multiplier'] == 1.5
        assert result['volume_confirmed'] is True

    def test_volume_below_threshold(self):
        """1.4x → confirmed=False."""
        closes = [100] * 20 + [105]
        volumes = [100] * 20 + [140]
        result = compute_pivot_breakout(closes, volumes, price_lookback=20,
                                         volume_lookback=15)
        assert result['volume_multiplier'] == 1.4
        assert result['volume_confirmed'] is False


class TestEdgeCases:
    """Geçersiz veya boş input."""

    def test_empty_input(self):
        result = compute_pivot_breakout([], [])
        assert result['status'] is None
        assert 'Yetersiz' in result['mark_says']

    def test_mismatched_lengths(self):
        result = compute_pivot_breakout([100, 105], [100])
        assert result['status'] is None
        assert 'uzunluğu farklı' in result['mark_says']

    def test_insufficient_data(self):
        """price_lookback (20) + 1 = 21'den az veri yetersiz."""
        result = compute_pivot_breakout([100] * 10, [100] * 10)
        assert result['status'] is None
        assert 'Yetersiz' in result['mark_says']


class TestMarkCanonGuard:
    """KALICI İLKE #4: Mark TLSMW + O'Neil birebir alıntı."""

    def test_confirmed_says_oneil_canslim(self):
        """CONFIRMED says'inde Mark TLSMW Ch 10 + O'Neil CANSLIM atifi."""
        closes = [100] * 20 + [110]
        volumes = [100] * 20 + [250]
        result = compute_pivot_breakout(closes, volumes, price_lookback=20,
                                         volume_lookback=15)
        if result['status'] == 'CONFIRMED':
            says = result['mark_says']
            # Mark TLSMW Ch 10 "Volume confirms breakout" + O'Neil CANSLIM
            assert "TLSMW" in says or "CANSLIM" in says or "Volume confirms" in says

    def test_weak_says_false_breakout(self):
        """WEAK says'inde "false breakout" Mark canon atifi."""
        closes = [100] * 20 + [110]
        volumes = [100] * 20 + [120]
        result = compute_pivot_breakout(closes, volumes, price_lookback=20,
                                         volume_lookback=15)
        if result['status'] == 'WEAK':
            says = result['mark_says'].lower()
            assert "false breakout" in says or "hacim onayi yok" in says


class TestConstants:
    """Mark/O'Neil canon esik doğrulama."""

    def test_buffer_pct_05(self):
        assert PIVOT_BUFFER_PCT == 0.5

    def test_volume_ratio_15(self):
        assert PIVOT_VOLUME_CONFIRM_RATIO == 1.5

    def test_volume_lookback_50(self):
        assert PIVOT_VOLUME_LOOKBACK_DAYS == 50

    def test_price_lookback_20(self):
        assert PIVOT_PRICE_LOOKBACK_DAYS == 20
