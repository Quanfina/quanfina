"""
KARAR #733 alt-paket (Paket 56): compute_breadth_divergence pytest.

Mark+O'Neil "A/D Line erken uyari" canon — index trend vs A/D Line
trend ayrisma 4 kategori (CONFIRMED_UP / BEARISH_DIVERGENCE /
BULLISH_DIVERGENCE / CONFIRMED_DOWN / NEUTRAL).
"""
import pytest
import sys
from pathlib import Path

HERE = Path(__file__).resolve().parent
PROJECT_ROOT = HERE.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from quanfina_math import (
    compute_breadth_divergence,
    DIVERGENCE_INDEX_TREND_THRESHOLD_PCT,
    DIVERGENCE_AD_TREND_THRESHOLD,
)


class TestDivergenceCategories:
    """4 ana kategori + NEUTRAL mapping."""

    def test_confirmed_up(self):
        """Index up + A/D up → CONFIRMED_UP."""
        # Index +5% (5 pts üzerinde)
        index_closes = [400.0] * 5 + [410, 415, 418, 420]
        # A/D ikinci yarisi cok daha güçlü (first 200/day, second 600/day)
        adv = [200] * 5 + [600] * 5
        dec = [150] * 5 + [100] * 5
        result = compute_breadth_divergence(index_closes, adv, dec, lookback_days=10)
        assert result['divergence'] == 'CONFIRMED_UP'
        assert result['severity'] == 'ok'

    def test_bearish_divergence(self):
        """Index up + A/D down → BEARISH_DIVERGENCE (KRITIK)."""
        # Index +3% rallyling
        index_closes = [400.0] * 5 + [405, 408, 410, 412]
        # A/D ikinci yarisi cok daha zayif (first 600/day, second 200/day)
        adv = [600] * 5 + [200] * 5
        dec = [100] * 5 + [400] * 5
        result = compute_breadth_divergence(index_closes, adv, dec, lookback_days=10)
        assert result['divergence'] == 'BEARISH_DIVERGENCE'
        assert result['severity'] == 'critical'
        # KALICI İLKE #4: O'Neil "erken uyari" alintisi
        assert "erken uyari" in result['mark_says'].lower() or "1999" in result['mark_says']

    def test_bullish_divergence(self):
        """Index down + A/D up → BULLISH_DIVERGENCE."""
        # Index -3% düşüyor
        index_closes = [420.0] * 5 + [415, 412, 410, 407]
        # A/D ikinci yarisi cok daha güçlü
        adv = [200] * 5 + [600] * 5
        dec = [400] * 5 + [100] * 5
        result = compute_breadth_divergence(index_closes, adv, dec, lookback_days=10)
        assert result['divergence'] == 'BULLISH_DIVERGENCE'
        assert result['severity'] == 'info'

    def test_confirmed_down(self):
        """Index down + A/D down → CONFIRMED_DOWN."""
        # Index -5% düşüyor
        index_closes = [420.0] * 5 + [415, 410, 405, 399]
        # A/D ikinci yarisi cok daha zayif
        adv = [600] * 5 + [200] * 5
        dec = [100] * 5 + [500] * 5
        result = compute_breadth_divergence(index_closes, adv, dec, lookback_days=10)
        assert result['divergence'] == 'CONFIRMED_DOWN'
        assert result['severity'] == 'warn'

    def test_neutral_below_thresholds(self):
        """Index düz + A/D düz → NEUTRAL."""
        index_closes = [400.0] * 10  # değişim yok
        adv = [300] * 10              # A/D net 0
        dec = [300] * 10
        result = compute_breadth_divergence(index_closes, adv, dec, lookback_days=10)
        assert result['divergence'] == 'NEUTRAL'
        assert result['severity'] == 'info'


class TestTrendCalculation:
    """Index ve A/D trend metrikleri doğru hesaplanıyor mu."""

    def test_index_change_pct_calculation(self):
        """Index_change_pct (start -> now) doğru hesaplama."""
        # 400 → 420, +5%
        index_closes = [400.0] * 5 + [410, 415, 418, 420]
        adv = [300] * 10
        dec = [300] * 10
        result = compute_breadth_divergence(index_closes, adv, dec, lookback_days=10)
        # lookback 10 ama len(index_closes)=9, n_index=min(10, 8)=8
        # start = closes[-8-1] = closes[0] = 400, now = 420
        assert result['index_change_pct'] is not None
        assert result['index_change_pct'] > 4  # ~5%

    def test_ad_trend_delta_positive(self):
        """A/D second half - first half pozitif olmali."""
        adv = [100] * 5 + [500] * 5
        dec = [100] * 5 + [100] * 5
        result = compute_breadth_divergence([400.0] * 10, adv, dec, lookback_days=10)
        assert result['ad_trend_delta'] is not None
        assert result['ad_trend_delta'] > 0

    def test_ad_trend_delta_negative(self):
        """A/D second half cok daha zayifsa negatif olmali."""
        adv = [500] * 5 + [100] * 5
        dec = [100] * 5 + [400] * 5
        result = compute_breadth_divergence([400.0] * 10, adv, dec, lookback_days=10)
        assert result['ad_trend_delta'] < 0


class TestEdgeCases:
    """Geçersiz veya boş input."""

    def test_empty_index_closes(self):
        result = compute_breadth_divergence([], [100], [100])
        assert result['divergence'] is None
        assert result['severity'] == 'info'

    def test_empty_advances(self):
        result = compute_breadth_divergence([400.0, 410.0], [], [])
        assert result['divergence'] is None

    def test_mismatched_advances_declines(self):
        result = compute_breadth_divergence([400.0, 410.0], [100, 200], [100])
        assert result['divergence'] is None
        assert 'uzunlugu farkli' in result['mark_says']

    def test_insufficient_window(self):
        """Tek gun veri → yetersiz pencere."""
        result = compute_breadth_divergence([400.0], [100], [100])
        assert result['divergence'] is None
        assert 'Yetersiz' in result['mark_says']


class TestMarkCanonGuard:
    """KALICI İLKE #4: Mark+O'Neil birebir alıntı koruma."""

    def test_bearish_divergence_oneil_warning(self):
        """BEARISH_DIVERGENCE markSays O'Neil erken uyari atifi."""
        # Index +3% ama A/D ikinci yarısı çok zayıf
        index_closes = [400.0] * 5 + [405, 408, 410, 412]
        adv = [600] * 5 + [150] * 5
        dec = [100] * 5 + [500] * 5
        result = compute_breadth_divergence(index_closes, adv, dec, lookback_days=10)
        if result['divergence'] == 'BEARISH_DIVERGENCE':
            says = result['mark_says'].lower()
            # O'Neil + tarihsel paten (1999/2007) atifi olmali
            assert "o'neil" in says or "1999" in says or "2007" in says

    def test_confirmed_up_mark_stage_2(self):
        """CONFIRMED_UP markSays Mark Stage 2 atifi."""
        index_closes = [400.0] * 5 + [410, 415, 418, 420]
        adv = [200] * 5 + [600] * 5
        dec = [150] * 5 + [100] * 5
        result = compute_breadth_divergence(index_closes, adv, dec, lookback_days=10)
        if result['divergence'] == 'CONFIRMED_UP':
            says = result['mark_says']
            assert "Stage 2" in says or "rally" in says.lower()


class TestConstants:
    """Esik canon doğrulama."""

    def test_index_threshold_1pct(self):
        assert DIVERGENCE_INDEX_TREND_THRESHOLD_PCT == 1.0

    def test_ad_threshold_500(self):
        assert DIVERGENCE_AD_TREND_THRESHOLD == 500
