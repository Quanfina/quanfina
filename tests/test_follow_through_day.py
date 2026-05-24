"""
KARAR #733 alt-paket (Paket 64): compute_follow_through_day pytest.

Mark/O'Neil "Follow-Through Day" canon — bear dip'ten sonra +1.7%
sicrama + hacim teyit -> Stage 1 -> Stage 2 gecis onayi.
"""
import pytest
import sys
from pathlib import Path

HERE = Path(__file__).resolve().parent
PROJECT_ROOT = HERE.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from quanfina_math import (
    compute_follow_through_day,
    FTD_MIN_GAIN_PCT,
    FTD_VOLUME_MULTIPLIER,
    FTD_WINDOW_MIN_DAYS,
    FTD_WINDOW_MAX_DAYS,
)


class TestFTDDetection:
    """FTD tespiti — dip + 4-10 gün penceresi + %1.7+ sıçrama + hacim."""

    def test_ftd_detected_with_volume(self):
        """Klasik FTD senaryo: dip + 5 gün sonra +2.0% sıçrama + hacim onceki >1.2x."""
        # 15 gün senaryo:
        # gün 0-4: yüksek (430→425→420→418→415)
        # gün 5: dip (410)
        # gün 6-9: zayıf rally (412, 411, 413, 415)
        # gün 10: FTD (415→425, +2.4%, volume spike)
        closes = [430, 425, 420, 418, 415, 410, 412, 411, 413, 415, 425, 423, 422, 421, 420]
        volumes = [100] * 10 + [180, 100, 100, 100, 100]  # gün 10 hacim spike
        result = compute_follow_through_day(closes, volumes, lookback_days=15)
        assert result['ftd_detected'] is True
        assert result['ftd_gain_pct'] is not None
        assert result['ftd_gain_pct'] >= FTD_MIN_GAIN_PCT
        assert result['volume_confirmed'] is True
        assert result['days_after_low'] >= FTD_WINDOW_MIN_DAYS

    def test_ftd_detected_no_volume_confirmation(self):
        """FTD %2 ama hacim onceki = bugun -> ZAYIF onaylama."""
        # gün 5 dip, gün 10 FTD ama hacim teyit yok (gun 9 hacim daha yuksek)
        closes = [430, 425, 420, 418, 415, 410, 412, 411, 413, 415, 425, 423, 422, 421, 420]
        volumes = [100] * 9 + [200, 100, 100, 100, 100, 100]  # gun 9 spike, gun 10 normal
        result = compute_follow_through_day(closes, volumes, lookback_days=15)
        assert result['ftd_detected'] is True
        assert result['volume_confirmed'] is False
        assert 'hacim' in result['mark_says'].lower() or 'zayif' in result['mark_says'].lower()


class TestFTDRejection:
    """FTD reddedilmesi gereken senaryolar."""

    def test_no_ftd_no_strong_day(self):
        """Pencerede %1.7+ sıçrama yok -> ftd_detected=False."""
        # gün 5 dip 410, sonraki günler düz seyir 411-413
        closes = [430, 425, 420, 418, 415, 410, 411, 412, 411, 412, 413, 412, 411, 412, 413]
        volumes = [100] * 15
        result = compute_follow_through_day(closes, volumes, lookback_days=15)
        assert result['ftd_detected'] is False
        assert 'sıçrama YOK' in result['mark_says'] or 'siçrama yok' in result['mark_says'].lower()

    def test_too_early_dip(self):
        """Dip henüz oldu, FTD penceresi (4. gün) henüz başlamadı."""
        # gün 13 dip (sondan 2. gün) -> sadece 1 gün geçti, pencere yok
        closes = [430, 425, 420, 418, 415, 414, 413, 412, 411, 410.5, 410.2, 410.1, 410.05, 410, 410.5]
        volumes = [100] * 15
        result = compute_follow_through_day(closes, volumes, lookback_days=15)
        assert result['ftd_detected'] is False
        assert 'FTD penceresi' in result['mark_says'] or 'henüz başlamadı' in result['mark_says']


class TestEdgeCases:
    """Geçersiz veya boş input."""

    def test_empty_input(self):
        result = compute_follow_through_day([], [])
        assert result['ftd_detected'] is False
        assert 'Yetersiz' in result['mark_says']

    def test_mismatched_lengths(self):
        result = compute_follow_through_day([400, 410], [100])
        assert result['ftd_detected'] is False
        assert 'uzunluğu farklı' in result['mark_says']

    def test_insufficient_window(self):
        """5 gün veri var, en az 6 gerek (FTD_WINDOW_MIN_DAYS + 2)."""
        result = compute_follow_through_day([400, 410, 405, 412, 415], [100] * 5)
        assert result['ftd_detected'] is False
        assert 'Yetersiz' in result['mark_says']

    def test_zero_prev_close_handled(self):
        """0 fiyat divide by zero — defensive."""
        closes = [0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0]
        volumes = [100] * 15
        result = compute_follow_through_day(closes, volumes)
        # Tüm fiyatlar 0 — dip de 0, gain hesabı skip edilecek
        assert result['ftd_detected'] is False


class TestMarkCanonGuard:
    """KALICI İLKE #4: Mark/O'Neil birebir alıntı koruma."""

    def test_ftd_says_stage_2_transition(self):
        """FTD onayı says'inde Mark Stage 1 -> Stage 2 + tarihsel paten."""
        closes = [430, 425, 420, 418, 415, 410, 412, 411, 413, 415, 425, 423, 422, 421, 420]
        volumes = [100] * 10 + [180, 100, 100, 100, 100]
        result = compute_follow_through_day(closes, volumes, lookback_days=15)
        if result['ftd_detected'] and result['volume_confirmed']:
            says = result['mark_says']
            # Mark Stage 1 -> Stage 2 + tarihsel paten (2003/2009/2020)
            assert "Stage" in says
            assert "2003" in says or "2009" in says or "2020" in says

    def test_oneil_volume_canon(self):
        """Hacim teyit yok says'inde Mark/O'Neil hacim canon atifi."""
        closes = [430, 425, 420, 418, 415, 410, 412, 411, 413, 415, 425, 423, 422, 421, 420]
        volumes = [100] * 9 + [200, 100, 100, 100, 100, 100]
        result = compute_follow_through_day(closes, volumes, lookback_days=15)
        if result['ftd_detected'] and not result['volume_confirmed']:
            says = result['mark_says'].lower()
            # Mark/O'Neil hacim canon (>1x onceki)
            assert "hacim" in says


class TestConstants:
    """FTD esik canon doğrulama."""

    def test_min_gain_pct_17(self):
        assert FTD_MIN_GAIN_PCT == 1.7

    def test_volume_multiplier_1x(self):
        assert FTD_VOLUME_MULTIPLIER == 1.0

    def test_window_min_days_4(self):
        assert FTD_WINDOW_MIN_DAYS == 4

    def test_window_max_days_10(self):
        assert FTD_WINDOW_MAX_DAYS == 10
