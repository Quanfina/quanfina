"""
Carr Stage Analizi — Stan Weinstein 4-Stage Detector pytest.

Stage 1 (Basing) / 2 (Advancing) / 3 (Topping) / 4 (Declining) coverage.
Notebook kaynak: kitaplar/Carr.md.
"""
import pytest
import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parent.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from quanfina_math import (
    compute_carr_stage,
    CARR_STAGE_MA_WINDOW,
    CARR_STAGE_SLOPE_FLAT_PCT,
    CARR_STAGE_PROXIMITY_PCT,
)


def _make_series(start: float, daily_pct: float, n: int) -> tuple[list[float], list[int]]:
    """Helper: günlük sabit % değişim ile fiyat serisi üret."""
    closes = [start]
    for _ in range(n - 1):
        closes.append(closes[-1] * (1 + daily_pct / 100))
    volumes = [1_000_000] * n
    return closes, volumes


# =====================================================================
# Test: Yetersiz veri
# =====================================================================

class TestInsufficientData:
    def test_empty(self):
        r = compute_carr_stage([], [])
        assert r['stage'] is None
        assert 'Yetersiz' in r['mark_says']

    def test_below_ma_window(self):
        # 149 < 150 default
        closes, vols = _make_series(100, 0.0, 149)
        r = compute_carr_stage(closes, vols)
        assert r['stage'] is None

    def test_mismatched_lengths(self):
        closes = [100] * 200
        volumes = [1000] * 100
        r = compute_carr_stage(closes, volumes)
        assert r['stage'] is None


# =====================================================================
# Test: 4-Stage tespiti
# =====================================================================

class TestStageDetection:
    def test_stage_2_advancing(self):
        """Sürekli yükseliş — price > MA + slope positive."""
        # 200 gün, günde +0.3% → ~80% yıllık rally
        closes, vols = _make_series(100, 0.3, 200)
        r = compute_carr_stage(closes, vols)
        assert r['stage'] == 2
        assert 'Advancing' in r['stage_label']
        assert r['price_vs_ma_pct'] > 0
        assert r['slope_pct_per_year'] > CARR_STAGE_SLOPE_FLAT_PCT

    def test_stage_4_declining(self):
        """Sürekli düşüş — price < MA + slope negative."""
        closes, vols = _make_series(200, -0.3, 200)
        r = compute_carr_stage(closes, vols)
        assert r['stage'] == 4
        assert 'Stage 4' in r['stage_label']
        assert r['price_vs_ma_pct'] < 0
        assert r['slope_pct_per_year'] < -CARR_STAGE_SLOPE_FLAT_PCT

    def test_stage_1_basing_flat(self):
        """Sabit fiyat — MA flat + price near MA."""
        closes, vols = _make_series(100, 0.0, 200)
        r = compute_carr_stage(closes, vols)
        assert r['stage'] == 1
        assert 'Stage 1' in r['stage_label']
        assert abs(r['slope_pct_per_year']) <= CARR_STAGE_SLOPE_FLAT_PCT
        assert abs(r['price_vs_ma_pct']) <= CARR_STAGE_PROXIMITY_PCT


# =====================================================================
# Test: Mark felsefe alıntıları (KALICI İLKE #4)
# =====================================================================

class TestMarkSays:
    def test_stage_2_says_present(self):
        closes, vols = _make_series(100, 0.3, 200)
        r = compute_carr_stage(closes, vols)
        assert r['mark_says']
        assert 'Mark' in r['mark_says'] or 'superperformance' in r['mark_says']

    def test_stage_4_says_warning(self):
        closes, vols = _make_series(200, -0.3, 200)
        r = compute_carr_stage(closes, vols)
        assert r['mark_says']
        assert 'UZAK DUR' in r['mark_says'] or 'Stage 4' in r['mark_says']


# =====================================================================
# Test: Sabitler birebir Carr/Weinstein canon
# =====================================================================

class TestCarrConstants:
    def test_ma_window_30_weeks(self):
        assert CARR_STAGE_MA_WINDOW == 150  # 30W × 5d

    def test_slope_flat_threshold(self):
        assert CARR_STAGE_SLOPE_FLAT_PCT == 1.0  # %1/yr

    def test_proximity_5_pct(self):
        assert CARR_STAGE_PROXIMITY_PCT == 5.0  # ±%5 near MA


# =====================================================================
# Test: Response shape
# =====================================================================

class TestResponseShape:
    def test_all_fields_present(self):
        closes, vols = _make_series(100, 0.1, 200)
        r = compute_carr_stage(closes, vols)
        for field in ('stage', 'stage_label', 'ma_value', 'price_vs_ma_pct',
                      'slope_pct_per_year', 'mark_says'):
            assert field in r

    def test_ma_value_is_numeric(self):
        closes, vols = _make_series(100, 0.0, 200)
        r = compute_carr_stage(closes, vols)
        assert isinstance(r['ma_value'], (int, float))
        # 200 gün sabit 100 → MA = 100
        assert abs(r['ma_value'] - 100.0) < 1.0
