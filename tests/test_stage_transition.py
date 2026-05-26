"""compute_stage_transition testleri — Mark TLSMW Ch 4 / Weinstein Stage 1→2 (P120)."""
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from quanfina_math import (
    compute_stage_transition,
    STAGE_MA_PERIOD,
    STAGE_EARLY_DAYS_MAX,
    STAGE_MATURE_DAYS_MIN,
    STAGE_SLOPE_LOOKBACK,
)


def test_empty():
    res = compute_stage_transition([])
    assert res['category'] is None


def test_short_data():
    # ma_period + slope_lookback = 180 gün minimum
    res = compute_stage_transition([100.0] * 100)
    assert res['category'] is None
    assert 'Yetersiz' in res['mark_says']


def test_no_transition_below_ma():
    """Fiyat MA altında — Stage 1 veya 3/4."""
    # 200 gün, ilk 100 gün 100$, son 100 gün 90$ (düşüş)
    closes = [100.0] * 100 + [90.0] * 100
    res = compute_stage_transition(closes)
    assert res['category'] == 'NO_TRANSITION'
    assert res['price_above_ma_pct'] < 0


def test_early_stage_2():
    """Yeni kırılım — son 5 gün üstte."""
    # 195 gün 100$, son 5 gün 110$ (yeni kırılım)
    closes = [100.0] * 195 + [110.0] * 5
    res = compute_stage_transition(closes)
    assert res['category'] == 'EARLY_STAGE_2'
    assert res['days_above_ma'] <= STAGE_EARLY_DAYS_MAX


def test_confirmed_stage_2():
    """Orta dönem trend — 20-50 gün üstte."""
    # 175 gün 100$, son 25 gün 115$
    closes = [100.0] * 175 + [115.0] * 25
    res = compute_stage_transition(closes)
    assert res['category'] == 'CONFIRMED_STAGE_2'


def test_stage_2_mature():
    """Olgun trend — 60+ gün MA üstünde (karışık geçmiş ki MA<current)."""
    # 60 gün 80$ + 60 gün 100$ + 120 gün 120$ → MA ~116, current 120, days_above ~90
    closes = [80.0] * 60 + [100.0] * 60 + [120.0] * 120
    res = compute_stage_transition(closes)
    assert res['category'] == 'STAGE_2_MATURE'
    assert res['days_above_ma'] >= STAGE_MATURE_DAYS_MIN


def test_volume_trend_rising():
    """Hacim son 10g önceki 20g'den yüksek = RISING."""
    closes = [100.0] * 175 + [115.0] * 25
    volumes = [1_000_000] * 190 + [1_500_000] * 10
    res = compute_stage_transition(closes, volumes)
    assert res['volume_trend'] == 'RISING'


def test_volume_trend_stable():
    closes = [100.0] * 175 + [115.0] * 25
    volumes = [1_000_000] * 200
    res = compute_stage_transition(closes, volumes)
    assert res['volume_trend'] == 'STABLE'


def test_volume_trend_falling():
    closes = [100.0] * 175 + [115.0] * 25
    volumes = [1_500_000] * 180 + [800_000] * 20
    res = compute_stage_transition(closes, volumes)
    assert res['volume_trend'] == 'FALLING'


def test_no_volumes_safe():
    closes = [100.0] * 175 + [115.0] * 25
    res = compute_stage_transition(closes)
    assert res['volume_trend'] is None
    assert res['category'] is not None


def test_field_validation():
    closes = [100.0] * 175 + [115.0] * 25
    res = compute_stage_transition(closes)
    assert res['ma_value'] is not None
    assert res['ma_value'] > 0
    assert res['price_above_ma_pct'] is not None
    assert res['slope_pct'] is not None


def test_mark_says_present():
    for closes_data in [
        [100.0] * 100 + [90.0] * 100,    # NO_TRANSITION
        [100.0] * 195 + [110.0] * 5,      # EARLY
        [100.0] * 175 + [115.0] * 25,     # CONFIRMED
        [100.0] * 100 + [120.0] * 100,    # MATURE
    ]:
        res = compute_stage_transition(closes_data)
        assert res['mark_says'] is not None
        assert len(res['mark_says']) > 10
