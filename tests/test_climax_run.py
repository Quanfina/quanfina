"""compute_climax_run testleri — Mark TLSMW Ch 9 Climax Run.

KARAR #733 alt-paket (Paket 86, 26 May 2026).

4 kategori: CLIMAX_TOP, POTENTIAL_CLIMAX, HEALTHY_ADVANCE, NONE.
"""

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from quanfina_math import (
    compute_climax_run,
    CLIMAX_RUN_LOOKBACK_DAYS,
    CLIMAX_RUN_GAIN_TOP_PCT,
)


# --------- Yetersiz veri ---------

def test_empty_closes():
    res = compute_climax_run([])
    assert res['category'] is None
    assert 'Yetersiz' in res['mark_says']


def test_short_closes():
    res = compute_climax_run([100.0] * 10)
    assert res['category'] is None


# --------- CLIMAX_TOP ---------

def test_climax_top_with_gaps():
    """%30 kazanç + 3 gap-up + yüksek hacim = CLIMAX_TOP."""
    # 20 günlük veri: ilk 5 düz, sonraki 15 parabolic
    closes = [100.0] * 5 + [
        100, 102, 104, 108, 112,
        118, 124, 130, 136, 144,
        148, 152, 156, 160, 130,  # son gün düşüş — climax sonrası
    ]
    # Wait, climax_run looks at last 15 days. Let me recompute.
    # Window: last 15 closes from above = positions 5-19
    # start_price = closes[5] = 100, current = closes[-1] = 130
    # gain = 30% ✓
    opens = [100.0] * 5 + [
        100, 105, 107, 111, 115,  # 3 gap-ups (105 vs 102, 107 vs 104, 111 vs 108)
        121, 127, 133, 139, 147,
        151, 155, 159, 163, 132,
    ]
    volumes = [1_000_000] * 5 + [
        1_100_000, 1_200_000, 1_500_000, 2_000_000, 2_500_000,
        3_000_000, 3_200_000, 3_500_000, 4_000_000, 4_500_000,
        4_800_000, 5_000_000, 5_200_000, 5_500_000, 6_000_000,
    ]
    res = compute_climax_run(closes, opens, volumes)
    assert res['category'] == 'CLIMAX_TOP', f"Got {res['category']}: gain={res['gain_pct']}, gaps={res['gap_up_days']}"
    assert res['gain_pct'] >= CLIMAX_RUN_GAIN_TOP_PCT
    assert res['gap_up_days'] >= 2


# --------- POTENTIAL_CLIMAX (gain yüksek, gap yok) ---------

def test_potential_climax_no_gaps():
    """%30 kazanç ama gap-up yok = POTENTIAL_CLIMAX."""
    closes = [100.0] * 5 + [
        100, 102, 104, 106, 108,
        112, 116, 120, 124, 128,
        130, 131, 132, 133, 130,  # final
    ]
    # opens = previous close (zero gap)
    opens = [closes[0]] + [closes[i - 1] for i in range(1, len(closes))]
    res = compute_climax_run(closes, opens)
    assert res['category'] == 'POTENTIAL_CLIMAX', f"Got {res['category']}: gain={res['gain_pct']}, gaps={res['gap_up_days']}"


def test_potential_climax_moderate_gain():
    """%18 kazanç (15-25 arası) = POTENTIAL_CLIMAX."""
    # window = last 15: 100 → 118 (gain=%18)
    closes = [100.0] * 10 + [
        100, 100, 100, 100, 100,
        103, 106, 109, 112, 115,
        118, 119, 120, 119, 118,
    ]
    res = compute_climax_run(closes)
    assert res['category'] == 'POTENTIAL_CLIMAX', f"Got {res['category']}: gain={res['gain_pct']}"


# --------- HEALTHY_ADVANCE ---------

def test_healthy_advance():
    """%8 normal trend = HEALTHY_ADVANCE."""
    closes = [100.0] * 5 + [
        100, 100.5, 101, 101.5, 102,
        103, 103.5, 104, 104.5, 105,
        106, 106.5, 107, 107.5, 108,
    ]
    res = compute_climax_run(closes)
    assert res['category'] == 'HEALTHY_ADVANCE'


# --------- NONE — trend yok ---------

def test_no_trend_flat():
    """Düz fiyat = NONE veya HEALTHY (0%)."""
    closes = [100.0] * 25
    res = compute_climax_run(closes)
    # gain = 0%, category = NONE (gain > 0 koşulu fail)
    assert res['category'] == 'NONE'


def test_negative_trend():
    """Düşüş trendi = NONE."""
    closes = [110.0] * 5 + [
        110, 108, 106, 104, 102,
        100, 98, 96, 94, 92,
        90, 88, 86, 84, 82,
    ]
    res = compute_climax_run(closes)
    assert res['category'] == 'NONE'


# --------- Gap counting edge case ---------

def test_gap_up_counting():
    """Gap-up sayısı doğru hesaplanmalı."""
    closes = [100.0] * 5 + [
        100, 105, 110, 115, 120,
        125, 130, 135, 140, 145,
        150, 152, 154, 156, 158,
    ]
    # opens dramatically higher than prev close in some days
    opens = [100.0] * 5 + [
        100,
        103,   # vs prev 100 → +3% gap ✓
        108,   # vs prev 105 → +2.86% no
        113,   # vs prev 110 → +2.73% no
        119,   # vs prev 115 → +3.5% gap ✓
        124,   # vs prev 120 → +3.33% gap ✓
        129, 134, 139, 144,
        149, 151, 153, 155, 157,
    ]
    res = compute_climax_run(closes, opens)
    assert res['gap_up_days'] >= 2, f"Expected ≥2 gaps, got {res['gap_up_days']}"


# --------- Volume ratio ---------

def test_volume_ratio_computed():
    """Hacim oranı doğru hesaplanmalı."""
    closes = [100.0] * 5 + list(range(100, 120))  # 20 days
    volumes = [1_000_000] * 15 + [3_000_000] * 5  # son 5 gün 3x
    res = compute_climax_run(closes, None, volumes)
    assert res['avg_volume_ratio'] is not None
    assert res['avg_volume_ratio'] > 2.0  # son 5 gün >> önceki 10 gün


# --------- mark_says formatı ---------

def test_mark_says_present():
    """mark_says her zaman dolu."""
    closes = [100.0] * 5 + list(range(100, 119))
    res = compute_climax_run(closes)
    assert res['mark_says'] is not None
    assert len(res['mark_says']) > 10


# --------- Backward compat (opens/volumes None) ---------

def test_no_opens_no_volumes():
    """opens ve volumes None ise gap_up_days ve volume_ratio None."""
    closes = [100.0] * 5 + list(range(100, 130))  # 30 days for safety
    res = compute_climax_run(closes)
    assert res['gap_up_days'] is None
    assert res['avg_volume_ratio'] is None
    assert res['category'] is not None  # gain bazında karar verilebilmeli
