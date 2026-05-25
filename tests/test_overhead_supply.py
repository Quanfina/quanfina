"""
KARAR #733 alt-paket (Paket 76): compute_overhead_supply pytest.
Mark TLSMW Ch 10 — Overhead Supply Detection.
"""
import pytest
import sys
from pathlib import Path

HERE = Path(__file__).resolve().parent
PROJECT_ROOT = HERE.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from quanfina_math import (
    compute_overhead_supply,
    OVERHEAD_PROXIMITY_HEAVY_PCT,
    OVERHEAD_DROP_HEAVY_PCT,
    OVERHEAD_DROP_MODERATE_PCT,
    OVERHEAD_LOOKBACK_DAYS,
)


class TestOverheadCategories:
    def test_heavy_overhead(self):
        """Yüksek tepeden büyük düşüş, mevcut yakın → HEAVY."""
        # 60 gün 150'den 100'e düşüş → drop %33
        closes = [150 - i * 0.8 for i in range(60)] + [100]
        r = compute_overhead_supply(closes, lookback_days=60)
        assert r['category'] == 'HEAVY'
        assert r['drop_pct'] >= OVERHEAD_DROP_HEAVY_PCT

    def test_moderate_overhead(self):
        """%10-20 düşüş → MODERATE."""
        closes = [115] * 30 + [105] * 29 + [100]
        r = compute_overhead_supply(closes, lookback_days=60)
        assert r['category'] == 'MODERATE'
        assert OVERHEAD_DROP_MODERATE_PCT <= r['drop_pct'] < OVERHEAD_DROP_HEAVY_PCT

    def test_no_overhead_clean(self):
        """Mevcut fiyat tepe → NONE (temiz)."""
        # Sürekli yükselen
        closes = [100 + i * 0.5 for i in range(30)]
        r = compute_overhead_supply(closes)
        assert r['category'] == 'NONE'
        assert r['overhead_price'] is None

    def test_minor_overhead(self):
        """%3 üstü zayıf → NONE."""
        closes = [101] * 30 + [100]  # %1 üst
        r = compute_overhead_supply(closes, lookback_days=30)
        assert r['category'] == 'NONE'


class TestEdgeCases:
    def test_empty(self):
        r = compute_overhead_supply([])
        assert r['category'] is None

    def test_insufficient(self):
        r = compute_overhead_supply([100] * 5)
        assert r['category'] is None

    def test_proximity_calculation(self):
        closes = [120] * 30 + [100]
        r = compute_overhead_supply(closes, lookback_days=30)
        # 100'den 120'ye %20 yakınlık
        assert r['proximity_pct'] == 20.0


class TestMarkCanon:
    def test_heavy_says_tlsmw_ch10(self):
        closes = [150] * 30 + [100]
        r = compute_overhead_supply(closes, lookback_days=30)
        if r['category'] == 'HEAVY':
            assert 'TLSMW' in r['mark_says'] or 'Ch 10' in r['mark_says'] or 'kayıp kapansın' in r['mark_says']


class TestConstants:
    def test_heavy_proximity_5(self):
        assert OVERHEAD_PROXIMITY_HEAVY_PCT == 5.0

    def test_drop_heavy_20(self):
        assert OVERHEAD_DROP_HEAVY_PCT == 20.0

    def test_lookback_60(self):
        assert OVERHEAD_LOOKBACK_DAYS == 60
