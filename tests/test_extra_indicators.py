"""
P425 (31 May 2026) — Ek Piyasa Göstergeleri birim testleri.
compute_faber_timing / compute_mcclellan_oscillator / compute_zweig_breadth_thrust.

Kural #26 (kanon formül) + Kural #28 (yetersiz veri -> data_sufficient False,
MOCK sayı YOK). Derin tarama F4/F5/F7 sentezi.
"""
import sys
from pathlib import Path

HERE = Path(__file__).resolve().parent
PROJECT_ROOT = HERE.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

import pytest

from quanfina_math import (
    compute_faber_timing,
    compute_mcclellan_oscillator,
    compute_zweig_breadth_thrust,
    _ema,
)


class TestEma:
    def test_insufficient(self):
        assert _ema([1, 2], 5) is None

    def test_empty(self):
        assert _ema([], 3) is None

    def test_flat_series(self):
        # Sabit seri -> EMA = o sabit
        assert _ema([5.0] * 20, 10) == pytest.approx(5.0)

    def test_known_progression(self):
        # Artan seride EMA son degere yakin, ama altinda (gecmis agirlik)
        e = _ema(list(range(1, 31)), 10)
        assert e is not None and 20 < e < 30


class TestFaberTiming:
    def test_insufficient_data_no_mock(self):
        r = compute_faber_timing([100.0] * 50)  # 50 < 210
        assert r['data_sufficient'] is False
        assert r['signal'] is None  # MOCK sayı YOK
        assert r['days_needed'] == 210

    def test_invested_above_sma(self):
        # Yükselen seri -> son fiyat 10-ay SMA üstünde -> INVESTED
        closes = [float(i) for i in range(1, 261)]  # 260 gün artan
        r = compute_faber_timing(closes)
        assert r['data_sufficient'] is True
        assert r['signal'] == 'INVESTED'
        assert r['price'] > r['sma_10mo']
        assert r['pct_vs_sma'] > 0

    def test_cash_below_sma(self):
        # Düşen seri -> son fiyat SMA altında -> CASH
        closes = [float(i) for i in range(260, 0, -1)]  # 260 gün azalan
        r = compute_faber_timing(closes)
        assert r['data_sufficient'] is True
        assert r['signal'] == 'CASH'
        assert r['price'] < r['sma_10mo']

    def test_realistic_spy_invested(self):
        # SPY-benzeri: ~210 gün, son fiyat 739 > SMA ~682 (canlı senaryo)
        closes = [680.0 + i * 0.3 for i in range(220)]
        r = compute_faber_timing(closes)
        assert r['data_sufficient'] is True
        assert r['signal'] == 'INVESTED'


class TestMcClellan:
    def test_insufficient_data_no_mock(self):
        # 11 gün < 39 -> yetersiz (canlı durum: scanner 11 gün)
        adv = [400] * 11
        dec = [300] * 11
        r = compute_mcclellan_oscillator(adv, dec)
        assert r['data_sufficient'] is False
        assert r['value'] is None
        assert r['days_needed'] == 39
        assert r['days_have'] == 11

    def test_bullish_when_breadth_improving(self):
        # Son günlerde advances >> declines -> fast EMA > slow EMA -> pozitif
        adv = [300] * 20 + [600] * 25  # son 25 gün güçlü
        dec = [400] * 20 + [150] * 25
        r = compute_mcclellan_oscillator(adv, dec)
        assert r['data_sufficient'] is True
        assert r['value'] > 0
        assert r['signal'] == 'BULLISH'

    def test_bearish_when_breadth_deteriorating(self):
        adv = [600] * 20 + [150] * 25
        dec = [150] * 20 + [600] * 25
        r = compute_mcclellan_oscillator(adv, dec)
        assert r['data_sufficient'] is True
        assert r['value'] < 0
        assert r['signal'] == 'BEARISH'


class TestZweigBreadthThrust:
    def test_insufficient_data_no_mock(self):
        r = compute_zweig_breadth_thrust([400] * 5, [300] * 5)  # 5 < 10
        assert r['data_sufficient'] is False
        assert r['ema_ratio'] is None
        assert r['days_needed'] == 10

    def test_thrust_zone_high_ratio(self):
        # advances >> declines -> oran >0.615 -> THRUST_ZONE
        adv = [800] * 15
        dec = [200] * 15
        r = compute_zweig_breadth_thrust(adv, dec)
        assert r['data_sufficient'] is True
        assert r['ema_ratio'] >= 0.615
        assert r['zone'] == 'THRUST_ZONE'
        assert r['thrust_active'] is True

    def test_oversold_low_ratio(self):
        # declines >> advances -> oran <0.40 -> OVERSOLD
        adv = [200] * 15
        dec = [800] * 15
        r = compute_zweig_breadth_thrust(adv, dec)
        assert r['data_sufficient'] is True
        assert r['ema_ratio'] <= 0.40
        assert r['zone'] == 'OVERSOLD'
        assert r['thrust_active'] is False

    def test_neutral_balanced(self):
        # dengeli -> 0.40-0.615 arası -> NEUTRAL
        adv = [500] * 15
        dec = [500] * 15
        r = compute_zweig_breadth_thrust(adv, dec)
        assert r['data_sufficient'] is True
        assert 0.40 < r['ema_ratio'] < 0.615
        assert r['zone'] == 'NEUTRAL'
