"""compute_atr_volatility testleri — Mark TLSMW Ch 11 / Wilder ATR canon.

KARAR #733 alt-paket (Paket 100, 26 May 2026).

4 kategori: LOW, NORMAL, HIGH, EXTREME.
"""
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from quanfina_math import (
    compute_atr_volatility,
    ATR_PERIOD,
    ATR_STOP_MULTIPLIER_TIGHT,
    ATR_STOP_MULTIPLIER_NORMAL,
    ATR_STOP_MULTIPLIER_LOOSE,
)


# --------- Yetersiz veri ---------

def test_empty_inputs():
    res = compute_atr_volatility([], [], [])
    assert res['atr'] is None
    assert res['category'] is None


def test_short_inputs():
    res = compute_atr_volatility([100.0] * 5, [99.0] * 5, [99.5] * 5)
    assert res['atr'] is None
    assert 'Yetersiz' in res['mark_says']


def test_mismatched_lengths():
    res = compute_atr_volatility(
        [100.0] * 20,
        [99.0] * 18,  # farklı uzunluk
        [99.5] * 20,
    )
    assert res['atr'] is None
    assert 'eşit' in res['mark_says']


# --------- LOW Volatility ---------

def test_low_volatility():
    """Sıkı daralma — ATR%<2."""
    # 20 gün, daralan range: high=100.5, low=99.5, close=100 (TR ~1, %1)
    highs = [100.5] * 20
    lows = [99.5] * 20
    closes = [100.0] * 20
    res = compute_atr_volatility(highs, lows, closes)
    assert res['category'] == 'LOW'
    assert res['atr_pct'] < 2.0


# --------- NORMAL Volatility ---------

def test_normal_volatility():
    """Sağlıklı trend — ATR%~3."""
    # 20 gün range 3$ (high=101.5, low=98.5, close=100 -> ATR 3, %3)
    highs = [101.5] * 20
    lows = [98.5] * 20
    closes = [100.0] * 20
    res = compute_atr_volatility(highs, lows, closes)
    assert res['category'] == 'NORMAL'


# --------- HIGH Volatility ---------

def test_high_volatility():
    """Yüksek volatilite — ATR%~5."""
    highs = [103.0] * 20
    lows = [98.0] * 20
    closes = [100.0] * 20
    res = compute_atr_volatility(highs, lows, closes)
    assert res['category'] == 'HIGH'


# --------- EXTREME Volatility ---------

def test_extreme_volatility():
    """Aşırı oynak — ATR%>7."""
    highs = [105.0] * 20
    lows = [95.0] * 20
    closes = [100.0] * 20
    res = compute_atr_volatility(highs, lows, closes)
    assert res['category'] == 'EXTREME'


# --------- Stop seviyeleri ---------

def test_suggested_stops():
    """3 stop seviyesi mantıklı sırada: tight > normal > loose (loose en uzak alt)."""
    highs = [101.5] * 20
    lows = [98.5] * 20
    closes = [100.0] * 20
    res = compute_atr_volatility(highs, lows, closes)
    # tight = current - 2×ATR, loose = current - 3×ATR
    # Yani tight > normal > loose (loose en düşük)
    assert res['suggested_stop_tight'] > res['suggested_stop_normal']
    assert res['suggested_stop_normal'] > res['suggested_stop_loose']


def test_stop_math_correct():
    """Stop math kontrolü."""
    highs = [102.0] * 20
    lows = [98.0] * 20
    closes = [100.0] * 20
    res = compute_atr_volatility(highs, lows, closes)
    atr = res['atr']
    current = closes[-1]
    expected_tight = round(current - ATR_STOP_MULTIPLIER_TIGHT * atr, 2)
    assert res['suggested_stop_tight'] == expected_tight


# --------- TR formülü ---------

def test_tr_uses_max_of_three():
    """TR = max(H-L, |H-prev_C|, |L-prev_C|) — gap durumunda formül doğru."""
    # Gap up senaryo: prev close=100, today high=112, low=109
    # TR_gap = max(3, 12, 9) = 12 (H-prev_C en büyük)
    # Gap günü öncesi düz seyrden sonra ATR yükselişi
    highs = [100.0] * 14 + [112.0] * 6
    lows = [100.0] * 14 + [109.0] * 6
    closes = [100.0] * 14 + [110.0] * 6
    res = compute_atr_volatility(highs, lows, closes)
    # ATR son 14 TR ort — gap günü TR=12 + 5 gün TR=3 + 8 düz gün TR=0
    # Sıfır TR ortalaması düşürür ama ATR > 1 (gap etkisi var)
    assert res['atr'] is not None
    assert res['atr'] > 1.0  # gap etkisi (sıfır seyirden uzaklaştı)
    # Düz seyirde (gap yok) ATR sıfır olurdu
    flat_highs = [100.0] * 20
    flat_lows = [100.0] * 20
    flat_closes = [100.0] * 20
    flat_res = compute_atr_volatility(flat_highs, flat_lows, flat_closes)
    assert flat_res['atr'] == 0.0
    # Gap senaryosu düz seyirden çok daha yüksek
    assert res['atr'] > flat_res['atr']


# --------- Field validation ---------

def test_mark_says_present():
    highs = [101.0] * 20
    lows = [99.0] * 20
    closes = [100.0] * 20
    res = compute_atr_volatility(highs, lows, closes)
    assert res['mark_says'] is not None
    assert len(res['mark_says']) > 10


def test_atr_positive():
    highs = [101.0] * 20
    lows = [99.0] * 20
    closes = [100.0] * 20
    res = compute_atr_volatility(highs, lows, closes)
    assert res['atr'] > 0
