"""compute_cup_with_handle testleri — O'Neil CAN SLIM HTMMIS Bol.15 (P455).

Sentetik OHLCV ile kupa-kulp geometrisi: pozitif (tam canon) + negatifler
(kusurlu kulp, zayif onceki trend, kupa yok, yetersiz veri). Esikler kitap-birebir
(prior +%30 / kupa %12-33 / kulp <=%15 / ust yari + 200MA / shakeout / U-sekil).
"""
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from quanfina_math import compute_cup_with_handle


def _build_cup_handle(prior_start=70.0, handle_low=92.0, handle_vol=450_000):
    """Sentetik cup-with-handle: prior uptrend -> kupa (U) -> kulp (ust yari).

    prior_start dusuk (70) => guclu onceki trend; yuksek (92) => zayif (<%30).
    handle_low dusuk (78) => kulp cok derin/alt yari (kusurlu).
    """
    highs, lows, closes, vols = [], [], [], []

    def add(p, v):
        highs.append(round(p + 0.5, 2))
        lows.append(round(p - 0.5, 2))
        closes.append(round(p, 2))
        vols.append(v)

    for i in range(60):                       # 1) onceki trend prior_start -> 100
        add(prior_start + (100 - prior_start) * i / 59, 800_000)
    for i in range(30):                        # 2) kupa sol kenar 100 -> 80
        add(100 - 20 * i / 29, 1_000_000)
    for i in range(10):                        # 3) yuvarlak dip ~80 (U)
        add(80 + (i % 3) * 0.5, 900_000)
    for i in range(35):                        # 4) sag toparlanma 80 -> 98
        add(80 + 18 * i / 34, 1_000_000)
    for i in range(12):                        # 5) kulp 98 -> handle_low (hacim dry-up)
        add(98 - (98 - handle_low) * i / 11, handle_vol)
    return highs, lows, closes, vols


def test_detects_proper_cup_handle():
    h, l, c, v = _build_cup_handle()
    r = compute_cup_with_handle(h, l, c, v)
    assert r['detected'] is True, f"faults: {r['faults']}"
    assert r['quality'] in ('EXCELLENT', 'GOOD')
    assert r['faults'] == []
    # Kupa derinligi O'Neil %12-33 araliginda
    assert 12.0 <= r['cup_depth_pct'] <= 33.0
    # Kulp <=%15, ust yarida, shakeout var
    assert r['handle_depth_pct'] <= 15.0
    assert r['handle_in_upper_half'] is True
    assert r['shakeout'] is True
    # Kupa suresi 7-65 hafta (35-325 gun)
    assert 35 <= r['cup_duration_days'] <= 325
    # Onceki trend >=%30
    assert r['prior_uptrend_pct'] >= 30.0


def test_pivot_is_handle_high():
    h, l, c, v = _build_cup_handle()
    r = compute_cup_with_handle(h, l, c, v)
    # Pivot = kulp zirvesi (toparlanma tepesi ~98.5), NOT +10 cent (kitapta yok)
    assert r['pivot_price'] is not None
    assert 97.0 <= r['pivot_price'] <= 99.5


def test_excellent_needs_volume_dryup():
    # Hacimsiz cagri -> dry-up bilinmez -> EXCELLENT degil GOOD
    h, l, c, _ = _build_cup_handle()
    r = compute_cup_with_handle(h, l, c, volumes=None)
    assert r['detected'] is True
    assert r['quality'] == 'GOOD'


def test_deep_handle_is_faulty():
    # Kulp >%15 derin (kupa dibi USTUNDE kalir) -> kusurlu (O'Neil s.164,178).
    # handle_low=82 -> depth ~%17.3, cup_low ~79.5 ustunde (yapi gecerli, fault yolu).
    h, l, c, v = _build_cup_handle(handle_low=82.0)
    r = compute_cup_with_handle(h, l, c, v)
    assert r['detected'] is False
    assert any('kulp' in f for f in r['faults']), f"faults: {r['faults']}"


def test_handle_below_cup_low_is_not_structure():
    # Kulp kupa dibinin de altina inerse -> bu artik cup-with-handle DEGIL (yapi yok)
    h, l, c, v = _build_cup_handle(handle_low=70.0)
    r = compute_cup_with_handle(h, l, c, v)
    assert r['detected'] is False
    assert r['quality'] == 'NONE'


def test_weak_prior_uptrend_faulty():
    # Onceki trend ~%9 (<%30) -> kusurlu (O'Neil s.165)
    h, l, c, v = _build_cup_handle(prior_start=92.0)
    r = compute_cup_with_handle(h, l, c, v)
    assert r['detected'] is False
    assert any('onceki trend' in f for f in r['faults'])
    assert r['prior_uptrend_pct'] < 30.0


def test_monotonic_uptrend_no_cup():
    # Sadece yukselen seri -> kupa yok (rim sonda)
    c = [70 + i * 0.5 for i in range(150)]
    h = [x + 0.5 for x in c]
    l = [x - 0.5 for x in c]
    r = compute_cup_with_handle(h, l, c, None)
    assert r['detected'] is False
    assert r['quality'] == 'NONE'


def test_insufficient_data():
    c = [100.0] * 40
    h = [100.5] * 40
    l = [99.5] * 40
    r = compute_cup_with_handle(h, l, c, None)
    assert r['detected'] is False
    assert r['quality'] == 'NONE'


def test_mark_says_present():
    h, l, c, v = _build_cup_handle()
    r = compute_cup_with_handle(h, l, c, v)
    assert isinstance(r['mark_says'], str) and len(r['mark_says']) > 10
