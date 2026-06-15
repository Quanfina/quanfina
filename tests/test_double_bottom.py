"""compute_double_bottom testleri — O'Neil/IBD Double Bottom (W) (P460).

Canon (MarketSmith/IBD/TraderLion): W = iki dip + orta tepe; 2. dip 1. dibi UNDERCUT
eder (tanimlayici); pivot = orta tepe; min 7 hf (35g); onceki advance >=%30; derinlik
<=%35 (red >%50). Sentetik W: prior advance -> L1 -> orta tepe -> L2(undercut) -> recovery.
"""
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from quanfina_math import compute_double_bottom


def _build_double_bottom(prior_start=70.0, rim=100.0, l1=85.0, mp=95.0, l2=83.0,
                         recover=93.0, base_vol=400_000, adv_vol=900_000, l2_vol=300_000,
                         recover_steps=10):
    """Sentetik W: prior advance(overshoot rim) -> L1 -> orta tepe -> L2 -> recovery.
    recover_steps: sag-taraf toparlanma uzunlugu (asimetrik W regresyon testi icin uzatilir)."""
    highs, lows, closes, vols = [], [], [], []

    def add(p, v):
        highs.append(round(p + 0.5, 2))
        lows.append(round(p - 0.5, 2))
        closes.append(round(p, 2))
        vols.append(v)

    def ramp(a, b, steps, vol):
        for i in range(steps):
            add(a + (b - a) * i / (steps - 1), vol)

    ramp(prior_start, rim, 50, adv_vol)   # onceki advance -> rim (sol kenar)
    ramp(rim, l1, 10, base_vol)            # 1. dip'e inis
    ramp(l1, mp, 10, base_vol)             # orta tepe'ye toparlanma
    ramp(mp, l2, 10, l2_vol)               # 2. dip'e inis (undercut), dusuk hacim
    ramp(l2, recover, recover_steps, base_vol)  # pivot'a dogru toparlanma (current)
    return highs, lows, closes, vols


def test_detects_proper_double_bottom():
    h, l, c, v = _build_double_bottom()
    r = compute_double_bottom(h, l, c, v)
    assert r['detected'] is True, f"faults: {r['faults']} | says: {r['mark_says']}"
    assert r['quality'] in ('EXCELLENT', 'GOOD')
    assert r['faults'] == []
    assert r['undercut'] is True
    assert r['second_low'] <= r['first_low']      # 2. dip 1. dibi undercut
    assert r['base_depth_pct'] <= 35.0
    assert r['prior_advance_pct'] >= 30.0
    assert r['base_duration_days'] >= 35


def test_pivot_is_middle_peak():
    h, l, c, v = _build_double_bottom()
    r = compute_double_bottom(h, l, c, v)
    # Pivot = orta tepe (~95.5), sag-ust/rim (100) DEGIL
    assert r['pivot_price'] is not None
    assert 94.0 <= r['pivot_price'] <= 97.0
    assert r['middle_peak'] == r['pivot_price']


def test_excellent_needs_dryup():
    # Hacimsiz -> dry-up bilinmez -> EXCELLENT degil GOOD
    h, l, c, _ = _build_double_bottom()
    r = compute_double_bottom(h, l, c, volumes=None)
    assert r['detected'] is True
    assert r['quality'] == 'GOOD'


def test_volume_dryup_promotes_excellent():
    # P468 (review): pozitif dry-up (l2 hacmi << baz) + derinlik<=30 -> EXCELLENT terfi.
    h, l, c, v = _build_double_bottom()
    r = compute_double_bottom(h, l, c, v)
    assert r['detected'] is True
    assert r['quality'] == 'EXCELLENT'


def test_asymmetric_w_long_recovery_peak_first():
    # P471 regresyon: W erken (l1/mp/l2 baz'in ilk bolumunde) + UZUN yukselen sag taraf.
    # Eski takvim-yari bolmesi (len//2) 2.dip'i rising-right'a kaydirip undercut'i
    # false-reject ediyordu. Peak-first orta tepeyi dogru bulur -> tespit korunur.
    h, l, c, v = _build_double_bottom(recover_steps=40)
    r = compute_double_bottom(h, l, c, v)
    assert r['detected'] is True, f"faults: {r['faults']} | says: {r['mark_says']}"
    assert r['undercut'] is True
    assert r['second_low'] <= r['first_low']
    # pivot orta tepe (~95.5) civari — rising-right zirvesi (~93) DEGIL
    assert 94.0 <= r['pivot_price'] <= 97.0


def test_no_undercut_is_not_double_bottom():
    # 2. dip 1. dibi gecmiyor (l2 > l1) -> O'Neil double bottom DEGIL -> NONE
    h, l, c, v = _build_double_bottom(l1=85.0, l2=88.0)
    r = compute_double_bottom(h, l, c, v)
    assert r['detected'] is False
    assert r['quality'] == 'NONE'
    assert "undercut" in r['mark_says'].lower()


def test_weak_prior_advance_faulty():
    # Onceki advance <%30 -> kusur (O'Neil baz kurali)
    h, l, c, v = _build_double_bottom(prior_start=88.0, rim=100.0)
    r = compute_double_bottom(h, l, c, v)
    assert r['detected'] is False
    assert any('onceki advance' in f for f in r['faults']), f"faults: {r['faults']}"
    assert r['prior_advance_pct'] < 30.0


def test_monotonic_uptrend_none():
    # Surekli yukselen -> rim sonda -> baz yok (NONE)
    c = [70 + i * 0.4 for i in range(90)]
    h = [x + 0.5 for x in c]
    l = [x - 0.5 for x in c]
    r = compute_double_bottom(h, l, c, None)
    assert r['detected'] is False
    assert r['quality'] == 'NONE'


def test_insufficient_data():
    c = [100.0] * 40
    h = [100.5] * 40
    l = [99.5] * 40
    r = compute_double_bottom(h, l, c, None)
    assert r['detected'] is False
    assert r['quality'] == 'NONE'


def test_mark_says_present():
    h, l, c, v = _build_double_bottom()
    r = compute_double_bottom(h, l, c, v)
    assert isinstance(r['mark_says'], str) and len(r['mark_says']) > 10
