"""compute_high_tight_flag testleri — O'Neil HTF = Minervini Power Play (P462).

Canon (IBD/MarketSmith/Minervini s.255): flagpole >=%100 (<=8hf) + flag sig %10-25
(3-6hf) + pivot = flag zirvesi. Sentetik: pole 50->105 (110%) -> flag pullback+tight.
"""
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from quanfina_math import compute_high_tight_flag


def _build_htf(pole_base=50.0, pole_top=105.0, pole_steps=30, flag_low=88.0,
               flag_steps=20, pole_vol=1_200_000, flag_vol=400_000):
    """Sentetik HTF: flagpole(pole_base->pole_top) + flag(pullback->recover, tight)."""
    highs, lows, closes, vols = [], [], [], []

    def add(p, v):
        highs.append(round(p + 0.5, 2))
        lows.append(round(p - 0.5, 2))
        closes.append(round(p, 2))
        vols.append(v)

    def ramp(a, b, steps, vol):
        for i in range(steps):
            add(a + (b - a) * i / (steps - 1), vol)

    ramp(pole_base, pole_top, pole_steps, pole_vol)         # flagpole (direk)
    half = flag_steps // 2
    ramp(pole_top, flag_low, half, flag_vol)                # bayrak pullback
    ramp(flag_low, (flag_low + pole_top) / 2, flag_steps - half, flag_vol)  # toparlanma (current)
    return highs, lows, closes, vols


def test_detects_proper_htf():
    h, l, c, v = _build_htf()
    r = compute_high_tight_flag(h, l, c, v)
    assert r['detected'] is True, f"faults: {r['faults']} | says: {r['mark_says']}"
    assert r['quality'] in ('EXCELLENT', 'GOOD')
    assert r['faults'] == []
    assert r['flagpole_pct'] >= 100.0
    assert r['flag_depth_pct'] <= 25.0
    assert 15 <= r['flag_duration_days'] <= 30


def test_pivot_is_flag_high():
    h, l, c, v = _build_htf()
    r = compute_high_tight_flag(h, l, c, v)
    # Pivot = bayrak/pole zirvesi (~105.5)
    assert r['pivot_price'] is not None
    assert 104.0 <= r['pivot_price'] <= 106.5


def test_excellent_needs_dryup():
    h, l, c, _ = _build_htf()
    r = compute_high_tight_flag(h, l, c, volumes=None)
    assert r['detected'] is True
    assert r['quality'] == 'GOOD'


def test_volume_dryup_promotes_excellent():
    # P468 (review): pozitif dry-up (bayrak hacmi << direk) + flag_depth<=20 -> EXCELLENT.
    h, l, c, v = _build_htf()
    r = compute_high_tight_flag(h, l, c, v)
    assert r['detected'] is True
    assert r['quality'] == 'EXCELLENT'


def test_pole_under_100_is_not_htf():
    # Flagpole <%100 -> High Tight Flag DEGIL (tanimlayici esik, Minervini s.255)
    h, l, c, v = _build_htf(pole_base=70.0, pole_top=105.0)
    r = compute_high_tight_flag(h, l, c, v)
    assert r['detected'] is False
    assert r['quality'] == 'NONE'
    assert "flagpole" in r['mark_says'].lower()


def test_deep_flag_faulty():
    # Bayrak >%25 derin -> kusur (O'Neil/Minervini)
    h, l, c, v = _build_htf(flag_low=72.0)
    r = compute_high_tight_flag(h, l, c, v)
    assert r['detected'] is False
    assert any('bayrak' in f for f in r['faults']), f"faults: {r['faults']}"
    assert r['flag_depth_pct'] > 25.0


def test_monotonic_uptrend_none():
    # Surekli yukselen -> pole_top sonda -> flag olusmadi (NONE)
    c = [50 + i * 0.7 for i in range(90)]
    h = [x + 0.5 for x in c]
    l = [x - 0.5 for x in c]
    r = compute_high_tight_flag(h, l, c, None)
    assert r['detected'] is False
    assert r['quality'] == 'NONE'


def test_insufficient_data():
    c = [100.0] * 30
    h = [100.5] * 30
    l = [99.5] * 30
    r = compute_high_tight_flag(h, l, c, None)
    assert r['detected'] is False
    assert r['quality'] == 'NONE'


def test_mark_says_present():
    h, l, c, v = _build_htf()
    r = compute_high_tight_flag(h, l, c, v)
    assert isinstance(r['mark_says'], str) and len(r['mark_says']) > 10
