"""compute_flat_base testleri — O'Neil/IBD Flat Base (P458).

Derin internet arastirma canon (IBD/MarketSmith/TraderLion): min 5 hafta (25g),
derinlik <=%15 (>%25 red), onceki advance >=%20 (later-stage), yatay/sideways,
pivot = baz zirvesi. Sentetik: advance(overshoot) -> tight flat baz.
"""
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from quanfina_math import compute_flat_base


def _build_flat_base(prior_start=70.0, peak=105.0, base_lo=96.0, base_hi=100.0,
                     base_len=40, base_vol=400_000, adv_vol=900_000):
    """advance prior_start->peak (overshoot, band ustu) sonra tight yatay flat baz."""
    highs, lows, closes, vols = [], [], [], []

    def add(p, v):
        highs.append(round(p + 0.5, 2))
        lows.append(round(p - 0.5, 2))
        closes.append(round(p, 2))
        vols.append(v)

    for i in range(50):                         # onceki advance (overshoot peak)
        add(prior_start + (peak - prior_start) * i / 49, adv_vol)
    mid = (base_lo + base_hi) / 2
    osc = [base_lo, mid, base_hi, mid]
    for i in range(base_len):                   # yatay flat baz
        add(osc[i % 4], base_vol)
    return highs, lows, closes, vols


def test_detects_proper_flat_base():
    h, l, c, v = _build_flat_base()
    r = compute_flat_base(h, l, c, v)
    assert r['detected'] is True, f"faults: {r['faults']} | says: {r['mark_says']}"
    assert r['quality'] in ('EXCELLENT', 'GOOD')
    assert r['faults'] == []
    assert r['base_depth_pct'] <= 15.0
    assert r['base_duration_days'] >= 25
    assert r['prior_advance_pct'] >= 20.0
    assert r['is_sideways'] is True


def test_pivot_is_base_high():
    h, l, c, v = _build_flat_base()
    r = compute_flat_base(h, l, c, v)
    # Pivot = baz icindeki en yuksek (~100.5), +10 cent IBD konvansiyonu kart tarafinda
    assert r['pivot_price'] is not None
    assert 99.5 <= r['pivot_price'] <= 101.0


def test_excellent_needs_dryup_and_tight():
    # Hacimsiz -> dry-up bilinmez -> EXCELLENT degil GOOD
    h, l, c, _ = _build_flat_base()
    r = compute_flat_base(h, l, c, volumes=None)
    assert r['detected'] is True
    assert r['quality'] == 'GOOD'


def test_volume_dryup_promotes_excellent():
    # P468 (review): pozitif dry-up + dar baz -> EXCELLENT terfi (negatif testin tersi).
    h, l, c, v = _build_flat_base()
    r = compute_flat_base(h, l, c, v)
    assert r['detected'] is True
    assert r['quality'] == 'EXCELLENT'


def test_wide_base_is_faulty():
    # Derinlik >%15 (genis/gevsek) -> kusur (IBD); 82-100 ~%18
    h, l, c, v = _build_flat_base(base_lo=82.0, base_hi=100.0)
    r = compute_flat_base(h, l, c, v)
    assert r['detected'] is False
    assert any('derinlik' in f for f in r['faults']), f"faults: {r['faults']}"
    assert r['base_depth_pct'] > 15.0


def test_no_prior_advance_faulty():
    # Onceki advance <%20 (later-stage degil) -> kusur (IBD/Invezz)
    h, l, c, v = _build_flat_base(prior_start=95.0, peak=101.0)
    r = compute_flat_base(h, l, c, v)
    assert r['detected'] is False
    assert any('onceki advance' in f for f in r['faults']), f"faults: {r['faults']}"
    assert r['prior_advance_pct'] < 20.0


def test_monotonic_uptrend_not_sideways():
    # Surekli yukselen seri -> yatay degil -> flat base degil (NONE)
    c = [70 + i * 0.4 for i in range(90)]
    h = [x + 0.5 for x in c]
    l = [x - 0.5 for x in c]
    r = compute_flat_base(h, l, c, None)
    assert r['detected'] is False
    assert r['quality'] == 'NONE'


def test_insufficient_data():
    c = [100.0] * 40
    h = [100.5] * 40
    l = [99.5] * 40
    r = compute_flat_base(h, l, c, None)
    assert r['detected'] is False
    assert r['quality'] == 'NONE'


def test_mark_says_present():
    h, l, c, v = _build_flat_base()
    r = compute_flat_base(h, l, c, v)
    assert isinstance(r['mark_says'], str) and len(r['mark_says']) > 10
