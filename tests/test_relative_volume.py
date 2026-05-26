"""compute_relative_volume testleri — Mark TLSMW Ch 6 (P106)."""
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from quanfina_math import compute_relative_volume


def test_empty():
    res = compute_relative_volume([])
    assert res['category'] is None


def test_short():
    res = compute_relative_volume([1_000_000] * 10)
    assert res['category'] is None


def test_drying():
    """%50 hacim — DRYING."""
    vols = [1_000_000] * 50 + [500_000]
    res = compute_relative_volume(vols)
    assert res['category'] == 'DRYING'
    assert res['rel_vol'] == 0.5


def test_quiet():
    """%85 hacim — QUIET."""
    vols = [1_000_000] * 50 + [850_000]
    res = compute_relative_volume(vols)
    assert res['category'] == 'QUIET'


def test_normal():
    """%125 hacim — NORMAL."""
    vols = [1_000_000] * 50 + [1_250_000]
    res = compute_relative_volume(vols)
    assert res['category'] == 'NORMAL'


def test_high_breakout():
    """%180 hacim — HIGH (Mark pivot kırılım eşiği)."""
    vols = [1_000_000] * 50 + [1_800_000]
    res = compute_relative_volume(vols)
    assert res['category'] == 'HIGH'


def test_surge():
    """%300 hacim — SURGE (climax/distribution)."""
    vols = [1_000_000] * 50 + [3_000_000]
    res = compute_relative_volume(vols)
    assert res['category'] == 'SURGE'


def test_field_validation():
    vols = [1_000_000] * 50 + [1_500_000]
    res = compute_relative_volume(vols)
    assert res['today_volume'] == 1_500_000
    assert res['avg_volume'] == 1_000_000
    assert res['rel_vol'] == 1.5
    assert len(res['mark_says']) > 10


def test_zero_avg_safe():
    """Sıfır ortalama hacim graceful."""
    vols = [0] * 50 + [1_000_000]
    res = compute_relative_volume(vols)
    assert res['rel_vol'] is None
    assert 'Geçersiz' in res['mark_says']
