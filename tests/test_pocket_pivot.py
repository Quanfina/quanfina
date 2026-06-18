"""P533 — compute_pocket_pivot (Minervini.md s.167 + Kacher/Morales).

Kanon kural: bugun YUKARI gun + hacmi son 10g en yuksek ASAGI-gun hacminden buyuk
(s.167). Baglam: Stage 2 (s.186) + 10-DMA donusu (Kacher/Morales). quality GOOD
(10-DMA temasi) / CANDIDATE (extended) / NONE. Kural #26: esikler kaynak-atifli.
"""
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from quanfina_math import compute_pocket_pivot, POCKET_PIVOT_MIN_BARS


def _uptrend_base():
    """60-bar temiz yukselis (50-DMA yukselen + close>50-DMA -> Stage 2) + 10-bar kuyruk
    (3+ asagi gun dusuk hacim, bugun guclu yukari + yuksek hacim)."""
    closes = [100 + i * 0.4 for i in range(60)]
    base = closes[-1]  # 123.6
    tail = [base + 0.3, base - 0.2, base - 0.5, base + 0.1, base - 0.3,
            base + 0.2, base - 0.4, base + 0.5, base - 0.1, base + 1.5]
    closes = closes + tail
    # hacim: ilk 60 = 1.0M; kuyruk asagi gun ~700k, yukari gun ~900k, BUGUN 2.0M (imza)
    tail_vol = [900_000, 700_000, 650_000, 900_000, 700_000,
                900_000, 680_000, 900_000, 720_000, 2_000_000]
    volumes = [1_000_000] * 60 + tail_vol
    opens = [c for c in closes]
    highs = [c + 0.3 for c in closes]
    lows = [c - 0.6 for c in closes]
    return opens, highs, lows, closes, volumes


def test_good_pocket_pivot_volume_and_ten_dma():
    o, h, lo, c, v = _uptrend_base()
    # bugun dusuk 10-DMA'ya temas etsin (off_ten_dma -> GOOD)
    sma10 = sum(c[-10:]) / 10
    lo[-1] = sma10 - 0.5  # gunun dusugu 10-DMA altinda, kapanis ustunde
    r = compute_pocket_pivot(o, h, lo, c, v)
    assert r["detected"] is True
    assert r["quality"] == "GOOD"
    assert r["off_ten_dma"] is True
    assert r["stage2"] is True
    # hacim imzasi: bugun (2.0M) > son 10g en yuksek asagi hacim (720k)
    assert r["max_down_volume_10d"] == 720_000.0
    assert r["volume_ratio"] is not None and r["volume_ratio"] > 1.0
    assert "s.167" in r["mark_says"]


def test_candidate_when_extended_above_ten_dma():
    o, h, lo, c, v = _uptrend_base()
    sma10 = sum(c[-10:]) / 10
    lo[-1] = sma10 + 0.3  # gunun dusugu 10-DMA USTUNDE -> extended -> CANDIDATE
    r = compute_pocket_pivot(o, h, lo, c, v)
    assert r["detected"] is True
    assert r["quality"] == "CANDIDATE"
    assert r["off_ten_dma"] is False


def test_no_volume_signature_rejected():
    o, h, lo, c, v = _uptrend_base()
    v[-1] = 100_000  # bugun hacmi son 10g asagi hacimlerin ALTINDA -> imza yok
    r = compute_pocket_pivot(o, h, lo, c, v)
    assert r["detected"] is False
    assert r["quality"] == "NONE"
    assert "hacim imzasi yok" in r["mark_says"]


def test_down_day_today_rejected():
    o, h, lo, c, v = _uptrend_base()
    c[-1] = c[-2] - 1.0  # bugun ASAGI gun -> up_day False -> imza yok
    r = compute_pocket_pivot(o, h, lo, c, v)
    assert r["detected"] is False


def test_not_stage2_rejected():
    # Dusus trendi + bugun YUKARI bounce (hacim imzasi gecer) AMA close < 50-DMA
    # -> Stage 2 degil reddi (s.186). Imza kontrolu gecip stage2 kapisina ulasir.
    closes = [200 - i * 0.5 for i in range(69)]
    closes.append(closes[-1] + 1.0)  # bugun yukari gun ama hala 50-DMA alti
    volumes = [1_000_000] * 69 + [3_000_000]  # bugun yuksek hacim -> imza gecer
    opens = [c for c in closes]
    highs = [c + 0.3 for c in closes]
    lows = [c - 0.6 for c in closes]
    r = compute_pocket_pivot(opens, highs, lows, closes, volumes)
    assert r["detected"] is False
    assert "Stage 2 degil" in r["mark_says"]


def test_insufficient_data_rejected():
    n = POCKET_PIVOT_MIN_BARS - 5
    closes = [100 + i * 0.4 for i in range(n)]
    volumes = [1_000_000] * n
    opens = [c for c in closes]
    highs = [c + 0.3 for c in closes]
    lows = [c - 0.6 for c in closes]
    r = compute_pocket_pivot(opens, highs, lows, closes, volumes)
    assert r["detected"] is False
    assert "Yetersiz veri" in r["mark_says"]
