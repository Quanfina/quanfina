"""compute_sell_strength testleri — Mark satis sinyalleri skorlu agregat (KARAR ADAY #976).

Canon (NotebookLM Quanfina Minervini): 200-MA/50-MA kirilim, Outside Day Neg Reversal
(gun ici >=%10 + dunku low alti), Climax Run (200-MA'dan >%50/>%100), Hard Stop (%10),
Sell Half (>=2x avg gain). Defansif -> SELL, offensive/50-MA -> REDUCE/WATCH.
"""
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from quanfina_math import compute_sell_strength


def _ohlc(closes, last_high=None, last_low=None):
    highs = [c + 0.5 for c in closes]
    lows = [c - 0.5 for c in closes]
    if last_high is not None:
        highs[-1] = last_high
    if last_low is not None:
        lows[-1] = last_low
    vols = [1_000_000.0] * len(closes)
    return highs, lows, closes, vols


def test_insufficient_data():
    h, l, c, v = _ohlc([100.0] * 30)
    r = compute_sell_strength(h, l, c, v)
    assert r["detected"] is False
    assert r["category"] == "HOLD"
    assert "Yetersiz" in r["mark_says"]


def test_healthy_uptrend_hold():
    # Yumusak yukselis: close > MA50 > MA200, asiri uzama yok -> sinyal yok
    c = [100.0 + i * 0.1 for i in range(250)]
    h, l, cc, v = _ohlc(c)
    r = compute_sell_strength(h, l, cc, v)
    assert r["detected"] is False
    assert r["category"] == "HOLD"
    assert r["signals"] == []


def test_ma200_break_sell():
    # close MA200 altina kirar -> defansif kesin -> SELL
    c = [120.0] * 249 + [100.0]
    h, l, cc, v = _ohlc(c)
    r = compute_sell_strength(h, l, cc, v)
    assert r["category"] == "SELL"
    assert any("200-MA" in s for s in r["defensive"])


def test_ma50_break_only_reduce():
    # close < MA50 ama > MA200 (guclu uptrend dip) -> azalt -> REDUCE
    c = [80.0 + 0.2 * i for i in range(249)] + [120.0]
    h, l, cc, v = _ohlc(c)
    r = compute_sell_strength(h, l, cc, v)
    assert r["category"] == "REDUCE"
    assert any("50-MA" in s for s in r["defensive"])
    assert not any("200-MA" in s for s in r["defensive"])


def test_outside_day_negative_reversal_sell():
    # gun ici +%11 (high 111 vs prev close 100) + dunku low alti kapanis -> kesin SELL
    c = [100.0] * 249 + [98.0]
    h, l, cc, v = _ohlc(c, last_high=111.0, last_low=97.0)
    r = compute_sell_strength(h, l, cc, v)
    assert r["category"] == "SELL"
    assert any("Outside Day" in s for s in r["defensive"])


def test_climax_warn_watch():
    # 200-MA'dan >%50 (<%100) uzama -> climax uyari -> WATCH
    c = [100.0] * 200 + [100.0 + 1.5 * j for j in range(50)]
    h, l, cc, v = _ohlc(c)
    r = compute_sell_strength(h, l, cc, v)
    assert r["category"] == "WATCH"
    assert r["pct_above_200ma"] > 50.0
    assert any("uyari" in s for s in r["offensive"])


def test_climax_strong_reduce():
    # 200-MA'dan >%100 -> sell into strength -> REDUCE
    c = [100.0] * 200 + [100.0 + 4.0 * j for j in range(50)]
    h, l, cc, v = _ohlc(c)
    r = compute_sell_strength(h, l, cc, v)
    assert r["category"] == "REDUCE"
    assert r["pct_above_200ma"] > 100.0
    assert any(">%100" in s for s in r["offensive"])


def test_hard_stop_sell():
    # HOLD serisi ama entry yuksek -> kayip <= -%10 -> Hard Stop -> SELL
    c = [100.0 + i * 0.1 for i in range(250)]  # close ~124.9
    h, l, cc, v = _ohlc(c)
    r = compute_sell_strength(h, l, cc, v, entry_price=140.0)
    assert r["category"] == "SELL"
    assert r["loss_pct"] <= -10.0
    assert any("Hard Stop" in s for s in r["defensive"])


def test_sell_half_offensive_reduce():
    # kar >= 2x avg gain -> Sell Half -> offensive -> REDUCE
    c = [100.0 + i * 0.1 for i in range(250)]  # close ~124.9, entry 100 -> ~%24.9
    h, l, cc, v = _ohlc(c)
    r = compute_sell_strength(h, l, cc, v, entry_price=100.0, avg_gain_pct=10.0)
    assert r["category"] == "REDUCE"
    assert any("Sell Half" in s for s in r["offensive"])


def test_mark_says_present_all_paths():
    for c in ([100.0 + i * 0.1 for i in range(250)], [120.0] * 249 + [100.0]):
        h, l, cc, v = _ohlc(c)
        r = compute_sell_strength(h, l, cc, v)
        assert isinstance(r["mark_says"], str) and len(r["mark_says"]) > 0
        assert 0 <= r["sell_strength"] <= 10
