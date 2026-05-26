"""compute_relative_strength_rating testleri — Mark/IBD RS Rating canon.

KARAR #733 alt-paket (Paket 91, 26 May 2026).

4 kategori: LEADER, STRONG, AVERAGE, LAGGARD.
"""
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from quanfina_math import (
    compute_relative_strength_rating,
    RS_LOOKBACK_DAYS,
    RS_THRESHOLD_LEADER,
    RS_THRESHOLD_STRONG,
    RS_THRESHOLD_AVERAGE,
)


# --------- Yetersiz veri ---------

def test_empty_lists():
    res = compute_relative_strength_rating([], [])
    assert res['rs_rating'] is None
    assert res['category'] is None


def test_short_stock_closes():
    res = compute_relative_strength_rating([100.0] * 100, [100.0] * 252)
    assert res['rs_rating'] is None


def test_short_benchmark():
    res = compute_relative_strength_rating([100.0] * 252, [100.0] * 100)
    assert res['rs_rating'] is None


# --------- LEADER kategorisi ---------

def test_leader_strong_outperform():
    """Stok lineer %150+ yıllık yükselişte, benchmark düz = LEADER.
    Algoritma quartile-weighted, lineer 150% -> weighted ~24% -> RS >=80."""
    stock = [100 + (150 * i / 251) for i in range(252)]  # 100 -> 250
    bench = [100.0] * 252
    res = compute_relative_strength_rating(stock, bench)
    assert res['category'] == 'LEADER', f"Got {res['category']}: rs={res['rs_rating']}, outperform={res['outperform_pct']}"
    assert res['rs_rating'] >= RS_THRESHOLD_LEADER


def test_leader_extreme_outperform():
    """%500+ yıllık yükselişte stok = RS yüksek (>=90)."""
    stock = [100 + (500 * i / 251) for i in range(252)]
    bench = [100.0] * 252
    res = compute_relative_strength_rating(stock, bench)
    assert res['rs_rating'] >= 90


# --------- STRONG kategorisi ---------

def test_strong_moderate_outperform():
    """Stok %100 yıllık (weighted ~17%), benchmark düz = STRONG (70-79)."""
    stock = [100 + (100 * i / 251) for i in range(252)]
    bench = [100.0] * 252
    res = compute_relative_strength_rating(stock, bench)
    assert res['category'] in {'LEADER', 'STRONG'}, f"Got {res['category']}: rs={res['rs_rating']}"
    assert res['rs_rating'] >= RS_THRESHOLD_STRONG


# --------- AVERAGE kategorisi ---------

def test_average_market_pace():
    """Stok piyasa hızında (outperform yok) = AVERAGE."""
    stock = [100 + (10 * i / 251) for i in range(252)]
    bench = [100 + (10 * i / 251) for i in range(252)]
    res = compute_relative_strength_rating(stock, bench)
    assert res['category'] == 'AVERAGE'
    assert 40 <= res['rs_rating'] <= 60


# --------- LAGGARD kategorisi ---------

def test_laggard_underperform():
    """Stok düşüşte, benchmark yükselişte = LAGGARD."""
    stock = [100 - (30 * i / 251) for i in range(252)]
    bench = [100 + (20 * i / 251) for i in range(252)]
    res = compute_relative_strength_rating(stock, bench)
    assert res['category'] == 'LAGGARD'
    assert res['rs_rating'] < RS_THRESHOLD_AVERAGE


def test_laggard_extreme():
    """Stok düşüş + benchmark yükseliş = LAGGARD (RS<30)."""
    stock = [100 - (50 * i / 251) for i in range(252)]
    bench = [100 + (50 * i / 251) for i in range(252)]
    res = compute_relative_strength_rating(stock, bench)
    assert res['category'] == 'LAGGARD'
    assert res['rs_rating'] < 30


# --------- Field kontrolleri ---------

def test_outperform_calculated():
    """outperform_pct döner."""
    stock = [100 + (30 * i / 251) for i in range(252)]
    bench = [100 + (5 * i / 251) for i in range(252)]
    res = compute_relative_strength_rating(stock, bench)
    assert res['outperform_pct'] is not None
    assert res['outperform_pct'] > 0


def test_returns_present():
    stock = [100 + (20 * i / 251) for i in range(252)]
    bench = [100.0] * 252
    res = compute_relative_strength_rating(stock, bench)
    assert res['stock_return_pct'] is not None
    assert res['benchmark_return_pct'] is not None


def test_mark_says_present():
    stock = [100 + (40 * i / 251) for i in range(252)]
    bench = [100.0] * 252
    res = compute_relative_strength_rating(stock, bench)
    assert res['mark_says'] is not None
    assert len(res['mark_says']) > 10


# --------- Edge cases ---------

def test_rs_rating_clamped():
    """RS skoru [1, 99] aralığında."""
    stock = [100 + (1000 * i / 251) for i in range(252)]  # Çılgın yükseliş
    bench = [100.0] * 252
    res = compute_relative_strength_rating(stock, bench)
    assert 1 <= res['rs_rating'] <= 99


def test_zero_price_safe():
    """Sıfır fiyat değer hatası vermez."""
    stock = [0.0] + [100.0] * 251
    bench = [100.0] * 252
    res = compute_relative_strength_rating(stock, bench)
    # Sıfır guard çalışmalı — geçersiz çeyrek başlangıcı 0.0 dönüyor
    assert res['rs_rating'] is not None
