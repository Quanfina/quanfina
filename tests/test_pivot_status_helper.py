"""
_compute_signal_pivot_status — Paket 384 (yfinance + MOCK fallback wire kanıtı).

Helper gerçek yfinance + fail-safe MOCK fallback wire (P81+P144). Sinyaller +
Screens + Trades enrichment'ta pivot_status alanı için kullanılır. Bu testler:

- Cache TTL davranışı (5 dk içinde tekrar çağrı network'e gitmez)
- Exception sızdırmama (caller asla try/except yapmak zorunda kalmasın)
- _OHLCV_CACHE seed ile network bypass (test_stock_detail_endpoints paterni)
- Status değer kontratı (None VEYA Mark canon kategorik string)
"""
import sys
import time
from pathlib import Path

import pytest

HERE = Path(__file__).resolve().parent
PROJECT_ROOT = HERE.parent
API_DIR = PROJECT_ROOT / "api"
for d in (str(PROJECT_ROOT), str(API_DIR)):
    if d not in sys.path:
        sys.path.insert(0, d)

try:
    import main as api_main
except ImportError:
    pytest.skip("fastapi yok", allow_module_level=True)


def _synthetic_bars(n=250, start=100.0):
    """Sentetik OHLCV (hafif uptrend) — Mark canon compute helper'lara yeterli."""
    bars = []
    for i in range(n):
        close = start + i * 0.5
        bars.append(api_main.OhlcvBar(
            time=f"2026-01-{(i % 28) + 1:02d}",
            open=close - 0.3, high=close + 0.8, low=close - 0.8,
            close=close, volume=1_000_000 + i * 1000,
        ))
    return bars


@pytest.fixture(autouse=True)
def _reset_pivot_cache():
    """Her test arası _PIVOT_STATUS_CACHE temizle (deterministik)."""
    api_main._PIVOT_STATUS_CACHE.clear()
    yield
    api_main._PIVOT_STATUS_CACHE.clear()


@pytest.fixture
def seeded_ohlcv():
    """yfinance bypass: _OHLCV_CACHE'a seed (test_stock_detail pateni)."""
    now = time.time()
    api_main._OHLCV_CACHE["TEST"] = (now, _synthetic_bars())
    yield "TEST"
    api_main._OHLCV_CACHE.pop("TEST", None)


class TestPivotStatusReturn:
    """Donus kontrat: None VEYA string (sinyal/uyari kategori)."""

    def test_seeded_returns_none_or_string(self, seeded_ohlcv):
        result = api_main._compute_signal_pivot_status(seeded_ohlcv, 225.0)
        assert result is None or isinstance(result, str)

    def test_empty_cache_yfinance_fail_no_exception(self):
        # Bilinmeyen sembol + network simulasyonu = MOCK fallback (Exception sizmaz)
        result = api_main._compute_signal_pivot_status("UNKNOWNZZZ", 100.0)
        # Helper exception yutar -> None veya string, ama crash YOK
        assert result is None or isinstance(result, str)


class TestCacheTtl:
    """5 dk cache davranisi — ayni semgol/fiyat tekrar cagrilirsa cache hit."""

    def test_cache_hit_within_ttl(self, seeded_ohlcv):
        # 1. cagri -> cache'e yazar
        result1 = api_main._compute_signal_pivot_status(seeded_ohlcv, 225.0)
        assert seeded_ohlcv in api_main._PIVOT_STATUS_CACHE
        ts1, val1 = api_main._PIVOT_STATUS_CACHE[seeded_ohlcv]
        # 2. cagri (ayni TTL icinde) -> cache hit, deger ayni
        result2 = api_main._compute_signal_pivot_status(seeded_ohlcv, 225.0)
        ts2, val2 = api_main._PIVOT_STATUS_CACHE[seeded_ohlcv]
        assert result1 == result2 == val1 == val2
        assert ts1 == ts2  # cache hit -> timestamp yenilenmedi

    def test_cache_expired_refetch(self, seeded_ohlcv):
        # Eski timestamp ile cache seed (TTL asilmis simulasyon)
        api_main._PIVOT_STATUS_CACHE[seeded_ohlcv] = (
            time.time() - api_main._PIVOT_STATUS_CACHE_TTL_SEC - 10,
            "STALE_VALUE",
        )
        # Yeni cagri -> cache expire, yeniden hesap
        api_main._compute_signal_pivot_status(seeded_ohlcv, 225.0)
        ts_new, _ = api_main._PIVOT_STATUS_CACHE[seeded_ohlcv]
        assert ts_new > time.time() - 5  # yeni timestamp (recent)


class TestExceptionSafety:
    """Helper Exception sizdirmamali — caller try/except'siz cagirabilmeli."""

    def test_zero_price_no_crash(self):
        # 0 fiyat -> _get_ohlcv start_price=0 -> compute helper'in edge davranisi
        # Helper crash etmemeli (caller None ile devam edebilmeli)
        result = api_main._compute_signal_pivot_status("EDGE", 0.0)
        assert result is None or isinstance(result, str)

    def test_negative_price_no_crash(self):
        # Sapma input — helper still graceful
        result = api_main._compute_signal_pivot_status("EDGE2", -10.0)
        assert result is None or isinstance(result, str)
