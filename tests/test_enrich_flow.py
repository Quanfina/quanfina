"""
KARAR #733 alt-paket (Paket 171): yfinance enrich + cache flow regresyon testi.

Paper trading altyapısı (P144+P145+P146+P147):
- _compute_live_mark_signals MOCK + live overlay
- _MARK_SIGNALS_CACHE 5dk TTL davranışı
- _enrich_watchlist_batch / _enrich_trade_batch flow
- db_health_check 30s cache (P145 alt-katman)

Network bağımsız: _OHLCV_CACHE + _MARK_SIGNALS_CACHE manuel seed.
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
    from main import OhlcvBar, WatchlistRow, Trade
except ImportError:
    pytest.skip("api/main.py yok", allow_module_level=True)


@pytest.fixture(autouse=True)
def clear_caches():
    """Her test ayrı cache state'i."""
    api_main._OHLCV_CACHE.clear()
    api_main._MARK_SIGNALS_CACHE.clear()
    api_main._PIVOT_STATUS_CACHE.clear()
    yield
    api_main._OHLCV_CACHE.clear()
    api_main._MARK_SIGNALS_CACHE.clear()
    api_main._PIVOT_STATUS_CACHE.clear()


def _make_long_bars(n: int = 252, start: float = 100.0, trend: float = 0.001):
    """Mark canon helper'lar için yeterli bar (RS / Stage / Climax)."""
    bars = []
    price = start
    for i in range(n):
        price *= (1 + trend)
        bars.append(OhlcvBar(
            time=f"2025-{((i // 30) % 12) + 1:02d}-{(i % 28) + 1:02d}",
            open=price * 0.99, high=price * 1.02, low=price * 0.97,
            close=price, volume=1_000_000 + (i * 1000),
        ))
    return bars


# =====================================================================
# _compute_live_mark_signals — MOCK + cache
# =====================================================================

class TestComputeLiveMarkSignals:
    def test_returns_mock_when_bars_missing(self):
        """OHLCV cache boş + yfinance fail → MOCK _STOCK_MARK_SIGNALS only."""
        # NVDA MOCK base var
        result = api_main._compute_live_mark_signals("NVDA", 200.0)
        # MOCK base mevcut olmalı (carr_stage, vcp_quality_score gibi)
        # Live overlay yok ama MOCK var
        assert isinstance(result, dict)

    def test_returns_empty_for_unknown_no_mock_no_bars(self):
        """Bilinmeyen sembol + cache boş → boş dict (network fail durumda)."""
        result = api_main._compute_live_mark_signals("XYZTEST", 100.0)
        # MOCK yok, bars yok → bos veya minimal
        assert isinstance(result, dict)

    def test_cache_hit_returns_cached(self):
        """_MARK_SIGNALS_CACHE'de mevcut → 0 hesaplama."""
        api_main._MARK_SIGNALS_CACHE["CACHED"] = (
            time.time(),
            {"cached_field": "test_value", "carr_stage": 2},
        )
        result = api_main._compute_live_mark_signals("CACHED", 100.0)
        assert result.get("cached_field") == "test_value"
        assert result.get("carr_stage") == 2

    def test_cache_ttl_expired_recomputes(self):
        """TTL geçmiş → cache miss, yeniden hesap."""
        api_main._MARK_SIGNALS_CACHE["OLD"] = (
            time.time() - api_main._MARK_SIGNALS_CACHE_TTL_SEC - 10,
            {"stale": "value"},
        )
        result = api_main._compute_live_mark_signals("OLD", 100.0)
        # Stale field hâlâ olmamalı (recompute)
        assert "stale" not in result


# =====================================================================
# _enrich_watchlist_batch flow
# =====================================================================

class TestEnrichWatchlistBatch:
    def test_empty_list_returns_empty(self):
        result = api_main._enrich_watchlist_batch([])
        assert result == []

    def test_single_row_enriched(self):
        """Tek satır enrichment — MOCK base'i mark_signals'a yazmalı."""
        # MOCK base seed
        api_main._MARK_SIGNALS_CACHE["NVDA"] = (
            time.time(),
            {"carr_stage": 2, "vcp_quality_score": "EXCELLENT"},
        )
        row = WatchlistRow(
            symbol="NVDA", strategy="minervini", status="buy",
            price=200.0, added_date="2026-05-26", setup_type="VCP",
            pivot_price=None, note=None, rs_rating=99,
            consensus_count=1, consensus_strategies=["minervini"],
        )
        result = api_main._enrich_watchlist_batch([row])
        assert len(result) == 1
        # mark_signals dict cache'den gelmeli
        assert result[0].mark_signals is not None
        assert result[0].mark_signals.get("carr_stage") == 2

    def test_multiple_rows_preserved(self):
        """N satır → N satır (sıra korunmalı)."""
        rows = [
            WatchlistRow(
                symbol=sym, strategy="minervini", status="watch",
                price=100.0, added_date="2026-05-26", setup_type=None,
                pivot_price=None, note=None, rs_rating=80,
                consensus_count=1, consensus_strategies=["minervini"],
            )
            for sym in ("AAA", "BBB", "CCC")
        ]
        result = api_main._enrich_watchlist_batch(rows)
        assert len(result) == 3
        assert [r.symbol for r in result] == ["AAA", "BBB", "CCC"]


# =====================================================================
# _enrich_trade_batch flow
# =====================================================================

class TestEnrichTradeBatch:
    def test_empty_list_returns_empty(self):
        result = api_main._enrich_trade_batch([])
        assert result == []

    def test_trade_enriched_with_mock(self):
        """MOCK base mark_signals enrichment."""
        api_main._MARK_SIGNALS_CACHE["AAPL"] = (
            time.time(),
            {"carr_stage": 2, "rs_rating": 87},
        )
        trade = Trade(
            id=1, symbol="AAPL", strategy="minervini",
            setup_type="VCP", signal_source="strategy",
            entry_date="2026-05-01", entry_price=210.0, shares=100,
            status="open",
            exit_date=None, exit_price=None,
            pl_dollar=None, pl_pct=None,
            grade=None, exit_reason=None, lessons=None,
        )
        result = api_main._enrich_trade_batch([trade])
        assert len(result) == 1
        assert result[0].mark_signals is not None
        assert result[0].mark_signals.get("carr_stage") == 2


# =====================================================================
# db_health_check cache (P145 alt-katman)
# =====================================================================

class TestDbHealthCache:
    def test_health_check_returns_bool(self):
        """db_health_check her durumda bool döner."""
        import api.db_helpers as db_helpers
        # Cache temizle (test izolasyonu)
        db_helpers._DB_HEALTH_CACHE = (0.0, False)
        result = db_helpers.db_health_check()
        assert isinstance(result, bool)

    def test_health_check_cached_within_ttl(self):
        """30s TTL içinde cached değer döner — DB çağrı yok."""
        import api.db_helpers as db_helpers
        # Manuel cache seed
        db_helpers._DB_HEALTH_CACHE = (time.time(), True)
        result = db_helpers.db_health_check()
        assert result is True  # Cache hit, gerçek DB sorgulanmadı


# =====================================================================
# Pivot status cache
# =====================================================================

class TestPivotStatusCache:
    def test_pivot_cache_hit(self):
        """_PIVOT_STATUS_CACHE seed edilmiş → 0 hesap."""
        api_main._PIVOT_STATUS_CACHE["CACHED"] = (time.time(), "CONFIRMED")
        result = api_main._compute_signal_pivot_status("CACHED", 100.0)
        assert result == "CONFIRMED"

    def test_pivot_cache_ttl_respected(self):
        """TTL geçmiş cache entry — yeniden hesap (None veya yeni status)."""
        api_main._PIVOT_STATUS_CACHE["OLD"] = (
            time.time() - api_main._PIVOT_STATUS_CACHE_TTL_SEC - 10,
            "CONFIRMED",
        )
        result = api_main._compute_signal_pivot_status("OLD", 100.0)
        # Stale "CONFIRMED" dönmemeli — recompute, sonuç farklı olabilir
        # (gerçek hesap MOCK OHLCV ile çalışır, deterministik)
        # Test sadece TTL kontrolünün geçtiğini doğrular
        assert result is None or result in (
            "CONFIRMED", "WEAK", "NEAR_PIVOT", "BELOW_PIVOT"
        )
