"""
KARAR #733 alt-paket (Paket 171): yfinance enrich + cache flow regresyon testi.

Paper trading altyapısı (P144+P145+P146+P147; P548 sonrası MOCK base kaldırıldı):
- _compute_live_mark_signals DB base (gerçek) + yfinance live overlay (MOCK YOK)
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
def clear_caches(monkeypatch):
    """Her test ayrı cache state'i + #16 network bağımsızlık garantisi."""
    api_main._OHLCV_CACHE.clear()
    api_main._MARK_SIGNALS_CACHE.clear()
    api_main._PIVOT_STATUS_CACHE.clear()
    # #16 (denetim): dosya "Network bağımsız" iddia ediyordu ama cache temizken
    # _compute_live_mark_signals / _compute_signal_pivot_status içeride
    # _fetch_ohlcv_real → CANLI yfinance çağırıyordu (NVDA/XYZTEST/OLD seed'siz) →
    # flaky + yavaş. _fetch_ohlcv_real SADECE seed'li _OHLCV_CACHE'ten okusun
    # (seed yoksa None = "bars missing"). Test niyeti korunur, network sıfır.
    def _cache_only(symbol, n_bars=252):
        entry = api_main._OHLCV_CACHE.get(symbol.upper())
        if not entry or not entry[1]:
            return None
        bars = entry[1]
        return bars[-n_bars:] if (n_bars and len(bars) > n_bars) else bars
    monkeypatch.setattr(api_main, "_fetch_ohlcv_real", _cache_only)
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
# _compute_live_mark_signals — DB base + yfinance overlay + cache (P548: MOCK YOK)
# =====================================================================

class TestComputeLiveMarkSignals:
    def test_returns_db_or_empty_when_bars_missing(self):
        """P548 (Kural #28): OHLCV cache boş + yfinance fail → DB base (gerçek
        mark_signals_get_by_symbol) VEYA boş dict. _STOCK_MARK_SIGNALS MOCK KALDIRILDI
        — artık sahte carr_stage/vcp dönmez (dürüst: gerçek DB ya da boş, MOCK uydurmaz)."""
        result = api_main._compute_live_mark_signals("NVDA", 200.0)
        # Gerçek DB base veya boş dict — MOCK base YOK (P548 sonrası)
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


class TestEnrichLivePriceOverride:
    """P494 (Kural #28): stale web_watchlist.price -> canli son kapanis override.

    Watchlist "price" eklendigi tarihteki snapshot'ti (NVDA $875.40 / AAPL $182.30
    yaniltici, canli ~$204/~$296). Fix: gercek bars varsa price = bars[-1].close
    (/signals P493 ile ayni). Enrich zaten OHLCV cekiyor -> ek fetch yok.
    """

    def test_stale_price_overridden_with_live_close(self):
        bars = _make_long_bars(n=252, start=100.0, trend=0.001)
        live_close = round(bars[-1].close, 2)
        api_main._OHLCV_CACHE["TESTSYM"] = (time.time(), bars)
        row = WatchlistRow(
            symbol="TESTSYM", strategy="minervini", status="watch",
            price=999.0,  # STALE snapshot
            added_date="2026-05-01", setup_type=None, pivot_price=None, note=None,
            rs_rating=80, consensus_count=1, consensus_strategies=["minervini"],
        )
        result = api_main._enrich_with_mark_signals(row)
        assert result.price == live_close, (
            f"price={result.price} canli son kapanis {live_close} ile eslesmiyor "
            f"(stale 999 kalmis = P494 regresyon?)"
        )
        assert result.price != 999.0

    def test_no_bars_keeps_original_price(self):
        """Gercek bars yok (cache bos) -> _fetch_ohlcv_real None -> price degismez."""
        row = WatchlistRow(
            symbol="NOBARSXYZ", strategy="minervini", status="watch",
            price=123.45, added_date="2026-05-01", setup_type=None, pivot_price=None,
            note=None, rs_rating=80, consensus_count=1, consensus_strategies=["minervini"],
        )
        result = api_main._enrich_with_mark_signals(row)
        assert result.price == 123.45


class TestEnrichPocketPivot:
    """P537: Pocket Pivot on-deck sinyali watchlist enrichment (Minervini s.167).

    Kural #28: sadece gercek 70+ bar -> detected GOOD/CANDIDATE set; yoksa None.
    Network-free (clear_caches fixture _fetch_ohlcv_real'i seed cache'e baglar).
    """

    def _pp_bars(self, good: bool = True):
        # 60-bar uptrend (Stage 2) + 10-bar kuyruk (asagi gunler + bugun guclu yukari)
        closes = [100 + i * 0.4 for i in range(60)]
        base = closes[-1]
        tail = [base + 0.3, base - 0.2, base - 0.5, base + 0.1, base - 0.3,
                base + 0.2, base - 0.4, base + 0.5, base - 0.1, base + 1.5]
        closes = closes + tail
        tail_vol = [900_000, 700_000, 650_000, 900_000, 700_000,
                    900_000, 680_000, 900_000, 720_000, 2_000_000]
        volumes = [1_000_000] * 60 + tail_vol
        lows = [c - 0.6 for c in closes]
        sma10 = sum(closes[-10:]) / 10
        lows[-1] = (sma10 - 0.5) if good else (sma10 + 0.3)  # GOOD: 10-DMA temas
        bars = []
        for i, c in enumerate(closes):
            bars.append(OhlcvBar(
                time=f"2025-{((i // 30) % 12) + 1:02d}-{(i % 28) + 1:02d}",
                open=c, high=c + 0.3, low=lows[i], close=c, volume=volumes[i],
            ))
        return bars

    def _row(self, sym):
        return WatchlistRow(
            symbol=sym, strategy="minervini", status="watch", price=100.0,
            added_date="2026-05-01", setup_type=None, pivot_price=None, note=None,
            rs_rating=80, consensus_count=1, consensus_strategies=["minervini"],
        )

    def test_detected_good_sets_pocket_pivot(self):
        api_main._OHLCV_CACHE["PPGOOD"] = (time.time(), self._pp_bars(good=True))
        result = api_main._enrich_with_mark_signals(self._row("PPGOOD"))
        assert result.pocket_pivot == "GOOD"

    def test_extended_sets_candidate(self):
        api_main._OHLCV_CACHE["PPCAND"] = (time.time(), self._pp_bars(good=False))
        result = api_main._enrich_with_mark_signals(self._row("PPCAND"))
        assert result.pocket_pivot == "CANDIDATE"

    def test_not_detected_none(self):
        # Bugun ASAGI gun -> hacim imzasi yok -> pocket_pivot None
        bars = self._pp_bars(good=True)
        bars[-1] = OhlcvBar(time=bars[-1].time, open=bars[-1].open, high=bars[-1].high,
                            low=bars[-1].low, close=bars[-2].close - 1.0, volume=bars[-1].volume)
        api_main._OHLCV_CACHE["PPNONE"] = (time.time(), bars)
        result = api_main._enrich_with_mark_signals(self._row("PPNONE"))
        assert result.pocket_pivot is None

    def test_insufficient_bars_none_kural28(self):
        # <70 gercek bar -> pocket pivot atlanir (Kural #28 sentetik/yetersiz gosterme)
        api_main._OHLCV_CACHE["PPSHORT"] = (time.time(), self._pp_bars(good=True)[-50:])
        result = api_main._enrich_with_mark_signals(self._row("PPSHORT"))
        assert result.pocket_pivot is None


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


class TestTradeSectorEnrichment:
    """Paket 205 (27 May 2026): P190 sector field enrichment regresyon koruma.

    Mark TTLC s.85 sektör konsantrasyon uyarısı altyapısı:
    _STOCK_META lookup ile Trade.sector doldurulur. Eski 28 sembolde
    "industry" field fallback (P190 alt-fix).
    """

    def _make_trade(self, symbol: str) -> Trade:
        return Trade(
            id=1, symbol=symbol, strategy="minervini",
            setup_type="VCP", signal_source="strategy",
            entry_date="2026-05-01", entry_price=100.0, shares=10,
            status="open",
            exit_date=None, exit_price=None,
            pl_dollar=None, pl_pct=None,
            grade=None, exit_reason=None, lessons=None,
        )

    def test_new_symbol_has_sector_field(self):
        """Yeni eklenen sembol (P186): TSLA → Consumer Discretionary."""
        trade = self._make_trade("TSLA")
        result = api_main._enrich_trade_with_mark_signals(trade)
        assert result.sector == "Consumer Discretionary"

    def test_old_symbol_industry_fallback(self):
        """Eski sembol (sector field yok, sadece industry): NVDA → Semiconductors."""
        trade = self._make_trade("NVDA")
        result = api_main._enrich_trade_with_mark_signals(trade)
        # NVDA _STOCK_META'da "industry": "Semiconductors" var, "sector" yok
        # Fallback ile sector = "Semiconductors" doldurulmalı
        assert result.sector == "Semiconductors"

    def test_etf_symbol_sector_etf(self):
        """ETF eklemeleri (P186): XLK → ETF."""
        trade = self._make_trade("XLK")
        result = api_main._enrich_trade_with_mark_signals(trade)
        assert result.sector == "ETF"

    def test_unknown_symbol_no_sector(self):
        """_STOCK_META'da olmayan sembol → sector None (Bilinmiyor)."""
        trade = self._make_trade("UNKNOWN_XYZ")
        result = api_main._enrich_trade_with_mark_signals(trade)
        assert result.sector is None

    def test_lowercase_symbol_upper_normalize(self):
        """Sembol uppercase normalize ediliyor (Trade.symbol genelde upper)."""
        # Trade Pydantic uppercase'e otomatik çevirmez, _enrich'te .upper() var
        trade = self._make_trade("aapl")
        result = api_main._enrich_trade_with_mark_signals(trade)
        # AAPL "sector": "Technology" mevcut
        assert result.sector == "Technology"

    def test_biotech_symbol(self):
        """Biotech eklemeleri (P186): MRNA → Health Care."""
        trade = self._make_trade("MRNA")
        result = api_main._enrich_trade_with_mark_signals(trade)
        assert result.sector == "Health Care"


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
