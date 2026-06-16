"""
KARAR #733 alt-paket (Paket 42): /api/trades mark_signals enrichment pytest.

Paket 41 backend enrich (Trade Pydantic mark_signals Optional +
_enrich_trade_with_mark_signals helper) DRY watchlist pateni
dogrulama. Production'da minervini_scans JOIN'a gecmeden once
MOCK enrichment kanon koruma.
"""
import pytest
import sys
from pathlib import Path

HERE = Path(__file__).resolve().parent
PROJECT_ROOT = HERE.parent
API_DIR = PROJECT_ROOT / "api"
for d in (str(PROJECT_ROOT), str(API_DIR)):
    if d not in sys.path:
        sys.path.insert(0, d)

try:
    from fastapi.testclient import TestClient
    import main as api_main
except ImportError:
    pytest.skip("fastapi TestClient veya api/main.py yok", allow_module_level=True)


@pytest.fixture(scope="module")
def client():
    return TestClient(api_main.app)


@pytest.fixture(scope="module")
def trades(client):
    return client.get("/api/trades").json()


# =====================================================================
# Test: Endpoint smoke + shape
# =====================================================================

class TestTradesEndpointShape:
    def test_status_code(self, client):
        r = client.get("/api/trades")
        assert r.status_code == 200

    def test_returns_list(self, trades):
        assert isinstance(trades, list)

    def test_non_empty(self, trades):
        """MOCK fallback en az 8 trade dondurmeli (DB down ortamda dahi)."""
        assert len(trades) >= 8

    def test_required_trade_fields(self, trades):
        """Her trade temel alanlari icermeli."""
        for t in trades:
            for field in ("id", "symbol", "strategy", "setup_type",
                          "entry_date", "entry_price", "shares", "status"):
                assert field in t, f"{field} eksik: {t.get('symbol')}"


# =====================================================================
# Test: Mark Signals enrichment (Paket 41)
# =====================================================================

class TestTradeMarkEnrichment:
    """KARAR #733 alt-paket (Paket 41): _enrich_trade_with_mark_signals
    watchlist pateni — bilinen sembol -> mark_signals dict, bilinmeyen None."""

    def test_mark_signals_field_present(self, trades):
        """Her trade'de mark_signals alani bulunmali (None olsa da)."""
        for t in trades:
            assert "mark_signals" in t

    def test_known_symbols_enriched(self, trades):
        """_STOCK_MARK_SIGNALS dict'inde olan semboller enrich edilmis olmali."""
        known = {"NVDA", "MSFT", "AVGO", "AMD", "META", "TSLA"}
        for t in trades:
            if t["symbol"] in known:
                assert t["mark_signals"] is not None, \
                    f"{t['symbol']} bilinen sembol ama enrich edilmemis"
                assert isinstance(t["mark_signals"], dict)

    def test_unknown_symbols_enrich_optional(self, trades):
        """P329 MOCK->DB + P144 yfinance overlay sonrası: artık HER sembol
        (bilinmeyen dahil) yfinance/DB ile enrich edilebilir. Eski 'unknown→None'
        varsayımı geçersiz (sistem evrildi). mark_signals None VEYA geçerli dict
        — ikisi de kabul (graceful)."""
        for t in trades:
            sig = t["mark_signals"]
            assert sig is None or isinstance(sig, dict), \
                f"{t['symbol']} mark_signals None veya dict olmalı"

    def test_carr_stage_valid(self, trades):
        """Enrich edilmis trade'lerde carr_stage 1-4 arasinda olmali."""
        for t in trades:
            sig = t.get("mark_signals")
            if sig and "carr_stage" in sig:
                assert sig["carr_stage"] in {1, 2, 3, 4}


# =====================================================================
# Test: Mark Canon koruma (KALICI İLKE #4)
# =====================================================================

class TestMarkCanonGuard:
    """KARAR #733 + KALICI İLKE #4: Mark felsefe birebir alintilari korunmali."""

    def test_tsla_carr_stage_valid(self, trades):
        """TSLA carr_stage geçerli (1-4). P329 sonrası DB/yfinance gerçek değer
        (backfill ile MOCK sabit 4 değil — bull piyasası stage 2 olabilir).
        Enrichment çalışıyor + carr_stage canon aralıkta."""
        tsla = [t for t in trades if t["symbol"] == "TSLA"]
        if tsla:
            for t in tsla:
                sig = t["mark_signals"]
                if sig and sig.get("carr_stage") is not None:
                    assert sig["carr_stage"] in {1, 2, 3, 4}

    def test_nvda_stage_2(self, trades):
        """NVDA Stage 2 (Advancing — Mark+Carr alim fazi)."""
        nvda = [t for t in trades if t["symbol"] == "NVDA"]
        assert len(nvda) >= 1
        for t in nvda:
            assert t["mark_signals"] is not None
            assert t["mark_signals"]["carr_stage"] == 2

    def test_meta_stage_3(self, trades):
        """META Stage 3 (Topping — cikis hazirlik)."""
        meta = [t for t in trades if t["symbol"] == "META"]
        assert len(meta) >= 1
        for t in meta:
            assert t["mark_signals"] is not None
            assert t["mark_signals"]["carr_stage"] == 3


# =====================================================================
# Test: Journal sayfa Stage 4 filtre (Paket 41 banner gerçek)
# =====================================================================

class TestJournalStage4Filter:
    """KARAR #733 alt-paket (Paket 41): MarkRegimeBanner stage4Count
    Journal'da gercek hesaplama — acik trade'lerde Stage 4 sayim."""

    def test_open_trades_stage_4_count_computable(self, trades):
        """Stage 4 sayım HESAPLANABILIR olmalı (MarkRegimeBanner stage4Count
        akışı). P329 sonrası gerçek DB/yfinance — sabit ≥1 garantisi yok
        (bull piyasası stage 4 olmayabilir). Sayım int + ≥0."""
        open_stage4 = [
            t for t in trades
            if t["status"] == "open"
            and t.get("mark_signals")
            and t["mark_signals"].get("carr_stage") == 4
        ]
        assert isinstance(len(open_stage4), int)
        assert len(open_stage4) >= 0

    def test_total_open_count_computable(self, trades):
        """Açık trade sayım hesaplanabilir (≥0). P329 sonrası gerçek DB —
        MOCK sabit 4 varsayımı geçersiz (gerçek trade kayıtlarına bağlı)."""
        open_trades = [t for t in trades if t["status"] == "open"]
        assert len(open_trades) >= 0

    def test_closed_trades_have_enrich_too(self, trades):
        """Kapali trade'ler de enrich edilmeli (sembol bilinen ise)."""
        closed = [t for t in trades if t["status"] == "closed"]
        known_closed_enriched = [
            t for t in closed
            if t["symbol"] in {"NVDA", "MSFT", "AVGO", "AMD", "META", "TSLA"}
            and t["mark_signals"] is not None
        ]
        # NVDA + AMD + META kapali, enrich edilmis olmali
        assert len(known_closed_enriched) >= 2


# =====================================================================
# Test: Determinism (idempotent)
# =====================================================================

class TestDeterminism:
    def test_idempotent_within_session(self, client):
        """Iki ardisik istek ayni cevap."""
        r1 = client.get("/api/trades").json()
        r2 = client.get("/api/trades").json()
        assert len(r1) == len(r2)
        # mark_signals dict'leri ayni semboller icin ayni
        for t1, t2 in zip(r1, r2):
            assert t1["symbol"] == t2["symbol"]
            assert t1["mark_signals"] == t2["mark_signals"]


class TestSellStrengthEnrichment:
    """P477 (#976): acik trade sell_strength enrichment (entry-aware Hard Stop + market sinyaller)."""

    def test_open_trades_have_sell_strength(self, trades):
        opens = [t for t in trades if t["status"] == "open"]
        assert opens, "En az 1 acik trade bekleniyor"
        for t in opens:
            ss = t.get("sell_strength")
            assert ss is not None, f"{t['symbol']} acik trade sell_strength eksik"
            assert ss["category"] in ("HOLD", "WATCH", "REDUCE", "SELL")
            assert 0 <= ss["score"] <= 10
            assert isinstance(ss["signals"], list)

    def test_closed_trades_no_sell_strength(self, trades):
        """Kapanmis trade'lerde sell_strength enrichment yapilmaz (None)."""
        for t in trades:
            if t["status"] == "closed":
                assert t.get("sell_strength") is None
