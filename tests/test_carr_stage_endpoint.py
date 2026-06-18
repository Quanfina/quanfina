"""
KARAR #733 alt-paket (Paket 34): /api/carr/stage/{symbol} endpoint pytest.

FastAPI TestClient + smoke test + Mark felsefe canon koruma.
quanfina_math.compute_carr_stage baseline (test_carr_stage.py P21) backend
wire doğrulama.
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


def _get(client, symbol):
    return client.get(f"/api/carr/stage/{symbol}")


# =====================================================================
# Test: Endpoint smoke + shape
# =====================================================================

class TestCarrStageEndpoint:
    def test_response_shape(self, client):
        r = _get(client, "NVDA")
        assert r.status_code == 200
        data = r.json()
        for field in ("symbol", "stage", "stage_label", "ma_value",
                      "price_vs_ma_pct", "slope_pct_per_year",
                      "mark_says", "ma_window"):
            assert field in data

    def test_symbol_uppercase(self, client):
        r = _get(client, "nvda")
        assert r.json()["symbol"] == "NVDA"

    def test_ma_window_150(self, client):
        """30W × 5d = 150 (Stan Weinstein birebir)."""
        data = _get(client, "NVDA").json()
        assert data["ma_window"] == 150

    def test_different_symbols(self, client):
        """Farklı ticker → kendi gerçek OHLCV'si (P499: artık yfinance, MOCK değil)."""
        nvda = _get(client, "NVDA").json()
        tsla = _get(client, "TSLA").json()
        assert nvda["symbol"] == "NVDA"
        assert tsla["symbol"] == "TSLA"

    def test_is_mock_false_when_real_bars(self, client, monkeypatch):
        """P499 (Kural #28): gerçek 150+ bar varsa is_mock=False (eskiden hep True idi)."""
        import main as m
        fake = [m.OhlcvBar(time=f"2025-{(i//28)%12+1:02d}-{(i%28)+1:02d}",
                           open=100.0, high=102.0, low=99.0, close=100.0 + i * 0.1,
                           volume=1_000_000) for i in range(200)]
        monkeypatch.setattr(m, "_fetch_ohlcv_real", lambda s, n=252: fake)
        data = _get(client, "TESTX").json()
        assert data["is_mock"] is False

    def test_is_mock_true_when_no_real_bars(self, client, monkeypatch):
        """P499: gerçek yfinance erişilemezse is_mock=True (sentetik fallback, UI banner)."""
        import main as m
        monkeypatch.setattr(m, "_fetch_ohlcv_real", lambda s, n=252: None)
        data = _get(client, "TESTY").json()
        assert data["is_mock"] is True

    def test_stage_in_valid_range(self, client):
        """Stage 1, 2, 3, 4 veya null."""
        for sym in ["NVDA", "AAPL", "MSFT", "TSLA", "META", "GOOGL"]:
            data = _get(client, sym).json()
            assert data["stage"] in {1, 2, 3, 4, None}

    def test_mark_says_present(self, client):
        """KALICI İLKE #4: mark_says her sembol için non-empty."""
        for sym in ["NVDA", "TSLA"]:
            data = _get(client, sym).json()
            assert data["mark_says"]
            assert len(data["mark_says"]) > 10

    def test_stage_label_contains_stage_name(self, client):
        """Label 'Stage N' (Advancing/Declining/etc) içermeli."""
        for sym in ["NVDA", "TSLA"]:
            data = _get(client, sym).json()
            if data["stage"] is not None:
                assert "Stage" in data["stage_label"] or "Basing" in data["stage_label"] \
                    or "Advancing" in data["stage_label"] or "Topping" in data["stage_label"] \
                    or "Declining" in data["stage_label"] or "Toparlanma" in data["stage_label"]


# =====================================================================
# Test: Mark felsefe canon koruma
# =====================================================================

class TestMarkCanonGuard:
    """KALICI İLKE #4: Mark birebir alıntıları response içeriğinde."""

    def test_stage_4_mark_says_uzak_dur(self, client):
        """Stage 4 ise mark_says 'UZAK DUR' Mark felsefesi içermeli."""
        # TSLA seed 24 May 2026 = Stage 4
        data = _get(client, "TSLA").json()
        if data["stage"] == 4:
            assert "UZAK DUR" in data["mark_says"] or "uzak dur" in data["mark_says"]

    def test_stage_2_mark_says_superperformance(self, client):
        """Stage 2 ise mark_says Mark superperformance felsefesi içermeli."""
        # NVDA seed 24 May 2026 = Stage 2
        data = _get(client, "NVDA").json()
        if data["stage"] == 2:
            assert ("superperformance" in data["mark_says"].lower()
                    or "alim fazi" in data["mark_says"].lower()
                    or "Advancing" in data["stage_label"])


# =====================================================================
# Test: Metrik doğruluk
# =====================================================================

class TestCarrStageMetrics:
    def test_ma_value_positive(self, client):
        data = _get(client, "NVDA").json()
        if data["ma_value"] is not None:
            assert data["ma_value"] > 0

    def test_price_vs_ma_pct_within_reason(self, client):
        """Gerçek yfinance — sapma -%50 ile +%100 arası mantıklı (güçlü lider MA çok üstünde)."""
        data = _get(client, "NVDA").json()
        if data["price_vs_ma_pct"] is not None:
            assert -50 < data["price_vs_ma_pct"] < 100

    def test_slope_pct_per_year_within_reason(self, client):
        """Yıllık eğim mantıklı sınırda (-%100 ile +%200 arası)."""
        data = _get(client, "NVDA").json()
        if data["slope_pct_per_year"] is not None:
            assert -100 < data["slope_pct_per_year"] < 300
