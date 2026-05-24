"""
KARAR ADAY #728 — Mark RBA endpoint pytest testleri.

Mark TTLC Sec 4 birebir: "Know the truth about your trading."

Bu testler /api/rba/metrics endpoint'ini ve compute_rba_metrics +
should_drop_setup zincirini cesitli pnl_pct senaryolarinda dogrular.

DB gerektirmez — quanfina_math.compute_rba_metrics direkt cagrilir.
Endpoint testi icin FastAPI TestClient kullanilir (httpx alt katmani).
"""
import pytest
import sys
import os
from pathlib import Path

# Hem kok venv hem api/.venv pytest desteklesin
HERE = Path(__file__).resolve().parent
PROJECT_ROOT = HERE.parent
API_DIR = PROJECT_ROOT / "api"
for d in (str(PROJECT_ROOT), str(API_DIR)):
    if d not in sys.path:
        sys.path.insert(0, d)

try:
    from quanfina_math import compute_rba_metrics, should_drop_setup, RBAMetrics
except ImportError:
    pytest.skip("quanfina_math import edilemedi", allow_module_level=True)


# =====================================================================
# Test: compute_rba_metrics — edge cases
# =====================================================================

class TestComputeRbaEdge:
    """Boundary cases — bos liste, hep kazanan, hep kaybeden, statistik anlamlilik."""

    def test_empty_trades(self):
        """Bos liste -> sifir state, statistically_significant=False."""
        rba = compute_rba_metrics([])
        assert rba.num_trades == 0
        assert rba.win_rate == 0.0
        assert rba.avg_gain_pct == 0.0
        assert rba.avg_loss_pct == 0.0
        assert rba.adjusted_ratio == 0.0
        assert rba.expectancy_pct == 0.0
        assert rba.is_statistically_significant is False

    def test_single_winner(self):
        """Tek kazanan trade -> win_rate=100%, adjusted_ratio=inf."""
        rba = compute_rba_metrics([{"pnl_pct": 15.0}])
        assert rba.num_trades == 1
        assert rba.win_rate == 1.0
        assert rba.avg_gain_pct == 15.0
        assert rba.avg_loss_pct == 0.0
        # No losers -> inf ratio
        assert rba.adjusted_ratio == float("inf")
        assert rba.is_statistically_significant is False  # <30 trade

    def test_single_loser(self):
        """Tek kaybeden trade -> win_rate=0%, adjusted_ratio=0."""
        rba = compute_rba_metrics([{"pnl_pct": -8.0}])
        assert rba.num_trades == 1
        assert rba.win_rate == 0.0
        assert rba.avg_loss_pct == -8.0
        # No winners -> 0 ratio
        assert rba.adjusted_ratio == 0.0

    def test_balanced_50_50(self):
        """50/50 win rate, avg_gain=10, avg_loss=-5 -> adjusted_ratio = 2.0"""
        trades = [
            {"pnl_pct": 10.0},
            {"pnl_pct": -5.0},
        ]
        rba = compute_rba_metrics(trades)
        assert rba.num_trades == 2
        assert rba.win_rate == 0.5
        assert rba.avg_gain_pct == 10.0
        assert rba.avg_loss_pct == -5.0
        # (0.5 * 10) / (0.5 * 5) = 2.0
        assert rba.adjusted_ratio == pytest.approx(2.0, rel=1e-3)
        # (0.5 * 10) - (0.5 * 5) = 2.5%
        assert rba.expectancy_pct == pytest.approx(2.5, rel=1e-3)

    def test_30_trades_statistical(self):
        """30 trade -> Mark kuralina gore istatistiksel anlamli."""
        trades = [{"pnl_pct": 5.0 if i % 2 == 0 else -3.0} for i in range(30)]
        rba = compute_rba_metrics(trades)
        assert rba.num_trades == 30
        assert rba.is_statistically_significant is True

    def test_29_trades_not_significant(self):
        """29 trade -> kil payi yetersiz."""
        trades = [{"pnl_pct": 5.0} for _ in range(29)]
        rba = compute_rba_metrics(trades)
        assert rba.is_statistically_significant is False

    def test_largest_gain_loss(self):
        """Largest gain/loss dogru tespit."""
        trades = [
            {"pnl_pct": 25.0},
            {"pnl_pct": -12.0},
            {"pnl_pct": 8.0},
            {"pnl_pct": -3.0},
        ]
        rba = compute_rba_metrics(trades)
        assert rba.largest_gain_pct == 25.0
        assert rba.largest_loss_pct == -12.0


# =====================================================================
# Test: should_drop_setup — Mark severity hiyerarşisi
# =====================================================================

class TestShouldDropSetup:
    """Mark Sec 4: Adjusted Ratio < 1.0 -> CRITICAL setup birak."""

    def _make_rba(self, num_trades, win_rate, avg_gain, avg_loss, adjusted_ratio, exp_pct, sig=True):
        return RBAMetrics(
            num_trades=num_trades,
            win_rate=win_rate,
            avg_gain_pct=avg_gain,
            avg_loss_pct=avg_loss,
            largest_gain_pct=avg_gain * 2,
            largest_loss_pct=avg_loss * 2,
            adjusted_ratio=adjusted_ratio,
            expectancy_pct=exp_pct,
            is_statistically_significant=sig,
        )

    def test_insufficient_data_info(self):
        """<30 trade -> INFO (Mark kurali gerekli)."""
        rba = self._make_rba(15, 0.5, 10.0, -5.0, 2.0, 2.5, sig=False)
        rec = should_drop_setup(rba)
        assert rec.severity == "INFO"
        assert "30 trade" in rec.message.lower() or "yeterli" in rec.message.lower()

    def test_critical_negative_edge(self):
        """Adjusted ratio < 1.0 -> CRITICAL (negatif edge, BIRAK)."""
        rba = self._make_rba(40, 0.4, 5.0, -10.0, 0.33, -4.0, sig=True)
        rec = should_drop_setup(rba)
        assert rec.severity == "CRITICAL"

    def test_warn_avg_loss_exceeds_gain(self):
        """abs(avg_loss) > avg_gain (ama ratio >= 1.0) -> WARNING."""
        # Mark kurali: zayifliyor. Kontrol: 1.0 <= ratio ama loss > gain
        rba = self._make_rba(50, 0.6, 5.0, -8.0, 1.05, 0.0, sig=True)
        rec = should_drop_setup(rba)
        assert rec.severity == "WARNING"

    def test_ok_healthy_setup(self):
        """Win rate 50%+ + ratio >= 1.5 + avg_gain > |avg_loss| -> OK."""
        rba = self._make_rba(45, 0.55, 12.0, -6.0, 2.44, 3.9, sig=True)
        rec = should_drop_setup(rba)
        assert rec.severity == "OK"


# =====================================================================
# Test: /api/rba/metrics endpoint (FastAPI TestClient)
# =====================================================================

@pytest.fixture(scope="module")
def client():
    """FastAPI TestClient — DB gerektirmez (RBA endpoint exception-safe)."""
    try:
        from fastapi.testclient import TestClient
        import main as api_main
        return TestClient(api_main.app)
    except ImportError:
        pytest.skip("fastapi TestClient veya api/main.py yok", allow_module_level=False)


class TestRbaEndpoint:
    """Endpoint smoke test — DB unreachable durumunda boş metrik döner."""

    def test_endpoint_default_response_shape(self, client):
        """GET /api/rba/metrics -> 200 + RbaResponse shape."""
        r = client.get("/api/rba/metrics")
        assert r.status_code == 200
        data = r.json()
        assert "metrics" in data
        assert "recommendation" in data
        m = data["metrics"]
        for field in ("num_trades", "win_rate", "avg_gain_pct", "avg_loss_pct",
                      "adjusted_ratio", "expectancy_pct", "is_statistically_significant"):
            assert field in m

    def test_endpoint_filter_strategy(self, client):
        """Strategy filter param geçirilebilir."""
        r = client.get("/api/rba/metrics?strategy=minervini")
        assert r.status_code == 200
        data = r.json()
        assert data.get("filter_strategy") == "minervini"

    def test_endpoint_filter_setup_type(self, client):
        """Setup type filter param."""
        r = client.get("/api/rba/metrics?setup_type=vcp")
        assert r.status_code == 200
        data = r.json()
        assert data.get("filter_setup_type") == "vcp"

    def test_endpoint_combined_filter(self, client):
        """Strategy + setup_type birlikte."""
        r = client.get("/api/rba/metrics?strategy=carr&setup_type=pullback")
        assert r.status_code == 200
        data = r.json()
        assert data.get("filter_strategy") == "carr"
        assert data.get("filter_setup_type") == "pullback"

    def test_endpoint_severity_in_valid_enum(self, client):
        """Recommendation.severity tanimli 4 degerden biri."""
        r = client.get("/api/rba/metrics")
        assert r.status_code == 200
        sev = r.json()["recommendation"]["severity"]
        assert sev in {"OK", "INFO", "WARNING", "CRITICAL"}
