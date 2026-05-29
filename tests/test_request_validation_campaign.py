"""
P387: Request body Pydantic validation kampanyasi regresyon testleri.

P385/P386 (TradeCreate/TradeUpdate) sonrasi sistemik bug-class kapatma —
diger API request body model'lerinde de aynı gt=0/ge=0/le=N disiplini.

Kapsam (4 endpoint):
- POST /api/watchlist (WatchlistRowCreate) — pivot_price gt=0, symbol min_length
- POST /api/pyramid/tier (PyramidTierRequest) — position+portfolio gt=0
- POST /api/risk/advisor (RiskAdvisorRequest) — portfolio gt=0, target/max_stop gt=0+le=100
- POST /api/risk/volume-asymmetry (VolumeAsymmetryRequest) — lookback_days gt=0
- POST /api/risk/tennis-ball (TennisBallRequest) — breakout_date_idx ge=0
"""
import sys
from pathlib import Path

import pytest

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
    pytest.skip("fastapi yok", allow_module_level=True)


@pytest.fixture(scope="module")
def client():
    return TestClient(api_main.app)


# =============================================================================
# WatchlistRowCreate — pivot_price gt=0, symbol min_length=1
# =============================================================================

VALID_WATCHLIST = {
    "symbol": "AAPL",
    "strategy": "minervini",
    "status": "watch",
    "setup_type": "vcp",
    "pivot_price": 200.0,
    "note": "VCP pivot @ $200",
}


class TestWatchlistValidation:
    def test_pivot_price_zero_returns_422(self, client):
        bad = {**VALID_WATCHLIST, "pivot_price": 0.0}
        r = client.post("/api/watchlist", json=bad)
        assert r.status_code == 422

    def test_pivot_price_negative_returns_422(self, client):
        bad = {**VALID_WATCHLIST, "pivot_price": -10.0}
        r = client.post("/api/watchlist", json=bad)
        assert r.status_code == 422

    def test_symbol_empty_returns_422(self, client):
        bad = {**VALID_WATCHLIST, "symbol": ""}
        r = client.post("/api/watchlist", json=bad)
        assert r.status_code == 422

    def test_symbol_too_long_returns_422(self, client):
        bad = {**VALID_WATCHLIST, "symbol": "TOOLONGSYMBOL123"}
        r = client.post("/api/watchlist", json=bad)
        assert r.status_code == 422

    def test_pivot_price_none_acceptable(self, client):
        # pivot_price None (Optional) -> 422 olmamali (None default)
        body = {k: v for k, v in VALID_WATCHLIST.items() if k != "pivot_price"}
        r = client.post("/api/watchlist", json=body)
        assert r.status_code != 422


# =============================================================================
# PyramidTierRequest — pos + portfolio gt=0
# =============================================================================

class TestPyramidValidation:
    def test_zero_position_value_returns_422(self, client):
        bad = {"position_value": 0.0, "portfolio_value": 100000.0}
        r = client.post("/api/pyramid/tier", json=bad)
        assert r.status_code == 422

    def test_negative_position_value_returns_422(self, client):
        bad = {"position_value": -100.0, "portfolio_value": 100000.0}
        r = client.post("/api/pyramid/tier", json=bad)
        assert r.status_code == 422

    def test_zero_portfolio_value_returns_422(self, client):
        # En kritik: portfolio=0 -> ZeroDivisionError (position_pct hesabi) onlenir
        bad = {"position_value": 1000.0, "portfolio_value": 0.0}
        r = client.post("/api/pyramid/tier", json=bad)
        assert r.status_code == 422


# =============================================================================
# RiskAdvisorRequest — portfolio gt=0, target/max_stop le=100, total_positions ge=0
# =============================================================================

class TestRiskAdvisorValidation:
    def test_zero_portfolio_returns_422(self, client):
        bad = {"portfolio_value": 0.0}
        r = client.post("/api/risk/advisor", json=bad)
        assert r.status_code == 422

    def test_negative_target_risk_pct_returns_422(self, client):
        bad = {"portfolio_value": 100000.0, "target_risk_pct": -1.0}
        r = client.post("/api/risk/advisor", json=bad)
        assert r.status_code == 422

    def test_target_risk_pct_over_100_returns_422(self, client):
        bad = {"portfolio_value": 100000.0, "target_risk_pct": 150.0}
        r = client.post("/api/risk/advisor", json=bad)
        assert r.status_code == 422

    def test_negative_total_positions_returns_422(self, client):
        bad = {"portfolio_value": 100000.0, "total_positions": -5}
        r = client.post("/api/risk/advisor", json=bad)
        assert r.status_code == 422

    def test_valid_minimal_input_no_422(self, client):
        # Sadece zorunlu alan -> 422 OLMAMALI (defaults gecer)
        r = client.post("/api/risk/advisor", json={"portfolio_value": 100000.0})
        assert r.status_code != 422


# =============================================================================
# VolumeAsymmetryRequest — lookback_days gt=0
# =============================================================================

class TestVolumeAsymmetryValidation:
    def test_zero_lookback_returns_422(self, client):
        bad = {"daily_history": [], "lookback_days": 0}
        r = client.post("/api/risk/volume-asymmetry", json=bad)
        assert r.status_code == 422

    def test_negative_lookback_returns_422(self, client):
        bad = {"daily_history": [], "lookback_days": -5}
        r = client.post("/api/risk/volume-asymmetry", json=bad)
        assert r.status_code == 422


# =============================================================================
# TennisBallRequest — breakout_date_idx ge=0
# =============================================================================

class TestTennisBallValidation:
    def test_negative_breakout_idx_returns_422(self, client):
        bad = {"breakout_date_idx": -1, "daily_history": []}
        r = client.post("/api/risk/tennis-ball", json=bad)
        assert r.status_code == 422

    def test_zero_breakout_idx_acceptable(self, client):
        # idx=0 ge=0 -> 422 olmamali (legit edge case: 0 sirali ilk gun)
        ok = {"breakout_date_idx": 0, "daily_history": []}
        r = client.post("/api/risk/tennis-ball", json=ok)
        assert r.status_code != 422
