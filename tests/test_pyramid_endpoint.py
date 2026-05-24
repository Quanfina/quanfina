"""
KARAR #732 — /api/pyramid/tier endpoint pytest.

FastAPI TestClient ile endpoint smoke + tier branching + Mark X kilit
coverage. quanfina_math.compute_pyramid_tier baseline (test_pyramid_tier.py
Paket 16) backend wire dogrulama.
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


def _post(client, position_value, portfolio_value=100_000, prev=False):
    return client.post("/api/pyramid/tier", json={
        "position_value": position_value,
        "portfolio_value": portfolio_value,
        "prev_tier_profitable": prev,
    })


# =====================================================================
# Test: Endpoint smoke + tier branching
# =====================================================================

class TestPyramidEndpointTiers:
    """KARAR #487 birebir: Pilot/Standart/Full + OVER_MAX branching."""

    def test_endpoint_default_response(self, client):
        """Endpoint canli + response shape dogru."""
        r = _post(client, 2000)
        assert r.status_code == 200
        data = r.json()
        for field in ("tier", "position_pct", "severity", "mark_says",
                      "pilot_range_pct", "standard_range_pct", "full_range_pct"):
            assert field in data

    def test_pilot_tier(self, client):
        # %2 -> PILOT
        r = _post(client, 2000)
        assert r.json()["tier"] == "PILOT"

    def test_standard_tier_locked(self, client):
        # %10 + prev=False -> STANDARD warn
        data = _post(client, 10_000).json()
        assert data["tier"] == "STANDARD"
        assert data["severity"] == "warn"

    def test_standard_tier_unlocked(self, client):
        # %10 + prev=True -> STANDARD ok
        data = _post(client, 10_000, prev=True).json()
        assert data["tier"] == "STANDARD"
        assert data["severity"] == "ok"

    def test_full_tier_unlocked(self, client):
        # %20 + prev=True -> FULL ok
        data = _post(client, 20_000, prev=True).json()
        assert data["tier"] == "FULL"
        assert data["severity"] == "ok"

    def test_over_max(self, client):
        # %30 -> OVER_MAX violation
        data = _post(client, 30_000).json()
        assert data["tier"] == "OVER_MAX"
        assert data["severity"] == "violation"

    def test_below_pilot(self, client):
        # %0.5 -> BELOW_PILOT
        data = _post(client, 500).json()
        assert data["tier"] == "BELOW_PILOT"
        assert data["next_tier"] == "PILOT"


# =====================================================================
# Test: Mark canon sabit referansları (KALICI İLKE #4)
# =====================================================================

class TestPyramidConstants:
    """Backend Mark KARAR #487 sabitlerini response'a koymalı."""

    def test_pilot_range(self, client):
        data = _post(client, 2000).json()
        # tuple JSON'da list olur
        assert data["pilot_range_pct"] == [1.0, 3.0]

    def test_standard_range(self, client):
        data = _post(client, 2000).json()
        assert data["standard_range_pct"] == [6.25, 12.5]

    def test_full_range(self, client):
        data = _post(client, 2000).json()
        assert data["full_range_pct"] == [15.0, 25.0]


# =====================================================================
# Test: Validation
# =====================================================================

class TestPyramidValidation:
    def test_zero_position_handled(self, client):
        """Zero pozisyon → BELOW_PILOT, exception YOK."""
        data = _post(client, 0).json()
        assert data["tier"] == "BELOW_PILOT"

    def test_zero_portfolio_handled(self, client):
        """Zero portfolio → BELOW_PILOT (helper graceful)."""
        data = _post(client, 1000, portfolio_value=0).json()
        assert data["tier"] == "BELOW_PILOT"

    def test_mark_says_present(self, client):
        """Her tier response mark_says non-empty."""
        for pos in [500, 2000, 10_000, 20_000, 30_000]:
            data = _post(client, pos).json()
            assert data["mark_says"]
            assert len(data["mark_says"]) > 10

    def test_negative_position_400_or_below(self, client):
        """Negatif pozisyon — Pydantic float kabul eder, helper BELOW_PILOT."""
        r = _post(client, -100)
        # FastAPI Pydantic float negatif kabul eder; helper graceful
        assert r.status_code == 200
        assert r.json()["tier"] == "BELOW_PILOT"
