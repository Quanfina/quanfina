"""
P515 (18 Haz 2026): /api/stock/{symbol}/bullish-divergence endpoint pytest.

FastAPI TestClient + smoke + shape + Carr canon (uptrend-dip, 2+ gosterge, entry=close).
quanfina_math.compute_bullish_divergence (P514, test_bullish_divergence.py 15/15) backend wire.
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
    return client.get(f"/api/stock/{symbol}/bullish-divergence")


class TestBullishDivergenceEndpoint:
    FIELDS = (
        "detected", "direction", "quality", "signal_close", "entry", "stop",
        "target", "risk_pct", "rr", "sma50", "sma200", "divergence_count",
        "divergence_indicators", "eyeball_checks", "mark_says", "is_mock",
    )

    def test_response_shape(self, client):
        r = _get(client, "NVDA")
        assert r.status_code == 200
        data = r.json()
        for f in self.FIELDS:
            assert f in data, f"eksik alan: {f}"

    def test_direction_long_only(self, client):
        data = _get(client, "AAPL").json()
        assert data["direction"] in ("LONG", None)

    def test_canon_consistency(self, client):
        """detected -> 2+ gosterge diverge + entry=close + uptrend + CANDIDATE (Kural #26)."""
        data = _get(client, "NVDA").json()
        if data["detected"]:
            assert data["quality"] == "CANDIDATE"
            assert data["divergence_count"] >= 2
            assert len(data["divergence_indicators"]) == data["divergence_count"]
            assert data["entry"] == data["signal_close"]  # entry=close
            assert data["stop"] is not None and data["stop"] < data["entry"]
            assert data["risk_pct"] <= 8.01
            assert data["sma50"] > data["sma200"]  # uptrend-dip
        else:
            assert data["entry"] is None

    def test_divergence_count_always_present(self, client):
        """divergence_count detected olmasa da 0-6 arasi raporlanir (seffaflik)."""
        data = _get(client, "AAPL").json()
        assert 0 <= data["divergence_count"] <= 6

    def test_unknown_symbol_no_crash(self, client):
        r = _get(client, "ZZZZZZ")
        assert r.status_code == 200
