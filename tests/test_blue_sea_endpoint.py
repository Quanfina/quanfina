"""
P518 (18 Haz 2026): /api/stock/{symbol}/blue-sea endpoint pytest (SHORT).

FastAPI TestClient + smoke + shape + Carr canon (SHORT, asimetri 0.8, signal-low entry).
quanfina_math.compute_blue_sea_breakdown (P517, test_blue_sea.py 8/8) backend wire.
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
    return client.get(f"/api/stock/{symbol}/blue-sea")


class TestBlueSeaEndpoint:
    FIELDS = (
        "detected", "direction", "quality", "signal_close", "entry", "stop",
        "target", "risk_pct", "rr", "low_40d", "low_260d", "high_260d",
        "obv", "macd", "mark_says", "is_mock",
    )

    def test_response_shape(self, client):
        r = _get(client, "SLB")
        assert r.status_code == 200
        data = r.json()
        for f in self.FIELDS:
            assert f in data, f"eksik alan: {f}"

    def test_direction_short_only(self, client):
        """Blue Sea SHORT-only (Blue Sky'in aynasi)."""
        data = _get(client, "NVDA").json()
        assert data["direction"] in ("SHORT", None)

    def test_canon_short_consistency(self, client):
        """detected -> SHORT: stop USTTE, target ALTTA; entry=signal low; %8 cap (Kural #26)."""
        data = _get(client, "SLB").json()
        if data["detected"]:
            assert data["direction"] == "SHORT"
            assert data["stop"] > data["entry"], "short stop entry ustunde"
            assert data["target"] < data["entry"], "short target entry altinda"
            assert data["risk_pct"] <= 8.01
            expected = data["entry"] - 2.0 * (data["stop"] - data["entry"])
            assert abs(data["target"] - expected) < 0.1
            # asimetri: 52h zirve %20 altinda (close > 0.8×high260)
            assert data["signal_close"] > 0.8 * data["high_260d"]
        else:
            assert data["entry"] is None

    def test_unknown_symbol_no_crash(self, client):
        r = _get(client, "ZZZZZZ")
        assert r.status_code == 200
