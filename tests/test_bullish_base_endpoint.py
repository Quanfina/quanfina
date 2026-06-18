"""
P513 (18 Haz 2026): /api/stock/{symbol}/bullish-base endpoint pytest.

FastAPI TestClient + smoke + shape + Carr canon (CONTRARIAN, entry=close).
quanfina_math.compute_bullish_base_breakout (P512, test_bullish_base.py 10/10) backend wire.
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
    return client.get(f"/api/stock/{symbol}/bullish-base")


class TestBullishBaseEndpoint:
    FIELDS = (
        "detected", "direction", "quality", "signal_close", "entry", "stop",
        "target", "risk_pct", "rr", "sma20", "sma50", "sma200", "obv", "macd",
        "eyeball_checks", "mark_says", "is_mock",
    )

    def test_response_shape(self, client):
        r = _get(client, "AAPL")
        assert r.status_code == 200
        data = r.json()
        for f in self.FIELDS:
            assert f in data, f"eksik alan: {f}"

    def test_direction_long_only(self, client):
        data = _get(client, "PFE").json()
        assert data["direction"] in ("LONG", None)

    def test_canon_entry_is_close(self, client):
        """detected -> CONTRARIAN: entry=signal_close (kirilim beklenmez, s.284) + CANDIDATE."""
        data = _get(client, "AAPL").json()
        if data["detected"]:
            assert data["quality"] == "CANDIDATE"
            assert data["entry"] == data["signal_close"]  # entry=close, signal high DEGIL
            assert data["stop"] is not None and data["stop"] < data["entry"]
            assert data["risk_pct"] <= 8.01
            assert len(data["eyeball_checks"]) >= 3
            # contrarian: downtrend baz (SMA50<SMA200)
            assert data["sma50"] < data["sma200"]
        else:
            assert data["entry"] is None

    def test_unknown_symbol_no_crash(self, client):
        r = _get(client, "ZZZZZZ")
        assert r.status_code == 200
