"""
JSON-safe float cap regression (Paket 350) — sonsuz oran (inf) → 99.0 koruma.

Bug sinifi: quanfina_math fonksiyonlari "tum kazanan / sifir payda" edge'inde
float('inf') donduruyordu. FastAPI response serializasyonu inf/nan'i reddeder
("Out of range float values are not JSON compliant") → 500 hatasi.

Bu test 3 capped fonksiyonu hem deger (99.0) hem JSON-serializability acisindan
kilitler. RBA bug'i (Paket 348) production'da /api/rba/metrics 500'une sebep
oldu; volume_asymmetry + market_breadth ayni sinif (Paket 350 proaktif tarama).

Kaynak: Kural #24 (Saglam Gidelim — Asama 5 PYTEST), Kural #26 (matematik kaynakli).
"""
import json
import sys
from pathlib import Path

HERE = Path(__file__).resolve().parent
PROJECT_ROOT = HERE.parent
API_DIR = PROJECT_ROOT / "api"
for _d in (str(PROJECT_ROOT), str(API_DIR)):
    if _d not in sys.path:
        sys.path.insert(0, _d)

from quanfina_math import (
    compute_rba_metrics,
    compute_volume_asymmetry,
    compute_market_breadth,
)


def _assert_json_safe(value):
    """json.dumps inf/nan'da ValueError firlatir (allow_nan=False) — JSON-safe teyit."""
    # FastAPI default encoder allow_nan=False davranisini taklit eder
    json.dumps(value, allow_nan=False)


class TestRbaAllWinnersCapped:
    """compute_rba_metrics — tum kazanan trade -> adjusted_ratio 99.0 (inf degil)."""

    def test_all_winners_returns_99_not_inf(self):
        rba = compute_rba_metrics([{"pnl_pct": 10.0}, {"pnl_pct": 15.0}])
        assert rba.win_rate == 1.0
        assert rba.adjusted_ratio == 99.0
        assert rba.adjusted_ratio != float("inf")

    def test_adjusted_ratio_json_serializable(self):
        rba = compute_rba_metrics([{"pnl_pct": 8.0}])
        _assert_json_safe({"adjusted_ratio": rba.adjusted_ratio})


class TestVolumeAsymmetryZeroDownVolumeCapped:
    """compute_volume_asymmetry — tum down-gunler 0 hacim -> ratio 99.0 (inf degil)."""

    def test_zero_down_volume_returns_99(self):
        # day0 baseline, day1 up (hacim>0), day2 down (hacim 0) -> down_avg=0
        history = [
            {"close": 100.0, "volume": 1000},
            {"close": 101.0, "volume": 5000},
            {"close": 99.0, "volume": 0},
        ]
        result = compute_volume_asymmetry(history, lookback_days=20)
        assert result["asymmetry_ratio"] == 99.0
        assert result["asymmetry_ratio"] != float("inf")

    def test_result_json_serializable(self):
        history = [
            {"close": 100.0, "volume": 1000},
            {"close": 102.0, "volume": 8000},
            {"close": 98.0, "volume": 0},
        ]
        result = compute_volume_asymmetry(history, lookback_days=20)
        _assert_json_safe(result)


class TestMarketBreadthZeroDeclinesCapped:
    """compute_market_breadth — sifir declines (cok guclu gun) -> ad_ratio 99.0."""

    def test_zero_declines_returns_99(self):
        advances = [300, 400]
        declines = [100, 0]  # today_dec == 0
        result = compute_market_breadth(advances, declines, lookback_days=20)
        assert result["ad_ratio"] == 99.0
        assert result["ad_ratio"] != float("inf")

    def test_result_json_serializable(self):
        advances = [250, 500]
        declines = [80, 0]
        result = compute_market_breadth(advances, declines, lookback_days=20)
        _assert_json_safe(result)


class TestVolumeAsymmetryEndpointJsonSafe:
    """Endpoint-seviye kanit: gercek FastAPI encoder inf edge'inde 200 doner (500 degil).

    json.dumps simulasyonundan daha yuksek fidelity — /api/risk/volume-asymmetry'nin
    asil HTTP cevabini test eder (bug bu siniri 500'lerdi)."""

    def _client(self):
        try:
            from fastapi.testclient import TestClient
            import main as api_main
        except ImportError:
            import pytest as _pt
            _pt.skip("fastapi yok")
        return TestClient(api_main.app)

    def test_zero_down_volume_endpoint_200_not_500(self):
        client = self._client()
        # up gun hacim>0, down gun hacim 0 -> down_avg=0 -> ratio inf edge
        payload = {
            "daily_history": [
                {"close": 100.0, "volume": 1000},
                {"close": 105.0, "volume": 5000},
                {"close": 100.0, "volume": 0},
            ],
            "lookback_days": 20,
        }
        r = client.post("/api/risk/volume-asymmetry", json=payload)
        assert r.status_code == 200, f"500 landmine geri geldi: {r.text}"
        assert r.json()["asymmetry_ratio"] == 99.0
