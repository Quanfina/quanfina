"""
/api/risk/* endpoint testleri (Paket 376) — trade-giris Mark risk araclari.

advisor + eps-acceleration + code-33 + tennis-ball + leader-fingerprint endpoint
seviyesinde test edilmemisti (helper'lar quanfina_math'te test edili, ama endpoint
wire + request validation + response serialize degildi). inf/NaN serialize 500
bug sinifi (P348-353) bu endpoint'lerde de olabilir -> her birinde 200 + JSON-safe
(allow_nan=False) + edge-case (bos/ekstrem girdi -> 500 yok) dogrulanir.
"""
import json
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


def _assert_json_safe(resp):
    # FastAPI 200 dondurduyse zaten serialize oldu; ek olarak allow_nan=False ile
    # inf/NaN sizmadigini dogrula (P350 bug sinifi guvencesi).
    json.dumps(resp.json(), allow_nan=False)


class TestRiskAdvisor:
    def test_valid_200_shape(self, client):
        r = client.post("/api/risk/advisor",
                        json={"portfolio_value": 100000.0, "target_risk_pct": 2.0, "max_stop_pct": 7.0})
        assert r.status_code == 200
        d = r.json()
        for k in ("position_dollars", "risk_pct", "tier", "six_rule_all_pass",
                  "six_rules", "recommended_stop_pct", "mark_constants"):
            assert k in d
        assert isinstance(d["six_rules"], list)
        _assert_json_safe(r)

    def test_with_rba_stats_no_500(self, client):
        # RBA opsiyonel alanlar + ekstrem -> serialize 500 yok
        r = client.post("/api/risk/advisor", json={
            "portfolio_value": 50000.0, "target_risk_pct": 1.0, "max_stop_pct": 5.0,
            "avg_gain_pct": 20.0, "avg_loss_pct": -8.0, "num_trades": 40, "is_best_name": True,
        })
        assert r.status_code == 200
        _assert_json_safe(r)


class TestEpsAcceleration:
    def test_mark_example_accelerating(self, client):
        # Mark TLSMW s.131 ornek [-5,10,28,56] -> hizlanan
        r = client.post("/api/risk/eps-acceleration",
                        json={"eps_growth_yoy_last_4q": [-5.0, 10.0, 28.0, 56.0]})
        assert r.status_code == 200
        d = r.json()
        assert d["accelerating"] is True
        assert "phase" in d and "tier" in d
        _assert_json_safe(r)

    def test_empty_list_invalid_no_500(self, client):
        r = client.post("/api/risk/eps-acceleration", json={"eps_growth_yoy_last_4q": []})
        assert r.status_code == 200
        assert r.json()["phase"] == "invalid"
        _assert_json_safe(r)


class TestCode33:
    def test_valid_200(self, client):
        r = client.post("/api/risk/code-33", json={
            "eps_growth_yoy_last_4q": [10.0, 20.0, 30.0, 40.0],
            "sales_growth_yoy_last_4q": [5.0, 10.0, 15.0, 20.0],
            "net_margin_last_4q": [8.0, 9.0, 10.0, 11.0],
        })
        assert r.status_code == 200
        d = r.json()
        assert d["pattern"] in ("CODE_33", "partial", "none")
        assert 0 <= d["pass_count"] <= 3
        _assert_json_safe(r)


class TestTennisBall:
    def test_valid_200(self, client):
        bars = [{"close": 100.0 + i, "high": 101.0 + i, "low": 99.0 + i, "volume": 1000}
                for i in range(20)]
        r = client.post("/api/risk/tennis-ball",
                        json={"breakout_date_idx": 5, "daily_history": bars})
        assert r.status_code == 200
        assert "pattern" in r.json()
        _assert_json_safe(r)


class TestLeaderFingerprint:
    def test_mark_humana_example(self, client):
        # Mark TLSMW s.184: advance 15-20%, pullback 5-10%
        r = client.post("/api/risk/leader-fingerprint", json={
            "advance_segments": [18.5, 22.1, 16.8],
            "pullback_segments": [7.2, 9.5, 5.8],
        })
        assert r.status_code == 200
        d = r.json()
        assert d["pattern"] in ("LEADER_FINGERPRINT", "LEADER_PARTIAL", "NOT_LEADER", "INVALID")
        _assert_json_safe(r)

    def test_empty_invalid_no_500(self, client):
        r = client.post("/api/risk/leader-fingerprint",
                        json={"advance_segments": [], "pullback_segments": []})
        assert r.status_code == 200
        _assert_json_safe(r)
