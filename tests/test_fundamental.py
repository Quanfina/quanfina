"""P542 — Fundamental endpoint (#834/#855 EPS wire). Minervini CANSLIM "C/A".

Kanon: Minervini.md s.26 "%25-%30+ Q/Q" -> eşik %25 (Kural #26). SOFT (s.1152).
_parse_pct (TEXT/float -> float) + eps_pass/sales_pass (≥%25) + has_data + no-data.
Network-free: _fetch_fundamental_raw monkeypatch.
"""
import sys
from pathlib import Path

import pytest

HERE = Path(__file__).resolve().parent
ROOT = HERE.parent
API = ROOT / "api"
for d in (str(ROOT), str(API)):
    if d not in sys.path:
        sys.path.insert(0, d)

try:
    from fastapi.testclient import TestClient
    import main as api_main
except ImportError:
    pytest.skip("fastapi yok", allow_module_level=True)


@pytest.fixture
def client():
    return TestClient(api_main.app)


class TestParsePct:
    def test_plain_number(self):
        assert api_main._parse_pct("88.5") == 88.5

    def test_with_sign_and_percent(self):
        assert api_main._parse_pct("+88.5%") == 88.5

    def test_float_input(self):
        assert api_main._parse_pct(35.0) == 35.0

    def test_empty_tokens_none(self):
        for v in (None, "", "-", "N/A", "n/a", "—", "null"):
            assert api_main._parse_pct(v) is None

    def test_garbage_none(self):
        assert api_main._parse_pct("abc") is None


class TestFundamentalEndpoint:
    def test_both_strong_pass(self, client, monkeypatch):
        monkeypatch.setattr(api_main, "_fetch_fundamental_raw", lambda s: ("88.5", "122.0"))
        r = client.get("/api/stock/TESTA/fundamental")
        assert r.status_code == 200
        j = r.json()
        assert j["has_data"] is True
        assert j["eps_pass"] is True and j["sales_pass"] is True
        assert "s.26" in j["mark_says"]

    def test_weak_eps_fails_threshold(self, client, monkeypatch):
        # EPS %10 < %25 -> eps_pass False (Minervini s.26 alt sınır)
        monkeypatch.setattr(api_main, "_fetch_fundamental_raw", lambda s: ("10", "30"))
        j = client.get("/api/stock/TESTB/fundamental").json()
        assert j["eps_pass"] is False and j["sales_pass"] is True

    def test_no_data(self, client, monkeypatch):
        monkeypatch.setattr(api_main, "_fetch_fundamental_raw", lambda s: (None, "N/A"))
        j = client.get("/api/stock/TESTC/fundamental").json()
        assert j["has_data"] is False
        assert j["eps_qoq"] is None and j["sales_qoq"] is None
        assert "soft" in j["mark_says"].lower() or "teyit" in j["mark_says"].lower()

    def test_threshold_is_canon_25(self, client, monkeypatch):
        monkeypatch.setattr(api_main, "_fetch_fundamental_raw", lambda s: ("25", "25"))
        j = client.get("/api/stock/TESTD/fundamental").json()
        assert j["threshold_pct"] == 25.0
        # tam eşik %25 -> pass (>=)
        assert j["eps_pass"] is True and j["sales_pass"] is True
