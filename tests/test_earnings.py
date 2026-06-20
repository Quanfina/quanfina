"""P543 — Earnings tarih farkındalığı endpoint (#843). Minervini s.249 "5g blackout".

Kanon (Kural #26): Minervini.md s.249 "5 gün veya daha az kala YENİ breakout YOK".
within_blackout (0<=gün<=5) + days_to_earnings + has_data. Network-free: _fetch_
earnings_date monkeypatch (relative tarihler — date.today() bağımsız deterministik).
"""
import sys
from datetime import date, timedelta
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


def _iso_in(days: int) -> str:
    return (date.today() + timedelta(days=days)).isoformat()


def test_within_blackout_3_days(client, monkeypatch):
    monkeypatch.setattr(api_main, "_fetch_earnings_date", lambda s: _iso_in(3))
    j = client.get("/api/stock/AAA/earnings").json()
    assert j["has_data"] is True
    assert j["days_to_earnings"] == 3
    assert j["within_blackout"] is True
    assert "s.249" in j["mark_says"]


def test_exactly_5_days_within_blackout(client, monkeypatch):
    # Sınır: tam 5 gün -> blackout İÇİ (Minervini "5 gün veya daha az")
    monkeypatch.setattr(api_main, "_fetch_earnings_date", lambda s: _iso_in(5))
    j = client.get("/api/stock/BBB/earnings").json()
    assert j["days_to_earnings"] == 5
    assert j["within_blackout"] is True


def test_outside_blackout_10_days(client, monkeypatch):
    monkeypatch.setattr(api_main, "_fetch_earnings_date", lambda s: _iso_in(10))
    j = client.get("/api/stock/CCC/earnings").json()
    assert j["within_blackout"] is False
    assert "serbest" in j["mark_says"].lower()


def test_past_earnings_negative_days(client, monkeypatch):
    monkeypatch.setattr(api_main, "_fetch_earnings_date", lambda s: _iso_in(-4))
    j = client.get("/api/stock/DDD/earnings").json()
    assert j["days_to_earnings"] == -4
    assert j["within_blackout"] is False


def test_no_data(client, monkeypatch):
    monkeypatch.setattr(api_main, "_fetch_earnings_date", lambda s: None)
    j = client.get("/api/stock/EEE/earnings").json()
    assert j["has_data"] is False
    assert j["earnings_date"] is None
    assert "manuel teyit" in j["mark_says"].lower()


def test_blackout_days_canon_5(client, monkeypatch):
    monkeypatch.setattr(api_main, "_fetch_earnings_date", lambda s: _iso_in(2))
    j = client.get("/api/stock/FFF/earnings").json()
    assert j["blackout_days"] == 5  # Minervini.md s.249
