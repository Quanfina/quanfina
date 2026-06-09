"""
P426 (31 May 2026) — /api/market/extra-indicators endpoint breadth kaynak seçim
fallback testi. yfinance/DB'ye GİTMEDEN (monkeypatch) — saf seçim mantığı:

  scan >=39 gün -> "scans"
  scan <39 + backfill >=39 -> "backfill"
  scan <39 + backfill fail + scan var -> "scans" (Zweig 10 gün yeter)
  hiçbiri -> "none"

Kural #28: yetersiz -> data_sufficient False (MOCK YOK). Bu test bugün eklenen
P426 fallback zincirinin test borcunu kapatır (yfinance live cağrı yerine mock).
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
    pytest.skip("fastapi yok", allow_module_level=True)


@pytest.fixture
def client():
    return TestClient(api_main.app)


# Sabit SPY closes — Faber'i deterministik kıl (yfinance'e gitme)
_FAKE_SPY = [680.0 + i * 0.3 for i in range(252)]  # yükselen -> INVESTED


def _patch_spy(monkeypatch):
    monkeypatch.setattr(api_main, "_index_closes_volumes",
                        lambda *a, **k: (_FAKE_SPY, [1_000_000] * 252))


def _series(n: int, adv: int, dec: int):
    """n günlük sabit (adv, dec) breadth listesi (tuple list)."""
    return [(adv, dec) for _ in range(n)]


def test_source_scans_when_sufficient(client, monkeypatch):
    # Scan geçmişi >=39 gün -> "scans" kaynağı, backfill ÇAĞRILMAZ
    _patch_spy(monkeypatch)
    monkeypatch.setattr(api_main, "breadth_history_from_scans",
                        lambda days=60: _series(45, 400, 300))
    # backfill çağrılırsa testi patlat (çağrılmamalı)
    monkeypatch.setattr(api_main, "_breadth_backfill",
                        lambda *a, **k: (_ for _ in ()).throw(AssertionError("backfill cagrilmamali")))
    r = client.get("/api/market/extra-indicators")
    assert r.status_code == 200
    d = r.json()
    assert d["breadth_source"] == "scans"
    assert d["mcclellan"]["data_sufficient"] is True
    assert d["faber"]["signal"] == "INVESTED"


def test_source_backfill_when_scans_short(client, monkeypatch):
    # Scan <39 (11 gün) + backfill >=39 -> "backfill"
    _patch_spy(monkeypatch)
    monkeypatch.setattr(api_main, "breadth_history_from_scans",
                        lambda days=60: _series(11, 400, 300))
    bf_adv = [400] * 50
    bf_dec = [300] * 50
    monkeypatch.setattr(api_main, "_breadth_backfill", lambda *a, **k: (bf_adv, bf_dec))
    r = client.get("/api/market/extra-indicators")
    d = r.json()
    assert d["breadth_source"] == "backfill"
    assert d["mcclellan"]["data_sufficient"] is True


def test_source_scans_fallback_when_backfill_fails(client, monkeypatch):
    # Scan <39 + backfill fail ([],[]) -> "scans" (McClellan yetersiz ama Zweig OK)
    _patch_spy(monkeypatch)
    monkeypatch.setattr(api_main, "breadth_history_from_scans",
                        lambda days=60: _series(11, 400, 300))
    monkeypatch.setattr(api_main, "_breadth_backfill", lambda *a, **k: ([], []))
    r = client.get("/api/market/extra-indicators")
    d = r.json()
    assert d["breadth_source"] == "scans"
    # McClellan 11<39 -> yetersiz (MOCK YOK)
    assert d["mcclellan"]["data_sufficient"] is False
    assert d["mcclellan"]["value"] is None
    # Zweig 11>=10 -> yeterli
    assert d["zweig"]["data_sufficient"] is True


def test_source_none_when_no_breadth(client, monkeypatch):
    # Hiç breadth yok -> "none", McClellan + Zweig ikisi de yetersiz
    _patch_spy(monkeypatch)
    monkeypatch.setattr(api_main, "breadth_history_from_scans", lambda days=60: [])
    monkeypatch.setattr(api_main, "_breadth_backfill", lambda *a, **k: ([], []))
    r = client.get("/api/market/extra-indicators")
    d = r.json()
    assert d["breadth_source"] == "none"
    assert d["mcclellan"]["data_sufficient"] is False
    assert d["zweig"]["data_sufficient"] is False


def test_response_shape(client, monkeypatch):
    # 3 gösterge + breadth_source alanları her zaman var
    _patch_spy(monkeypatch)
    monkeypatch.setattr(api_main, "breadth_history_from_scans", lambda days=60: _series(45, 400, 300))
    r = client.get("/api/market/extra-indicators")
    d = r.json()
    assert set(d.keys()) >= {"faber", "mcclellan", "zweig", "breadth_source"}
    for k in ("faber", "mcclellan", "zweig"):
        assert "data_sufficient" in d[k]
