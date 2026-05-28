"""/api/watchlist CRUD endpoint pytest (KARAR #469 + watchlist hiyerarşi).

POST (201) -> GET -> PATCH -> promote (watch->on_deck->focus->buy) -> DELETE.
409 duplicate. DB-bagimli (web_watchlist gercek write). Cleanup garantili.
DB erisilemezse skip.
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
    from db_helpers import db_health_check
except ImportError:
    pytest.skip("fastapi/db_helpers yok", allow_module_level=True)

if not db_health_check():
    pytest.skip("Cloud SQL erisilemez", allow_module_level=True)


@pytest.fixture(scope="module")
def client():
    return TestClient(api_main.app)


TEST_SYM = "ZZWLTEST"
TEST_STRAT = "minervini"

BODY = {
    "symbol": TEST_SYM,
    "strategy": TEST_STRAT,
    "status": "watch",
    "setup_type": "VCP",
    "pivot_price": 100.0,
    "note": "test kayit",
}


@pytest.fixture
def created_row(client):
    """POST ile watchlist satiri olustur, yield, sonunda DELETE (cleanup)."""
    # Onceki test kalintisini temizle
    client.delete(f"/api/watchlist/{TEST_SYM}/{TEST_STRAT}")
    r = client.post("/api/watchlist", json=BODY)
    assert r.status_code == 201, f"POST: {r.status_code} {r.text[:200]}"
    yield r.json()
    client.delete(f"/api/watchlist/{TEST_SYM}/{TEST_STRAT}")


def test_create_201(created_row):
    assert created_row["symbol"] == TEST_SYM
    assert created_row["strategy"] == TEST_STRAT
    assert created_row["status"] == "watch"


def test_appears_in_list(client, created_row):
    r = client.get("/api/watchlist")
    assert r.status_code == 200
    syms = [(x["symbol"], x["strategy"]) for x in r.json()]
    assert (TEST_SYM, TEST_STRAT) in syms


def test_duplicate_409(client, created_row):
    """Ayni symbol+strategy ikinci kez -> 409 conflict."""
    r = client.post("/api/watchlist", json=BODY)
    assert r.status_code == 409


def test_patch_update(client, created_row):
    """PATCH note + setup_type guncelle."""
    r = client.patch(
        f"/api/watchlist/{TEST_SYM}/{TEST_STRAT}",
        json={"note": "guncellendi", "setup_type": "Pullback"},
    )
    assert r.status_code == 200
    assert r.json()["note"] == "guncellendi"


def test_promote_hierarchy(client, created_row):
    """promote: watch -> on_deck (Watch->OnDeck->Focus->Buy hiyerarsi)."""
    r = client.post(f"/api/watchlist/{TEST_SYM}/{TEST_STRAT}/promote")
    assert r.status_code == 200
    # watch'tan bir ust kademe
    assert r.json()["status"] in ("on_deck", "focus", "buy")


def test_delete_204(client):
    """DELETE -> 204, sonra listede yok."""
    client.delete(f"/api/watchlist/{TEST_SYM}/{TEST_STRAT}")
    r = client.post("/api/watchlist", json=BODY)
    assert r.status_code == 201
    rd = client.delete(f"/api/watchlist/{TEST_SYM}/{TEST_STRAT}")
    assert rd.status_code == 204
    syms = [(x["symbol"], x["strategy"]) for x in client.get("/api/watchlist").json()]
    assert (TEST_SYM, TEST_STRAT) not in syms


def test_patch_nonexistent_404(client):
    """Olmayan satir PATCH -> 404."""
    r = client.patch(
        "/api/watchlist/ZZNONEXIST/minervini",
        json={"note": "x"},
    )
    assert r.status_code == 404


def test_invalid_strategy_422(client):
    """Gecersiz strategy (Literal disi) -> 422."""
    bad = {**BODY, "strategy": "invalid_strat"}
    r = client.post("/api/watchlist", json=bad)
    assert r.status_code == 422
