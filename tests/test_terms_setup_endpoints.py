"""
/api/terms + /api/terms/{key} + /api/setup-types (Paket 379) — smoke testleri.

Bu endpoint'ler static MOCK (sabit terminoloji sozlugu + setup tipleri; DB tablosu
yok, static-by-design). Smoke coverage: 200 + sekil + 404 (gecersiz term). Boylece
TUM endpoint'ler en az smoke kapsama girer (endpoint coverage tam).
"""
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


class TestTerms:
    def test_list_200_non_empty(self, client):
        r = client.get("/api/terms")
        assert r.status_code == 200
        data = r.json()
        assert isinstance(data, list) and len(data) > 0
        assert "key" in data[0]

    def test_valid_key_200(self, client):
        key = client.get("/api/terms").json()[0]["key"]
        r = client.get(f"/api/terms/{key}")
        assert r.status_code == 200
        assert r.json()["key"] == key

    def test_invalid_key_404(self, client):
        r = client.get("/api/terms/bu_terim_yok_xyz")
        assert r.status_code == 404


class TestSetupTypes:
    def test_list_200_non_empty(self, client):
        r = client.get("/api/setup-types")
        assert r.status_code == 200
        data = r.json()
        assert isinstance(data, list) and len(data) > 0
