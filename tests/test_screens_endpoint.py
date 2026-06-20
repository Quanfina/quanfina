"""
/api/screens + /api/screens/{slug} (Paket 377) — Tarama sayfasi endpoint testleri.

Cekirdek Tarama sayfasi (minervini_scans DB-backed, MOCK fallback). Endpoint
seviyesinde test edilmemisti. list_screens (mevcut ekranlar) + slug dispatch
(gecerli->200 sonuc, gecersiz->404) + JSON-safe. Slug canli listeden alinir
(hardcode yok -> ekran seti degisirse test kirilmaz).
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


class TestListScreens:
    def test_200_non_empty(self, client):
        r = client.get("/api/screens")
        assert r.status_code == 200
        data = r.json()
        assert isinstance(data, list) and len(data) > 0

    def test_screen_meta_shape(self, client):
        item = client.get("/api/screens").json()[0]
        for k in ("slug", "label", "filter_summary", "category"):
            assert k in item
        assert isinstance(item["slug"], str) and item["slug"]


class TestScreenResults:
    def _first_slug(self, client):
        return client.get("/api/screens").json()[0]["slug"]

    def test_valid_slug_200_list(self, client):
        slug = self._first_slug(client)
        r = client.get(f"/api/screens/{slug}")
        assert r.status_code == 200, f"slug {slug} -> {r.status_code}: {r.text[:200]}"
        assert isinstance(r.json(), list)
        json.dumps(r.json(), allow_nan=False)  # serialize-safe (inf/NaN yok)

    def test_result_row_shape_when_present(self, client):
        slug = self._first_slug(client)
        rows = client.get(f"/api/screens/{slug}").json()
        if rows:  # DB bayat/bos olabilir; varsa sekil dogrula
            assert "symbol" in rows[0]
            assert isinstance(rows[0]["symbol"], str)

    def test_limit_param_respected(self, client):
        slug = self._first_slug(client)
        rows = client.get(f"/api/screens/{slug}?limit=3").json()
        assert len(rows) <= 3

    def test_invalid_slug_404(self, client):
        r = client.get("/api/screens/bu_slug_yok_12345")
        assert r.status_code == 404
        assert "bulunamadi" in r.json()["detail"].lower()

    def test_pocket_pivot_slug_valid_200(self, client):
        """P534: Pocket Pivot bulk screen slug gecerli (404 DEGIL) + JSON-safe liste."""
        r = client.get("/api/screens/pocket_pivot?limit=5")
        assert r.status_code == 200, f"pocket_pivot -> {r.status_code}: {r.text[:200]}"
        data = r.json()
        assert isinstance(data, list)
        json.dumps(data, allow_nan=False)  # NaN/inf yok

    @pytest.mark.parametrize("slug", ["cup_handle", "flat_base", "double_bottom", "high_tight_flag"])
    def test_base_detector_slugs_valid_200(self, client, slug):
        """P551: O'Neil base detector bulk screen slug'lari gecerli (404 DEGIL) + JSON-safe."""
        r = client.get(f"/api/screens/{slug}?limit=5")
        assert r.status_code == 200, f"{slug} -> {r.status_code}: {r.text[:200]}"
        data = r.json()
        assert isinstance(data, list)
        json.dumps(data, allow_nan=False)


class TestScreenResilience:
    """Paket 381 — Transient DB hatasi (OperationalError) graceful MOCK fallback.

    Bug: db_health_check 30s TTL cache yaniltici 'True' donderirse gercek sorgu
    (screen_get_results_dispatch) patlardi -> ham 500. Cloud SQL flaky/pool/timeout
    paper trading'i kirardi. Fix: try/except -> MOCK don (docstring kontrati).
    """

    def test_db_query_raises_returns_mock_not_500(self, client, monkeypatch):
        # Saglik check 'True' (cache yaniltici) AMA gercek sorgu OperationalError atar
        monkeypatch.setattr(api_main, "db_health_check", lambda: True)

        def _boom(*a, **k):
            raise RuntimeError("simulated Cloud SQL OperationalError")

        monkeypatch.setattr(api_main, "screen_get_results_dispatch", _boom)

        slug = client.get("/api/screens").json()[0]["slug"]
        r = client.get(f"/api/screens/{slug}")
        assert r.status_code == 200, f"DB patlayinca 500 degil MOCK olmali: {r.status_code}"
        data = r.json()
        assert isinstance(data, list) and len(data) > 0  # MOCK satirlari geldi
        json.dumps(data, allow_nan=False)

    def test_invalid_slug_still_404_when_db_healthy(self, client, monkeypatch):
        # 404 yolu DB sorgusundan ONCE (valid_slugs kontrol) — fallback bunu yutmamali
        monkeypatch.setattr(api_main, "db_health_check", lambda: True)
        r = client.get("/api/screens/bu_slug_yok_12345")
        assert r.status_code == 404
