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

    def test_carr_9_setups_present(self, client):
        """P528: 9 Carr setup paper trade etiketlenebilir olmali (Carr stratejisi %100)."""
        keys = {s["key"] for s in client.get("/api/setup-types").json()}
        carr = {"mean_reversion", "pullback", "blue_sky", "coiled_spring", "bullish_base",
                "bullish_divergence", "blue_sea", "gap_down", "rising_wedge"}
        assert carr <= keys, f"setup-types'ta eksik Carr setup: {carr - keys}"

    def test_frontend_setup_labels_cover_backend(self, client):
        """P530: Backend SETUP_TYPES (kanon) ⊆ frontend SETUP_LABELS (web/types/trade.ts).

        Kural #24 UI↔backend tutarlilik: backend yeni setup_type ekler ama frontend label
        sozlugu unutulursa journal SETUP sutunu ham slug gosterir. Bu test drift'i yakalar.
        """
        import re

        keys = {s["key"] for s in client.get("/api/setup-types").json()}
        trade_ts = PROJECT_ROOT / "web" / "types" / "trade.ts"
        text = trade_ts.read_text(encoding="utf-8")
        # SETUP_LABELS bloğunu izole et, sonra "slug:" anahtarlarini cek
        block = text.split("SETUP_LABELS", 1)[1].split("};", 1)[0]
        fe_keys = set(re.findall(r"^\s*([a-z_]+)\s*:", block, re.MULTILINE))
        missing = keys - fe_keys
        assert not missing, f"SETUP_LABELS'ta eksik backend setup_type: {missing}"
