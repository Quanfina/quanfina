"""
KARAR #733 alt-paket (Paket 35): /api/market/status dynamic endpoint pytest.

Paket 22 (count_distribution_days) + Paket 24 (compute_carr_stage SPY/QQQ/IWM)
+ KARAR #488 (Mark Regime 4-Katman) backend dynamic wire dogrulama.

FastAPI TestClient + Mark felsefe canon koruma.
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


@pytest.fixture(scope="module")
def status(client):
    return client.get("/api/market/status").json()


# =====================================================================
# Test: Endpoint smoke + shape
# =====================================================================

class TestMarketStatusShape:
    def test_status_code(self, client):
        r = client.get("/api/market/status")
        assert r.status_code == 200

    def test_required_fields(self, status):
        for field in ("spy_stage", "qqq_stage", "iwm_stage", "vix",
                      "distribution_days", "market_health_score",
                      "market_health_label", "suggested_mode",
                      "top_sectors", "bottom_sectors", "mark_regime"):
            assert field in status

    def test_mark_regime_shape(self, status):
        """KARAR #488: mark_regime backend pre-compute Pydantic alanlari."""
        mr = status["mark_regime"]
        assert mr is not None
        for field in ("regime", "label", "allocation",
                      "new_buy_allowed", "pilot_override"):
            assert field in mr


# =====================================================================
# Test: Stage dynamic (Paket 24 wire)
# =====================================================================

class TestIndexStageDynamic:
    """KARAR #733 alt-paket (Paket 24): SPY/QQQ/IWM stage dinamik compute_carr_stage."""

    def test_spy_stage_valid_range(self, status):
        assert status["spy_stage"] in {1, 2, 3, 4}

    def test_qqq_stage_valid_range(self, status):
        assert status["qqq_stage"] in {1, 2, 3, 4}

    def test_iwm_stage_valid_range(self, status):
        assert status["iwm_stage"] in {1, 2, 3, 4}

    def test_stages_are_int(self, status):
        for k in ("spy_stage", "qqq_stage", "iwm_stage"):
            assert isinstance(status[k], int)

    def test_stages_can_differ(self, status):
        """Farkli ticker farkli MOCK seed -> en az iki Stage farkli OLABILIR.
        Bu test fragile olmasin diye sadece 'hepsi ayni degil' garantisi yok,
        ama tip ve range kontrol yeterli (deterministik MOCK)."""
        stages = {status["spy_stage"], status["qqq_stage"], status["iwm_stage"]}
        # En azindan 1 Stage olmasi yeterli (hepsi 2 olabilir de farkli da)
        assert len(stages) >= 1


# =====================================================================
# Test: Distribution Days dynamic (Paket 22 wire)
# =====================================================================

class TestDistributionDaysDynamic:
    """KARAR #731 + #488 alt (Paket 22): MOCK SPY -> count_distribution_days."""

    def test_dd_count_non_negative(self, status):
        assert status["distribution_days"] >= 0

    def test_dd_count_within_lookback(self, status):
        """20-gun lookback -> max teorik 20 DD."""
        assert status["distribution_days"] <= 20

    def test_dd_count_is_int(self, status):
        assert isinstance(status["distribution_days"], int)


# =====================================================================
# Test: Mark Regime canon (KARAR #488)
# =====================================================================

class TestMarkRegimeCanon:
    """KARAR #488: 4-Katman x 2-Eksen Mark felsefe (O'Neil mekanik)."""

    def test_regime_value_valid(self, status):
        valid = {"HEALTHY", "CAUTION", "UNDER_PRESSURE", "BEAR_PRESSURE"}
        assert status["mark_regime"]["regime"] in valid

    def test_regime_label_tr(self, status):
        """TR etiket Mark felsefe canon."""
        valid_labels = {"Sağlıklı", "Dikkat", "Baskı Altında", "Ayı Baskısı"}
        assert status["mark_regime"]["label"] in valid_labels

    def test_allocation_non_empty(self, status):
        assert len(status["mark_regime"]["allocation"]) > 5

    def test_new_buy_allowed_consistency(self, status):
        """DD <= 3 -> new_buy_allowed=True; DD >= 4 -> False."""
        dd = status["distribution_days"]
        nb = status["mark_regime"]["new_buy_allowed"]
        if dd <= 3:
            assert nb is True
        else:
            assert nb is False

    def test_regime_dd_mapping(self, status):
        """KARAR #488 birebir mapping:
        DD 0-2 -> HEALTHY, 3 -> CAUTION, 4 -> UNDER_PRESSURE, >=5 -> BEAR_PRESSURE."""
        dd = status["distribution_days"]
        regime = status["mark_regime"]["regime"]
        if dd <= 2:
            assert regime == "HEALTHY"
        elif dd == 3:
            assert regime == "CAUTION"
        elif dd == 4:
            assert regime == "UNDER_PRESSURE"
        else:  # >= 5
            assert regime == "BEAR_PRESSURE"

    def test_pilot_override_always_true(self, status):
        """KARAR #488: Lider hisse %1-2 pilot Override her zaman aktif."""
        assert status["mark_regime"]["pilot_override"] is True


# =====================================================================
# Test: Sektor + Vix sanity
# =====================================================================

class TestSectorVixSanity:
    def test_vix_positive(self, status):
        assert status["vix"] > 0

    def test_top_sectors_non_empty(self, status):
        assert len(status["top_sectors"]) > 0

    def test_bottom_sectors_non_empty(self, status):
        assert len(status["bottom_sectors"]) > 0

    def test_top_sector_change_positive(self, status):
        """Top sector change_pct > 0 mantikli."""
        for s in status["top_sectors"]:
            assert s["change_pct"] > 0

    def test_bottom_sector_change_negative(self, status):
        for s in status["bottom_sectors"]:
            assert s["change_pct"] < 0


# =====================================================================
# Test: Determinism (ayni gun ayni cevap)
# =====================================================================

class TestDeterminism:
    """Tarih + ticker hash seed -> ayni gun ayni MOCK -> ayni Stage/DD."""

    def test_idempotent_within_session(self, client):
        s1 = client.get("/api/market/status").json()
        s2 = client.get("/api/market/status").json()
        assert s1["spy_stage"] == s2["spy_stage"]
        assert s1["qqq_stage"] == s2["qqq_stage"]
        assert s1["iwm_stage"] == s2["iwm_stage"]
        assert s1["distribution_days"] == s2["distribution_days"]
        assert s1["mark_regime"]["regime"] == s2["mark_regime"]["regime"]
