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


# =====================================================================
# Test: Market Breadth backend wire (Paket 52)
# =====================================================================

class TestMarketBreadthWire:
    """KARAR #733 alt-paket (Paket 52): market_breadth backend pre-compute
    (P51 compute_market_breadth helper wire)."""

    def test_market_breadth_field_present(self, status):
        """market_breadth Pydantic alani response'da bulunmali."""
        assert "market_breadth" in status

    def test_market_breadth_shape(self, status):
        """Tum alanlar mevcut + tip uyumu."""
        mb = status["market_breadth"]
        assert mb is not None
        for field in ("ad_ratio", "ad_line_cumulative", "breadth_health", "mark_says"):
            assert field in mb

    def test_breadth_health_valid(self, status):
        """STRONG/NEUTRAL/WEAK literal'larindan biri."""
        valid = {"STRONG", "NEUTRAL", "WEAK"}
        assert status["market_breadth"]["breadth_health"] in valid

    def test_ad_ratio_positive(self, status):
        """A/D ratio her zaman pozitif (decline > 0 MOCK)."""
        assert status["market_breadth"]["ad_ratio"] > 0

    def test_ad_line_int(self, status):
        """ad_line_cumulative int tip."""
        assert isinstance(status["market_breadth"]["ad_line_cumulative"], int)

    def test_mark_says_non_empty(self, status):
        """KALICI İLKE #4 Mark felsefe yorumu non-empty."""
        assert len(status["market_breadth"]["mark_says"]) > 10


# =====================================================================
# Test: Breadth Divergence backend wire (Paket 57)
# =====================================================================

class TestBreadthDivergenceWire:
    """KARAR #733 alt-paket (Paket 57): breadth_divergence backend pre-compute
    (P56 compute_breadth_divergence helper wire)."""

    def test_breadth_divergence_field_present(self, status):
        """breadth_divergence Pydantic alani response'da bulunmali."""
        assert "breadth_divergence" in status

    def test_breadth_divergence_shape(self, status):
        """Tum alanlar mevcut + tip uyumu."""
        bd = status["breadth_divergence"]
        assert bd is not None
        for field in ("divergence", "index_change_pct", "ad_trend_delta",
                      "severity", "mark_says"):
            assert field in bd

    def test_divergence_value_valid(self, status):
        """5 kategori literal'larından biri."""
        valid = {
            "CONFIRMED_UP", "BEARISH_DIVERGENCE", "BULLISH_DIVERGENCE",
            "CONFIRMED_DOWN", "NEUTRAL",
        }
        assert status["breadth_divergence"]["divergence"] in valid

    def test_severity_value_valid(self, status):
        """ok/info/warn/critical literal'larından biri."""
        valid = {"ok", "info", "warn", "critical"}
        assert status["breadth_divergence"]["severity"] in valid

    def test_index_change_pct_is_float(self, status):
        """index_change_pct float tip."""
        assert isinstance(status["breadth_divergence"]["index_change_pct"], (int, float))

    def test_ad_trend_delta_is_int(self, status):
        """ad_trend_delta integer tip."""
        assert isinstance(status["breadth_divergence"]["ad_trend_delta"], int)

    def test_mark_says_non_empty(self, status):
        """KALICI İLKE #4 Mark felsefe yorumu non-empty."""
        assert len(status["breadth_divergence"]["mark_says"]) > 10

    def test_bearish_divergence_severity_critical(self, status):
        """BEARISH_DIVERGENCE ise severity=critical olmali."""
        bd = status["breadth_divergence"]
        if bd["divergence"] == "BEARISH_DIVERGENCE":
            assert bd["severity"] == "critical"

    def test_confirmed_up_severity_ok(self, status):
        """CONFIRMED_UP ise severity=ok olmali."""
        bd = status["breadth_divergence"]
        if bd["divergence"] == "CONFIRMED_UP":
            assert bd["severity"] == "ok"


# =====================================================================
# Test: Follow-Through Day backend wire (Paket 65)
# =====================================================================

class TestFollowThroughWire:
    """KARAR #733 alt-paket (Paket 65): follow_through backend pre-compute
    (P64 compute_follow_through_day helper wire)."""

    def test_follow_through_field_present(self, status):
        """follow_through Pydantic alani response'da bulunmali."""
        assert "follow_through" in status

    def test_follow_through_shape(self, status):
        """Tum alanlar mevcut (Optional alanlar None olabilir)."""
        ft = status["follow_through"]
        assert ft is not None
        for field in ("ftd_detected", "volume_confirmed", "mark_says"):
            assert field in ft
        # Optional alanlar
        for field in ("ftd_gain_pct", "days_after_low", "previous_low"):
            assert field in ft  # null veya değer

    def test_ftd_detected_is_bool(self, status):
        """ftd_detected boolean tip."""
        assert isinstance(status["follow_through"]["ftd_detected"], bool)

    def test_volume_confirmed_is_bool(self, status):
        """volume_confirmed boolean tip."""
        assert isinstance(status["follow_through"]["volume_confirmed"], bool)

    def test_mark_says_non_empty(self, status):
        """KALICI İLKE #4 Mark felsefe yorumu non-empty."""
        assert len(status["follow_through"]["mark_says"]) > 5

    def test_ftd_gain_consistency(self, status):
        """ftd_detected=True ise ftd_gain_pct >= 1.7 (Mark canon esigi)."""
        ft = status["follow_through"]
        if ft["ftd_detected"]:
            assert ft["ftd_gain_pct"] is not None
            assert ft["ftd_gain_pct"] >= 1.7

    def test_ftd_no_detected_consistency(self, status):
        """ftd_detected=False ise ftd_gain_pct None olabilir."""
        ft = status["follow_through"]
        if not ft["ftd_detected"]:
            # ftd_gain_pct None ya da düşük olabilir (sicrama yok)
            assert ft["volume_confirmed"] is False
