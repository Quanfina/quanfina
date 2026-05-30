"""
TradeCreate Pydantic validation — Paket 385 bug fix regresyon testleri.

P385 keşfi: entry_price=0 -> _calc_pl ZeroDivisionError -> 500 hatasi
(add_trade endpoint sat. 3134, status=closed durumunda). Pydantic gt=0
ön kapı + _calc_pl defensive arka kapi (defense in depth) -> 500 yok.

Bu testler kullanıcının (Sn. Ferit paper trading) yanlış girdi vermesinde
422 (validation error) dönüşünü garanti eder, 500 değil.
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


# Geçerli baseline (her validation testi için kopya alıp tek alanı bozar)
VALID_TRADE = {
    "symbol": "AAPL",
    "strategy": "minervini",
    "setup_type": "vcp",
    "signal_source": "strategy",
    "entry_date": "2026-05-30",
    "entry_price": 100.0,
    "shares": 100,
    "status": "open",
    "plan_entry_trigger": "VCP pivot $102",
    "plan_stop": 95.0,
    "plan_target": 115.0,
    "plan_size_pct": 5.0,
    "plan_exit_strategy": "Target hit veya stop ihlal",
    "plan_time_horizon": "swing",
}


class TestTradeCreateEntryPriceValidation:
    """P385 ana bug: entry_price=0 -> _calc_pl ZeroDivisionError 500."""

    def test_entry_price_zero_returns_422(self, client):
        # P385 kritik bug fix: entry=0 422 olmali (eskiden 500)
        bad = {**VALID_TRADE, "entry_price": 0.0}
        r = client.post("/api/trades", json=bad)
        assert r.status_code == 422, f"entry=0 422 olmali, oldu: {r.status_code}"
        assert "entry_price" in r.text.lower() or "greater than" in r.text.lower()

    def test_entry_price_negative_returns_422(self, client):
        bad = {**VALID_TRADE, "entry_price": -10.0}
        r = client.post("/api/trades", json=bad)
        assert r.status_code == 422

    def test_entry_price_zero_with_closed_status_no_500(self, client):
        # En tehlikeli senaryo: closed + entry=0 -> _calc_pl cagrilir -> 500
        # P385 sonrasi: Pydantic 422 ile blok (helper'a hic ulasmaz)
        bad = {
            **VALID_TRADE,
            "entry_price": 0.0,
            "status": "closed",
            "exit_date": "2026-05-31",
            "exit_price": 105.0,
        }
        r = client.post("/api/trades", json=bad)
        # 422 (Pydantic) bekleniyor — 500 (Internal Server Error) ASLA
        assert r.status_code == 422, f"closed+entry=0 422 olmali, oldu: {r.status_code}: {r.text[:200]}"


class TestTradeCreateSharesValidation:
    def test_shares_zero_returns_422(self, client):
        bad = {**VALID_TRADE, "shares": 0}
        r = client.post("/api/trades", json=bad)
        assert r.status_code == 422

    def test_shares_negative_returns_422(self, client):
        bad = {**VALID_TRADE, "shares": -100}
        r = client.post("/api/trades", json=bad)
        assert r.status_code == 422


class TestTradeCreatePlanValidation:
    """Plan alanları gt=0 (long mantığı: fiyat > 0 zorunlu)."""

    def test_plan_stop_zero_returns_422(self, client):
        bad = {**VALID_TRADE, "plan_stop": 0.0}
        r = client.post("/api/trades", json=bad)
        assert r.status_code == 422

    def test_plan_target_zero_returns_422(self, client):
        bad = {**VALID_TRADE, "plan_target": 0.0}
        r = client.post("/api/trades", json=bad)
        assert r.status_code == 422

    def test_plan_size_pct_zero_returns_422(self, client):
        bad = {**VALID_TRADE, "plan_size_pct": 0.0}
        r = client.post("/api/trades", json=bad)
        assert r.status_code == 422

    def test_plan_size_pct_over_100_returns_422(self, client):
        # Portföyün %100'unden fazlasini riske etmek -> 422
        bad = {**VALID_TRADE, "plan_size_pct": 150.0}
        r = client.post("/api/trades", json=bad)
        assert r.status_code == 422


class TestTradeCreateExitPriceValidation:
    """exit_price ge=0 (delisting/total loss senaryosu icin 0 OK, negatif yasak)."""

    def test_exit_price_zero_open_ok(self):
        # P395 refactor: HTTP yerine Pydantic model direkt (Cloud SQL flaky'den
        # bagimsiz). Niyet: exit_price=0 ge=0 -> ValidationError raise OLMAMALI
        # (delisting/total loss senaryosu).
        good = {**VALID_TRADE, "exit_price": 0.0}
        # Constructor raise etmezse Pydantic gectiği teyit edilir.
        api_main.TradeCreate(**good)

    def test_exit_price_negative_returns_422(self, client):
        bad = {**VALID_TRADE, "exit_price": -10.0}
        r = client.post("/api/trades", json=bad)
        assert r.status_code == 422


class TestValidBaselineStillAcceptable:
    """Baseline VALID_TRADE Pydantic validation'i gecmeli — refactor sonrasi
    legitimate trade kayit'in kirilmadigini garanti et (regresyon koruma)."""

    def test_valid_trade_passes_pydantic(self):
        # P395 refactor: HTTP yerine Pydantic model direkt (Cloud SQL bagimsiz).
        # Niyet: VALID_TRADE baseline ValidationError raise OLMAMALI — refactor
        # sonrasi legitimate kayit kirilmamasi garantisi.
        obj = api_main.TradeCreate(**VALID_TRADE)
        # Field kanit (P385 gt=0 disiplini regresyon korumasi)
        assert obj.entry_price == 100.0
        assert obj.shares == 100
        assert obj.plan_size_pct == 5.0


# =============================================================================
# Paket 386: TradeUpdate (PATCH endpoint) validation regresyon
# =============================================================================

class TestTradeUpdateValidation:
    """PATCH /api/trades/{id} field validation (P386 TradeCreate paralel disiplin).

    Endpoint var olan trade'i guncelliyor (404 yoksa). Validation gap'leri
    direkt 422 dönmesini test eder (404'e bile gerek yok — Pydantic önce
    cagrilir, geçersiz body 422 olur).
    """

    def test_patch_exit_price_negative_returns_422(self, client):
        # Negatif exit_price -> 422 (ge=0)
        r = client.patch("/api/trades/999999", json={"exit_price": -10.0})
        assert r.status_code == 422, f"Negatif exit_price 422 olmali: {r.status_code}"

    def test_patch_exit_price_zero_acceptable(self):
        # P395 refactor: Pydantic model direkt (HTTP DB roundtrip yok, Cloud SQL
        # flaky bagimsiz). Niyet: exit_price=0 ge=0 -> ValidationError raise
        # OLMAMALI (delisting/total loss senaryosu).
        api_main.TradeUpdate(exit_price=0.0)

    def test_patch_plan_stop_zero_returns_422(self, client):
        r = client.patch("/api/trades/999999", json={"plan_stop": 0.0})
        assert r.status_code == 422

    def test_patch_plan_stop_negative_returns_422(self, client):
        r = client.patch("/api/trades/999999", json={"plan_stop": -5.0})
        assert r.status_code == 422

    def test_patch_plan_target_zero_returns_422(self, client):
        r = client.patch("/api/trades/999999", json={"plan_target": 0.0})
        assert r.status_code == 422

    def test_patch_plan_size_pct_zero_returns_422(self, client):
        r = client.patch("/api/trades/999999", json={"plan_size_pct": 0.0})
        assert r.status_code == 422

    def test_patch_plan_size_pct_over_100_returns_422(self, client):
        # Portfoy %150 riske etmek -> 422 (le=100)
        r = client.patch("/api/trades/999999", json={"plan_size_pct": 150.0})
        assert r.status_code == 422

    def test_patch_plan_size_pct_negative_returns_422(self, client):
        r = client.patch("/api/trades/999999", json={"plan_size_pct": -5.0})
        assert r.status_code == 422

    def test_patch_valid_empty_body_no_422(self):
        # P395 refactor: Pydantic direkt. Niyet: bos body -> ValidationError
        # OLMAMALI (TradeUpdate tum alanlar Optional, hicbir alan zorunlu degil).
        api_main.TradeUpdate()

    def test_patch_valid_field_no_422(self):
        # P395 refactor: Pydantic direkt. Niyet: legitimate field (grade+lessons)
        # ValidationError OLMAMALI.
        obj = api_main.TradeUpdate(grade="A", lessons="VCP perfect")
        assert obj.grade == "A"
        assert obj.lessons == "VCP perfect"
