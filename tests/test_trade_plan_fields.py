"""
KARAR ADAY #717 - Mark TTLC Sec 1 Trade Plan 6 zorunlu alan testleri.

Mark birebir: "Without a written plan, you have only hope" / "Always go in with a plan"

Bu testler api/main.py TradeCreate/TradeUpdate Pydantic modellerinin
6 yeni plan_* alanini dogru validasyondan gecirdigini kontrol eder.

DB gerektirmez - sadece Pydantic schema testleri.
"""
import pytest
import sys
import os
from pathlib import Path

# api/ klasorunu sys.path'e ekle (db_helpers + main.py icin)
API_DIR = Path(__file__).resolve().parent.parent / "api"
if str(API_DIR) not in sys.path:
    sys.path.insert(0, str(API_DIR))

# main.py basliyor ama uvicorn yok - sadece Pydantic modellerini import ediyoruz
# import_module ile sadece Trade/TradeCreate/TradeUpdate alalim
try:
    from pydantic import ValidationError
    from main import Trade, TradeCreate, TradeUpdate, TimeHorizon
except ImportError:
    pytest.skip("pydantic/api bagimliligi yok (root venv) — api/.venv ile kos", allow_module_level=True)


# =====================================================================
# Test: TradeCreate - 6 plan alani ZORUNLU
# =====================================================================

class TestTradeCreatePlanRequired:
    """TradeCreate'te plan_* alanlari zorunlu olmali (Mark felsefesi)."""

    BASE_TRADE = {
        "symbol": "NVDA",
        "strategy": "minervini",
        "setup_type": "vcp",
        "signal_source": "strategy",
        "entry_date": "2026-05-24",
        "entry_price": 150.00,
        "shares": 100,
    }

    PLAN_FIELDS = {
        "plan_entry_trigger": "VCP 3 daralma + hacim teyitli pivot kirilimi",
        "plan_stop": 142.50,
        "plan_target": 165.00,
        "plan_size_pct": 6.25,
        "plan_exit_strategy": "2R'de yari sat, kalani 50MA trail",
        "plan_time_horizon": "swing",
    }

    def test_full_plan_passes(self):
        """6 plan alani tam dolu -> gecmeli."""
        data = {**self.BASE_TRADE, **self.PLAN_FIELDS}
        trade = TradeCreate(**data)
        assert trade.plan_entry_trigger == "VCP 3 daralma + hacim teyitli pivot kirilimi"
        assert trade.plan_stop == 142.50
        assert trade.plan_target == 165.00
        assert trade.plan_size_pct == 6.25
        assert trade.plan_exit_strategy == "2R'de yari sat, kalani 50MA trail"
        assert trade.plan_time_horizon == "swing"

    def test_missing_plan_entry_trigger_fails(self):
        data = {**self.BASE_TRADE, **self.PLAN_FIELDS}
        del data["plan_entry_trigger"]
        with pytest.raises(ValidationError) as exc_info:
            TradeCreate(**data)
        assert "plan_entry_trigger" in str(exc_info.value)

    def test_missing_plan_stop_fails(self):
        data = {**self.BASE_TRADE, **self.PLAN_FIELDS}
        del data["plan_stop"]
        with pytest.raises(ValidationError) as exc_info:
            TradeCreate(**data)
        assert "plan_stop" in str(exc_info.value)

    def test_missing_plan_target_fails(self):
        data = {**self.BASE_TRADE, **self.PLAN_FIELDS}
        del data["plan_target"]
        with pytest.raises(ValidationError) as exc_info:
            TradeCreate(**data)
        assert "plan_target" in str(exc_info.value)

    def test_missing_plan_size_pct_fails(self):
        data = {**self.BASE_TRADE, **self.PLAN_FIELDS}
        del data["plan_size_pct"]
        with pytest.raises(ValidationError) as exc_info:
            TradeCreate(**data)
        assert "plan_size_pct" in str(exc_info.value)

    def test_missing_plan_exit_strategy_fails(self):
        data = {**self.BASE_TRADE, **self.PLAN_FIELDS}
        del data["plan_exit_strategy"]
        with pytest.raises(ValidationError) as exc_info:
            TradeCreate(**data)
        assert "plan_exit_strategy" in str(exc_info.value)

    def test_missing_plan_time_horizon_fails(self):
        data = {**self.BASE_TRADE, **self.PLAN_FIELDS}
        del data["plan_time_horizon"]
        with pytest.raises(ValidationError) as exc_info:
            TradeCreate(**data)
        assert "plan_time_horizon" in str(exc_info.value)


# =====================================================================
# Test: TimeHorizon enum literal kontrol
# =====================================================================

class TestTimeHorizonEnum:
    """Mark'in 3 trade tipi: swing/position/core (TLSMW + MM cross-reference)."""

    BASE_TRADE = {
        "symbol": "NVDA",
        "strategy": "minervini",
        "setup_type": "vcp",
        "signal_source": "strategy",
        "entry_date": "2026-05-24",
        "entry_price": 150.00,
        "shares": 100,
        "plan_entry_trigger": "test",
        "plan_stop": 142.50,
        "plan_target": 165.00,
        "plan_size_pct": 6.25,
        "plan_exit_strategy": "test",
    }

    @pytest.mark.parametrize("horizon", ["swing", "position", "core"])
    def test_valid_horizons(self, horizon):
        data = {**self.BASE_TRADE, "plan_time_horizon": horizon}
        trade = TradeCreate(**data)
        assert trade.plan_time_horizon == horizon

    def test_invalid_horizon_fails(self):
        data = {**self.BASE_TRADE, "plan_time_horizon": "scalp"}  # YASAK - Mark dilinde yok
        with pytest.raises(ValidationError) as exc_info:
            TradeCreate(**data)
        assert "plan_time_horizon" in str(exc_info.value)

    def test_uppercase_horizon_fails(self):
        # Literal case-sensitive
        data = {**self.BASE_TRADE, "plan_time_horizon": "SWING"}
        with pytest.raises(ValidationError):
            TradeCreate(**data)


# =====================================================================
# Test: TradeUpdate - plan alanlari opsiyonel (sonradan duzeltme)
# =====================================================================

class TestTradeUpdatePlanOptional:
    """Mevcut trade plan revizesi - tum plan alanlari Optional."""

    def test_empty_update_passes(self):
        TradeUpdate()  # Hicbir alan zorunlu degil

    def test_only_plan_stop_update(self):
        upd = TradeUpdate(plan_stop=140.00)
        assert upd.plan_stop == 140.00
        assert upd.plan_target is None

    def test_partial_plan_update(self):
        upd = TradeUpdate(
            plan_entry_trigger="Revize: hacim teyit eksikti",
            plan_stop=138.00,
            lessons="Plan disinda hareket ettim, ders aldim",
        )
        assert upd.plan_entry_trigger == "Revize: hacim teyit eksikti"
        assert upd.plan_stop == 138.00
        assert upd.lessons == "Plan disinda hareket ettim, ders aldim"


# =====================================================================
# Test: Trade response model - eski trade'ler icin plan_* None geri uyum
# =====================================================================

class TestTradeResponsePlanOptional:
    """DB'den donen Trade icin plan_* None olabilir (geriye uyum)."""

    def test_old_trade_no_plan_fields(self):
        """Migration 008 oncesi trade'ler - plan_* tum None"""
        trade = Trade(
            id=1,
            symbol="NVDA",
            strategy="minervini",
            setup_type="vcp",
            entry_date="2026-04-22",
            entry_price=132.50,
            shares=100,
            status="open",
        )
        assert trade.plan_entry_trigger is None
        assert trade.plan_stop is None
        assert trade.plan_target is None
        assert trade.plan_size_pct is None
        assert trade.plan_exit_strategy is None
        assert trade.plan_time_horizon is None

    def test_new_trade_with_plan(self):
        trade = Trade(
            id=2,
            symbol="MSFT",
            strategy="minervini",
            setup_type="power_play",
            entry_date="2026-05-24",
            entry_price=412.00,
            shares=50,
            status="open",
            plan_entry_trigger="Power Play HTF",
            plan_stop=395.00,
            plan_target=460.00,
            plan_size_pct=12.5,
            plan_exit_strategy="2R partial, 50MA trail",
            plan_time_horizon="position",
        )
        assert trade.plan_time_horizon == "position"
        assert trade.plan_size_pct == 12.5
