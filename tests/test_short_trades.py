"""P538 — SHORT paper trading backend (invest_type, yön-farkında P&L, validation, guard).

Carr SHORT setupları (Blue Sea/Gap Down/Rising Wedge) paper-trade. invest_type 1=LONG
2=SHORT. SHORT P&L = (entry-exit)*qty (fiyat düşünce kazanç). Migration 013 kolon-guard:
kolon yoksa SHORT 400 ile bloklanır (veri bozulmaz), LONG normal akar.
"""
import sys
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
    from pydantic import ValidationError
    import main as api_main
    from main import TradeCreate
except ImportError:
    pytest.skip("fastapi yok", allow_module_level=True)


@pytest.fixture
def client():
    return TestClient(api_main.app)


# ---- _calc_pl yön-farkında ----

class TestCalcPlDirection:
    def test_long_profit_when_price_rises(self):
        pld, plp = api_main._calc_pl(100.0, 110.0, 10, invest_type=1)
        assert pld == 100.0 and plp == 10.0

    def test_short_profit_when_price_falls(self):
        # SHORT: 100'den gir, 90'a düş -> kâr (entry-exit)*qty = +100
        pld, plp = api_main._calc_pl(100.0, 90.0, 10, invest_type=2)
        assert pld == 100.0 and plp == 10.0

    def test_short_loss_when_price_rises(self):
        # SHORT: 100'den gir, 110'a çık -> zarar -100
        pld, plp = api_main._calc_pl(100.0, 110.0, 10, invest_type=2)
        assert pld == -100.0 and plp == -10.0

    def test_default_is_long(self):
        assert api_main._calc_pl(100.0, 110.0, 10) == (100.0, 10.0)


# ---- TradeCreate SHORT yön validation ----

def _tc(**over):
    base = dict(
        symbol="GME", strategy="carr", setup_type="blue_sea", signal_source="strategy",
        entry_date="2026-06-20", entry_price=100.0, shares=10,
        plan_entry_trigger="Carr Blue Sea SHORT (s.289)", plan_stop=106.0, plan_target=88.0,
        plan_size_pct=5.0, plan_exit_strategy="2R aşağı kapat", plan_time_horizon="swing",
        invest_type=2,
    )
    base.update(over)
    return base


class TestTradeCreateShortValidation:
    def test_short_valid_ordering(self):
        tc = TradeCreate(**_tc())  # stop 106 > entry 100 > target 88
        assert tc.invest_type == 2

    def test_short_stop_below_entry_rejected(self):
        with pytest.raises(ValidationError):
            TradeCreate(**_tc(plan_stop=94.0))  # stop ALTTA -> SHORT'ta yanlış

    def test_short_target_above_entry_rejected(self):
        with pytest.raises(ValidationError):
            TradeCreate(**_tc(plan_target=112.0))  # hedef ÜSTTE -> SHORT'ta yanlış

    def test_long_unaffected_by_short_rule(self):
        # LONG (invest_type=1) eski davranış: gt=0 yeter, ordering zorlanmaz (geriye uyum)
        tc = TradeCreate(**_tc(invest_type=1, plan_stop=94.0, plan_target=112.0))
        assert tc.invest_type == 1


# ---- Endpoint SHORT kolon-guard (Migration 013) ----

class TestShortColumnGuard:
    def test_short_blocked_when_column_absent(self, client, monkeypatch):
        monkeypatch.setattr(api_main, "_web_trades_has_invest_type", lambda: False)
        r = client.post("/api/trades", json=_tc(status="open"))
        assert r.status_code == 400
        assert "Migration 013" in r.json()["detail"]

    def test_short_persists_when_column_present(self, client, monkeypatch):
        monkeypatch.setattr(api_main, "_web_trades_has_invest_type", lambda: True)
        captured = {}

        def _fake_insert(trade):
            captured.update(trade)
            return 123

        def _fake_get(tid):
            return {
                "id": tid, "symbol": "GME", "strategy": "carr", "setup_type": "blue_sea",
                "invest_type": captured.get("invest_type", 1), "signal_source": "strategy",
                "entry_date": "2026-06-20", "entry_price": 100.0, "exit_date": None,
                "exit_price": None, "shares": 10, "status": "open",
                "pl_dollar": None, "pl_pct": None, "grade": None, "exit_reason": None,
                "lessons": None, "plan_entry_trigger": "x", "plan_stop": 106.0,
                "plan_target": 88.0, "plan_size_pct": 5.0, "plan_exit_strategy": "x",
                "plan_time_horizon": "swing", "stop_loss": 106.0, "target_price": 88.0,
                "audible_reason": None,
            }

        monkeypatch.setattr(api_main, "trades_insert", _fake_insert)
        monkeypatch.setattr(api_main, "trades_get_by_id", _fake_get)
        r = client.post("/api/trades", json=_tc(status="open"))
        assert r.status_code == 201, r.text
        assert captured.get("invest_type") == 2  # SHORT DB'ye yön ile gitti
        assert r.json()["invest_type"] == 2

    def test_short_closed_pl_direction(self, client, monkeypatch):
        """SHORT kapalı trade: exit < entry -> kâr (yön-farkında P&L endpoint'te)."""
        monkeypatch.setattr(api_main, "_web_trades_has_invest_type", lambda: True)
        captured = {}
        monkeypatch.setattr(api_main, "trades_insert",
                            lambda t: (captured.update(t), 1)[1])
        monkeypatch.setattr(api_main, "trades_get_by_id",
                            lambda tid: {**captured, "id": tid, "exit_date": "2026-06-25",
                                         "sector": None})
        r = client.post("/api/trades", json=_tc(
            status="closed", exit_date="2026-06-25", exit_price=90.0,
            grade="A", exit_reason="target",
        ))
        assert r.status_code == 201, r.text
        # SHORT 100->90: pl_dollar = (100-90)*10 = +100 (kâr)
        assert captured["pl_dollar"] == 100.0
        assert captured["pl_pct"] == 10.0
