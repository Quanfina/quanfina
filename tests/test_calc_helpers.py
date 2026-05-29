"""
_calc_pl + _calc_rr — Paper trading matematiğinin kalbi (P385 test kapsamı).

Bu iki helper trade kayıt + signal R/R + dashboard P&L için her satırda
çağrılır. Yanlış hesap = Sn. Ferit'in yanlış trade kararı. Test edilmemişti
(kritik path coverage gap — P385 keşfi). Decimal precision + None handling +
edge case (ZeroDivisionError potansiyeli) + Mark "Risk first" disiplini.
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
    import main as api_main
except ImportError:
    pytest.skip("fastapi yok", allow_module_level=True)


_calc_pl = api_main._calc_pl
_calc_rr = api_main._calc_rr


# =============================================================================
# _calc_pl — Trade P&L hesabı (Decimal precision, paper P&L UI'a yansır)
# =============================================================================

class TestCalcPlLongProfit:
    """Long pozisyon karli kapanis — entry < exit."""

    def test_round_profit(self):
        # 100 hisse @ $100 -> $110 = +$1000, +%10
        assert _calc_pl(100.0, 110.0, 100) == (1000.00, 10.00)

    def test_small_profit(self):
        # 10 hisse @ $50 -> $52.50 = +$25, +%5
        assert _calc_pl(50.0, 52.5, 10) == (25.00, 5.00)

    def test_fractional_pct(self):
        # 100 hisse @ $100 -> $103.33 = +$333, +%3.33
        pl_dollar, pl_pct = _calc_pl(100.0, 103.33, 100)
        assert pl_dollar == 333.00
        assert pl_pct == 3.33


class TestCalcPlLongLoss:
    """Long pozisyon zararli kapanis — entry > exit."""

    def test_round_loss(self):
        # 100 hisse @ $100 -> $90 = -$1000, -%10
        assert _calc_pl(100.0, 90.0, 100) == (-1000.00, -10.00)

    def test_stop_loss_typical(self):
        # Mark 7% stop senaryosu: 100 hisse @ $100 -> $93 = -$700, -%7
        pl_dollar, pl_pct = _calc_pl(100.0, 93.0, 100)
        assert pl_dollar == -700.00
        assert pl_pct == -7.00


class TestCalcPlPrecision:
    """Decimal precision tuzaklari — float arithmetic yanlislari yakalanmasın."""

    def test_float_subtraction_precision(self):
        # 0.1 + 0.2 = 0.30000000000000004 float tuzagi -> Decimal cozer
        pl_dollar, pl_pct = _calc_pl(0.1, 0.3, 1000)
        assert pl_dollar == 200.00  # (0.3 - 0.1) * 1000 = 200
        assert pl_pct == 200.00      # (0.3 - 0.1) / 0.1 * 100 = 200%

    def test_high_price_precision(self):
        # AVGO benzeri yuksek fiyat
        pl_dollar, pl_pct = _calc_pl(1450.30, 1525.00, 10)
        assert pl_dollar == 747.00
        assert round(pl_pct, 2) == 5.15

    def test_rounding_half_up(self):
        # Decimal ROUND_HALF_UP davranisi - 0.005 -> 0.01 (banker's rounding degil)
        pl_dollar, pl_pct = _calc_pl(100.0, 100.005, 10)
        assert pl_dollar == 0.05  # (0.005 * 10) = 0.05
        # 0.005 / 100 * 100 = 0.005 → ROUND_HALF_UP -> 0.01
        assert pl_pct == 0.01


class TestCalcPlEdgeCases:
    """Edge case'ler — paper trading sirasinda gercek input."""

    def test_zero_exit_price(self):
        # Total loss (delisting senaryosu): exit=0
        pl_dollar, pl_pct = _calc_pl(100.0, 0.0, 100)
        assert pl_dollar == -10000.00
        assert pl_pct == -100.00

    def test_zero_entry_graceful(self):
        # P385: entry=0 -> (0.0, 0.0) defensive (ZeroDivisionError yerine).
        # Pydantic gt=0 ön kapı zaten engeller (422), ama legacy DB satirinda
        # entry=0 olabilir -> helper safe (defense in depth).
        assert _calc_pl(0.0, 10.0, 100) == (0.0, 0.0)

    def test_zero_shares(self):
        # shares=0 -> 0 P&L (anlamsiz trade ama crash etmemeli)
        pl_dollar, pl_pct = _calc_pl(100.0, 110.0, 0)
        assert pl_dollar == 0.00
        assert pl_pct == 10.00  # pct entry/exit oranina bagli, shares-bagimsiz


# =============================================================================
# _calc_rr — Risk/Reward oranı (signals endpoint, AddTradeDialog plan)
# =============================================================================

class TestCalcRrLong:
    """Long R/R: target > price > stop."""

    def test_2_to_1_ratio(self):
        # price=100, stop=95 (risk=5), target=110 (reward=10) -> R/R = 2.0
        assert _calc_rr(100.0, 95.0, 110.0) == 2.0

    def test_3_to_1_ratio(self):
        # price=100, stop=90 (risk=10), target=130 (reward=30) -> R/R = 3.0
        assert _calc_rr(100.0, 90.0, 130.0) == 3.0

    def test_below_1_ratio(self):
        # Kötü R/R: price=100, stop=90 (risk=10), target=105 (reward=5) -> 0.5
        assert _calc_rr(100.0, 90.0, 105.0) == 0.5

    def test_rounding_2_decimal(self):
        # 1/3 = 0.333... -> 0.33
        assert _calc_rr(100.0, 90.0, 103.33) == 0.33


class TestCalcRrNoneInputs:
    """None input -> None (signal/trade plan eksikse R/R hesaplanamaz)."""

    def test_stop_none(self):
        assert _calc_rr(100.0, None, 110.0) is None

    def test_target_none(self):
        assert _calc_rr(100.0, 95.0, None) is None

    def test_both_none(self):
        assert _calc_rr(100.0, None, None) is None


class TestCalcRrInvalidGeometry:
    """Long olmayan geometriler -> None (long-only kontrat)."""

    def test_stop_above_price(self):
        # stop=110 > price=100 -> risk<=0 -> None (long mantiksiz)
        assert _calc_rr(100.0, 110.0, 130.0) is None

    def test_target_below_price(self):
        # target=90 < price=100 -> reward<=0 -> None (long target asagidan)
        assert _calc_rr(100.0, 95.0, 90.0) is None

    def test_stop_equal_price(self):
        # stop=price -> risk=0 -> None (sifirla bolme onlenir)
        assert _calc_rr(100.0, 100.0, 110.0) is None

    def test_target_equal_price(self):
        # target=price -> reward=0 -> None
        assert _calc_rr(100.0, 95.0, 100.0) is None


class TestCalcRrBoundaryAndMarkCanon:
    """Mark canon Risk first: 2:1 min beklenir (TLSMW Bol. 8)."""

    def test_mark_min_2_to_1(self):
        # Mark TLSMW Bol. 8: min 2:1 R/R kabul edilebilir
        rr = _calc_rr(100.0, 95.0, 110.0)
        assert rr is not None and rr >= 2.0  # Mark canon kabul siniri

    def test_high_rr_signal(self):
        # Yuksek conviction (5:1+): UI'da "öncelikli sinyal"
        rr = _calc_rr(100.0, 98.0, 110.0)
        assert rr is not None and rr >= 5.0

    def test_realistic_paper_trade_avgo(self):
        # AVGO MOCK senaryo: price=1450.30, stop=1420 (risk=30.30),
        # target=1525 (reward=74.70) -> R/R ≈ 2.47
        rr = _calc_rr(1450.30, 1420.0, 1525.0)
        assert rr is not None and 2.4 < rr < 2.5
