"""
tests/test_quanfina_math.py — quanfina_math.py için unit testler.
DB bağlantısı yok. Pure fonksiyon testleri.
"""

import math
import pytest
from quanfina_math import (
    change_dollars,
    change_percentage,
    percent_change,
    dollar_change,
    r_multiple,
    risk_dollars,
    stop_loss_percentage,
    stop_loss_break_even_shares,
    off_52w_high_pct,
    position_value,
    v50_pct,
    vrr_volume_run_rate,
    spread_dollars,
    spread_percentage,
    sma20_distance_pct,
    LONG,
    SHORT,
    StopRecommendation,
)


# ---------------------------------------------------------------------------
# F1 + F2 — Günlük Değişim
# ---------------------------------------------------------------------------

class TestChangeCalculations:
    def test_change_dollars_positive(self):
        assert change_dollars(105.0, 100.0) == pytest.approx(5.0)

    def test_change_dollars_negative(self):
        assert change_dollars(95.0, 100.0) == pytest.approx(-5.0)

    def test_change_percentage_normal(self):
        assert change_percentage(110.0, 100.0) == pytest.approx(10.0)

    def test_change_percentage_zero_close(self):
        assert change_percentage(100.0, 0.0) == 0.0

    def test_change_percentage_loss(self):
        assert change_percentage(90.0, 100.0) == pytest.approx(-10.0)


# ---------------------------------------------------------------------------
# F3 + F4 — Yüzde / Dolar K/Z (Long/Short ayrı)
# ---------------------------------------------------------------------------

class TestPercentAndDollarChange:
    # LONG
    def test_percent_change_long_profit(self):
        assert percent_change(100.0, 120.0, LONG) == pytest.approx(20.0)

    def test_percent_change_long_loss(self):
        assert percent_change(100.0, 80.0, LONG) == pytest.approx(-20.0)

    # SHORT
    def test_percent_change_short_profit(self):
        # Short'ta fiyat düşünce kâr
        assert percent_change(100.0, 80.0, SHORT) == pytest.approx(20.0)

    def test_percent_change_short_loss(self):
        # Short'ta fiyat yükselince zarar
        assert percent_change(100.0, 120.0, SHORT) == pytest.approx(-20.0)

    def test_percent_change_zero_entry(self):
        assert percent_change(0.0, 100.0, LONG) == 0.0

    # Dolar değişim
    def test_dollar_change_long(self):
        assert dollar_change(100.0, 130.0, 10, LONG) == pytest.approx(300.0)

    def test_dollar_change_short(self):
        # 100'den giriş, 70'e düştü → 10 hisse × 30$ = 300$
        assert dollar_change(100.0, 70.0, 10, SHORT) == pytest.approx(300.0)

    def test_dollar_change_short_loss(self):
        # 100'den short, 120'ye çıktı → -200$ zarar
        assert dollar_change(100.0, 120.0, 10, SHORT) == pytest.approx(-200.0)


# ---------------------------------------------------------------------------
# F5 — R-Multiple
# ---------------------------------------------------------------------------

class TestRMultiple:
    def test_r_multiple_long_normal(self):
        # entry=100, stop=90 → risk=10; current=120 → pnl=20 → R=2
        assert r_multiple(100.0, 90.0, 120.0, LONG, shares=1) == pytest.approx(2.0)

    def test_r_multiple_short_normal(self):
        # entry=100, stop=110 → risk=10; current=80 → pnl=20 → R=2
        assert r_multiple(100.0, 110.0, 80.0, SHORT, shares=1) == pytest.approx(2.0)

    def test_r_multiple_stop_violation_long(self):
        # entry <= stop → stop ihlali → R=0
        assert r_multiple(90.0, 100.0, 120.0, LONG) == 0.0

    def test_r_multiple_stop_violation_short(self):
        # entry >= stop for short is normal; entry=100 stop=100 → ihlal
        assert r_multiple(100.0, 100.0, 80.0, SHORT) == 0.0

    def test_r_multiple_negative_pnl_returns_zero(self):
        # Zarar durumunda R negatif değil, sıfır
        assert r_multiple(100.0, 90.0, 85.0, LONG, shares=1) == 0.0

    def test_r_multiple_with_shares(self):
        # entry=100, stop=90, current=115, shares=100
        # pnl = (115-100) × 100 = 1500; risk_distance = 10 (per share) → R = 150
        assert r_multiple(100.0, 90.0, 115.0, LONG, shares=100) == pytest.approx(150.0)


# ---------------------------------------------------------------------------
# F6 + F7 — Risk Dolarları / Stop Yüzdesi
# ---------------------------------------------------------------------------

class TestRiskAndStop:
    def test_risk_dollars_long(self):
        # entry=100, stop=90, 50 hisse → risk=500$
        assert risk_dollars(100.0, 90.0, 50, LONG) == pytest.approx(500.0)

    def test_risk_dollars_short(self):
        # entry=100, stop=110, 50 hisse → risk=500$
        assert risk_dollars(100.0, 110.0, 50, SHORT) == pytest.approx(500.0)

    def test_risk_dollars_negative_guard(self):
        # Hatalı girdi: stop entry'den yüksek olduğunda (Long) risk = 0
        assert risk_dollars(90.0, 100.0, 50, LONG) == 0.0

    def test_stop_loss_percentage_long(self):
        # stop, entry'den %10 aşağıda → -10%
        result = stop_loss_percentage(100.0, 90.0, LONG)
        assert result == pytest.approx(-10.0)

    def test_stop_loss_percentage_short(self):
        # Short stop: entry=100, stop=110 → short için +10% (kötü yön)
        result = stop_loss_percentage(100.0, 110.0, SHORT)
        assert result == pytest.approx(-10.0)


# ---------------------------------------------------------------------------
# F8 — Break-Even Shares
# ---------------------------------------------------------------------------

class TestBreakEvenShares:
    def test_break_even_long_profit(self):
        # entry=100, stop=90 (risk=10/hisse), current=110 (profit=10/hisse)
        # 100 hisse × 10$ risk = 1000$ / 10$ profit = 100 hisse sat → ceil(100)=100
        result = stop_loss_break_even_shares(100.0, 90.0, 110.0, 100, LONG)
        assert result == 100

    def test_break_even_long_big_profit(self):
        # entry=100, stop=90 (risk=10), current=150 (profit=50), 100 hisse
        # 1000 / 50 = 20 hisse → ceil(20)=20
        result = stop_loss_break_even_shares(100.0, 90.0, 150.0, 100, LONG)
        assert result == 20

    def test_break_even_no_profit_returns_all(self):
        # Kâr yok → tümünü döndür
        result = stop_loss_break_even_shares(100.0, 90.0, 95.0, 50, LONG)
        assert result == 50

    def test_break_even_short(self):
        # entry=100, stop=110 (risk=10/hisse), current=80 (profit=20/hisse), 100 hisse
        # 1000 / 20 = 50 hisse → ceil(50)=50
        result = stop_loss_break_even_shares(100.0, 110.0, 80.0, 100, SHORT)
        assert result == 50


# ---------------------------------------------------------------------------
# F9 + F10 — Pozisyon Metrikleri
# ---------------------------------------------------------------------------

class TestPositionMetrics:
    def test_off_52w_high_below(self):
        # Fiyat zirvenin %10 altında
        assert off_52w_high_pct(100.0, 90.0) == pytest.approx(-10.0)

    def test_off_52w_high_above(self):
        # Fiyat zirveyi %5 geçti
        assert off_52w_high_pct(100.0, 105.0) == pytest.approx(5.0)

    def test_off_52w_high_zero_guard(self):
        assert off_52w_high_pct(0.0, 100.0) == 0.0

    def test_position_value(self):
        assert position_value(150.0, 200) == pytest.approx(30_000.0)

    def test_position_value_zero(self):
        assert position_value(0.0, 100) == 0.0


# ---------------------------------------------------------------------------
# F11 + F12 — Hacim Metrikleri
# ---------------------------------------------------------------------------

class TestVolumeMetrics:
    def test_v50_above_average(self):
        # Anlık hacim ortalamanın 2 katı → %100
        assert v50_pct(2_000_000, 1_000_000) == pytest.approx(100.0)

    def test_v50_below_average(self):
        # Anlık hacim ortalamanın yarısı → -%50
        assert v50_pct(500_000, 1_000_000) == pytest.approx(-50.0)

    def test_v50_zero_avg_guard(self):
        assert v50_pct(1_000_000, 0) == 0.0

    def test_vrr_market_closed(self):
        # Market kapalıyken basit V50 oranı döner
        result = vrr_volume_run_rate(2_000_000, 1_000_000, is_market_open=False)
        assert result == pytest.approx(100.0)

    def test_vrr_market_open_returns_float(self):
        # Market açıkken float döndürdüğünü doğrula
        result = vrr_volume_run_rate(1_500_000, 1_000_000, is_market_open=True)
        assert isinstance(result, float)


# ---------------------------------------------------------------------------
# F13 + F14 + F15 — Spread ve SMA20
# ---------------------------------------------------------------------------

class TestSpreadAndSMA:
    def test_spread_dollars(self):
        assert spread_dollars(100.05, 100.00) == pytest.approx(0.05)

    def test_spread_percentage(self):
        # mid = 100.025, spread = 0.05 → ~0.05%
        result = spread_percentage(100.05, 100.00)
        assert result == pytest.approx(0.04998, rel=1e-3)

    def test_spread_percentage_zero_mid(self):
        assert spread_percentage(0.0, 0.0) == 0.0

    def test_sma20_above(self):
        # Fiyat SMA20'nin %10 üzerinde
        assert sma20_distance_pct(110.0, 100.0) == pytest.approx(10.0)

    def test_sma20_below(self):
        # Fiyat SMA20'nin %5 altında
        assert sma20_distance_pct(95.0, 100.0) == pytest.approx(-5.0)

    def test_sma20_zero_guard(self):
        assert sma20_distance_pct(100.0, 0.0) == 0.0


# ---------------------------------------------------------------------------
# Konu 16 — Stop Yönetimi
# ---------------------------------------------------------------------------

class TestStopRecommendationDataclass:

    def test_minimal_construction(self):
        rec = StopRecommendation(severity="OK", message="test")
        assert rec.severity == "OK"
        assert rec.message == "test"
        assert rec.suggested_value is None

    def test_full_construction(self):
        rec = StopRecommendation(severity="WARNING", message="test mesaji", suggested_value=99.5)
        assert rec.severity == "WARNING"
        assert rec.suggested_value == 99.5

    def test_severity_literals(self):
        for sev in ["OK", "INFO", "WARNING", "CRITICAL"]:
            rec = StopRecommendation(severity=sev, message="x")
            assert rec.severity == sev


class TestCheckInitialStop:

    def test_ideal_stop_long(self):
        from quanfina_math import check_initial_stop
        rec = check_initial_stop(entry_price=100, stop_loss=96)
        assert rec.severity == "OK"
        assert "%4.0" in rec.message

    def test_warning_stop_long(self):
        from quanfina_math import check_initial_stop
        rec = check_initial_stop(entry_price=100, stop_loss=92)
        assert rec.severity == "WARNING"
        assert rec.suggested_value == 95.0

    def test_critical_stop_long(self):
        from quanfina_math import check_initial_stop
        rec = check_initial_stop(entry_price=100, stop_loss=85)
        assert rec.severity == "CRITICAL"

    def test_short_position(self):
        from quanfina_math import check_initial_stop
        rec = check_initial_stop(entry_price=100, stop_loss=104, invest_type="SHORT")
        assert rec.severity == "OK"

    def test_invalid_input(self):
        from quanfina_math import check_initial_stop
        rec = check_initial_stop(entry_price=0, stop_loss=95)
        assert rec.severity == "CRITICAL"


class TestShouldMoveToBreakeven:

    def test_no_gain_yet(self):
        from quanfina_math import should_move_to_breakeven
        rec = should_move_to_breakeven(entry_price=100, stop_loss=95, current_price=100)
        assert rec.severity == "OK"

    def test_2x_trigger_long(self):
        from quanfina_math import should_move_to_breakeven
        # entry 100, stop 95 (%5), current 110 (+%10 = 2x)
        rec = should_move_to_breakeven(entry_price=100, stop_loss=95, current_price=110)
        assert rec.severity == "INFO"
        assert rec.suggested_value == 100.0

    def test_3x_trigger_long(self):
        from quanfina_math import should_move_to_breakeven
        # entry 100, stop 95 (%5), current 116 (+%16 > 3x)
        rec = should_move_to_breakeven(entry_price=100, stop_loss=95, current_price=116)
        assert rec.severity == "WARNING"

    def test_already_breakeven(self):
        from quanfina_math import should_move_to_breakeven
        rec = should_move_to_breakeven(entry_price=100, stop_loss=100, current_price=110)
        assert rec.severity == "OK"
        assert "breakeven" in rec.message.lower()

    def test_short_position(self):
        from quanfina_math import should_move_to_breakeven
        # SHORT: entry 100, stop 105, current 88 (-12% = +%12 kar, 2.4x stop)
        rec = should_move_to_breakeven(
            entry_price=100, stop_loss=105, current_price=88, invest_type="SHORT"
        )
        assert rec.severity == "INFO"


class TestShouldSellHalf:

    def test_below_threshold(self):
        from quanfina_math import should_sell_half
        rec = should_sell_half(pnl_pct=15.0)
        assert rec.severity == "OK"

    def test_at_static_threshold(self):
        from quanfina_math import should_sell_half
        # default avg=12 → threshold=max(20, 30)=30; pnl_pct=30 eşiği tam tetikler
        rec = should_sell_half(pnl_pct=30.0)
        assert rec.severity == "INFO"

    def test_dynamic_threshold(self):
        from quanfina_math import should_sell_half
        # avg 15 → 2.5x = 37.5; %25 < 37.5 → henüz erken
        rec = should_sell_half(pnl_pct=25.0, user_avg_gain_pct=15.0)
        assert rec.severity == "OK"

    def test_severely_late(self):
        from quanfina_math import should_sell_half
        # threshold=30, WARNING sınırı 30*1.5=45; pnl_pct=50 aşıyor
        rec = should_sell_half(pnl_pct=50.0)
        assert rec.severity == "WARNING"


class TestCheck50maTrailStop:

    def test_long_above_ma50(self):
        from quanfina_math import check_50ma_trail_stop
        rec = check_50ma_trail_stop(current_price=110, ma50=100)
        assert rec.severity == "OK"

    def test_long_below_ma50(self):
        from quanfina_math import check_50ma_trail_stop
        rec = check_50ma_trail_stop(current_price=95, ma50=100)
        assert rec.severity == "CRITICAL"

    def test_climax_run_active(self):
        from quanfina_math import check_50ma_trail_stop
        rec = check_50ma_trail_stop(current_price=120, ma50=100, is_climax_run=True)
        assert rec.severity == "WARNING"
        assert "20-MA" in rec.message

    def test_short_below_ma50(self):
        from quanfina_math import check_50ma_trail_stop
        rec = check_50ma_trail_stop(current_price=90, ma50=100, invest_type="SHORT")
        assert rec.severity == "OK"

    def test_short_above_ma50(self):
        from quanfina_math import check_50ma_trail_stop
        rec = check_50ma_trail_stop(current_price=110, ma50=100, invest_type="SHORT")
        assert rec.severity == "CRITICAL"


class TestCheckVolatilityPositionSize:

    def test_normal_volatility(self):
        from quanfina_math import check_volatility_position_size
        rec = check_volatility_position_size(atr=3, current_price=100, proposed_position_pct=15)
        assert rec.severity == "OK"

    def test_high_volatility_warning(self):
        from quanfina_math import check_volatility_position_size
        # ATR 6, fiyat 100 → %6 → uyarı (önerilen %15 > %10 sınır)
        rec = check_volatility_position_size(atr=6, current_price=100, proposed_position_pct=15)
        assert rec.severity == "WARNING"
        assert rec.suggested_value == 10.0

    def test_bucking_bronco(self):
        from quanfina_math import check_volatility_position_size
        rec = check_volatility_position_size(atr=10, current_price=100, proposed_position_pct=20)
        assert rec.severity == "CRITICAL"
        assert rec.suggested_value == 5.0


# ---------------------------------------------------------------------------
# Konu 11 — Distribution Days
# ---------------------------------------------------------------------------

class TestCountDistributionDays:

    def test_healthy_market(self):
        from quanfina_math import count_distribution_days
        history = [
            ("2026-01-01", 100, 1000),
            ("2026-01-02", 102, 1100),
            ("2026-01-03", 104, 1200),
            ("2026-01-04", 106, 1300),
        ]
        rec = count_distribution_days(history)
        assert rec.severity == "OK"
        assert rec.suggested_value == 0.0

    def test_under_pressure(self):
        from quanfina_math import count_distribution_days
        history = [
            ("2026-01-01", 100, 1000),
            ("2026-01-02", 99, 1100),
            ("2026-01-03", 98, 1200),
            ("2026-01-04", 97, 1300),
            ("2026-01-05", 96, 1400),
        ]
        rec = count_distribution_days(history)
        assert rec.severity == "WARNING"
        assert rec.suggested_value == 4.0

    def test_critical_pressure(self):
        from quanfina_math import count_distribution_days
        history = [("2026-01-01", 100, 1000)] + [
            (f"2026-01-{i+2:02d}", 100 - i, 1000 + i * 100) for i in range(1, 7)
        ]
        rec = count_distribution_days(history)
        assert rec.severity == "CRITICAL"

    def test_insufficient_data(self):
        from quanfina_math import count_distribution_days
        rec = count_distribution_days([("2026-01-01", 100, 1000)])
        assert rec.severity == "OK"
        assert rec.suggested_value == 0.0

    def test_lookback_truncation(self):
        from quanfina_math import count_distribution_days
        history = [(f"2026-01-{i+1:02d}", 100, 1000) for i in range(30)]
        rec = count_distribution_days(history, lookback_days=20)
        assert rec.severity == "OK"
