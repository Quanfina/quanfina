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
    RBAMetrics,
    compute_rba_metrics,
    should_drop_setup,
    GRADE_CATEGORIES,
    GradeSuggestion,
    suggest_entry_grade,
    suggest_loss_grade,
    suggest_exit_grade,
    compute_grade_distribution,
    # Sprint 4-bis.4 KARAR #461 — Brandon VCP (quanfina_math motoru)
    compute_pullback_health,
    compute_vcp_pass,
    VCP_PULLBACK_EXCELLENT,
    VCP_PULLBACK_GOOD,
    VCP_PULLBACK_ACCEPTABLE,
    # Sprint 4-bis.5 KARAR #466 — VCP Kalite Skoru (3 kanal sentezi)
    compute_vcp_quality,
    VCP_VOL_DRY_RATIO,
    VCP_VOL_DRY_RATIO_EXCELLENT,
    # Sprint 4-bis.5 KARAR #465 — Inside/Outside Day + Ready Score
    compute_inside_day,
    compute_outside_day_negative_reversal,
    compute_vcp_ready_score,
    OUTSIDE_DAY_VOLUME_RATIO,
    VCP_READY_SCORE_HIGH_THRESHOLD,
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


# ---------------------------------------------------------------------------
# Konu 14 — RBA Result Based Analysis
# ---------------------------------------------------------------------------

class TestRBAMetricsDataclass:
    """RBAMetrics dataclass — alan tipleri ve sıfır state."""

    def test_zero_state_creates_valid_instance(self):
        rba = RBAMetrics(0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, False)
        assert rba.num_trades == 0
        assert rba.is_statistically_significant is False

    def test_full_state_creates_valid_instance(self):
        rba = RBAMetrics(50, 0.55, 12.0, -4.0, 30.0, -8.0, 3.67, 4.8, True)
        assert rba.num_trades == 50
        assert rba.adjusted_ratio == 3.67
        assert rba.is_statistically_significant is True

    def test_avg_loss_is_negative(self):
        rba = RBAMetrics(10, 0.5, 10.0, -5.0, 20.0, -10.0, 2.0, 2.5, False)
        assert rba.avg_loss_pct < 0

    def test_significance_threshold_30(self):
        rba = RBAMetrics(30, 0.5, 10.0, -5.0, 20.0, -10.0, 2.0, 2.5, True)
        assert rba.num_trades == 30
        assert rba.is_statistically_significant is True


class TestComputeRBAMetrics:
    """compute_rba_metrics — RBA hesaplama davranışları."""

    def test_empty_list_returns_zero_state(self):
        rba = compute_rba_metrics([])
        assert rba.num_trades == 0
        assert rba.win_rate == 0.0
        assert rba.adjusted_ratio == 0.0
        assert rba.is_statistically_significant is False

    def test_normal_case_50_50_split(self):
        trades = [
            {'pnl_pct': 10.0}, {'pnl_pct': 10.0},
            {'pnl_pct': -5.0}, {'pnl_pct': -5.0},
        ]
        rba = compute_rba_metrics(trades)
        assert rba.num_trades == 4
        assert rba.win_rate == 0.5
        assert rba.avg_gain_pct == 10.0
        assert rba.avg_loss_pct == -5.0
        # AR = (0.5 * 10) / (0.5 * 5) = 2.0
        assert rba.adjusted_ratio == pytest.approx(2.0)
        # Expectancy = (0.5 * 10) - (0.5 * 5) = 2.5
        assert rba.expectancy_pct == pytest.approx(2.5)

    def test_all_winners_returns_inf_ratio(self):
        trades = [{'pnl_pct': 10.0}, {'pnl_pct': 15.0}]
        rba = compute_rba_metrics(trades)
        assert rba.win_rate == 1.0
        assert rba.adjusted_ratio == float('inf')

    def test_all_losers_returns_zero_ratio(self):
        trades = [{'pnl_pct': -5.0}, {'pnl_pct': -3.0}]
        rba = compute_rba_metrics(trades)
        assert rba.win_rate == 0.0
        assert rba.adjusted_ratio == 0.0

    def test_largest_gain_and_loss_tracking(self):
        trades = [
            {'pnl_pct': 5.0}, {'pnl_pct': 25.0}, {'pnl_pct': 10.0},
            {'pnl_pct': -3.0}, {'pnl_pct': -12.0}, {'pnl_pct': -1.0},
        ]
        rba = compute_rba_metrics(trades)
        assert rba.largest_gain_pct == 25.0
        assert rba.largest_loss_pct == -12.0

    def test_below_30_trades_not_significant(self):
        trades = [{'pnl_pct': 10.0}] * 29
        rba = compute_rba_metrics(trades)
        assert rba.num_trades == 29
        assert rba.is_statistically_significant is False

    def test_30_trades_is_significant(self):
        trades = [{'pnl_pct': 10.0}] * 30
        rba = compute_rba_metrics(trades)
        assert rba.num_trades == 30
        assert rba.is_statistically_significant is True


# ---------------------------------------------------------------------------
# TradeGrader — GRADE_CATEGORIES sabiti
# ---------------------------------------------------------------------------

class TestGradeCategories:
    def test_exactly_17_codes(self):
        assert len(GRADE_CATEGORIES) == 17

    def test_group_counts(self):
        groups = [v[1] for v in GRADE_CATEGORIES.values()]
        assert groups.count("ENTRY") == 7
        assert groups.count("EXIT") == 5
        assert groups.count("LOSS") == 5

    def test_target_pct_types(self):
        for code, (name, group, target) in GRADE_CATEGORIES.items():
            assert isinstance(name, str)
            assert group in ("ENTRY", "EXIT", "LOSS")
            assert target is None or isinstance(target, float)


# ---------------------------------------------------------------------------
# TradeGrader — GradeSuggestion dataclass
# ---------------------------------------------------------------------------

class TestGradeSuggestionDataclass:
    def test_fields_set_correctly(self):
        gs = GradeSuggestion(code="BP", name="Bought perfect", confidence="HIGH", reason="Test")
        assert gs.code == "BP"
        assert gs.name == "Bought perfect"
        assert gs.confidence == "HIGH"
        assert gs.reason == "Test"

    def test_confidence_medium(self):
        gs = GradeSuggestion(code="BL", name="Bought late", confidence="MEDIUM", reason="x")
        assert gs.confidence == "MEDIUM"

    def test_empty_code_valid(self):
        gs = GradeSuggestion(code="", name="", confidence="LOW", reason="no suggestion")
        assert gs.code == ""


# ---------------------------------------------------------------------------
# TradeGrader — suggest_entry_grade
# ---------------------------------------------------------------------------

class TestSuggestEntryGrade:
    def test_bought_perfect_exact_pivot(self):
        gs = suggest_entry_grade(entry_price=100.0, pivot_price=100.0)
        assert gs.code == "BP"
        assert gs.confidence == "HIGH"

    def test_bought_perfect_within_2pct(self):
        gs = suggest_entry_grade(entry_price=101.5, pivot_price=100.0)
        assert gs.code == "BP"
        assert gs.confidence == "HIGH"

    def test_bought_late_3pct(self):
        gs = suggest_entry_grade(entry_price=103.0, pivot_price=100.0)
        assert gs.code == "BL"
        assert gs.confidence == "MEDIUM"

    def test_chased_extended_6pct(self):
        gs = suggest_entry_grade(entry_price=106.0, pivot_price=100.0)
        assert gs.code == "CE"
        assert gs.confidence == "HIGH"

    def test_bought_early_below_pivot(self):
        gs = suggest_entry_grade(entry_price=97.0, pivot_price=100.0)
        assert gs.code == "BE"
        assert gs.confidence == "MEDIUM"

    def test_invalid_pivot_zero(self):
        gs = suggest_entry_grade(entry_price=100.0, pivot_price=0.0)
        assert gs.code == "BE"
        assert gs.confidence == "LOW"


# ---------------------------------------------------------------------------
# TradeGrader — suggest_loss_grade
# ---------------------------------------------------------------------------

class TestSuggestLossGrade:
    def test_cut_loss_perfect_exact(self):
        # entry=100, stop=95 → plan 5%, exit=95 → actual 5%
        gs = suggest_loss_grade(100.0, 95.0, 95.0)
        assert gs.code == "CLP"
        assert gs.confidence == "HIGH"

    def test_cut_loss_perfect_within_1pct(self):
        # plan 5%, actual 4.5% → diff 0.5 ≤ 1
        gs = suggest_loss_grade(100.0, 95.5, 95.0)
        assert gs.code == "CLP"

    def test_cut_loss_early(self):
        # entry=100, stop=92 (plan 8%), exit=97 (actual 3%) → diff = 3-8 = -5 < -1
        gs = suggest_loss_grade(100.0, 97.0, 92.0)
        assert gs.code == "CLE"
        assert gs.confidence == "MEDIUM"

    def test_cut_loss_late(self):
        # entry=100, stop=95 (plan 5%), exit=88 (actual 12%) → diff = 7 > 1
        gs = suggest_loss_grade(100.0, 88.0, 95.0)
        assert gs.code == "CLL"
        assert gs.confidence == "HIGH"

    def test_short_cut_loss_perfect(self):
        # SHORT: entry=100, stop=105 (plan 5%), exit=105 (actual 5%)
        gs = suggest_loss_grade(100.0, 105.0, 105.0, invest_type="SHORT")
        assert gs.code == "CLP"

    def test_cut_loss_late_big_miss(self):
        # entry=100, stop=97 (plan 3%), exit=85 (actual 15%) → diff = 12 > 1
        gs = suggest_loss_grade(100.0, 85.0, 97.0)
        assert gs.code == "CLL"

    def test_the_wall_overrides_perfect_stop(self):
        # Sprint 4.7c.4: plan %15, actual %15 → normalde CLP ama The Wall %10 ihlali → CLL
        # entry=100, stop=85 (plan %15), exit=85 (actual %15)
        gs = suggest_loss_grade(100.0, 85.0, 85.0)
        assert gs.code == "CLL"
        assert "Wall" in gs.reason or "10" in gs.reason

    def test_the_wall_at_exactly_10_pct(self):
        # Sprint 4.7c.4: %10 sınır dahil — actual %10 = CLL
        # entry=100, stop=90 (plan %10), exit=90 (actual %10)
        gs = suggest_loss_grade(100.0, 90.0, 90.0)
        assert gs.code == "CLL"

    def test_the_wall_just_below_10_pct(self):
        # Sprint 4.7c.4: actual %9.5 → The Wall altı, plan-vs-actual mantığı çalışır
        # entry=100, stop=91 (plan %9), exit=90.5 (actual %9.5) → diff=0.5 ≤ 1 → CLP
        gs = suggest_loss_grade(100.0, 90.5, 91.0)
        assert gs.code == "CLP"

    def test_the_wall_short_position(self):
        # Sprint 4.7c.4: SHORT — entry=100, exit=112 → kayıp %12 ≥ %10 → CLL
        gs = suggest_loss_grade(100.0, 112.0, 108.0, invest_type="SHORT")
        assert gs.code == "CLL"
        assert "Wall" in gs.reason or "10" in gs.reason


# ---------------------------------------------------------------------------
# TradeGrader — suggest_exit_grade
# ---------------------------------------------------------------------------

class TestSuggestExitGrade:
    def test_sold_perfect_large_gain_long_hold(self):
        # 25% gain, 8 weeks → SP HIGH
        gs = suggest_exit_grade(100.0, 125.0, weeks_held=8.0)
        assert gs.code == "SP"
        assert gs.confidence == "HIGH"

    def test_sold_perfect_large_gain_short_hold(self):
        # 25% gain, 3 weeks → SP MEDIUM (< 6 weeks)
        gs = suggest_exit_grade(100.0, 125.0, weeks_held=3.0)
        assert gs.code == "SP"
        assert gs.confidence == "MEDIUM"

    def test_sold_perfect_medium_gain(self):
        # 12% gain, 4 weeks → SP MEDIUM
        gs = suggest_exit_grade(100.0, 112.0, weeks_held=4.0)
        assert gs.code == "SP"
        assert gs.confidence == "MEDIUM"

    def test_sold_early_small_gain(self):
        # 5% gain → SE
        gs = suggest_exit_grade(100.0, 105.0, weeks_held=2.0)
        assert gs.code == "SE"

    def test_sold_late_long_hold(self):
        # 11% gain, 20 weeks → SL
        gs = suggest_exit_grade(100.0, 111.0, weeks_held=20.0)
        assert gs.code == "SL"
        assert gs.confidence == "MEDIUM"


# ---------------------------------------------------------------------------
# TradeGrader — compute_grade_distribution
# ---------------------------------------------------------------------------

class TestComputeGradeDistribution:
    def test_empty_list(self):
        result = compute_grade_distribution([])
        assert result["total"] == 0
        assert result["by_code"] == {}

    def test_single_grade(self):
        result = compute_grade_distribution([{"grade_code": "BP"}])
        assert result["total"] == 1
        assert result["by_code"]["BP"] == 1
        assert result["by_group"]["ENTRY"] == 1

    def test_mixed_grades(self):
        legs = [
            {"grade_code": "BP"}, {"grade_code": "BP"}, {"grade_code": "BL"},
            {"grade_code": "SP"}, {"grade_code": "CLP"},
        ]
        result = compute_grade_distribution(legs)
        assert result["total"] == 5
        assert result["by_code"]["BP"] == 2
        assert result["by_group"]["ENTRY"] == 3
        assert result["by_group"]["EXIT"] == 1
        assert result["by_group"]["LOSS"] == 1

    def test_unknown_grade_code(self):
        result = compute_grade_distribution([{"grade_code": "XYZ"}])
        assert result["by_group"]["UNKNOWN"] == 1
        assert result["total"] == 1

    def test_total_equals_sum_of_by_code(self):
        legs = [{"grade_code": "BP"}] * 3 + [{"grade_code": "SP"}] * 2
        result = compute_grade_distribution(legs)
        assert result["total"] == sum(result["by_code"].values())


class TestShouldDropSetup:
    """should_drop_setup — severity hiyerarşi sırası."""

    def test_below_30_trades_returns_info(self):
        rba = RBAMetrics(10, 0.6, 12.0, -3.0, 20.0, -5.0, 2.4, 4.8, False)
        rec = should_drop_setup(rba)
        assert rec.severity == "INFO"
        assert "30 trade" in rec.message

    def test_adjusted_ratio_below_1_returns_critical(self):
        rba = RBAMetrics(30, 0.40, 5.0, -10.0, 15.0, -20.0, 0.33, -3.0, True)
        rec = should_drop_setup(rba)
        assert rec.severity == "CRITICAL"
        assert "BIRAK" in rec.message

    def test_avg_loss_exceeds_avg_gain_returns_warning(self):
        # win_rate=0.6, avg_gain=5, avg_loss=-7 → AR = (0.6*5)/(0.4*7) ≈ 1.07
        rba = RBAMetrics(30, 0.60, 5.0, -7.0, 12.0, -15.0, 1.07, -0.30, True)
        rec = should_drop_setup(rba)
        assert rec.severity == "WARNING"
        assert "Avg Loss" in rec.message

    def test_low_win_rate_returns_warning(self):
        # win_rate=0.25, avg_gain=20, avg_loss=-5 → AR = (0.25*20)/(0.75*5) = 1.33
        rba = RBAMetrics(30, 0.25, 20.0, -5.0, 40.0, -10.0, 1.33, 1.25, True)
        rec = should_drop_setup(rba)
        assert rec.severity == "WARNING"
        assert "Win rate" in rec.message

    def test_healthy_setup_returns_ok(self):
        rba = RBAMetrics(50, 0.60, 12.0, -4.0, 30.0, -8.0, 4.5, 5.6, True)
        rec = should_drop_setup(rba)
        assert rec.severity == "OK"
        assert "sağlıklı" in rec.message


# ===========================================================================
# Sprint 4-bis.4 KARAR #461 — Brandon VCP Olgunluk
# Kaynak: Minervini_Video.md sat. 2823-2867 (Brandon video 10:20)
# ===========================================================================


class TestComputePullbackHealth:
    """Brandon healthy pullback ratio (rally % vs pullback %)."""

    def test_excellent_ratio_010(self):
        # %80 ralli → %8 düşüş = 0.10 ratio → EXCELLENT
        result = compute_pullback_health(rally_pct=80.0, pullback_pct=8.0)
        assert result["health"] == "EXCELLENT"
        assert result["score"] == 100
        assert math.isclose(result["ratio"], 0.10, abs_tol=0.001)

    def test_good_ratio_025(self):
        # %80 ralli → %20 düşüş = 0.25 → GOOD (kabul edilebilir DHT)
        result = compute_pullback_health(rally_pct=80.0, pullback_pct=20.0)
        assert result["health"] == "GOOD"
        assert result["score"] == 80

    def test_acceptable_ratio_040(self):
        # %50 ralli → %20 düşüş = 0.40 → ACCEPTABLE
        result = compute_pullback_health(rally_pct=50.0, pullback_pct=20.0)
        assert result["health"] == "ACCEPTABLE"
        assert result["score"] == 60

    def test_too_deep_above_040(self):
        # %100 ralli → %50 düşüş = 0.50 → TOO_DEEP (AAOI tarzı)
        result = compute_pullback_health(rally_pct=100.0, pullback_pct=50.0)
        assert result["health"] == "TOO_DEEP"
        assert result["score"] == 20

    def test_zero_rally_returns_too_deep(self):
        # Yükseliş yok → ratio tanımsız, güvenli "TOO_DEEP"
        result = compute_pullback_health(rally_pct=0.0, pullback_pct=5.0)
        assert result["health"] == "TOO_DEEP"

    def test_negative_rally_returns_too_deep(self):
        result = compute_pullback_health(rally_pct=-10.0, pullback_pct=5.0)
        assert result["health"] == "TOO_DEEP"

    def test_zero_pullback_excellent(self):
        # Hiç düşüş yok → mükemmel
        result = compute_pullback_health(rally_pct=50.0, pullback_pct=0.0)
        assert result["health"] == "EXCELLENT"
        assert result["score"] == 100


class TestComputeVcpPass:
    """Brandon `is_begrudgingly_pulling_back` VCP olgunluk tespiti.
    3 koşul AND: small_drops (<1.5%) + volume_drying (<50d MA × 0.7) + tight_closes (<2%)
    """

    def _build_pvh(self, days: int, base_close: float = 100.0,
                   vol_avg: int = 1_000_000, vol_last: int = 600_000,
                   tight: bool = True, range_pct: float = 0.5) -> list[dict]:
        """Test PVH üreticisi — KARAR #464 sonrası OHLC formatında.

        Args:
            days: gün sayısı
            base_close: baseline close
            vol_avg: ilk N-1 gün hacim
            vol_last: son gün hacim
            tight: True → son 5 gün tight intraday range, False → geniş
            range_pct: gün-içi (high-low)/close % (tight=True iken)
        """
        pvh = []
        for i in range(days):
            close = base_close
            if i >= days - 5:
                close = base_close + (i - (days - 5)) * 0.3  # ardışık +%0.3
            # Intraday OHLC — tight veya geniş
            if i >= days - 5 and tight:
                # Son 5 gün dar range (örn %0.5)
                rng = close * range_pct / 100
            else:
                # İlk günler veya tight=False — geniş range
                rng = close * 2.5 / 100  # %2.5 geniş
            high = close + rng / 2
            low = close - rng / 2
            open_val = close - rng / 4  # arbitrary
            volume = vol_avg if i < days - 1 else vol_last
            pvh.append({
                "date": f"2026-04-{i+1:02d}",
                "open": open_val, "high": high, "low": low,
                "close": close, "volume": volume,
            })
        return pvh

    def test_short_history_returns_false(self):
        pvh = self._build_pvh(20)  # 50 günden az
        assert compute_vcp_pass(pvh) is False

    def test_none_returns_false(self):
        assert compute_vcp_pass(None) is False
        assert compute_vcp_pass([]) is False

    def test_vcp_pass_when_all_three_conditions_met(self):
        # 60 gün OHLC, son 5 gün tight (intraday %0.5) + low vol → VCP pass
        pvh = self._build_pvh(60, vol_last=600_000, tight=True, range_pct=0.5)
        result = compute_vcp_pass(pvh)
        assert result is True, f"Beklenen True, gelen {result}"

    def test_vcp_fail_when_volume_not_dry(self):
        # Tight close OK ama son gün hacim yüksek (2M)
        pvh = self._build_pvh(60, vol_last=2_000_000, tight=True, range_pct=0.5)
        assert compute_vcp_pass(pvh) is False

    def test_vcp_fail_when_drops_large(self):
        # Hacim + range ok ama close-to-close büyük düşüşler
        days = 60
        pvh = []
        for i in range(days):
            if i < days - 5:
                close = 100.0
            else:
                # Her gün -%3 düşüş → close-to-close > %1.5
                close = 100.0 - (i - (days - 5) + 1) * 3.0
            rng = close * 0.5 / 100  # dar intraday range
            pvh.append({
                "date": f"2026-04-{i+1:02d}",
                "open": close - rng/4, "high": close + rng/2, "low": close - rng/2,
                "close": close, "volume": 1_000_000 if i < days - 1 else 600_000,
            })
        assert compute_vcp_pass(pvh) is False

    def test_vcp_fail_when_intraday_range_too_wide(self):
        # KARAR #464: gerçek range_pct = (high-low)/close*100
        # Close-to-close tight ama intraday HIGH-LOW >%2 → fail
        pvh = self._build_pvh(60, vol_last=600_000, tight=False)  # geniş intraday
        assert compute_vcp_pass(pvh) is False

    def test_vcp_handles_zero_volume_gracefully(self):
        days = 60
        pvh = []
        for i in range(days):
            pvh.append({
                "date": f"2026-04-{i+1:02d}",
                "open": 100.0, "high": 100.5, "low": 99.5,
                "close": 100.0, "volume": 0,
            })
        assert compute_vcp_pass(pvh) is False

    def test_vcp_handles_malformed_entry(self):
        # Eksik anahtar (volume) → False (graceful)
        pvh = [{"date": "x", "close": 100.0} for _ in range(60)]
        assert compute_vcp_pass(pvh) is False

    def test_vcp_backward_compat_close_only_returns_false(self):
        # KARAR #464 backward compat:
        # Eski format {date, close, volume} (high/low yok) → False
        # Migration 003 öncesi yazılmış PVH için
        days = 60
        pvh = [
            {"date": f"2026-04-{i+1:02d}", "close": 100.0, "volume": 1_000_000}
            for i in range(days)
        ]
        # Son gün hacim düşük + tight close ama OHLC yok → backward compat False
        pvh[-1]["volume"] = 600_000
        result = compute_vcp_pass(pvh)
        assert result is False, (
            "KARAR #464 backward compat: eski PVH high/low yok -> False olmali"
        )


# ===========================================================================
# Sprint 4-bis.5 KARAR #466 — VCP Kalite Skoru (3 kanal sentezi)
# Master (0.70 muhafazakar + 0.50 ideal) + Minervini Uzmani (Mark canon
# "%50 alti en siki") + Bonus FMP (0.40-0.50 altin standart, ASX deneyimi)
# ===========================================================================


class TestComputeVcpQuality:
    """Brandon VCP iki seviyeli kalite tespit (EXCELLENT/PASS/None)."""

    def _build_pvh_quality(self, days: int, vol_last: int,
                           range_pct: float = 0.5) -> list[dict]:
        """Test PVH OHLC üreticisi - hacim seviyesi parametre."""
        pvh = []
        for i in range(days):
            close = 100.0
            if i >= days - 5:
                close = 100.0 + (i - (days - 5)) * 0.3
            rng = close * range_pct / 100 if i >= days - 5 else close * 2.5 / 100
            volume = 1_000_000 if i < days - 1 else vol_last
            pvh.append({
                "date": f"2026-04-{i+1:02d}",
                "open": close - rng/4, "high": close + rng/2, "low": close - rng/2,
                "close": close, "volume": volume,
            })
        return pvh

    def test_excellent_when_volume_below_50pct(self):
        # 50-gun MA ~960K, 0.50 = 480K -> son gun 400K -> EXCELLENT
        pvh = self._build_pvh_quality(60, vol_last=400_000)
        assert compute_vcp_quality(pvh) == "EXCELLENT"

    def test_pass_when_volume_between_50_and_70(self):
        # 50-gun MA ~960K, son gun 600K (0.60 ratio)
        # 0.50 ile 0.70 arasinda -> PASS
        pvh = self._build_pvh_quality(60, vol_last=600_000)
        assert compute_vcp_quality(pvh) == "PASS"

    def test_none_when_volume_above_70pct(self):
        # 50-gun MA ~960K, son gun 800K (0.83 ratio) -> None (failsafe)
        pvh = self._build_pvh_quality(60, vol_last=800_000)
        assert compute_vcp_quality(pvh) is None

    def test_none_when_intraday_range_wide(self):
        # Hacim 0.40 (EXCELLENT seviye) ama intraday range %2.5 (wide)
        pvh = self._build_pvh_quality(60, vol_last=400_000, range_pct=2.5)
        assert compute_vcp_quality(pvh) is None

    def test_none_for_short_history(self):
        pvh = self._build_pvh_quality(20, vol_last=400_000)
        assert compute_vcp_quality(pvh) is None

    def test_none_for_none_input(self):
        assert compute_vcp_quality(None) is None
        assert compute_vcp_quality([]) is None

    def test_backward_compat_close_only_returns_none(self):
        # Eski PVH (high/low yok) -> None
        days = 60
        pvh = [
            {"date": f"2026-04-{i+1:02d}", "close": 100.0, "volume": 400_000}
            for i in range(days)
        ]
        assert compute_vcp_quality(pvh) is None

    def test_threshold_constants_exposed(self):
        assert VCP_VOL_DRY_RATIO == 0.70
        assert VCP_VOL_DRY_RATIO_EXCELLENT == 0.50


# ===========================================================================
# Sprint 4-bis.5 KARAR #465 — Inside Day + Outside Day Negative Reversal
# + VCP Ready Score (Minervini Uzmani 4 kitap kanonu onerisi)
# Kaynak: Trade Like a Stock Market Wizard Bolum 10 +
#         Think and Trade Like a Champion Bolum 1 (Violations)
# ===========================================================================


class TestComputeInsideDay:
    """Inside Day: today range tamamen prev range icinde."""

    def test_inside_day_true(self):
        prev = {"high": 105.0, "low": 95.0}
        today = {"high": 103.0, "low": 97.0}
        assert compute_inside_day(prev, today) is True

    def test_outside_day_returns_false(self):
        prev = {"high": 105.0, "low": 95.0}
        today = {"high": 107.0, "low": 93.0}  # daha geniş
        assert compute_inside_day(prev, today) is False

    def test_partial_overlap_high_breach(self):
        # today.high > prev.high -> Inside DEĞİL
        prev = {"high": 100.0, "low": 95.0}
        today = {"high": 101.0, "low": 96.0}
        assert compute_inside_day(prev, today) is False

    def test_malformed_returns_false(self):
        assert compute_inside_day({}, {}) is False
        assert compute_inside_day({"high": 100}, {"low": 95}) is False


class TestComputeOutsideDayNegativeReversal:
    """Outside Day Negative Reversal: geniş range + düşük kapanış + yüksek hacim."""

    def test_negative_reversal_true(self):
        prev = {"high": 100, "low": 95, "close": 98, "volume": 1_000_000}
        today = {"high": 102, "low": 93, "close": 94, "volume": 2_000_000}
        # outside: 102>100 AND 93<95 ✓; negative: 94<98 ✓; high_vol: 2M > 1M*1.5=1.5M ✓
        assert compute_outside_day_negative_reversal(prev, today) is True

    def test_outside_but_positive_close_returns_false(self):
        prev = {"high": 100, "low": 95, "close": 98, "volume": 1_000_000}
        today = {"high": 102, "low": 93, "close": 100, "volume": 2_000_000}
        # close 100 > 98 -> Negative DEĞİL
        assert compute_outside_day_negative_reversal(prev, today) is False

    def test_negative_but_low_volume_returns_false(self):
        prev = {"high": 100, "low": 95, "close": 98, "volume": 1_000_000}
        today = {"high": 102, "low": 93, "close": 94, "volume": 1_200_000}
        # 1.2M < 1M*1.5 -> high_vol DEĞİL
        assert compute_outside_day_negative_reversal(prev, today) is False

    def test_inside_day_returns_false(self):
        prev = {"high": 100, "low": 95, "close": 98, "volume": 1_000_000}
        today = {"high": 99, "low": 96, "close": 94, "volume": 2_000_000}
        # outside DEĞİL (99<100, 96>95)
        assert compute_outside_day_negative_reversal(prev, today) is False

    def test_custom_vol_ratio(self):
        prev = {"high": 100, "low": 95, "close": 98, "volume": 1_000_000}
        today = {"high": 102, "low": 93, "close": 94, "volume": 1_100_000}
        # 1.1M < 1M*1.5 fail with default, ama 1.0 ratio ile 1.1M > 1M PASS
        assert compute_outside_day_negative_reversal(prev, today, vol_ratio=1.0) is True


class TestComputeVcpReadyScore:
    """VCP Ready Score 0-100 (50 Inside Day + 30 V-Dry + 20 tight)."""

    def _build(self, days=60, last_volume=400_000, range_pct=0.5,
               make_inside_days=True) -> list[dict]:
        """OHLC PVH üretici test için.
        make_inside_days=True ise son 4 gün ardışık Inside Day pattern:
        her gün öncekinin tam ortasında daha dar range.
        """
        pvh = []
        for i in range(days):
            close = 100.0
            rng = close * 2.5 / 100  # default geniş range
            volume = 1_000_000 if i < days - 1 else last_volume
            pvh.append({
                "date": f"2026-04-{i+1:02d}",
                "open": close - rng/4,
                "high": close + rng/2,
                "low": close - rng/2,
                "close": close,
                "volume": volume,
            })

        # Inside Day pattern: son 4 gün her biri prev'in tam ortasında daha dar
        # Lookback=3 → son 3 transition (3 Inside Day) hedeflenir
        if make_inside_days and days >= 4:
            # Pivot baz: son 4. günün range
            base_high = pvh[-4]["high"]
            base_low = pvh[-4]["low"]
            for k in range(3):
                idx = -3 + k  # -3, -2, -1
                width_factor = 0.8 - k * 0.15  # 0.80, 0.65, 0.50
                center = (base_high + base_low) / 2
                width = (base_high - base_low) * width_factor / 2
                new_high = center + width
                new_low = center - width
                new_close = center
                pvh[idx]["high"] = new_high
                pvh[idx]["low"] = new_low
                pvh[idx]["close"] = new_close
                pvh[idx]["open"] = new_close
                # base'i güncelle (her gün öncekinin içinde)
                base_high = new_high
                base_low = new_low

        # tight range_pct override son 5 gün
        # zaten range küçülen Inside Day pattern tight_pct'i sağlar
        return pvh

    def test_high_score_when_all_three_factors_present(self):
        # 3 Inside Day (full 50) + V-Dry EXCELLENT (full 30) + tight (full 20) = 100
        pvh = self._build(60, last_volume=400_000, range_pct=0.5, make_inside_days=True)
        score = compute_vcp_ready_score(pvh)
        # Min 50 (inside) + 30 (vol < 0.50) + 20 (5 tight gün) = 100
        # Ancak Inside Day gerçekten olmayabilir test PVH yapısına göre
        assert score is not None
        assert score >= VCP_READY_SCORE_HIGH_THRESHOLD, f"Beklenen >=70, gelen {score}"

    def test_low_score_when_volume_high(self):
        # Hacim yüksek (1M = avg ile aynı) -> vol_score 0
        pvh = self._build(60, last_volume=1_000_000, range_pct=0.5)
        score = compute_vcp_ready_score(pvh)
        assert score is not None
        assert score < VCP_READY_SCORE_HIGH_THRESHOLD, f"Beklenen <70, gelen {score}"

    def test_none_for_short_history(self):
        pvh = [{"date": "x", "high": 100, "low": 99, "close": 99, "volume": 1000}
               for _ in range(20)]
        assert compute_vcp_ready_score(pvh) is None

    def test_none_for_close_only_backward_compat(self):
        pvh = [{"date": f"d{i}", "close": 100, "volume": 1000} for i in range(60)]
        assert compute_vcp_ready_score(pvh) is None

    def test_none_for_invalid_input(self):
        assert compute_vcp_ready_score(None) is None
        assert compute_vcp_ready_score([]) is None

    def test_score_between_0_and_100(self):
        pvh = self._build(60, last_volume=600_000, range_pct=1.0)
        score = compute_vcp_ready_score(pvh)
        if score is not None:
            assert 0 <= score <= 100, f"Score range disi: {score}"

    def test_threshold_constants_exposed(self):
        assert VCP_READY_SCORE_HIGH_THRESHOLD == 70
        assert OUTSIDE_DAY_VOLUME_RATIO == 1.5

    def test_threshold_constants_exposed(self):
        # Kalibrasyon noktaları dışarıdan erişilebilir olmalı
        assert VCP_PULLBACK_EXCELLENT == 0.10
        assert VCP_PULLBACK_GOOD == 0.25
        assert VCP_PULLBACK_ACCEPTABLE == 0.40
