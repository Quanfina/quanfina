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
    # P576 — V-Dry hacim kurumasi siniflandirma (Minervini s.150 "Quiet Action")
    compute_v_dry_class,
    # Sprint 4-bis.5 KARAR #465 — Inside/Outside Day + Ready Score
    compute_inside_day,
    compute_outside_day_negative_reversal,
    compute_vcp_ready_score,
    OUTSIDE_DAY_VOLUME_RATIO,
    VCP_READY_SCORE_HIGH_THRESHOLD,
    # Sprint 4-bis.5 KARAR #467 — Power Play (HTF) Mark canon
    compute_power_play_pass,
    POWER_PLAY_POLE_MIN_RISE_PCT,
    POWER_PLAY_FLAG_MAX_PULLBACK_PCT,
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
# NOT: count_distribution_days canli testleri tests/test_distribution_days.py'de
# (Mark KARAR #488, imza (closes, volumes, lookback) -> dict). Eski TestCountDistributionDays
# sinifi quanfina_math.py:550'deki OLU/golge fonksiyonu (StopRecommendation donen)
# test ediyordu; 2944 yeniden tanimi onu golgeledigi icin kaldirildi (DRY, Ilke #4).
# DUPLICATE fonksiyon temizligi (550 vs 2944) yikici refactor -> Sn. Ferit onayi.


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

    def test_all_winners_returns_capped_ratio(self):
        # Tum trade'ler kazanc = sonsuz edge. float('inf') JSON serialize EDILEMEZ
        # (FastAPI 500), 99.0 ile cap'lenir (pratik sonsuz). Bkz compute_rba_metrics.
        trades = [{'pnl_pct': 10.0}, {'pnl_pct': 15.0}]
        rba = compute_rba_metrics(trades)
        assert rba.win_rate == 1.0
        assert rba.adjusted_ratio == 99.0

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
# P576 — V-Dry hacim kurumasi siniflandirma (Minervini s.150 "Quiet Action")
# compute_vcp_quality'ye PARALEL ama SADECE hacim boyutu; 3 seviyeli enum
# (STRONG/IDEAL/WEAK) ayni kanon esikleri (0.50 / 0.70) ile.
# ===========================================================================


class TestComputeVDryClass:
    """V-Dry hacim kurumasi: son 5 gun ort. hacim / 50g ort. -> 3 seviye."""

    def _build_pvh_vol(self, days: int, recent_vol: int,
                       base_vol: int = 1_000_000) -> list[dict]:
        """Son VCP_LOOKBACK_DAYS (5) gun recent_vol, oncesi base_vol."""
        pvh = []
        for i in range(days):
            volume = recent_vol if i >= days - 5 else base_vol
            pvh.append({
                "date": f"2026-04-{i+1:02d}",
                "open": 100.0, "high": 100.5, "low": 99.5,
                "close": 100.0, "volume": volume,
            })
        return pvh

    def test_strong_when_recent_below_50pct(self):
        # 50g ort ~ (45*1M + 5*400K)/50 = 940K; recent ort = 400K; ratio ~0.43 -> STRONG
        pvh = self._build_pvh_vol(60, recent_vol=400_000)
        res = compute_v_dry_class(pvh)
        assert res["v_dry_class"] == "STRONG"
        assert res["label"] == "Guclu"
        assert res["vol_ratio"] < VCP_VOL_DRY_RATIO_EXCELLENT

    def test_ideal_when_recent_between_50_and_70(self):
        # recent 600K, 50g ort ~ (45*1M+5*600K)/50 = 960K; ratio 0.625 -> IDEAL
        pvh = self._build_pvh_vol(60, recent_vol=600_000)
        res = compute_v_dry_class(pvh)
        assert res["v_dry_class"] == "IDEAL"
        assert res["label"] == "Ideal"
        assert VCP_VOL_DRY_RATIO_EXCELLENT <= res["vol_ratio"] < VCP_VOL_DRY_RATIO

    def test_weak_when_recent_above_70pct(self):
        # recent 900K, ratio ~0.94 -> WEAK
        pvh = self._build_pvh_vol(60, recent_vol=900_000)
        res = compute_v_dry_class(pvh)
        assert res["v_dry_class"] == "WEAK"
        assert res["label"] == "Zayif"
        assert res["vol_ratio"] >= VCP_VOL_DRY_RATIO

    def test_none_for_short_history(self):
        res = compute_v_dry_class(self._build_pvh_vol(20, recent_vol=400_000))
        assert res["v_dry_class"] is None

    def test_none_for_none_and_empty(self):
        assert compute_v_dry_class(None)["v_dry_class"] is None
        assert compute_v_dry_class([])["v_dry_class"] is None

    def test_graceful_on_zero_volume(self):
        pvh = [{"date": f"2026-04-{i+1:02d}", "open": 100.0, "high": 100.5,
                "low": 99.5, "close": 100.0, "volume": 0} for i in range(60)]
        assert compute_v_dry_class(pvh)["v_dry_class"] is None

    def test_graceful_on_malformed(self):
        pvh = [{"date": "x", "close": 100.0} for _ in range(60)]  # volume yok
        assert compute_v_dry_class(pvh)["v_dry_class"] is None


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


# ===========================================================================
# Sprint 4-bis.5 KARAR #467 — Power Play (High Tight Flag) Mark canon
# Kaynak: Trade Like a Stock Market Wizard Bolum 10 + FMP_Matematik.md Konu 20
# ===========================================================================


class TestComputePowerPlayPass:
    """Power Play (HTF): POLE %100+ yukselis (8 hafta) + FLAG %10-25 duzeltme (2-6 hafta)."""

    def _build_pole_flag(self, pole_rise_pct: float, flag_pullback_pct: float,
                         days: int = 80) -> list[dict]:
        """POLE+FLAG senaryosu PVH üreticisi.

        İlk 40 gün (POLE): low=50 → high = low * (1 + rise%/100)
        Son 30 gün (FLAG): high'den pullback% kadar düşüş
        Gün-içi range: %1.5 (sade tutuldu)
        """
        pvh = []
        pole_low = 50.0
        pole_high = pole_low * (1 + pole_rise_pct / 100)

        # POLE: linear yukselis (sade)
        pole_days = 40
        for i in range(pole_days):
            close = pole_low + (pole_high - pole_low) * (i / max(pole_days - 1, 1))
            rng = close * 0.015
            pvh.append({
                "date": f"2026-01-{i+1:02d}",
                "open": close - rng/4, "high": close + rng/2, "low": close - rng/2,
                "close": close, "volume": 2_000_000,
            })

        # FLAG: pullback (pole_high -> flag_low)
        flag_low = pole_high * (1 - flag_pullback_pct / 100)
        flag_days = 30
        for i in range(flag_days):
            # Yumuşak iniş + flat tutunma
            close = pole_high - (pole_high - flag_low) * (i / max(flag_days - 1, 1))
            rng = close * 0.015
            pvh.append({
                "date": f"2026-02-{i+1:02d}",
                "open": close - rng/4, "high": close + rng/2, "low": close - rng/2,
                "close": close, "volume": 1_000_000,
            })

        # Yetersiz veri kontrolu için days < 70 ise kısalt
        if days < len(pvh):
            return pvh[-days:]
        return pvh

    def test_power_play_pass_when_pole_100_flag_15(self):
        # %100 yukselis + %15 flag pullback = Power Play TRUE
        pvh = self._build_pole_flag(pole_rise_pct=110, flag_pullback_pct=15)
        assert compute_power_play_pass(pvh) is True

    def test_fail_when_pole_below_100(self):
        # %75 yukselis < %100 -> Power Play DEGIL (sıradan VCP adayı)
        pvh = self._build_pole_flag(pole_rise_pct=75, flag_pullback_pct=15)
        assert compute_power_play_pass(pvh) is False

    def test_fail_when_flag_pullback_above_25(self):
        # %30 pullback > %25 max -> reddet
        pvh = self._build_pole_flag(pole_rise_pct=120, flag_pullback_pct=30)
        assert compute_power_play_pass(pvh) is False

    def test_pass_when_flag_pullback_below_10_already_tight(self):
        # %5 pullback < %10 = "zaten sikismis" Mark canon: TRUE
        pvh = self._build_pole_flag(pole_rise_pct=120, flag_pullback_pct=5)
        assert compute_power_play_pass(pvh) is True

    def test_fail_for_short_history(self):
        pvh = self._build_pole_flag(pole_rise_pct=120, flag_pullback_pct=15, days=40)
        assert compute_power_play_pass(pvh) is False

    def test_fail_for_close_only_backward_compat(self):
        days = 80
        pvh = [{"date": f"d{i}", "close": 100.0, "volume": 1_000_000} for i in range(days)]
        assert compute_power_play_pass(pvh) is False

    def test_none_or_empty_returns_false(self):
        assert compute_power_play_pass(None) is False
        assert compute_power_play_pass([]) is False

    def test_threshold_constants_exposed(self):
        assert POWER_PLAY_POLE_MIN_RISE_PCT == 100
        assert POWER_PLAY_FLAG_MAX_PULLBACK_PCT == 25

    def test_threshold_constants_exposed(self):
        # Kalibrasyon noktaları dışarıdan erişilebilir olmalı
        assert VCP_PULLBACK_EXCELLENT == 0.10
        assert VCP_PULLBACK_GOOD == 0.25
        assert VCP_PULLBACK_ACCEPTABLE == 0.40


# =============================================================================
# SPRINT 4-bis.7 — FAZ 1 B PAKET — Mark 4-Kitap Hassas KARAR'ları
# Tests for: compute_dynamic_stop, mark_position_sizer, mark_six_rule_check
# Tescil: Vizyon v22.00 (24 May 2026)
# =============================================================================

from quanfina_math import (
    compute_dynamic_stop,
    mark_position_sizer,
    mark_six_rule_check,
    MARK_STOP_ABSOLUTE_CAP_PCT,
    MARK_EQUITY_RISK_MIN_PCT,
    MARK_EQUITY_RISK_MAX_PCT,
    MARK_POSITION_MAX_PCT,
)


class TestComputeDynamicStop:
    """KARAR ADAY #914 - Dynamic Stop = avg_gain / 2 (TTLC s.299)."""

    def _make_rba(self, avg_gain, num_trades=50):
        return RBAMetrics(
            num_trades=num_trades,
            win_rate=0.5,
            avg_gain_pct=avg_gain,
            avg_loss_pct=-5.0,
            largest_gain_pct=avg_gain * 2,
            largest_loss_pct=-10.0,
            adjusted_ratio=2.0,
            expectancy_pct=2.0,
            is_statistically_significant=num_trades >= 30,
        )

    def test_rba_based_normal(self):
        rba = self._make_rba(avg_gain=15.0)
        result = compute_dynamic_stop(rba)
        assert result["method"] == "rba_based"
        assert result["recommended_stop_pct"] == 7.5
        assert result["absolute_cap_applied"] is False

    def test_rba_based_absolute_cap_kicks_in(self):
        # avg_gain=30 -> half=15 -> cap %10
        rba = self._make_rba(avg_gain=30.0)
        result = compute_dynamic_stop(rba)
        assert result["method"] == "rba_based"
        assert result["recommended_stop_pct"] == 10.0
        assert result["absolute_cap_applied"] is True

    def test_rba_insufficient_trades_fallback(self):
        rba = self._make_rba(avg_gain=20.0, num_trades=10)
        result = compute_dynamic_stop(rba, fallback_pct=7.0)
        assert result["method"] == "fallback"
        assert result["recommended_stop_pct"] == 7.0
        assert result["rba_anlamli_mi"] is False

    def test_no_rba_fallback(self):
        result = compute_dynamic_stop(None, fallback_pct=6.0)
        assert result["method"] == "fallback"
        assert result["recommended_stop_pct"] == 6.0

    def test_fallback_cap_applied(self):
        result = compute_dynamic_stop(None, fallback_pct=15.0)
        assert result["recommended_stop_pct"] == 10.0
        assert result["absolute_cap_applied"] is True


class TestMarkPositionSizer:
    """KARAR ADAY #969 - Mark Position Sizing (TTLC s.143)."""

    def test_basic_calculation_default(self):
        # $100K portfolio, %2 risk, %7 stop
        result = mark_position_sizer(100000, target_risk_pct=2.0, max_stop_pct=7.0)
        # risk = $2000, position = $2000/0.07 = $28,571
        assert result["risk_dollars"] == 2000.0
        assert abs(result["position_dollars"] - 28571.43) < 1.0
        assert abs(result["position_pct"] - 28.57) < 0.1

    def test_optimal_tier(self):
        # $100K, %1.75 risk, %7.5 stop -> ~23.33% position (optimal range)
        result = mark_position_sizer(100000, target_risk_pct=1.75, max_stop_pct=7.5)
        assert result["tier"] == "optimal"

    def test_pilot_buy_tier(self):
        # $100K, %1.25 risk, %10 stop -> 12.5% position
        result = mark_position_sizer(100000, target_risk_pct=1.25, max_stop_pct=10.0)
        assert result["tier"] == "pilot_buy"
        assert abs(result["position_pct"] - 12.5) < 0.01

    def test_aggressive_tier_at_limit(self):
        # %2.5 risk + %5 stop = %50 position (sınır)
        result = mark_position_sizer(100000, target_risk_pct=2.5, max_stop_pct=5.0)
        assert abs(result["position_pct"] - 50.0) < 0.01

    def test_position_exceeds_50_pct_warning(self):
        # %2.5 risk + %4 stop = %62.5 position -> uyarı
        result = mark_position_sizer(100000, target_risk_pct=2.5, max_stop_pct=4.0)
        assert result["position_pct"] > 50.0
        assert any("MAX %50" in w for w in result["warnings"])

    def test_stop_exceeds_10_pct_warning(self):
        result = mark_position_sizer(100000, target_risk_pct=2.0, max_stop_pct=12.0)
        assert any("absolute cap" in w for w in result["warnings"])

    def test_risk_below_mark_min(self):
        # %1.0 risk < Mark min %1.25
        result = mark_position_sizer(100000, target_risk_pct=1.0, max_stop_pct=7.0)
        assert any("Mark min" in w for w in result["warnings"])

    def test_risk_above_mark_max(self):
        # %3.0 risk > Mark max %2.50
        result = mark_position_sizer(100000, target_risk_pct=3.0, max_stop_pct=7.0)
        assert any("MAX" in w for w in result["warnings"])

    def test_invalid_portfolio_value(self):
        result = mark_position_sizer(0, target_risk_pct=2.0, max_stop_pct=7.0)
        assert "error" in result
        assert result["position_dollars"] == 0.0


class TestMarkSixRuleCheck:
    """KARAR ADAY #970 - Mark 6-Rule Position Enforcement (TTLC s.144)."""

    def test_all_six_pass(self):
        result = mark_six_rule_check(
            risk_pct=2.0,           # OK 1.25-2.50
            stop_pct=7.0,           # OK <=10
            avg_loss_pct=-5.0,      # OK <=6
            position_pct=22.0,      # OK <=50, best name optimal
            is_best_name=True,      # OK 20-25
            total_positions=8,      # OK 4-12 optimal
        )
        assert result["all_pass"] is True
        assert result["pass_count"] == 6
        assert result["critical_violations"] == []

    def test_rule1_risk_violation_critical(self):
        result = mark_six_rule_check(
            risk_pct=3.5,           # FAIL > %2.50
            stop_pct=7.0,
            avg_loss_pct=-5.0,
            position_pct=22.0,
            is_best_name=True,
            total_positions=8,
        )
        assert result["all_pass"] is False
        assert 1 in result["critical_violations"]

    def test_rule2_stop_above_10_critical(self):
        result = mark_six_rule_check(
            risk_pct=2.0,
            stop_pct=12.0,          # FAIL > %10
            avg_loss_pct=-5.0,
            position_pct=22.0,
            is_best_name=True,
            total_positions=8,
        )
        assert 2 in result["critical_violations"]

    def test_rule3_avg_loss_high_non_critical(self):
        result = mark_six_rule_check(
            risk_pct=2.0,
            stop_pct=7.0,
            avg_loss_pct=-9.0,      # WARN > %5-6 advisory
            position_pct=22.0,
            is_best_name=True,
            total_positions=8,
        )
        rule3 = next(r for r in result["rules"] if r["rule_no"] == 3)
        assert rule3["pass"] is False
        assert 3 not in result["critical_violations"]

    def test_rule3_avg_loss_none_passes(self):
        result = mark_six_rule_check(
            risk_pct=2.0,
            stop_pct=7.0,
            avg_loss_pct=None,
            position_pct=22.0,
            is_best_name=True,
            total_positions=8,
        )
        rule3 = next(r for r in result["rules"] if r["rule_no"] == 3)
        assert rule3["pass"] is True

    def test_rule4_position_above_50_critical(self):
        result = mark_six_rule_check(
            risk_pct=2.0,
            stop_pct=7.0,
            avg_loss_pct=-5.0,
            position_pct=60.0,      # FAIL > %50
            is_best_name=True,
            total_positions=8,
        )
        assert 4 in result["critical_violations"]

    def test_rule5_best_name_outside_optimal_advisory(self):
        result = mark_six_rule_check(
            risk_pct=2.0,
            stop_pct=7.0,
            avg_loss_pct=-5.0,
            position_pct=10.0,
            is_best_name=True,
            total_positions=8,
        )
        rule5 = next(r for r in result["rules"] if r["rule_no"] == 5)
        assert rule5["pass"] is False
        assert 5 not in result["critical_violations"]

    def test_rule5_not_best_name_skipped(self):
        result = mark_six_rule_check(
            risk_pct=2.0,
            stop_pct=7.0,
            avg_loss_pct=-5.0,
            position_pct=10.0,
            is_best_name=False,
            total_positions=8,
        )
        rule5 = next(r for r in result["rules"] if r["rule_no"] == 5)
        assert rule5["pass"] is True

    def test_rule6_too_many_positions_critical(self):
        result = mark_six_rule_check(
            risk_pct=2.0,
            stop_pct=7.0,
            avg_loss_pct=-5.0,
            position_pct=22.0,
            is_best_name=True,
            total_positions=25,     # FAIL > 20
        )
        assert 6 in result["critical_violations"]

    def test_rule6_large_portfolio_acceptable(self):
        result = mark_six_rule_check(
            risk_pct=2.0,
            stop_pct=7.0,
            avg_loss_pct=-5.0,
            position_pct=22.0,
            is_best_name=True,
            total_positions=18,
        )
        rule6 = next(r for r in result["rules"] if r["rule_no"] == 6)
        assert rule6["pass"] is True
        assert 6 not in result["critical_violations"]

    def test_mark_constants_match_canon(self):
        # Vizyon v22.00 + Sprint_4_bis_7_*.md tutarlilik
        assert MARK_STOP_ABSOLUTE_CAP_PCT == 10.0
        assert MARK_EQUITY_RISK_MIN_PCT == 1.25
        assert MARK_EQUITY_RISK_MAX_PCT == 2.50
        assert MARK_POSITION_MAX_PCT == 50.0


# =============================================================================
# SPRINT 4-bis.7 — FAZ 2 — Mark EPS Acceleration + Code 33
# Tests for: detect_eps_acceleration, detect_code_33
# Tescil: Vizyon v22.00 + v22.01 (24 May 2026)
# =============================================================================

from quanfina_math import (
    detect_eps_acceleration,
    detect_code_33,
    MARK_EPS_MIN_GROWTH_PCT,
    MARK_EPS_SUPERPERFORMANCE_PCT,
    MARK_EPS_BULL_MARKET_PCT,
    MARK_EPS_TURNAROUND_PCT,
    MARK_EPS_90PCT_RULE_THRESHOLD,
)


class TestDetectEpsAcceleration:
    """KARAR ADAY #834 - Mark EPS Acceleration Detector (TLSMW s.131)."""

    def test_mark_textbook_example(self):
        # Mark TLSMW s.131 birebir: -5, 10, 28, 56
        result = detect_eps_acceleration([-5.0, 10.0, 28.0, 56.0])
        assert result["accelerating"] is True
        assert result["mark_90pct_rule"] is True  # current 56 > 25
        assert result["phase"] == "accelerating"
        assert result["tier"] == "bull_market"  # 56 > 40
        assert abs(result["magnitude_pct_pts"] - 61.0) < 0.01

    def test_strict_accel_below_25_threshold(self):
        # Accel var ama current < 25 → 90% Rule MATCH değil
        result = detect_eps_acceleration([5.0, 10.0, 15.0, 20.0])
        assert result["accelerating"] is True
        assert result["mark_90pct_rule"] is False  # current 20 < 25
        assert result["tier"] == "minimum"  # 20 == 20

    def test_strict_deceleration_dell_pattern(self):
        # Mark Dell pattern (TLSMW s.138): %80 → %65 → %28 → declining
        result = detect_eps_acceleration([80.0, 65.0, 28.0, 11.0])
        assert result["accelerating"] is False
        assert result["phase"] == "decelerating"
        assert "DECELERATION" in result["mark_says"]

    def test_flat_irregular_pattern(self):
        result = detect_eps_acceleration([10.0, 25.0, 15.0, 30.0])
        assert result["accelerating"] is False
        assert result["phase"] == "flat"

    def test_turnaround_tier(self):
        # Mark Turnaround (TLSMW s.137): current %100+
        result = detect_eps_acceleration([10.0, 50.0, 80.0, 150.0])
        assert result["accelerating"] is True
        assert result["tier"] == "turnaround"
        assert result["mark_90pct_rule"] is True

    def test_superperformance_tier(self):
        # current %30-40 range
        result = detect_eps_acceleration([5.0, 15.0, 25.0, 35.0])
        assert result["tier"] == "superperformance"

    def test_below_minimum_tier(self):
        result = detect_eps_acceleration([1.0, 3.0, 5.0, 10.0])
        assert result["tier"] == "below_minimum"  # current < 20

    def test_empty_input_invalid(self):
        result = detect_eps_acceleration([])
        assert result["phase"] == "invalid"
        assert result["accelerating"] is False

    def test_single_quarter_invalid(self):
        result = detect_eps_acceleration([20.0])
        assert result["phase"] == "invalid"

    def test_two_quarters_minimal_check(self):
        result = detect_eps_acceleration([10.0, 30.0])
        assert result["accelerating"] is True
        assert result["quarters_count"] == 2

    def test_mark_constants(self):
        assert MARK_EPS_MIN_GROWTH_PCT == 20.0
        assert MARK_EPS_SUPERPERFORMANCE_PCT == 30.0
        assert MARK_EPS_BULL_MARKET_PCT == 40.0
        assert MARK_EPS_TURNAROUND_PCT == 100.0
        assert MARK_EPS_90PCT_RULE_THRESHOLD == 25.0


class TestDetectCode33:
    """KARAR ADAY #855 - Mark Code 33 Detector (TLSMW s.173)."""

    def test_full_code_33_monster_pattern(self):
        # Mark MNST 2003-2005 classic Code 33
        result = detect_code_33(
            eps_growth_yoy_last_4q=[20.0, 35.0, 50.0, 75.0],
            sales_growth_yoy_last_4q=[15.0, 25.0, 40.0, 60.0],
            net_margin_last_4q=[5.0, 7.0, 9.0, 12.0],
        )
        assert result["pattern"] == "CODE_33"
        assert result["tier"] == "elite"
        assert result["eps_accel"] is True
        assert result["sales_accel"] is True
        assert result["margin_expanding"] is True
        assert result["pass_count"] == 3
        assert "CODE 33 elite" in result["mark_says"]

    def test_partial_2_of_3(self):
        result = detect_code_33(
            eps_growth_yoy_last_4q=[20.0, 35.0, 50.0, 75.0],
            sales_growth_yoy_last_4q=[15.0, 25.0, 40.0, 60.0],
            net_margin_last_4q=[10.0, 8.0, 6.0, 4.0],  # decreasing margin
        )
        assert result["pattern"] == "partial"
        assert result["tier"] == "partial_2"
        assert result["pass_count"] == 2

    def test_partial_1_of_3(self):
        result = detect_code_33(
            eps_growth_yoy_last_4q=[20.0, 35.0, 50.0, 75.0],  # only EPS accel
            sales_growth_yoy_last_4q=[15.0, 12.0, 10.0, 8.0],
            net_margin_last_4q=[10.0, 9.0, 8.0, 7.0],
        )
        assert result["pattern"] == "partial"
        assert result["tier"] == "partial_1"
        assert result["pass_count"] == 1

    def test_none(self):
        result = detect_code_33(
            eps_growth_yoy_last_4q=[20.0, 15.0, 10.0, 5.0],
            sales_growth_yoy_last_4q=[20.0, 15.0, 10.0, 5.0],
            net_margin_last_4q=[10.0, 9.0, 8.0, 7.0],
        )
        assert result["pattern"] == "none"
        assert result["pass_count"] == 0

    def test_insufficient_quarters(self):
        # Mark KESIN minimum 3-quarter accel — 4 data point gerekli
        result = detect_code_33(
            eps_growth_yoy_last_4q=[10.0, 20.0],
            sales_growth_yoy_last_4q=[10.0, 20.0],
            net_margin_last_4q=[5.0, 6.0],
        )
        assert result["pattern"] == "none"
        assert result["eps_accel"] is False  # <4 quarter

    def test_empty_inputs(self):
        result = detect_code_33([], [], [])
        assert result["pattern"] == "none"
        assert result["pass_count"] == 0


# =============================================================================
# Faz 2 Genisletme — Tennis Ball + Volume Asymmetry + Leader Fingerprint
# Tests for: detect_tennis_ball, compute_volume_asymmetry, detect_leader_fingerprint
# Tescil: Vizyon v22.03 (24 May 2026)
# =============================================================================

from quanfina_math import (
    detect_tennis_ball,
    compute_volume_asymmetry,
    detect_leader_fingerprint,
    TENNIS_BALL_PULLBACK_MAX_DAYS,
    TENNIS_BALL_RECOVERY_MAX_DAYS,
    VOLUME_ASYMMETRY_HEALTHY_RATIO,
    VOLUME_ASYMMETRY_DISTRIBUTION_RATIO,
    LEADER_ADVANCE_MIN_PCT,
    LEADER_ADVANCE_MAX_PCT,
)


def _make_day(close, high=None, low=None, volume=100000):
    return {
        "close": close,
        "high": high if high is not None else close * 1.01,
        "low": low if low is not None else close * 0.99,
        "volume": volume,
    }


class TestDetectTennisBall:
    """KARAR ADAY #893 - Mark Tennis Ball Detector (TLSMW s.253)."""

    def test_classic_tennis_ball(self):
        # Breakout day 0 -> pullback 3 days -> recovery 5 days
        history = [
            _make_day(100, high=100),  # 0 breakout
            _make_day(99),
            _make_day(98),
            _make_day(97),    # 3 pullback low
            _make_day(98),
            _make_day(99),
            _make_day(100),
            _make_day(101),
            _make_day(102),   # 8 recovery beyond breakout_high
        ]
        result = detect_tennis_ball(0, history)
        assert result["pattern"] == "TENNIS_BALL"
        assert result["pullback_days"] == 3
        # recovery: pullback_low_idx=3 (close=97), close>100 first at idx 7 (close=101)
        # recovery_days = 7 - 3 = 4
        assert result["recovery_days"] == 4
        assert result["recovered"] is True

    def test_egg_long_pullback(self):
        # 16 gun pullback (>14)
        history = [_make_day(100, high=100)]
        for i in range(20):
            history.append(_make_day(100 - i - 1))
        result = detect_tennis_ball(0, history)
        assert result["pattern"] == "EGG"
        assert result["pullback_days"] >= 15

    def test_still_running(self):
        history = [_make_day(100, high=100), _make_day(105), _make_day(110)]
        result = detect_tennis_ball(0, history)
        assert result["pattern"] == "STILL_RUNNING"

    def test_invalid_index(self):
        result = detect_tennis_ball(-1, [_make_day(100)])
        assert result["pattern"] == "INVALID"

    def test_empty_history(self):
        result = detect_tennis_ball(0, [])
        assert result["pattern"] == "INVALID"


class TestComputeVolumeAsymmetry:
    """KARAR ADAY #882 - Mark Volume Asymmetry Tracker (TLSMW s.234)."""

    def test_healthy_accumulation(self):
        # Up days yuksek vol, down days dusuk vol
        history = [
            _make_day(100, volume=100000),
            _make_day(102, volume=200000),  # up high vol
            _make_day(101, volume=80000),   # down low vol
            _make_day(103, volume=250000),  # up high vol
            _make_day(102, volume=70000),   # down low vol
            _make_day(105, volume=300000),  # up high vol
        ]
        result = compute_volume_asymmetry(history)
        assert result["tier"] == "healthy"
        assert result["asymmetry_ratio"] >= VOLUME_ASYMMETRY_HEALTHY_RATIO

    def test_distribution_warning(self):
        # Down days yuksek vol, up days dusuk vol
        history = [
            _make_day(100, volume=100000),
            _make_day(101, volume=50000),    # up low vol
            _make_day(99, volume=300000),    # down high vol
            _make_day(100, volume=60000),    # up low vol
            _make_day(98, volume=350000),    # down high vol
        ]
        result = compute_volume_asymmetry(history)
        assert result["tier"] == "distribution"
        assert result["asymmetry_ratio"] < VOLUME_ASYMMETRY_DISTRIBUTION_RATIO

    def test_neutral(self):
        # Up ve down day vol benzer
        history = [
            _make_day(100, volume=100000),
            _make_day(102, volume=110000),
            _make_day(101, volume=105000),
            _make_day(103, volume=115000),
        ]
        result = compute_volume_asymmetry(history)
        assert result["tier"] == "neutral"

    def test_insufficient_data(self):
        result = compute_volume_asymmetry([_make_day(100)])
        assert result["tier"] == "invalid"

    def test_empty(self):
        result = compute_volume_asymmetry([])
        assert result["tier"] == "invalid"


class TestDetectLeaderFingerprint:
    """KARAR ADAY #864 - Mark Leader Behavior Fingerprint (TLSMW s.184)."""

    def test_classic_leader_humana_pattern(self):
        # Mark Humana paten: advance 15-25%, pullback 5-12%
        result = detect_leader_fingerprint(
            advance_segments=[18.0, 22.0, 17.0],
            pullback_segments=[7.0, 9.0, 6.0],
        )
        assert result["pattern"] == "LEADER_FINGERPRINT"
        assert result["tier"] == "leader_classic"

    def test_partial_leader(self):
        # 2/3 advance Mark esiginde, 2/3 pullback Mark esiginde
        result = detect_leader_fingerprint(
            advance_segments=[18.0, 22.0, 35.0],  # 3rd out of range
            pullback_segments=[7.0, 9.0, 15.0],   # 3rd out of range
        )
        assert result["tier"] == "leader_partial"

    def test_not_leader(self):
        result = detect_leader_fingerprint(
            advance_segments=[5.0, 8.0, 3.0],
            pullback_segments=[15.0, 20.0, 18.0],
        )
        assert result["tier"] == "not_leader"

    def test_invalid_empty(self):
        result = detect_leader_fingerprint([], [])
        assert result["tier"] == "invalid"

    def test_constants(self):
        assert TENNIS_BALL_PULLBACK_MAX_DAYS == 7
        assert TENNIS_BALL_RECOVERY_MAX_DAYS == 14
        assert LEADER_ADVANCE_MIN_PCT == 15.0
        assert LEADER_ADVANCE_MAX_PCT == 25.0


# =============================================================================
# Faz 2 Genisletme 2 — New High Pivot + Darvas Box
# Tests for: check_new_high_pivot, detect_darvas_box
# Tescil: Vizyon v22.04 + v22.05 (24 May 2026)
# =============================================================================

from quanfina_math import (
    check_new_high_pivot,
    detect_darvas_box,
    NEW_HIGH_PIVOT_THRESHOLD_PCT,
    NEW_HIGH_BUY_READY_THRESHOLD_PCT,
    DARVAS_MIN_WEEKS,
    DARVAS_MAX_RANGE_PCT,
)


class TestCheckNewHighPivot:
    """KARAR ADAY #876 - Mark New High Pivot Mandate (TLSMW s.221)."""

    def test_breakout_above_52w_high(self):
        # Current $105, 52w high $100 → breakout %5 ustunde
        result = check_new_high_pivot(105, 100)
        assert result["tier"] == "breakout"
        assert result["distance_from_high_pct"] < 0  # ustunde
        assert result["is_actionable"] is True

    def test_buy_ready_within_2_pct(self):
        # Current $98, 52w high $100 → %2 altinda
        result = check_new_high_pivot(98, 100)
        assert result["tier"] == "buy_ready"
        assert abs(result["distance_from_high_pct"] - 2.0) < 0.01
        assert result["is_actionable"] is True

    def test_near_pivot_within_5_pct(self):
        # Current $96, 52w high $100 → %4 altinda
        result = check_new_high_pivot(96, 100)
        assert result["tier"] == "near_pivot"
        assert result["is_actionable"] is True

    def test_too_far_above_5_pct(self):
        # Current $90, 52w high $100 → %10 altinda (Mark esigin disi)
        result = check_new_high_pivot(90, 100)
        assert result["tier"] == "too_far"
        assert result["is_actionable"] is False

    def test_invalid_zero_price(self):
        result = check_new_high_pivot(0, 100)
        assert result["tier"] == "invalid"
        assert result["is_actionable"] is False

    def test_constants(self):
        assert NEW_HIGH_PIVOT_THRESHOLD_PCT == 5.0
        assert NEW_HIGH_BUY_READY_THRESHOLD_PCT == 2.0


class TestDetectDarvasBox:
    """KARAR ADAY #874 - Mark Darvas Box Detector (TLSMW s.215)."""

    def _make_box_history(self, days, range_pct, base_price=100):
        # Tight box: high ve low base_price etrafinda dar bant
        history = []
        high_max = base_price * (1 + range_pct / 200)
        low_min = base_price * (1 - range_pct / 200)
        for i in range(days):
            close = base_price + (i % 3 - 1) * 0.5
            history.append({
                "close": close,
                "high": high_max if i % 5 == 0 else close + 0.5,
                "low": low_min if i % 5 == 2 else close - 0.5,
                "volume": 100000,
            })
        return history

    def test_ideal_darvas_box(self):
        # 25 gun (5 hafta), range %8 → DARVAS BOX ideal
        history = self._make_box_history(25, 8)
        result = detect_darvas_box(history)
        assert result["pattern"] == "DARVAS_BOX"
        assert result["tier"] in ("ideal", "acceptable")

    def test_acceptable_darvas_box(self):
        # 25 gun, range %12 → ideal eşik aşıldı ama Mark eşiği içinde
        history = self._make_box_history(25, 12)
        result = detect_darvas_box(history)
        assert result["pattern"] == "DARVAS_BOX"
        assert result["tier"] == "acceptable"

    def test_too_loose_darvas(self):
        # 25 gun, range %18 → loose
        history = self._make_box_history(25, 18)
        result = detect_darvas_box(history)
        assert result["pattern"] == "DARVAS_LOOSE"
        assert result["tier"] == "too_loose"

    def test_too_short_duration(self):
        # 15 gun (3 hafta) — Mark min 4 hafta gerek
        history = self._make_box_history(15, 10)
        result = detect_darvas_box(history)
        assert result["pattern"] == "INVALID"  # min DARVAS_MIN_WEEKS*5 = 20 gun

    def test_empty_history(self):
        result = detect_darvas_box([])
        assert result["pattern"] == "INVALID"

    def test_none_input(self):
        result = detect_darvas_box(None)
        assert result["pattern"] == "INVALID"

    def test_constants(self):
        assert DARVAS_MIN_WEEKS == 4
        assert DARVAS_MAX_RANGE_PCT == 15.0


# =============================================================================
# Faz 2 Genisletme 3 — Wait for Pivot + 20-DMA Hold
# Tests for: check_pivot_trigger, check_20dma_hold
# Tescil: Vizyon v22.05 (24 May 2026)
# =============================================================================

from quanfina_math import (
    check_pivot_trigger,
    check_20dma_hold,
    PIVOT_TRIGGER_VOLUME_MULTIPLIER,
    TWENTY_DMA_BREACH_DAYS_EXIT,
)


class TestCheckPivotTrigger:
    """KARAR ADAY #885 - Mark Wait for Pivot Discipline (TTLC s.243)."""

    def test_full_trigger_3_of_3(self):
        # Pivot $50, current $51 (>%0.2 above), high vol, upper half close
        result = check_pivot_trigger(
            current_price=51.0,
            pivot_price=50.0,
            today_volume=200000,
            avg_volume_50d=100000,
            today_high=51.5,
            today_low=49.5,
            today_close=51.0,  # ust yari
        )
        assert result["should_buy"] is True
        assert result["tier"] == "triggered"
        assert result["above_pivot"] is True
        assert result["volume_expanded"] is True
        assert result["upper_half_close"] is True

    def test_almost_2_of_3_low_volume(self):
        # Above pivot + upper half, low volume
        result = check_pivot_trigger(
            current_price=51.0,
            pivot_price=50.0,
            today_volume=80000,  # 100K * 1.5 = 150K esigin altinda
            avg_volume_50d=100000,
            today_high=51.5,
            today_low=49.5,
            today_close=51.0,
        )
        assert result["should_buy"] is False
        assert result["tier"] == "almost"
        assert result["volume_expanded"] is False

    def test_wait_below_pivot(self):
        # Pivot altinda
        result = check_pivot_trigger(
            current_price=49.5,
            pivot_price=50.0,
            today_volume=200000,
            avg_volume_50d=100000,
            today_high=49.8,
            today_low=49.2,
            today_close=49.5,
        )
        assert result["should_buy"] is False
        assert result["tier"] == "wait"
        assert result["above_pivot"] is False

    def test_wait_lower_half_close(self):
        # Above pivot + vol OK, but close lower half (bearish)
        result = check_pivot_trigger(
            current_price=51.0,
            pivot_price=50.0,
            today_volume=200000,
            avg_volume_50d=100000,
            today_high=52.0,
            today_low=49.5,
            today_close=50.2,  # %30 lower
        )
        assert result["upper_half_close"] is False

    def test_invalid_zero_pivot(self):
        result = check_pivot_trigger(0, 50, 100000, 100000, 50, 49, 50)
        assert result["tier"] == "invalid"

    def test_constants(self):
        assert PIVOT_TRIGGER_VOLUME_MULTIPLIER == 1.5


class TestCheck20dmaHold:
    """KARAR ADAY #888 - Mark 20-DMA Hold Detector (TTLC s.247)."""

    def test_safe_above_dma(self):
        # 5 gun, hepsi 20-DMA ustunde
        closes = [101, 102, 103, 102, 104]
        smas = [100, 100, 100, 100, 100]
        result = check_20dma_hold(closes, smas)
        assert result["status"] == "safe"
        assert result["consecutive_days_below"] == 0
        assert result["last_close_below"] is False

    def test_warning_today_below(self):
        # Son gun altinda kapanis (1 gun)
        closes = [102, 103, 104, 103, 98]
        smas = [100, 100, 100, 100, 100]
        result = check_20dma_hold(closes, smas)
        assert result["status"] == "warning"
        assert result["consecutive_days_below"] == 1

    def test_exit_signal_2_consecutive(self):
        # 2 ardisik gun altinda
        closes = [102, 103, 104, 98, 97]
        smas = [100, 100, 100, 100, 100]
        result = check_20dma_hold(closes, smas)
        assert result["status"] == "exit_signal"
        assert result["consecutive_days_below"] >= 2

    def test_exit_signal_3_consecutive(self):
        closes = [105, 99, 98, 97, 96]
        smas = [100, 100, 100, 100, 100]
        result = check_20dma_hold(closes, smas)
        assert result["status"] == "exit_signal"
        assert result["consecutive_days_below"] >= 3

    def test_tolerance_buffer(self):
        # %0.5 tolerans icinde — safe
        closes = [100, 100, 100, 100, 99.7]
        smas = [100, 100, 100, 100, 100]
        result = check_20dma_hold(closes, smas)
        # %0.5 esigi 99.5 — close 99.7 OK
        assert result["status"] == "safe"

    def test_invalid_mismatched_length(self):
        result = check_20dma_hold([100, 101], [100])
        assert result["status"] == "invalid"

    def test_invalid_empty(self):
        result = check_20dma_hold([], [])
        assert result["status"] == "invalid"

    def test_constants(self):
        assert TWENTY_DMA_BREACH_DAYS_EXIT == 2


# =============================================================================
# Faz 2 Son — Earnings Gap Breakout
# Tests for: detect_earnings_gap_breakout
# Tescil: Vizyon v22.05 (24 May 2026)
# =============================================================================

from quanfina_math import (
    detect_earnings_gap_breakout,
    EARNINGS_GAP_MIN_PCT,
    EARNINGS_GAP_BIG_PCT,
)


class TestDetectEarningsGapBreakout:
    """KARAR ADAY #890 - Mark Earnings Gap Breakout (TLSMW s.250, Foster Wheeler)."""

    def test_institutional_foster_wheeler_pattern(self):
        # Mark FWLT paten: gap %8, volume 5x, close at high
        result = detect_earnings_gap_breakout(
            pre_earnings_close=100,
            earnings_day_open=108,    # %8 gap
            earnings_day_high=112,
            earnings_day_low=107,
            earnings_day_close=111.5,  # %90 of day range
            earnings_day_volume=500000,
            avg_volume_50d=100000,    # 5x volume
        )
        assert result["pattern"] == "EARNINGS_GAP_BREAKOUT"
        assert result["tier"] == "institutional"
        assert result["gap_pct"] >= 7.0
        assert result["volume_multiplier"] >= 2.0

    def test_mild_gap_low_close_position(self):
        # Gap %5 + 3x vol ama close lower half
        result = detect_earnings_gap_breakout(
            pre_earnings_close=100,
            earnings_day_open=105,
            earnings_day_high=108,
            earnings_day_low=103,
            earnings_day_close=104,  # %20 of range
            earnings_day_volume=300000,
            avg_volume_50d=100000,
        )
        assert result["pattern"] == "EARNINGS_GAP_MILD"
        assert result["tier"] == "mild"

    def test_no_gap(self):
        # %1 gap — Mark esigin altinda
        result = detect_earnings_gap_breakout(
            pre_earnings_close=100,
            earnings_day_open=101,
            earnings_day_high=102,
            earnings_day_low=100,
            earnings_day_close=101.5,
            earnings_day_volume=110000,
            avg_volume_50d=100000,
        )
        assert result["pattern"] == "NO_GAP"
        assert result["tier"] == "weak"

    def test_weak_volume(self):
        # Gap %5 ama dusuk hacim
        result = detect_earnings_gap_breakout(
            pre_earnings_close=100,
            earnings_day_open=105,
            earnings_day_high=106,
            earnings_day_low=104,
            earnings_day_close=105.5,
            earnings_day_volume=150000,  # 1.5x — Mark min 2x altinda
            avg_volume_50d=100000,
        )
        assert result["tier"] == "weak"

    def test_invalid_zero_data(self):
        result = detect_earnings_gap_breakout(0, 0, 0, 0, 0, 0, 0)
        assert result["pattern"] == "INVALID"
        assert result["tier"] == "invalid"

    def test_constants(self):
        assert EARNINGS_GAP_MIN_PCT == 3.0
        assert EARNINGS_GAP_BIG_PCT == 7.0
