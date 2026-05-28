"""
market_calendar.py pytest (Paket 362) — ABD borsa takvimi + saat dilimi mantigi.

Paper trading icin KRITIK: "piyasa acik mi", tatil, yari gun (early close),
ET<->TR donusumu yanlissa trade timing/sinyal yanlis olur. Tum fonksiyonlar
SAF (deterministik, DB/network yok) — sabit tarih/saatle test edilir.

Hafta gunu capasi: 2024-01-01 = Pazartesi -> 2026-01-01 = Persembe.
  2026-01-01 Per (New Year tatil) / 01-02 Cum / 01-03 Cmt / 01-04 Paz / 01-05 Pzt
"""
import sys
from datetime import date, datetime
from pathlib import Path

import pytest

HERE = Path(__file__).resolve().parent
PROJECT_ROOT = HERE.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from market_calendar import (
    is_weekend,
    is_us_holiday,
    get_holiday_name,
    is_early_close_day,
    is_us_market_day,
    last_trading_day_before,
    next_trading_day_after,
    is_us_market_open,
    to_tr_time,
    to_et_time,
    should_scan_today,
)


class TestIsWeekend:
    def test_saturday_sunday_true(self):
        assert is_weekend(date(2026, 1, 3)) is True   # Cmt
        assert is_weekend(date(2026, 1, 4)) is True   # Paz

    def test_weekday_false(self):
        assert is_weekend(date(2026, 1, 2)) is False  # Cum
        assert is_weekend(date(2026, 1, 5)) is False  # Pzt


class TestHolidays:
    def test_known_holidays(self):
        assert is_us_holiday(date(2026, 1, 1)) is True    # New Year
        assert is_us_holiday(date(2026, 12, 25)) is True  # Christmas
        assert is_us_holiday(date(2026, 5, 25)) is True   # Memorial Day

    def test_regular_day_not_holiday(self):
        assert is_us_holiday(date(2026, 1, 2)) is False

    def test_holiday_name(self):
        assert get_holiday_name(date(2026, 12, 25)) == "Christmas Day"
        assert get_holiday_name(date(2026, 1, 2)) is None


class TestEarlyClose:
    def test_early_close_days(self):
        assert is_early_close_day(date(2026, 11, 27)) is True  # Black Friday
        assert is_early_close_day(date(2026, 12, 24)) is True  # Christmas Eve

    def test_regular_day_not_early_close(self):
        assert is_early_close_day(date(2026, 1, 2)) is False

    def test_early_close_still_trading_day(self):
        # Yari gun de islem gunu sayilir (sadece kapanis erken)
        assert is_us_market_day(date(2026, 11, 27)) is True


class TestIsUsMarketDay:
    def test_trading_weekday(self):
        assert is_us_market_day(date(2026, 1, 2)) is True   # Cum
        assert is_us_market_day(date(2026, 1, 5)) is True   # Pzt

    def test_weekend_not_trading(self):
        assert is_us_market_day(date(2026, 1, 3)) is False  # Cmt

    def test_holiday_not_trading(self):
        assert is_us_market_day(date(2026, 1, 1)) is False  # New Year


class TestTradingDayNavigation:
    def test_last_trading_day_on_trading_day_returns_self(self):
        assert last_trading_day_before(date(2026, 1, 2)) == date(2026, 1, 2)  # Cum

    def test_last_trading_day_from_weekend(self):
        assert last_trading_day_before(date(2026, 1, 3)) == date(2026, 1, 2)  # Cmt -> Cum
        assert last_trading_day_before(date(2026, 1, 4)) == date(2026, 1, 2)  # Paz -> Cum

    def test_last_trading_day_from_holiday(self):
        # New Year (Per tatil) -> 2025-12-31 (Car, islem gunu)
        assert last_trading_day_before(date(2026, 1, 1)) == date(2025, 12, 31)

    def test_next_trading_day_skips_weekend(self):
        assert next_trading_day_after(date(2026, 1, 2)) == date(2026, 1, 5)  # Cum -> Pzt
        assert next_trading_day_after(date(2026, 1, 4)) == date(2026, 1, 5)  # Paz -> Pzt


class TestIsUsMarketOpen:
    def test_regular_day_mid_session_open(self):
        assert is_us_market_open(datetime(2026, 1, 2, 10, 0)) is True   # Cum 10:00 ET

    def test_before_open_closed(self):
        assert is_us_market_open(datetime(2026, 1, 2, 9, 0)) is False   # pre-market

    def test_after_close_closed(self):
        assert is_us_market_open(datetime(2026, 1, 2, 16, 30)) is False

    def test_exactly_open_time_open(self):
        assert is_us_market_open(datetime(2026, 1, 2, 9, 30)) is True   # open<=dt

    def test_exactly_close_time_closed(self):
        # dt < close (16:00 < 16:00 = False) -> kapali
        assert is_us_market_open(datetime(2026, 1, 2, 16, 0)) is False

    def test_weekend_closed(self):
        assert is_us_market_open(datetime(2026, 1, 3, 11, 0)) is False  # Cmt

    def test_holiday_closed(self):
        assert is_us_market_open(datetime(2026, 1, 1, 11, 0)) is False  # New Year

    def test_early_close_before_1pm_open(self):
        assert is_us_market_open(datetime(2026, 11, 27, 12, 0)) is True  # Black Friday 12:00

    def test_early_close_after_1pm_closed(self):
        assert is_us_market_open(datetime(2026, 11, 27, 13, 30)) is False  # 13:00 kapandi


class TestTimezoneConversions:
    def test_et_to_tr_winter_est(self):
        # Ocak = EST (UTC-5), TR = UTC+3 -> +8 saat. 9:30 ET -> 17:30 TR
        tr = to_tr_time(datetime(2026, 1, 2, 9, 30))
        assert (tr.hour, tr.minute) == (17, 30)

    def test_et_to_tr_summer_edt(self):
        # Temmuz = EDT (UTC-4), TR = UTC+3 -> +7 saat. 9:30 ET -> 16:30 TR
        tr = to_tr_time(datetime(2026, 7, 2, 9, 30))
        assert (tr.hour, tr.minute) == (16, 30)

    def test_tr_to_et_winter(self):
        # 17:30 TR (UTC+3) -> 9:30 ET (EST UTC-5)
        et = to_et_time(datetime(2026, 1, 2, 17, 30))
        assert (et.hour, et.minute) == (9, 30)


class TestShouldScanToday:
    def test_trading_day_runs(self):
        ok, reason = should_scan_today(date(2026, 1, 2))  # Cum
        assert ok is True

    def test_weekend_skips(self):
        ok, reason = should_scan_today(date(2026, 1, 3))  # Cmt
        assert ok is False
        assert "Hafta sonu" in reason

    def test_holiday_skips(self):
        ok, reason = should_scan_today(date(2026, 1, 1))  # New Year
        assert ok is False
        assert "New Year" in reason
