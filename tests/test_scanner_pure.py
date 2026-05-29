"""
scanner.py SAF (pure) fonksiyon testleri (Paket 370).

scanner.py 1955 satir günlük tarama motoru — büyük kısmı network/DB-coupled
(Finviz/yfinance/Cloud SQL), kapsamlı test YOK. Bu test SAF fonksiyonları kapsar
(network/DB gerekmez, deterministik): grade hesabı + yüzde parse + earnings tarih
parse. Bunlar tarama sonuçlarını şekillendirir (grade → screen sonucu → /minervini
sayfası P368). In-place test (scanner.py DEĞİŞMEZ → sıfır risk); ileride güvenli
refactor için temel.
"""
import sys
from datetime import date
from pathlib import Path

import pytest

HERE = Path(__file__).resolve().parent
PROJECT_ROOT = HERE.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

try:
    from scanner import _parse_pct, _compute_grade, parse_earnings_date
except ImportError as e:
    pytest.skip(f"scanner import edilemedi: {e}", allow_module_level=True)


class TestComputeGrade:
    """_compute_grade — EPS Q/Q + Sales Q/Q → A/B/C/D (tarama grade'i, screen sürücüsü).

    Eşikler (strict >): A: EPS>40 & Sales>25 · B: EPS>25 & Sales>15 ·
    C: EPS>20 & Sales>10 · aksi/None: D.
    """

    def test_grade_a(self):
        assert _compute_grade(50, 30) == "A"
        assert _compute_grade(41, 26) == "A"   # sınır üstü

    def test_grade_b(self):
        assert _compute_grade(30, 20) == "B"    # A değil (30<=40), B (30>25, 20>15)
        assert _compute_grade(40, 25) == "B"    # A sınırı: 40 NOT >40 → B

    def test_grade_c(self):
        assert _compute_grade(25, 15) == "C"    # B sınırı: 25 NOT >25 → C (25>20, 15>10)
        assert _compute_grade(21, 11) == "C"

    def test_grade_d_below_thresholds(self):
        assert _compute_grade(20, 10) == "D"    # C sınırı: 20 NOT >20 → D
        assert _compute_grade(10, 5) == "D"
        assert _compute_grade(0, 0) == "D"

    def test_grade_d_none_inputs(self):
        # Yetersiz veri → D (Mark: veri yoksa güvenli taraf)
        assert _compute_grade(None, 30) == "D"
        assert _compute_grade(50, None) == "D"
        assert _compute_grade(None, None) == "D"

    def test_grade_requires_both_metrics(self):
        # Yüksek EPS ama düşük Sales → A/B/C downgrade
        assert _compute_grade(100, 5) == "D"    # EPS>40 ama Sales 5<=10
        assert _compute_grade(100, 12) == "C"   # Sales 12 → sadece C eşiği


class TestParsePct:
    def test_percent_strings(self):
        assert _parse_pct("96.65%") == 96.65
        assert _parse_pct("-12.3%") == -12.3
        assert _parse_pct("50") == 50.0       # % yoksa da parse

    def test_empty_and_dash_and_nan(self):
        assert _parse_pct("") is None
        assert _parse_pct("-") is None
        assert _parse_pct("nan") is None
        assert _parse_pct("NaN") is None
        assert _parse_pct(None) is None

    def test_invalid_returns_none(self):
        assert _parse_pct("abc") is None
        assert _parse_pct("12.3.4%") is None


class TestParseEarningsDate:
    def test_empty_invalid_none(self):
        assert parse_earnings_date("") is None
        assert parse_earnings_date("-") is None
        assert parse_earnings_date("N/A") is None
        assert parse_earnings_date(None) is None

    def test_single_token_none(self):
        assert parse_earnings_date("Apr") is None

    def test_invalid_month_day_none(self):
        assert parse_earnings_date("Zzz 99 AMC") is None

    def test_valid_date_parsed(self):
        # "Dec 15 AMC" → bu yıl veya gelecek yıl Aralık 15 (>= bugün-180gün)
        d = parse_earnings_date("Dec 15 AMC")
        assert isinstance(d, date)
        assert d.month == 12 and d.day == 15

    def test_valid_date_with_session_suffix(self):
        # "Apr 30 BMO" → ay/gün stabil (yıl bugüne göre)
        d = parse_earnings_date("Apr 30 BMO")
        assert isinstance(d, date)
        assert d.month == 4 and d.day == 30
