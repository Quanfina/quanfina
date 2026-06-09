"""
P423 (31 May 2026 — Kural #28 TEMEL "D" belirsizligi): _grade_no_data + _is_empty_pct
birim testleri.

scanner_helpers._compute_grade eps_qoq/sales_qoq None -> "D" (yetersiz veri).
Gercekten zayif temel de "D". UI ayrimi icin db_helpers._grade_no_data:
- grade='D' + (eps VEYA sales bos) -> True  (veri yok, zayif DEGIL)
- grade='D' + (ikisi de dolu)      -> False (gercekten zayif)
- grade A/B/C                       -> False (zaten veri var)
"""
import sys
from pathlib import Path

HERE = Path(__file__).resolve().parent
PROJECT_ROOT = HERE.parent
API_DIR = PROJECT_ROOT / "api"
for d in (str(PROJECT_ROOT), str(API_DIR)):
    if d not in sys.path:
        sys.path.insert(0, d)

import pytest

try:
    from db_helpers import _grade_no_data, _is_empty_pct
except ImportError:
    pytest.skip("db_helpers import edilemedi (DB ortam yok)", allow_module_level=True)


class TestIsEmptyPct:
    @pytest.mark.parametrize("v", [None, "", " ", "-", "N/A", "n/a", "na", "none", "NaN", "—", "null"])
    def test_empty_tokens(self, v):
        assert _is_empty_pct(v) is True

    @pytest.mark.parametrize("v", ["45.2", "45.2%", "12", "0", "0.0", "-3.5", "100"])
    def test_real_values_not_empty(self, v):
        # Gercek sayi (negatif dahil) -> veri VAR
        assert _is_empty_pct(v) is False


class TestGradeNoData:
    def test_d_with_both_empty_is_no_data(self):
        # D + eps yok + sales yok -> veri yok (True)
        assert _grade_no_data("D", None, None) is True

    def test_d_with_eps_empty_is_no_data(self):
        # _compute_grade: eps None -> D. sales dolu olsa bile veri eksik.
        assert _grade_no_data("D", None, "20") is True

    def test_d_with_sales_empty_is_no_data(self):
        assert _grade_no_data("D", "30", "") is True

    def test_d_with_both_present_is_real_weak(self):
        # D + eps=%10 + sales=%5 -> gercekten zayif (False, kirmizi kalir)
        assert _grade_no_data("D", "10", "5") is False

    def test_d_with_real_negative_values_is_weak(self):
        # Negatif buyume gercek veridir -> zayif D (False)
        assert _grade_no_data("D", "-5.2", "-3.1") is False

    @pytest.mark.parametrize("grade", ["A", "B", "C"])
    def test_abc_never_no_data(self, grade):
        # A/B/C zaten veri var demek -> asla no_data
        assert _grade_no_data(grade, None, None) is False

    def test_none_grade_not_no_data(self):
        # grade None (hic grade yok) -> no_data DEGIL (ayri durum, UI "—" gosterir)
        assert _grade_no_data(None, None, None) is False

    def test_percent_suffix_tolerated(self):
        # "%" sonekli gercek deger -> veri var
        assert _grade_no_data("D", "45.2%", "25.1%") is False
