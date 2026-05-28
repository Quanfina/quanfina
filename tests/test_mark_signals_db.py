"""mark_signals_get_by_symbol pytest (KARAR ADAY #735 — MOCK->DB gecis).

Migration 004-009 sonrasi minervini_scans Mark canon kolonlari okuma.
Read-only test (DB degistirmez) — graceful NULL davranisi + tip garantisi.
DB erisilemezse modul skip.
"""
import pytest
import sys
from pathlib import Path

HERE = Path(__file__).resolve().parent
PROJECT_ROOT = HERE.parent
API_DIR = PROJECT_ROOT / "api"
for d in (str(PROJECT_ROOT), str(API_DIR)):
    if d not in sys.path:
        sys.path.insert(0, d)

try:
    from db_helpers import mark_signals_get_by_symbol, db_health_check
except ImportError:
    pytest.skip("db_helpers import yok", allow_module_level=True)

# DB erisilemezse tum modul skip (CI / Cloud SQL paused)
if not db_health_check():
    pytest.skip("Cloud SQL erisilemez (paused/IP)", allow_module_level=True)


def test_return_type_is_dict():
    """Her zaman dict doner (None degil)."""
    result = mark_signals_get_by_symbol("AAPL")
    assert isinstance(result, dict)


def test_unknown_symbol_empty_dict():
    """DB'de olmayan sembol -> bos dict (graceful)."""
    result = mark_signals_get_by_symbol("ZZ_INVALID_TICKER_999")
    assert result == {}


def test_known_symbol_no_crash():
    """DB'de var olan sembol (AAPL) -> dict, crash yok."""
    result = mark_signals_get_by_symbol("AAPL")
    assert isinstance(result, dict)


def test_null_columns_excluded():
    """NULL/False kolonlar dict'e eklenmez (MarkSignals graceful).

    Mevcut tarama (22 May) Mark kolonlari NULL -> bos veya kismi dict.
    power_play_pass sadece True ise key olur (False atlanir)."""
    result = mark_signals_get_by_symbol("AAPL")
    # power_play_pass varsa MUTLAKA True olmali (False asla eklenmez)
    if "power_play_pass" in result:
        assert result["power_play_pass"] is True


def test_carr_stage_int_when_present():
    """carr_stage varsa int tipinde (1-4)."""
    # Birkac sembol dene — biri dolu olabilir (scanner sonrasi)
    for sym in ("AAPL", "NVDA", "MSFT", "AVGO", "AMD"):
        result = mark_signals_get_by_symbol(sym)
        if "carr_stage" in result:
            assert isinstance(result["carr_stage"], int)
            assert result["carr_stage"] in (1, 2, 3, 4)


def test_vcp_quality_valid_value_when_present():
    """vcp_quality_score varsa 'EXCELLENT' veya 'PASS'."""
    for sym in ("AAPL", "NVDA", "MSFT", "AVGO"):
        result = mark_signals_get_by_symbol(sym)
        if "vcp_quality_score" in result:
            assert result["vcp_quality_score"] in ("EXCELLENT", "PASS")


def test_vcp_ready_score_int_when_present():
    """vcp_ready_score varsa int."""
    for sym in ("AAPL", "NVDA", "AMD"):
        result = mark_signals_get_by_symbol(sym)
        if "vcp_ready_score" in result:
            assert isinstance(result["vcp_ready_score"], int)


def test_no_false_or_null_keys():
    """Donen dict'te hicbir deger None veya False olmamali (graceful filtre)."""
    for sym in ("AAPL", "NVDA", "MSFT", "TSLA", "META"):
        result = mark_signals_get_by_symbol(sym)
        for key, val in result.items():
            assert val is not None, f"{sym}.{key} None olmamali"
            # power_play_pass disinda False de olmamali (bool alan sadece True eklenir)
            if isinstance(val, bool):
                assert val is True, f"{sym}.{key} False olmamali"
