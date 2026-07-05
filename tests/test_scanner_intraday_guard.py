"""scanner.run_scan intraday partial-bar guard testleri (B6-01, 05 Tem 2026).

Kok neden (FAZ-1 denetim B6-01): run_scan gun-seviyesi takvim guard'ina (hafta
sonu/tatil) sahipti ama SAAT kontrolu yoktu. ABD ana seansi (9:30-16:00 ET) ACIKKEN
manuel /scan -> yfinance/Finviz partial (kapanmamis) bar'i "bugunun close'u" sanip
DB'ye yazar -> point-in-time butunlugu bozulur (H#17 ailesi — sessiz bozuk veri).

Fix: should_scan_today blogundan sonra is_us_market_open() guard'i. Bu testler 4 yolu
sabitler: (a) acik+no-force -> BLOCK, (b) kapali -> gecer, (c) force -> bypass,
(d) scan_date_override -> bypass. is_us_market_open + should_scan_today mock'lu; guard'in
DB'ye ULASMADAN return ettigini health_check_finviz sentinel'i ile dogrular (DB/network yok).
"""
import sys
from pathlib import Path

import pytest

HERE = Path(__file__).resolve().parent
PROJECT_ROOT = HERE.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

try:
    import scanner
    import market_calendar
except ImportError as e:  # pragma: no cover
    pytest.skip(f"scanner import edilemedi: {e}", allow_module_level=True)


class _ReachedDBError(RuntimeError):
    """health_check_finviz'e ULASILDI sentinel'i — guard gecmis demektir.

    ScannerHealthError DEGIL (o run_scan icinde yakalanir); RuntimeError propagate eder
    -> 'guard'i gecti' ile 'guard bloklamadan return etti' ayrimini net yapar.
    """


@pytest.fixture
def guard_env(monkeypatch):
    """Guard'a kadar deterministik ortam: should_scan_today True (takvim gecer),
    health_check_finviz sentinel (cagirilirsa guard'i gecti demek)."""
    monkeypatch.setattr(
        market_calendar, "should_scan_today",
        lambda today_et=None: (True, "test trading day"), raising=False,
    )

    reached = {"db": False}

    def _sentinel_health():
        reached["db"] = True
        raise _ReachedDBError("guard gecti — DB asamasina ulasildi")

    monkeypatch.setattr(scanner, "health_check_finviz", _sentinel_health, raising=False)
    return reached


def test_a_intraday_open_no_force_blocks(guard_env, monkeypatch):
    # Market ACIK + force yok + override yok -> BLOCK, DB'ye ULASMADAN return None
    monkeypatch.setattr(market_calendar, "is_us_market_open", lambda dt=None: True, raising=False)
    result = scanner.run_scan()
    assert result is None
    assert guard_env["db"] is False, "Intraday guard bloklamadi — DB asamasina ulasti"


def test_b_market_closed_proceeds(guard_env, monkeypatch):
    # Market KAPALI (kapanis sonrasi/nightly) -> guard gecer, DB asamasina ulasir
    monkeypatch.setattr(market_calendar, "is_us_market_open", lambda dt=None: False, raising=False)
    with pytest.raises(_ReachedDBError):
        scanner.run_scan()
    assert guard_env["db"] is True


def test_c_force_bypasses_open_guard(guard_env, monkeypatch):
    # Market ACIK ama force=True -> bilerek zorlama, guard bypass, DB asamasina ulasir
    monkeypatch.setattr(market_calendar, "is_us_market_open", lambda dt=None: True, raising=False)
    with pytest.raises(_ReachedDBError):
        scanner.run_scan(force=True)
    assert guard_env["db"] is True


def test_d_date_override_bypasses_open_guard(guard_env, monkeypatch):
    # Market ACIK ama scan_date_override set (backfill) -> EOD kabul, guard bypass
    monkeypatch.setattr(market_calendar, "is_us_market_open", lambda dt=None: True, raising=False)
    with pytest.raises(_ReachedDBError):
        scanner.run_scan(scan_date_override="2026-06-15")
    assert guard_env["db"] is True
