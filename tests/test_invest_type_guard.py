"""
P567 (20 Haz 2026): _web_trades_has_invest_type cache — Migration 013 sonrası RESTART YOK.

Bug (önce): sonuç kalıcı cache'leniyordu (None→False sabit). Migration 013 çalıştırılınca
False cache sabit kalıp SHORT'u API restart'a kadar bloklardı. Fix: SADECE pozitif (True)
kalıcı; False her çağrı yeniden sorgular → migration sonrası SHORT restart'sız aktif.
Kolon sadece ADD edilir, asla DROP (Kodlama Standardı #2) → True kalıcı güvenli.
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
    import db_helpers
    _HAS = True
except ImportError:
    _HAS = False


class _FakeResult:
    def __init__(self, has): self._has = has
    def first(self): return (1,) if self._has else None


class _FakeConn:
    def __init__(self, has): self._has = has
    def __enter__(self): return self
    def __exit__(self, *a): return False
    def execute(self, *a, **k): return _FakeResult(self._has)


class _FakeEngine:
    """connect() bağlam yöneticisi — kolon var/yok kontrollü."""
    def __init__(self, has): self.has = has
    def connect(self): return _FakeConn(self.has)


@pytest.mark.skipif(not _HAS, reason="db_helpers import yok")
class TestInvestTypeGuardCache:
    def test_column_exists_true_and_cached(self, monkeypatch):
        monkeypatch.setattr(db_helpers, "_INVEST_TYPE_COL", None)
        monkeypatch.setattr(db_helpers, "engine", _FakeEngine(True))
        assert db_helpers._web_trades_has_invest_type() is True
        assert db_helpers._INVEST_TYPE_COL is True

    def test_column_missing_returns_false_not_sticky(self, monkeypatch):
        # Kolon yok → False AMA kalıcı False cache'lenMEZ (None kalır → tekrar sorgulanır)
        monkeypatch.setattr(db_helpers, "_INVEST_TYPE_COL", None)
        monkeypatch.setattr(db_helpers, "engine", _FakeEngine(False))
        assert db_helpers._web_trades_has_invest_type() is False
        assert db_helpers._INVEST_TYPE_COL is not True  # True'ya sabitlenmedi

    def test_migration_then_active_without_restart(self, monkeypatch):
        # KRİTİK: migration ÖNCE yok (False) → SONRA var (True) — RESTART YOK
        monkeypatch.setattr(db_helpers, "_INVEST_TYPE_COL", None)
        monkeypatch.setattr(db_helpers, "engine", _FakeEngine(False))
        assert db_helpers._web_trades_has_invest_type() is False  # migration öncesi
        # Migration 013 çalıştı → kolon artık var (aynı process, restart yok)
        monkeypatch.setattr(db_helpers, "engine", _FakeEngine(True))
        assert db_helpers._web_trades_has_invest_type() is True   # restart'sız aktif

    def test_once_true_stays_true_no_query(self, monkeypatch):
        # True bir kez set → kolon "yok" görünse bile True (cache, sorgu yok; kolon düşmez)
        monkeypatch.setattr(db_helpers, "_INVEST_TYPE_COL", True)
        monkeypatch.setattr(db_helpers, "engine", _FakeEngine(False))
        assert db_helpers._web_trades_has_invest_type() is True

    def test_db_error_graceful_false(self, monkeypatch):
        class _BoomEngine:
            def connect(self): raise RuntimeError("DB down")
        monkeypatch.setattr(db_helpers, "_INVEST_TYPE_COL", None)
        monkeypatch.setattr(db_helpers, "engine", _BoomEngine())
        assert db_helpers._web_trades_has_invest_type() is False  # patlamaz
