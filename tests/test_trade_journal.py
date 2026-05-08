"""
TradeJournal CRUD testleri.
Her test savepoint/ROLLBACK TO SAVEPOINT ile izole edilir — veri DB'de kalmaz.
"""

import pytest
from datetime import date, datetime

import sys, os
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from db_connection import get_connection
from trade_journal import TradeJournal, ALLOWED_UPDATE_FIELDS


# ── Fixtures ──────────────────────────────────────────────────────────────────

@pytest.fixture
def journal():
    """Her test için fresh TradeJournal — sonunda ROLLBACK TO SAVEPOINT ile temizlenir."""
    conn = get_connection()
    conn.autocommit = False
    cur = conn.cursor()
    cur.execute("SAVEPOINT test_savepoint")
    cur.close()
    tj = TradeJournal(conn=conn)
    yield tj
    cur = conn.cursor()
    cur.execute("ROLLBACK TO SAVEPOINT test_savepoint")
    cur.close()
    conn.close()


def _add_sample_trade(tj, symbol='NVDA', invest_type=1,
                      entry_price=100.0, stop_loss=95.0, quantity=100) -> int:
    return tj.add_trade(
        symbol=symbol,
        invest_type=invest_type,
        entry_date=date(2025, 1, 15),
        entry_price=entry_price,
        stop_loss=stop_loss,
        quantity=quantity,
    )


# ── TestAddTrade ──────────────────────────────────────────────────────────────

class TestAddTrade:

    def test_basic_insert_returns_id(self, journal):
        tid = _add_sample_trade(journal)
        assert isinstance(tid, int)
        assert tid > 0

    def test_trade_type_derived_from_invest_type_long(self, journal):
        tid = _add_sample_trade(journal, invest_type=1)
        trade = journal.get_trade(tid)
        assert trade['trade_type'] == 'Long'
        assert trade['invest_type'] == 1

    def test_trade_type_derived_from_invest_type_short(self, journal):
        tid = journal.add_trade(
            symbol='TSLA', invest_type=2,
            entry_date=date(2025, 1, 15),
            entry_price=200.0, stop_loss=210.0, quantity=50,
        )
        trade = journal.get_trade(tid)
        assert trade['trade_type'] == 'Short'
        assert trade['invest_type'] == 2

    def test_risk_amount_auto_calculated_long(self, journal):
        # entry=100, stop=95, qty=100 → risk = 5*100 = 500
        tid = _add_sample_trade(journal, entry_price=100.0, stop_loss=95.0, quantity=100)
        trade = journal.get_trade(tid)
        assert float(trade['risk_amount']) == pytest.approx(500.0)
        assert float(trade['risk_pct']) == pytest.approx(5.0)

    def test_risk_amount_auto_calculated_short(self, journal):
        # entry=200, stop=210, qty=50, SHORT → risk = 10*50 = 500
        tid = journal.add_trade(
            symbol='TSLA', invest_type=2,
            entry_date=date(2025, 1, 15),
            entry_price=200.0, stop_loss=210.0, quantity=50,
        )
        trade = journal.get_trade(tid)
        assert float(trade['risk_amount']) == pytest.approx(500.0)

    def test_snapshot_columns_stored(self, journal):
        tid = journal.add_trade(
            symbol='AAPL', invest_type=1,
            entry_date=date(2025, 1, 15),
            entry_price=150.0, stop_loss=145.0, quantity=10,
            entry_tpr_score=8.5, entry_high_52wk=180.0,
        )
        trade = journal.get_trade(tid)
        assert float(trade['entry_tpr_score']) == pytest.approx(8.5)
        assert float(trade['entry_high_52wk']) == pytest.approx(180.0)

    def test_invalid_invest_type_raises(self, journal):
        with pytest.raises(ValueError, match='invest_type'):
            journal.add_trade(
                symbol='X', invest_type=3,
                entry_date=date(2025, 1, 15),
                entry_price=50.0, stop_loss=48.0, quantity=10,
            )

    def test_default_status_is_open(self, journal):
        tid = _add_sample_trade(journal)
        trade = journal.get_trade(tid)
        assert trade['status'] == 'Open'


# ── TestLegOperations ─────────────────────────────────────────────────────────

class TestLegOperations:

    def test_add_leg_returns_id(self, journal):
        tid = _add_sample_trade(journal)
        lid = journal.add_leg(tid, shares=100, price=100.0, leg_date=date(2025, 1, 15))
        assert isinstance(lid, int)
        assert lid > 0

    def test_leg_idx_auto_increment(self, journal):
        tid = _add_sample_trade(journal)
        l1 = journal.add_leg(tid, shares=50, price=100.0, leg_date=date(2025, 1, 15))
        l2 = journal.add_leg(tid, shares=50, price=102.0, leg_date=date(2025, 1, 20))
        trade = journal.get_trade(tid, include_legs=True)
        idxs = [leg['leg_idx'] for leg in trade['legs']]
        assert idxs == [1, 2]

    def test_multiple_legs_attached_to_trade(self, journal):
        tid = _add_sample_trade(journal)
        for i in range(3):
            journal.add_leg(tid, shares=33, price=100.0 + i, leg_date=date(2025, 1, 15))
        trade = journal.get_trade(tid, include_legs=True)
        assert len(trade['legs']) == 3

    def test_explicit_leg_idx(self, journal):
        tid = _add_sample_trade(journal)
        journal.add_leg(tid, shares=100, price=100.0, leg_date=date(2025, 1, 15), leg_idx=5)
        trade = journal.get_trade(tid, include_legs=True)
        assert trade['legs'][0]['leg_idx'] == 5

    def test_duplicate_leg_idx_raises(self, journal):
        tid = _add_sample_trade(journal)
        journal.add_leg(tid, shares=100, price=100.0, leg_date=date(2025, 1, 15), leg_idx=1)
        with pytest.raises(ValueError):
            journal.add_leg(tid, shares=50, price=101.0, leg_date=date(2025, 1, 16), leg_idx=1)


# ── TestExitOperations ────────────────────────────────────────────────────────

class TestExitOperations:

    def test_add_exit_returns_id(self, journal):
        tid = _add_sample_trade(journal)
        lid = journal.add_leg(tid, shares=100, price=100.0, leg_date=date(2025, 1, 15))
        eid = journal.add_exit(lid, shares=50, price=105.0, exit_date=date(2025, 2, 1))
        assert isinstance(eid, int)
        assert eid > 0

    def test_exit_stored_with_reason(self, journal):
        tid = _add_sample_trade(journal)
        lid = journal.add_leg(tid, shares=100, price=100.0, leg_date=date(2025, 1, 15))
        eid = journal.add_exit(lid, shares=100, price=110.0,
                               exit_date=date(2025, 2, 1), reason='target')
        trade = journal.get_trade(tid, include_legs=True, include_exits=True)
        assert trade['legs'][0]['exits'][0]['reason'] == 'target'

    def test_all_valid_reasons_accepted(self, journal):
        tid = _add_sample_trade(journal)
        for reason in ('sbe', 'stop', 'target', 'manual', 'trailing'):
            lid = journal.add_leg(tid, shares=10, price=100.0, leg_date=date(2025, 1, 15))
            journal.add_exit(lid, shares=10, price=105.0,
                             exit_date=date(2025, 2, 1), reason=reason)

    def test_invalid_reason_raises(self, journal):
        tid = _add_sample_trade(journal)
        lid = journal.add_leg(tid, shares=100, price=100.0, leg_date=date(2025, 1, 15))
        with pytest.raises(ValueError, match='reason'):
            journal.add_exit(lid, shares=50, price=105.0,
                             exit_date=date(2025, 2, 1), reason='yanlış')


# ── TestStopHistory ───────────────────────────────────────────────────────────

class TestStopHistory:

    def test_update_stop_returns_id(self, journal):
        tid = _add_sample_trade(journal)
        sid = journal.update_stop(tid, new_stop_price=97.0)
        assert isinstance(sid, int)
        assert sid > 0

    def test_stop_history_row_created(self, journal):
        tid = _add_sample_trade(journal)
        journal.update_stop(tid, new_stop_price=97.0, reason='trailing', note='SBE kondu')
        trade = journal.get_trade(tid, include_stops=True)
        assert len(trade['stops']) == 1
        assert float(trade['stops'][0]['stop_price']) == pytest.approx(97.0)
        assert trade['stops'][0]['reason'] == 'trailing'

    def test_trades_stop_loss_updated(self, journal):
        tid = _add_sample_trade(journal, stop_loss=95.0)
        journal.update_stop(tid, new_stop_price=98.0)
        trade = journal.get_trade(tid)
        assert float(trade['stop_loss']) == pytest.approx(98.0)

    def test_invalid_stop_reason_raises(self, journal):
        tid = _add_sample_trade(journal)
        with pytest.raises(ValueError, match='reason'):
            journal.update_stop(tid, new_stop_price=97.0, reason='yanlış')


# ── TestGetTrade ──────────────────────────────────────────────────────────────

class TestGetTrade:

    def test_returns_correct_trade(self, journal):
        tid = _add_sample_trade(journal, symbol='GOOGL')
        trade = journal.get_trade(tid)
        assert trade['id'] == tid
        assert trade['symbol'] == 'GOOGL'

    def test_returns_none_for_nonexistent(self, journal):
        assert journal.get_trade(999999) is None

    def test_soft_deleted_not_returned(self, journal):
        tid = _add_sample_trade(journal)
        journal.remove_trade(tid, hard=False)
        assert journal.get_trade(tid) is None

    def test_include_legs_flag(self, journal):
        tid = _add_sample_trade(journal)
        journal.add_leg(tid, shares=100, price=100.0, leg_date=date(2025, 1, 15))
        trade = journal.get_trade(tid, include_legs=True)
        assert 'legs' in trade
        assert len(trade['legs']) == 1

    def test_include_exits_nested_in_legs(self, journal):
        tid = _add_sample_trade(journal)
        lid = journal.add_leg(tid, shares=100, price=100.0, leg_date=date(2025, 1, 15))
        journal.add_exit(lid, shares=50, price=105.0, exit_date=date(2025, 2, 1))
        trade = journal.get_trade(tid, include_legs=True, include_exits=True)
        assert len(trade['legs'][0]['exits']) == 1

    def test_include_stops_flag(self, journal):
        tid = _add_sample_trade(journal)
        journal.update_stop(tid, new_stop_price=97.0)
        trade = journal.get_trade(tid, include_stops=True)
        assert 'stops' in trade
        assert len(trade['stops']) == 1


# ── TestGetTrades ─────────────────────────────────────────────────────────────

class TestGetTrades:

    def test_filter_by_status(self, journal):
        t1 = _add_sample_trade(journal, symbol='AAA')
        t2 = _add_sample_trade(journal, symbol='BBB')
        journal.update_trade(t2, status='Closed')
        open_trades = journal.get_trades(status='Open')
        symbols = [t['symbol'] for t in open_trades]
        assert 'AAA' in symbols
        assert 'BBB' not in symbols

    def test_filter_by_symbol(self, journal):
        _add_sample_trade(journal, symbol='NVDA')
        _add_sample_trade(journal, symbol='AAPL')
        result = journal.get_trades(symbol='NVDA')
        assert all(t['symbol'] == 'NVDA' for t in result)

    def test_limit_applied(self, journal):
        for _ in range(5):
            _add_sample_trade(journal)
        result = journal.get_trades(limit=2)
        assert len(result) <= 2

    def test_include_deleted_flag(self, journal):
        tid = _add_sample_trade(journal, symbol='TODEL')
        journal.remove_trade(tid, hard=False)
        without_deleted = journal.get_trades(symbol='TODEL')
        with_deleted = journal.get_trades(symbol='TODEL', include_deleted=True)
        assert len(without_deleted) == 0
        assert len(with_deleted) == 1


# ── TestUpdateTrade ───────────────────────────────────────────────────────────

class TestUpdateTrade:

    def test_update_allowed_field(self, journal):
        tid = _add_sample_trade(journal)
        result = journal.update_trade(tid, notes='Güncellendi')
        assert result is True
        trade = journal.get_trade(tid)
        assert trade['notes'] == 'Güncellendi'

    def test_update_multiple_fields(self, journal):
        tid = _add_sample_trade(journal)
        journal.update_trade(tid, notes='Not', manual_grade='A', strategy='breakout')
        trade = journal.get_trade(tid)
        assert trade['notes'] == 'Not'
        assert trade['manual_grade'] == 'A'

    def test_invalid_field_raises_value_error(self, journal):
        tid = _add_sample_trade(journal)
        with pytest.raises(ValueError, match='İzin verilmeyen'):
            journal.update_trade(tid, entry_price=999.0)

    def test_update_nonexistent_returns_false(self, journal):
        result = journal.update_trade(999999, notes='yok')
        assert result is False


# ── TestRemoveTrade ───────────────────────────────────────────────────────────

class TestRemoveTrade:

    def test_soft_delete_sets_deleted_at(self, journal):
        tid = _add_sample_trade(journal)
        result = journal.remove_trade(tid, hard=False)
        assert result is True
        assert journal.get_trade(tid) is None

    def test_hard_delete_removes_trade(self, journal):
        tid = _add_sample_trade(journal)
        lid = journal.add_leg(tid, shares=100, price=100.0, leg_date=date(2025, 1, 15))
        result = journal.remove_trade(tid, hard=True)
        assert result is True
        assert journal.get_trade(tid) is None

    def test_hard_delete_cascades_to_legs(self, journal):
        tid = _add_sample_trade(journal)
        lid = journal.add_leg(tid, shares=100, price=100.0, leg_date=date(2025, 1, 15))
        journal.remove_trade(tid, hard=True)
        with journal._cursor() as cur:
            cur.execute("SELECT id FROM trade_legs WHERE id = %s", (lid,))
            assert cur.fetchone() is None

    def test_remove_nonexistent_returns_false(self, journal):
        result = journal.remove_trade(999999, hard=False)
        assert result is False


# ── TestCloseTrade ────────────────────────────────────────────────────────────

class TestCloseTrade:

    def test_close_trade_updates_status(self, journal):
        tid = _add_sample_trade(journal, entry_price=100.0)
        summary = journal.close_trade(tid, exit_date=date(2025, 3, 1), exit_price=110.0)
        assert summary['trade_id'] == tid
        trade = journal.get_trade(tid)
        assert trade['status'] == 'Closed'

    def test_close_trade_pnl_long(self, journal):
        # entry=100, exit=110, qty=100 → pnl = 1000
        tid = _add_sample_trade(journal, entry_price=100.0, stop_loss=95.0, quantity=100)
        summary = journal.close_trade(tid, exit_date=date(2025, 3, 1), exit_price=110.0)
        assert summary['total_pnl'] == pytest.approx(1000.0)

    def test_close_trade_pnl_short(self, journal):
        # entry=200, stop=210, exit=190, qty=50, SHORT → pnl = (200-190)*50 = 500
        tid = journal.add_trade(
            symbol='TSLA', invest_type=2,
            entry_date=date(2025, 1, 15),
            entry_price=200.0, stop_loss=210.0, quantity=50,
        )
        summary = journal.close_trade(tid, exit_date=date(2025, 3, 1), exit_price=190.0)
        assert summary['total_pnl'] == pytest.approx(500.0)

    def test_close_trade_with_legs(self, journal):
        tid = _add_sample_trade(journal, entry_price=100.0)
        journal.add_leg(tid, shares=60, price=100.0, leg_date=date(2025, 1, 15))
        journal.add_leg(tid, shares=40, price=102.0, leg_date=date(2025, 1, 20))
        summary = journal.close_trade(tid, exit_date=date(2025, 3, 1), exit_price=112.0)
        assert summary['closed_legs'] == 2

    def test_close_already_closed_raises(self, journal):
        tid = _add_sample_trade(journal)
        journal.close_trade(tid, exit_date=date(2025, 3, 1), exit_price=110.0)
        with pytest.raises(ValueError, match='zaten kapalı'):
            journal.close_trade(tid, exit_date=date(2025, 3, 2), exit_price=115.0)

    def test_close_partial_exit_leg(self, journal):
        # Leg'in yarısı zaten çıkmış — close_trade kalan yarıyı çıkarmalı
        tid = _add_sample_trade(journal, entry_price=100.0)
        lid = journal.add_leg(tid, shares=100, price=100.0, leg_date=date(2025, 1, 15))
        journal.add_exit(lid, shares=50, price=108.0, exit_date=date(2025, 2, 1))
        summary = journal.close_trade(tid, exit_date=date(2025, 3, 1), exit_price=112.0)
        assert summary['closed_legs'] == 1  # 1 leg'in kalanı kapatıldı


# ── TestComputeSummary ────────────────────────────────────────────────────────

class TestComputeSummary:

    def _setup_closed_trades(self, journal):
        """2 kazanan, 1 kayıp trade oluşturur."""
        for entry, exit_, qty in [(100, 110, 100), (50, 55, 200), (80, 75, 100)]:
            tid = journal.add_trade(
                symbol='X', invest_type=1,
                entry_date=date(2025, 1, 10),
                entry_price=entry, stop_loss=entry - 5, quantity=qty,
            )
            pnl = (exit_ - entry) * qty
            pnl_pct = (exit_ - entry) / entry * 100
            journal.update_trade(
                tid, status='Closed', exit_price=exit_,
                profit_loss=pnl, pnl_pct=pnl_pct,
            )

    def test_total_trades_count(self, journal):
        self._setup_closed_trades(journal)
        # date(2025,12,31) filtresi: production DB'deki 2026 tarihli trade'leri dışlar
        s = journal.compute_summary(end_date=date(2025, 12, 31))
        assert s['total_trades'] == 3

    def test_win_rate(self, journal):
        self._setup_closed_trades(journal)
        # date(2025,12,31) filtresi: production DB'deki 2026 tarihli trade'leri dışlar
        s = journal.compute_summary(end_date=date(2025, 12, 31))
        assert s['wins'] == 2
        assert s['losses'] == 1
        assert s['win_rate'] == pytest.approx(66.67, abs=0.1)

    def test_date_filter(self, journal):
        self._setup_closed_trades(journal)
        # Farklı tarihli trade ekle
        tid = journal.add_trade(
            symbol='Y', invest_type=1,
            entry_date=date(2026, 6, 1),
            entry_price=100.0, stop_loss=95.0, quantity=10,
        )
        journal.update_trade(tid, status='Closed', profit_loss=50.0, pnl_pct=5.0)
        s = journal.compute_summary(end_date=date(2025, 12, 31))
        assert s['total_trades'] == 3  # 2026 tarihli hariç


# ── TestContextManager ────────────────────────────────────────────────────────

class TestContextManager:

    def test_with_block_closes_internal_conn(self):
        with TradeJournal() as tj:
            assert tj.conn is not None
            assert not tj._external_conn
        assert tj.conn.closed != 0  # psycopg2: 0=açık, != 0 = kapalı

    def test_with_block_does_not_close_external_conn(self):
        conn = get_connection()
        conn.autocommit = False
        cur = conn.cursor()
        cur.execute("SAVEPOINT ctx_test")
        cur.close()
        try:
            with TradeJournal(conn=conn) as tj:
                assert tj._external_conn
            # Dışarıdan verilen conn hâlâ açık olmalı
            assert conn.closed == 0
        finally:
            cur = conn.cursor()
            cur.execute("ROLLBACK TO SAVEPOINT ctx_test")
            cur.close()
            conn.close()
