"""
TradeJournal — trades, trade_legs, trade_exits, stop_history CRUD katmanı.

Production trade platformlarında required-fields ve multi-leg tracking pattern'i
yaygın; bu modül o yapıyı temiz bir API'ye dönüştürür.

Bağlantı yönetimi:
  - TradeJournal()          → kendi bağlantısını açar; commit/rollback yönetir.
  - TradeJournal(conn=conn) → dışarıdan bağlantı alır; commit yapmaz (caller yönetir).
    Bu ikinci mod test fixture'larında rollback ile veri temizlemeye olanak tanır.

9_Trade_Journal.py (journal_entries) ile çakışmaz — o tablo ayrı bir amaç için.
"""

import logging
import psycopg2
import psycopg2.extras
from db_connection import get_connection

log = logging.getLogger(__name__)

ALLOWED_UPDATE_FIELDS = {
    'notes', 'code', 'manual_grade', 'monalert_mode', 'strategy',
    'status', 'exit_date', 'exit_price', 'profit_loss', 'pnl_pct',
}

_VALID_EXIT_REASONS = {'sbe', 'stop', 'target', 'manual', 'trailing'}
_VALID_STOP_REASONS = {'initial', 'manual', 'trailing', 'sbe'}


class TradeJournal:

    def __init__(self, conn=None):
        self._external_conn = conn is not None
        self.conn = conn if conn is not None else get_connection()

    # ── Transaction helpers ───────────────────────────────────────────────

    def _commit(self):
        if not self._external_conn:
            self.conn.commit()

    def _rollback(self):
        if not self._external_conn:
            self.conn.rollback()

    def _cursor(self):
        return self.conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)

    # ── Context manager ───────────────────────────────────────────────────

    def __enter__(self):
        return self

    def __exit__(self, *_):
        if not self._external_conn:
            self.conn.close()

    # ── Internal helpers ──────────────────────────────────────────────────

    @staticmethod
    def _invest_to_trade_type(invest_type: int) -> str:
        return 'Long' if invest_type == 1 else 'Short'

    @staticmethod
    def _compute_risk(entry: float, stop: float, qty: int, invest_type: int):
        stop_dist = (entry - stop) if invest_type == 1 else (stop - entry)
        risk_amount = stop_dist * qty
        risk_pct = (stop_dist / entry * 100) if entry != 0 else 0.0
        return risk_amount, risk_pct

    # ── Public API ────────────────────────────────────────────────────────

    def add_trade(
        self,
        symbol: str,
        invest_type: int,
        entry_date,
        entry_price: float,
        stop_loss: float,
        quantity: int,
        portfolio_id: int = 1,
        strategy: str = None,
        notes: str = None,
        code: str = None,
        entry_tpr_score: float = None,
        entry_rpr_score: float = None,
        entry_buyrisk_score: float = None,
        entry_buyrisk_8w: float = None,
        entry_buyrisk_10w: float = None,
        entry_high_52wk: float = None,
        entry_sma_20: float = None,
        entry_avg_vol_50: int = None,
        risk_amount: float = None,
        risk_pct: float = None,
        r_multiple: float = None,
        status: str = 'Open',
        trade_type: str = None,
    ) -> int:
        """
        Yeni trade ekler.

        invest_type: 1=LONG, 2=SHORT
        trade_type verilmezse invest_type'tan otomatik türetilir.
        risk_amount / risk_pct verilmezse entry/stop/qty'dan hesaplanır.
        Returns: yeni trade id
        """
        if invest_type not in (1, 2):
            raise ValueError("invest_type 1 (LONG) veya 2 (SHORT) olmalı")

        trade_type = trade_type or self._invest_to_trade_type(invest_type)

        calc_ra, calc_rp = self._compute_risk(entry_price, stop_loss, quantity, invest_type)
        if risk_amount is None:
            risk_amount = calc_ra
        if risk_pct is None:
            risk_pct = calc_rp

        try:
            with self._cursor() as cur:
                cur.execute("""
                    INSERT INTO trades (
                        symbol, trade_type, invest_type, entry_date, entry_price,
                        stop_loss, quantity, portfolio_id, strategy, notes, code,
                        entry_tpr_score, entry_rpr_score, entry_buyrisk_score,
                        entry_buyrisk_8w, entry_buyrisk_10w, entry_high_52wk,
                        entry_sma_20, entry_avg_vol_50,
                        risk_amount, risk_pct, r_multiple, status
                    ) VALUES (
                        %s, %s, %s, %s, %s,
                        %s, %s, %s, %s, %s, %s,
                        %s, %s, %s,
                        %s, %s, %s,
                        %s, %s,
                        %s, %s, %s, %s
                    ) RETURNING id
                """, (
                    symbol, trade_type, invest_type, entry_date, entry_price,
                    stop_loss, quantity, portfolio_id, strategy, notes, code,
                    entry_tpr_score, entry_rpr_score, entry_buyrisk_score,
                    entry_buyrisk_8w, entry_buyrisk_10w, entry_high_52wk,
                    entry_sma_20, entry_avg_vol_50,
                    risk_amount, risk_pct, r_multiple, status,
                ))
                trade_id = cur.fetchone()['id']
            self._commit()
            return trade_id
        except psycopg2.IntegrityError as e:
            self._rollback()
            raise ValueError(f"add_trade constraint hatası: {e}") from e
        except Exception as e:
            self._rollback()
            log.error(f"add_trade error: {e}")
            raise

    def add_leg(
        self,
        trade_id: int,
        shares: float,
        price: float,
        leg_date,
        leg_idx: int = None,
        note: str = None,
    ) -> int:
        """
        Trade'e yeni leg ekler.
        leg_idx verilmezse mevcut max+1 otomatik atanır.
        Returns: yeni leg id
        """
        try:
            with self._cursor() as cur:
                if leg_idx is None:
                    cur.execute(
                        "SELECT COALESCE(MAX(leg_idx), 0) + 1 AS next_idx "
                        "FROM trade_legs WHERE trade_id = %s",
                        (trade_id,)
                    )
                    leg_idx = cur.fetchone()['next_idx']

                cur.execute("""
                    INSERT INTO trade_legs (trade_id, leg_idx, shares, price, leg_date, note)
                    VALUES (%s, %s, %s, %s, %s, %s) RETURNING id
                """, (trade_id, leg_idx, shares, price, leg_date, note))
                leg_id = cur.fetchone()['id']
            self._commit()
            return leg_id
        except psycopg2.IntegrityError as e:
            self._rollback()
            raise ValueError(f"add_leg constraint hatası: {e}") from e
        except Exception as e:
            self._rollback()
            log.error(f"add_leg error: {e}")
            raise

    def add_exit(
        self,
        leg_id: int,
        shares: float,
        price: float,
        exit_date,
        reason: str = None,
        note: str = None,
    ) -> int:
        """
        Leg'e çıkış kaydı ekler.
        reason: 'sbe' | 'stop' | 'target' | 'manual' | 'trailing' | None
        Returns: yeni exit id
        """
        if reason is not None and reason not in _VALID_EXIT_REASONS:
            raise ValueError(
                f"Geçersiz reason: '{reason}'. Beklenen: {_VALID_EXIT_REASONS}"
            )
        try:
            with self._cursor() as cur:
                cur.execute("""
                    INSERT INTO trade_exits (leg_id, shares, price, exit_date, reason, note)
                    VALUES (%s, %s, %s, %s, %s, %s) RETURNING id
                """, (leg_id, shares, price, exit_date, reason, note))
                exit_id = cur.fetchone()['id']
            self._commit()
            return exit_id
        except psycopg2.IntegrityError as e:
            self._rollback()
            raise ValueError(f"add_exit constraint hatası: {e}") from e
        except Exception as e:
            self._rollback()
            log.error(f"add_exit error: {e}")
            raise

    def update_stop(
        self,
        trade_id: int,
        new_stop_price: float,
        reason: str = 'manual',
        note: str = None,
    ) -> int:
        """
        Stop fiyatını günceller: stop_history'e kayıt ekler VE
        trades.stop_loss'u günceller (tek transaction).
        Returns: yeni stop_history id
        """
        if reason not in _VALID_STOP_REASONS:
            raise ValueError(
                f"Geçersiz reason: '{reason}'. Beklenen: {_VALID_STOP_REASONS}"
            )
        try:
            with self._cursor() as cur:
                cur.execute("""
                    INSERT INTO stop_history (trade_id, stop_price, reason, note)
                    VALUES (%s, %s, %s, %s) RETURNING id
                """, (trade_id, new_stop_price, reason, note))
                stop_id = cur.fetchone()['id']
                cur.execute(
                    "UPDATE trades SET stop_loss = %s, updated_at = NOW() WHERE id = %s",
                    (new_stop_price, trade_id)
                )
            self._commit()
            return stop_id
        except Exception as e:
            self._rollback()
            log.error(f"update_stop error: {e}")
            raise

    def get_trade(
        self,
        trade_id: int,
        include_legs: bool = False,
        include_exits: bool = False,
        include_stops: bool = False,
    ) -> dict | None:
        """
        Tek trade döndürür. Soft-deleted'leri filtreler (deleted_at IS NULL).
        include_legs → 'legs' key eklenir
        include_exits → her leg altında 'exits' key eklenir (include_legs de gerekli)
        include_stops → 'stops' key eklenir
        """
        with self._cursor() as cur:
            cur.execute(
                "SELECT * FROM trades WHERE id = %s AND deleted_at IS NULL",
                (trade_id,)
            )
            row = cur.fetchone()
            if row is None:
                return None
            trade = dict(row)

            if include_legs or include_exits:
                cur.execute(
                    "SELECT * FROM trade_legs WHERE trade_id = %s ORDER BY leg_idx",
                    (trade_id,)
                )
                legs = [dict(r) for r in cur.fetchall()]
                if include_exits:
                    for leg in legs:
                        cur.execute(
                            "SELECT * FROM trade_exits WHERE leg_id = %s ORDER BY exit_date",
                            (leg['id'],)
                        )
                        leg['exits'] = [dict(r) for r in cur.fetchall()]
                trade['legs'] = legs

            if include_stops:
                cur.execute(
                    "SELECT * FROM stop_history WHERE trade_id = %s ORDER BY set_at",
                    (trade_id,)
                )
                trade['stops'] = [dict(r) for r in cur.fetchall()]

        return trade

    def get_trades(
        self,
        status: str = None,
        portfolio_id: int = None,
        symbol: str = None,
        limit: int = None,
        include_deleted: bool = False,
    ) -> list[dict]:
        """
        Trade listesi döndürür. Tüm filtreler opsiyonel, sıralama entry_date DESC.
        """
        clauses, params = [], []

        if not include_deleted:
            clauses.append("deleted_at IS NULL")
        if status is not None:
            clauses.append("status = %s")
            params.append(status)
        if portfolio_id is not None:
            clauses.append("portfolio_id = %s")
            params.append(portfolio_id)
        if symbol is not None:
            clauses.append("symbol = %s")
            params.append(symbol)

        where = ("WHERE " + " AND ".join(clauses)) if clauses else ""
        limit_clause = f"LIMIT {int(limit)}" if limit else ""

        sql = f"SELECT * FROM trades {where} ORDER BY entry_date DESC {limit_clause}"
        with self._cursor() as cur:
            cur.execute(sql, params)
            return [dict(r) for r in cur.fetchall()]

    def update_trade(self, trade_id: int, **kwargs) -> bool:
        """
        Trade günceller. Sadece ALLOWED_UPDATE_FIELDS içindeki kolonlar kabul edilir.
        İzin dışı kolon → ValueError.
        Returns: True başarılı / False trade yok veya soft-deleted
        """
        if not kwargs:
            return False
        invalid = set(kwargs) - ALLOWED_UPDATE_FIELDS
        if invalid:
            raise ValueError(f"İzin verilmeyen kolon(lar): {invalid}")

        set_clause = ", ".join(f"{k} = %({k})s" for k in kwargs)
        kwargs['_trade_id'] = trade_id
        try:
            with self._cursor() as cur:
                cur.execute(
                    f"UPDATE trades SET {set_clause}, updated_at = NOW() "
                    f"WHERE id = %(_trade_id)s AND deleted_at IS NULL",
                    kwargs
                )
                affected = cur.rowcount
            self._commit()
            return affected > 0
        except Exception as e:
            self._rollback()
            log.error(f"update_trade error: {e}")
            raise

    def remove_trade(self, trade_id: int, hard: bool = False) -> bool:
        """
        hard=False (default): soft delete — deleted_at = now()
        hard=True: gerçek DELETE, CASCADE ile leg/exit/stop da silinir
        Returns: True başarılı / False trade yok
        """
        try:
            with self._cursor() as cur:
                if hard:
                    cur.execute("DELETE FROM trades WHERE id = %s", (trade_id,))
                else:
                    cur.execute(
                        "UPDATE trades SET deleted_at = NOW(), updated_at = NOW() "
                        "WHERE id = %s AND deleted_at IS NULL",
                        (trade_id,)
                    )
                affected = cur.rowcount
            self._commit()
            return affected > 0
        except Exception as e:
            self._rollback()
            log.error(f"remove_trade error: {e}")
            raise

    def close_trade(
        self,
        trade_id: int,
        exit_date,
        exit_price: float,
        reason: str = 'manual',
    ) -> dict:
        """
        Trade'i kapatır.
        Açık leg'lerin kalan payları exit_price'tan çıkış alır.
        P&L ve pnl_pct hesaplanarak trades güncellenir.
        Returns: {'trade_id', 'total_pnl', 'pnl_pct', 'closed_legs'}
        """
        trade = self.get_trade(trade_id, include_legs=True, include_exits=True)
        if trade is None:
            raise ValueError(f"Trade {trade_id} bulunamadı veya silinmiş")
        if trade['status'] == 'Closed':
            raise ValueError(f"Trade {trade_id} zaten kapalı")

        invest_type = trade.get('invest_type') or (
            1 if trade['trade_type'] == 'Long' else 2
        )
        legs = trade.get('legs', [])
        total_pnl = 0.0
        closed_legs = 0
        entry_value = 0.0

        try:
            with self._cursor() as cur:
                if legs:
                    for leg in legs:
                        already_exited = sum(
                            float(e['shares']) for e in leg.get('exits', [])
                        )
                        remaining = float(leg['shares']) - already_exited
                        if remaining <= 0:
                            continue
                        leg_price = float(leg['price'])
                        cur.execute("""
                            INSERT INTO trade_exits
                                (leg_id, shares, price, exit_date, reason)
                            VALUES (%s, %s, %s, %s, %s)
                        """, (leg['id'], remaining, exit_price, exit_date, reason))
                        closed_legs += 1
                        pnl_sign = 1 if invest_type == 1 else -1
                        total_pnl += pnl_sign * (float(exit_price) - leg_price) * remaining
                        entry_value += leg_price * remaining
                else:
                    qty = float(trade['quantity'])
                    ep = float(trade['entry_price'])
                    pnl_sign = 1 if invest_type == 1 else -1
                    total_pnl = pnl_sign * (float(exit_price) - ep) * qty
                    entry_value = ep * qty

                pnl_pct = (total_pnl / entry_value * 100) if entry_value != 0 else 0.0

                cur.execute("""
                    UPDATE trades
                    SET status = 'Closed', exit_date = %s, exit_price = %s,
                        profit_loss = %s, pnl_pct = %s, updated_at = NOW()
                    WHERE id = %s
                """, (exit_date, exit_price, total_pnl, pnl_pct, trade_id))

            self._commit()
            return {
                'trade_id': trade_id,
                'total_pnl': round(total_pnl, 4),
                'pnl_pct': round(pnl_pct, 4),
                'closed_legs': closed_legs,
            }
        except Exception as e:
            self._rollback()
            log.error(f"close_trade error: {e}")
            raise

    def compute_summary(
        self,
        start_date=None,
        end_date=None,
        portfolio_id: int = None,
    ) -> dict:
        """
        Kapalı trade istatistikleri. Soft-deleted'leri filtreler.
        Returns: total_trades, wins, losses, win_rate, avg_gain_pct,
                 avg_loss_pct, total_pnl, max_winner, max_loser
        """
        clauses = ["status = 'Closed'", "deleted_at IS NULL"]
        params = []

        if start_date is not None:
            clauses.append("entry_date >= %s")
            params.append(start_date)
        if end_date is not None:
            clauses.append("entry_date <= %s")
            params.append(end_date)
        if portfolio_id is not None:
            clauses.append("portfolio_id = %s")
            params.append(portfolio_id)

        where = "WHERE " + " AND ".join(clauses)
        with self._cursor() as cur:
            cur.execute(f"""
                SELECT
                    COUNT(*)                                          AS total_trades,
                    COUNT(*) FILTER (WHERE profit_loss > 0)           AS wins,
                    COUNT(*) FILTER (WHERE profit_loss <= 0)          AS losses,
                    AVG(pnl_pct) FILTER (WHERE profit_loss > 0)       AS avg_gain_pct,
                    AVG(pnl_pct) FILTER (WHERE profit_loss <= 0)      AS avg_loss_pct,
                    SUM(profit_loss)                                  AS total_pnl,
                    MAX(profit_loss)                                  AS max_winner,
                    MIN(profit_loss)                                  AS max_loser
                FROM trades {where}
            """, params)
            row = cur.fetchone()

        total = int(row['total_trades'])
        wins = int(row['wins'])
        return {
            'total_trades': total,
            'wins': wins,
            'losses': int(row['losses']),
            'win_rate': round((wins / total * 100) if total > 0 else 0.0, 2),
            'avg_gain_pct': float(row['avg_gain_pct'] or 0),
            'avg_loss_pct': float(row['avg_loss_pct'] or 0),
            'total_pnl': float(row['total_pnl'] or 0),
            'max_winner': float(row['max_winner'] or 0),
            'max_loser': float(row['max_loser'] or 0),
        }
