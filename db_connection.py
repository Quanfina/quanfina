import os
import logging
import psycopg2
import pandas as pd
from sqlalchemy import create_engine
from urllib.parse import quote_plus
from dotenv import load_dotenv

log = logging.getLogger(__name__)

load_dotenv()

def _is_unix_socket(host: str) -> bool:
    return host.startswith("/")


def get_connection():
    host     = os.getenv("PG_HOST")
    db       = os.getenv("PG_DATABASE")
    user     = os.getenv("PG_USER")
    password = os.getenv("PG_PASSWORD")

    if _is_unix_socket(host):
        return psycopg2.connect(
            host=host,
            dbname=db,
            user=user,
            password=password,
        )
    else:
        port = os.getenv("PG_PORT", "5432")
        return psycopg2.connect(
            host=host,
            port=port,
            dbname=db,
            user=user,
            password=password,
            sslmode="require",
        )


def get_engine():
    host     = os.getenv("PG_HOST")
    db       = os.getenv("PG_DATABASE")
    user     = os.getenv("PG_USER")
    password = quote_plus(os.getenv("PG_PASSWORD"))

    if _is_unix_socket(host):
        return create_engine(
            f"postgresql+psycopg2://{user}:{password}@/{db}?host={host}"
        )
    else:
        port = os.getenv("PG_PORT", "5432")
        return create_engine(
            f"postgresql+psycopg2://{user}:{password}@{host}:{port}/{db}?sslmode=require"
        )


def get_trades(status=None):
    engine = get_engine()
    if status:
        return pd.read_sql_query(
            "SELECT * FROM trades WHERE status = %(status)s ORDER BY id DESC",
            engine, params={"status": status}
        )
    return pd.read_sql_query("SELECT * FROM trades ORDER BY id DESC", engine)


def insert_trade(data: dict):
    conn = get_connection()
    cur = conn.cursor()
    cur.execute("""
        INSERT INTO trades (
            symbol, trade_type, strategy, entry_date, entry_price, stop_loss,
            quantity, risk_amount, risk_pct, risk_equity_pct, position_size_pct,
            breakeven, sbe_pct, sbe_shares, r_multiple, status,
            commission, portfolio_id, position_size_dollars, notes
        ) VALUES (
            %s, %s, %s, %s, %s, %s,
            %s, %s, %s, %s, %s,
            %s, %s, %s, %s, %s,
            %s, %s, %s, %s
        )
    """, (
        data["symbol"],
        data.get("trade_type", "Long"),
        data.get("strategy"),
        data.get("entry_date"),
        data.get("entry_price"),
        data.get("stop_loss"),
        data.get("quantity"),
        data.get("risk_amount"),
        data.get("risk_pct"),
        data.get("risk_equity_pct"),
        data.get("position_size_pct"),
        data.get("breakeven"),
        data.get("sbe_pct"),
        data.get("sbe_shares"),
        data.get("r_multiple"),
        data.get("status", "Open"),
        data.get("commission", 0),
        data.get("portfolio_id", 1),
        data.get("position_size_dollars"),
        data.get("notes"),
    ))
    conn.commit()
    conn.close()


def get_portfolio(portfolio_id: int = 1):
    """Portföy bilgisini dict olarak döndür."""
    conn = get_connection()
    cur = conn.cursor()
    cur.execute("""
        SELECT id, name, starting_value, current_value,
               created_at, updated_at
        FROM portfolios WHERE id = %s
    """, (portfolio_id,))
    row = cur.fetchone()
    conn.close()
    if not row:
        return None
    return {
        "id": row[0], "name": row[1],
        "starting_value": float(row[2]),
        "current_value": float(row[3]),
        "created_at": row[4], "updated_at": row[5],
    }


def update_portfolio(portfolio_id: int, current_value: float) -> bool:
    """Portföy current_value'sini güncelle."""
    conn = get_connection()
    cur = conn.cursor()
    cur.execute("""
        UPDATE portfolios
        SET current_value = %s, updated_at = NOW()
        WHERE id = %s
    """, (current_value, portfolio_id))
    affected = cur.rowcount
    conn.commit()
    conn.close()
    return affected > 0


def update_portfolio_starting(portfolio_id: int, starting_value: float) -> bool:
    """Portföy starting_value'sini güncelle (YTD baseline)."""
    conn = get_connection()
    cur = conn.cursor()
    cur.execute("""
        UPDATE portfolios
        SET starting_value = %s, updated_at = NOW()
        WHERE id = %s
    """, (starting_value, portfolio_id))
    affected = cur.rowcount
    conn.commit()
    conn.close()
    return affected > 0


def get_journal(category=None):
    engine = get_engine()
    if category:
        return pd.read_sql_query(
            "SELECT * FROM journal_entries WHERE category = %(category)s ORDER BY id DESC",
            engine, params={"category": category}
        )
    return pd.read_sql_query("SELECT * FROM journal_entries ORDER BY id DESC", engine)


def insert_journal(date_val, category, content, linked_trade_id=None):
    conn = get_connection()
    cur = conn.cursor()
    cur.execute("""
        INSERT INTO journal_entries (date, category, content, linked_trade_id)
        VALUES (%s, %s, %s, %s)
    """, (date_val, category, content, linked_trade_id))
    conn.commit()
    conn.close()


# ──────────────────────────────────────────────────
# Sprint 3C — Trade CRUD Helpers
# ──────────────────────────────────────────────────

def update_trade(trade_id: int, data: dict) -> bool:
    """
    Trade kaydını günceller. data dict'i sadece güncellenecek field'ları içerir.
    Allowed fields: symbol, trade_type, strategy, entry_date, entry_price,
                   stop_loss, quantity, commission, notes, risk_amount,
                   risk_pct, risk_equity_pct, position_size_pct, breakeven,
                   sbe_pct, sbe_shares, position_size_dollars
    """
    if not data:
        return False

    allowed = {
        'symbol', 'trade_type', 'strategy', 'entry_date', 'entry_price',
        'stop_loss', 'quantity', 'commission', 'notes', 'risk_amount',
        'risk_pct', 'risk_equity_pct', 'position_size_pct', 'breakeven',
        'sbe_pct', 'sbe_shares', 'position_size_dollars'
    }
    filtered = {k: v for k, v in data.items() if k in allowed}
    if not filtered:
        return False

    set_clause = ', '.join(f"{k} = %({k})s" for k in filtered.keys())
    filtered['trade_id'] = trade_id

    conn = get_connection()
    try:
        with conn.cursor() as cur:
            cur.execute(
                f"UPDATE trades SET {set_clause}, updated_at = NOW() "
                f"WHERE id = %(trade_id)s",
                filtered
            )
            conn.commit()
            return cur.rowcount > 0
    except Exception as e:
        conn.rollback()
        log.error(f"update_trade error: {e}")
        return False
    finally:
        conn.close()


def close_trade(trade_id: int, exit_price: float, exit_date,
                exit_notes: str = None) -> bool:
    """
    Trade'i kapatır. P&L ve r_multiple otomatik hesaplanır.

    Long P&L:  (exit - entry) * quantity - commission
    Short P&L: (entry - exit) * quantity - commission

    r_multiple = profit_loss / risk_amount

    Markets 360 aB() formülünün Python karşılığı.
    """
    conn = get_connection()
    try:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT trade_type, entry_price, quantity, commission, "
                "risk_amount, notes FROM trades WHERE id = %s AND status = 'Open'",
                (trade_id,)
            )
            row = cur.fetchone()
            if not row:
                log.warning(f"close_trade: trade {trade_id} not found or already closed")
                return False

            trade_type, entry_price, quantity, commission, risk_amount, current_notes = row

            entry_p = float(entry_price)
            exit_p  = float(exit_price)
            qty     = float(quantity)
            comm    = float(commission or 0)

            if trade_type == 'Short':
                profit_loss = (entry_p - exit_p) * qty - comm
            else:  # Long
                profit_loss = (exit_p - entry_p) * qty - comm

            pnl_pct    = (profit_loss / (entry_p * qty)) * 100 if entry_p * qty > 0 else 0
            r_multiple = profit_loss / float(risk_amount) if risk_amount and float(risk_amount) > 0 else 0

            final_notes = current_notes or ''
            if exit_notes:
                final_notes = f"{final_notes}\n\n[Çıkış: {exit_date}] {exit_notes}".strip()

            cur.execute(
                "UPDATE trades SET status = 'Closed', exit_date = %s, exit_price = %s, "
                "profit_loss = %s, pnl_pct = %s, r_multiple = %s, notes = %s, "
                "updated_at = NOW() WHERE id = %s",
                (exit_date, exit_price, profit_loss, pnl_pct, r_multiple,
                 final_notes if exit_notes else current_notes, trade_id)
            )
            conn.commit()
            log.info(f"close_trade: {trade_id} closed. P&L: {profit_loss:.2f} ({pnl_pct:.2f}%), R: {r_multiple:.2f}")
            return cur.rowcount > 0
    except Exception as e:
        conn.rollback()
        log.error(f"close_trade error: {e}")
        return False
    finally:
        conn.close()


def delete_trade(trade_id: int, soft: bool = True) -> bool:
    """
    Trade siler.
    soft=True (default): status='Deleted' olarak işaretle (geri alınabilir)
    soft=False: Tam DELETE (geri dönüşü yok)
    """
    conn = get_connection()
    try:
        with conn.cursor() as cur:
            if soft:
                cur.execute(
                    "UPDATE trades SET status = 'Deleted', updated_at = NOW() "
                    "WHERE id = %s",
                    (trade_id,)
                )
            else:
                cur.execute("DELETE FROM trades WHERE id = %s", (trade_id,))
            conn.commit()
            return cur.rowcount > 0
    except Exception as e:
        conn.rollback()
        log.error(f"delete_trade error: {e}")
        return False
    finally:
        conn.close()
