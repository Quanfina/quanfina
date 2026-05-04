import os
import psycopg2
import pandas as pd
from sqlalchemy import create_engine
from urllib.parse import quote_plus
from dotenv import load_dotenv

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
            commission, portfolio_id, notes
        ) VALUES (
            %s, %s, %s, %s, %s, %s,
            %s, %s, %s, %s, %s,
            %s, %s, %s, %s, %s,
            %s, %s, %s
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
        data.get("notes"),
    ))
    conn.commit()
    conn.close()


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
