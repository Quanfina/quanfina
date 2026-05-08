import os
import logging
from datetime import datetime, date
import psycopg2
import pandas as pd
from sqlalchemy import create_engine
from urllib.parse import quote_plus
from dotenv import load_dotenv
from quanfina_math import dollar_change, percent_change

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
            symbol, trade_type, invest_type, strategy, entry_date, entry_price, stop_loss,
            quantity, risk_amount, risk_pct, risk_equity_pct, position_size_pct,
            breakeven, sbe_pct, sbe_shares, r_multiple, status,
            commission, portfolio_id, position_size_dollars, notes
        ) VALUES (
            %s, %s, %s, %s, %s, %s,
            %s, %s, %s, %s, %s, %s,
            %s, %s, %s, %s, %s,
            %s, %s, %s, %s
        )
    """, (
        data["symbol"],
        data.get("trade_type", "Long"),
        data.get("invest_type"),
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
    invest_type NULL ise trade_type'tan türetilir (legacy kayıtlar için fallback).
    pnl_pct fiyat bazlı (komisyon hariç); profit_loss komisyon dahil.
    """
    if isinstance(exit_date, date) and not isinstance(exit_date, datetime):
        exit_date = datetime.combine(exit_date, datetime.min.time())

    conn = get_connection()
    try:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT trade_type, invest_type, entry_price, quantity, commission, "
                "risk_amount, notes FROM trades WHERE id = %s AND status = 'Open'",
                (trade_id,)
            )
            row = cur.fetchone()
            if not row:
                log.warning(f"close_trade: trade {trade_id} not found or already closed")
                return False

            trade_type, invest_type_db, entry_price, quantity, commission, risk_amount, current_notes = row
            invest_type = invest_type_db or (1 if trade_type == 'Long' else 2)

            entry_p = float(entry_price)
            exit_p  = float(exit_price)
            qty     = float(quantity)
            comm    = float(commission or 0)

            profit_loss = dollar_change(entry_p, exit_p, int(qty), invest_type) - comm
            pnl_pct     = percent_change(entry_p, exit_p, invest_type)
            r_multiple  = profit_loss / float(risk_amount) if risk_amount and float(risk_amount) > 0 else 0

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
            closed_rows = cur.rowcount

            # leg_exits: leg olan trade'lerde kalan payı kaydet
            cur.execute(
                "SELECT id, shares FROM trade_legs WHERE trade_id = %s ORDER BY leg_idx",
                (trade_id,)
            )
            legs = cur.fetchall()
            for leg_id, leg_shares in legs:
                cur.execute(
                    "SELECT COALESCE(SUM(shares), 0) FROM leg_exits WHERE leg_id = %s",
                    (leg_id,)
                )
                already_exited = float(cur.fetchone()[0])
                remaining = float(leg_shares) - already_exited
                if remaining > 0:
                    cur.execute(
                        "INSERT INTO leg_exits (leg_id, exit_idx, shares, price, exit_date, reason) "
                        "VALUES (%s, "
                        "(SELECT COALESCE(MAX(exit_idx), 0) + 1 FROM leg_exits WHERE leg_id = %s), "
                        "%s, %s, %s, 'manual')",
                        (leg_id, leg_id, remaining, exit_price, exit_date)
                    )
                    log.info(f"close_trade: leg_exits INSERT leg_id={leg_id} shares={remaining}")

            conn.commit()
            log.info(f"close_trade: {trade_id} closed. P&L: {profit_loss:.2f} ({pnl_pct:.2f}%), R: {r_multiple:.2f}")
            return closed_rows > 0
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


def add_to_list(user_id: int, symbol: str, list_code: str,
                strategy: str = "minervini", note: str = "",
                setup_type_id: int = None, pivot_price: float = None) -> bool:
    """symbol_lists tablosuna hisse ekler.

    Args:
        user_id: Kullanici ID
        symbol: Ticker (otomatik upper+strip)
        list_code: 'watch', 'on_deck', 'focus', veya 'buy'
        strategy: Default 'minervini'
        note: Opsiyonel not
        setup_type_id: Opsiyonel setup_types FK (1-6)
        pivot_price: Opsiyonel pivot fiyati

    Returns:
        True: Eklendi
        False: Zaten vardi (ON CONFLICT)
    """
    conn = get_connection()
    cur = conn.cursor()
    try:
        cur.execute(
            "INSERT INTO symbol_lists "
            "(user_id, symbol, list_type, strategy, note, setup_type_id, pivot_price) "
            "VALUES (%s, %s, %s, %s, %s, %s, %s) "
            "ON CONFLICT (user_id, symbol, list_type, strategy) DO NOTHING",
            (user_id, symbol.upper().strip(), list_code, strategy, note,
             setup_type_id, pivot_price)
        )
        added = cur.rowcount > 0
        conn.commit()
        return added
    except Exception as e:
        conn.rollback()
        log.error(f"add_to_list error: {e}")
        return False
    finally:
        conn.close()


def get_setup_types() -> list:
    """setup_types tablosunu (id, name) listesi olarak dondurur (sort_order'a gore).

    Returns:
        Liste: [(1, 'VCP'), (2, 'CWH'), (3, 'Power Play'), ...]
    """
    conn = get_connection()
    cur = conn.cursor()
    try:
        cur.execute("SELECT id, name FROM setup_types ORDER BY sort_order")
        return cur.fetchall()
    finally:
        conn.close()


def get_grade_categories() -> list[dict]:
    """trade_grade_categories tablosundan 17 TradeGrader kategorisini dondurur.

    Sprint 4.7c.2'de seed edildi (Bundle gradeThresholds birebir).
    sort_order'a gore sirali.

    Returns:
        [{'id': 1, 'code': 'BP', 'name': 'Bought perfect',
          'short_name': 'Perfect', 'leg_type': 'ENTRY',
          'target_pct': 80.0, 'sort_order': 10}, ...]

    Kaynak: notebook/EK10_TradeGrader_Sentezi.md (Bolum 6)
    """
    conn = get_connection()
    cur = conn.cursor()
    try:
        cur.execute("""
            SELECT id, code, name, short_name, leg_type, target_pct, sort_order
            FROM trade_grade_categories
            ORDER BY sort_order
        """)
        rows = cur.fetchall()
        return [
            {
                "id":         r[0],
                "code":       r[1],
                "name":       r[2],
                "short_name": r[3],
                "leg_type":   r[4],
                "target_pct": float(r[5]) if r[5] is not None else None,
                "sort_order": r[6],
            }
            for r in rows
        ]
    finally:
        conn.close()


def update_leg_exit_grade(
    leg_exit_id: int,
    grade_category_id: int,
    annotation: str = None,
) -> bool:
    """leg_exits satırına grade_category_id + opsiyonel annotation atar.

    Sprint 4.7d.2 — close_trade sonrası kullanıcı grade önerisini kabul/değiştirirse çağrılır.

    Returns:
        bool: UPDATE başarılı mı
    """
    conn = get_connection()
    cur = conn.cursor()
    try:
        cur.execute(
            "UPDATE leg_exits SET grade_category_id = %s, annotation = %s WHERE id = %s",
            (grade_category_id, annotation, leg_exit_id),
        )
        updated = cur.rowcount > 0
        conn.commit()
        return updated
    except Exception as e:
        conn.rollback()
        log.error(f"update_leg_exit_grade error: {e}")
        return False
    finally:
        conn.close()


def get_latest_leg_exits_for_trade(trade_id: int) -> list[dict]:
    """Bir trade'in grade_category_id NULL olan leg_exits satirlarini dondurur.

    Sprint 4.7d.2 — close_trade sonrasi grade atama icin kullanilir.
    Birden fazla leg varsa hepsini dondurur (close_trade her acik leg icin ayri INSERT).

    Returns:
        list[dict]: [{'id': ..., 'leg_id': ..., 'exit_idx': ...,
                      'shares': ..., 'price': ...}, ...]
                    Bos trade veya hepsi grade'li ise [] doner.
    """
    conn = get_connection()
    cur = conn.cursor()
    try:
        cur.execute(
            """SELECT le.id, le.leg_id, le.exit_idx, le.shares, le.price
               FROM leg_exits le
               JOIN trade_legs tl ON tl.id = le.leg_id
               WHERE tl.trade_id = %s AND le.grade_category_id IS NULL
               ORDER BY le.id DESC""",
            (trade_id,),
        )
        rows = cur.fetchall()
        return [
            {"id": r[0], "leg_id": r[1], "exit_idx": r[2],
             "shares": float(r[3]), "price": float(r[4])}
            for r in rows
        ]
    except Exception as e:
        log.error(f"get_latest_leg_exits_for_trade error: {e}")
        return []
    finally:
        conn.close()


def promote_to_list(user_id: int, symbol: str, from_list: str, to_list: str,
                    strategy: str = "minervini", note: str = None,
                    setup_type_id: int = None, pivot_price: float = None) -> bool:
    """Bir hisseyi bir listeden digerine atomic olarak tasir.

    Args:
        user_id: Kullanici ID
        symbol: Ticker (otomatik upper+strip)
        from_list: Kaynak liste ('watch', 'on_deck', 'focus', 'buy')
        to_list: Hedef liste
        strategy: Default 'minervini'
        note: None ise eski not korunur, deger varsa overwrite
        setup_type_id: Hedefte ayarlanacak setup_types FK
        pivot_price: Hedefte ayarlanacak pivot fiyati

    Returns:
        True: Tasima basarili
        False: Kaynak listede hisse yok veya hata
    """
    sym = symbol.upper().strip()
    conn = get_connection()
    cur = conn.cursor()
    try:
        cur.execute(
            "SELECT note FROM symbol_lists "
            "WHERE user_id = %s AND symbol = %s AND list_type = %s AND strategy = %s",
            (user_id, sym, from_list, strategy)
        )
        row = cur.fetchone()
        if not row:
            conn.rollback()
            return False
        existing_note = row[0] or ""
        final_note = note if note is not None else existing_note

        cur.execute(
            "INSERT INTO symbol_lists "
            "(user_id, symbol, list_type, strategy, note, setup_type_id, pivot_price) "
            "VALUES (%s, %s, %s, %s, %s, %s, %s) "
            "ON CONFLICT (user_id, symbol, list_type, strategy) "
            "DO UPDATE SET note = EXCLUDED.note, "
            "              setup_type_id = EXCLUDED.setup_type_id, "
            "              pivot_price = EXCLUDED.pivot_price",
            (user_id, sym, to_list, strategy, final_note, setup_type_id, pivot_price)
        )

        cur.execute(
            "DELETE FROM symbol_lists "
            "WHERE user_id = %s AND symbol = %s AND list_type = %s AND strategy = %s",
            (user_id, sym, from_list, strategy)
        )

        conn.commit()
        return True
    except Exception as e:
        conn.rollback()
        log.error(f"promote_to_list error: {e}")
        return False
    finally:
        conn.close()


def remove_from_list(user_id: int, symbol: str, list_code: str,
                     strategy: str = "minervini") -> bool:
    """symbol_lists tablosundan hisse siler (hard delete).

    Returns:
        True: Silindi
        False: Bulunamadi
    """
    conn = get_connection()
    cur = conn.cursor()
    try:
        cur.execute(
            "DELETE FROM symbol_lists "
            "WHERE user_id = %s AND symbol = %s AND list_type = %s AND strategy = %s",
            (user_id, symbol.upper().strip(), list_code, strategy)
        )
        removed = cur.rowcount > 0
        conn.commit()
        return removed
    except Exception as e:
        conn.rollback()
        log.error(f"remove_from_list error: {e}")
        return False
    finally:
        conn.close()


# ──────────────────────────────────────────────────
# Schema Migration — Trade Journal Tabloları
# ──────────────────────────────────────────────────

def init_trade_journal_tables() -> int:
    """
    Trade Journal mimarisinin PostgreSQL tablolarını oluşturur / günceller.
    İdempotent — tekrar çalıştırılabilir, mevcut veriyi bozmaz.
    Manuel çalıştırılır, uygulama başlangıcında otomatik çağrılmaz.

    Döndürür: temizlenen orphan journal_entries satır sayısı.
    """
    conn = get_connection()
    try:
        with conn.cursor() as cur:

            # ── 1. trades: 13 yeni kolon ───────────────────────────────────
            for stmt in [
                "ALTER TABLE trades ADD COLUMN IF NOT EXISTS invest_type         SMALLINT CHECK (invest_type IN (1, 2))",
                "ALTER TABLE trades ADD COLUMN IF NOT EXISTS code                TEXT",
                "ALTER TABLE trades ADD COLUMN IF NOT EXISTS entry_tpr_score     REAL",
                "ALTER TABLE trades ADD COLUMN IF NOT EXISTS entry_rpr_score     REAL",
                "ALTER TABLE trades ADD COLUMN IF NOT EXISTS entry_buyrisk_score REAL",
                "ALTER TABLE trades ADD COLUMN IF NOT EXISTS entry_buyrisk_8w    REAL",
                "ALTER TABLE trades ADD COLUMN IF NOT EXISTS entry_buyrisk_10w   REAL",
                "ALTER TABLE trades ADD COLUMN IF NOT EXISTS entry_high_52wk     REAL",
                "ALTER TABLE trades ADD COLUMN IF NOT EXISTS entry_sma_20        REAL",
                "ALTER TABLE trades ADD COLUMN IF NOT EXISTS entry_avg_vol_50    BIGINT",
                "ALTER TABLE trades ADD COLUMN IF NOT EXISTS manual_grade        TEXT",
                "ALTER TABLE trades ADD COLUMN IF NOT EXISTS monalert_mode       TEXT DEFAULT 'off' CHECK (monalert_mode IN ('off', 'monitor', 'monitor_email'))",
                "ALTER TABLE trades ADD COLUMN IF NOT EXISTS deleted_at          TIMESTAMP",
            ]:
                cur.execute(stmt)
            log.info("init_trade_journal_tables: trades kolonları güncellendi")

            # ── 2. trade_legs ──────────────────────────────────────────────
            cur.execute("""
                CREATE TABLE IF NOT EXISTS trade_legs (
                    id         SERIAL PRIMARY KEY,
                    trade_id   INTEGER NOT NULL REFERENCES trades(id) ON DELETE CASCADE,
                    leg_idx    INTEGER NOT NULL,
                    shares     NUMERIC NOT NULL,
                    price      NUMERIC NOT NULL,
                    leg_date   DATE NOT NULL,
                    note       TEXT,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    UNIQUE (trade_id, leg_idx)
                )
            """)
            cur.execute("CREATE INDEX IF NOT EXISTS idx_trade_legs_trade_id ON trade_legs(trade_id)")

            # ── 3. trade_exits ─────────────────────────────────────────────
            cur.execute("""
                CREATE TABLE IF NOT EXISTS trade_exits (
                    id         SERIAL PRIMARY KEY,
                    leg_id     INTEGER NOT NULL REFERENCES trade_legs(id) ON DELETE CASCADE,
                    shares     NUMERIC NOT NULL,
                    price      NUMERIC NOT NULL,
                    exit_date  DATE NOT NULL,
                    reason     TEXT CHECK (reason IN ('sbe', 'stop', 'target', 'manual', 'trailing')),
                    note       TEXT,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            """)
            cur.execute("CREATE INDEX IF NOT EXISTS idx_trade_exits_leg_id ON trade_exits(leg_id)")

            # ── 4. stop_history ────────────────────────────────────────────
            cur.execute("""
                CREATE TABLE IF NOT EXISTS stop_history (
                    id         SERIAL PRIMARY KEY,
                    trade_id   INTEGER NOT NULL REFERENCES trades(id) ON DELETE CASCADE,
                    stop_price NUMERIC NOT NULL,
                    set_at     TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    reason     TEXT CHECK (reason IN ('initial', 'manual', 'trailing', 'sbe')),
                    note       TEXT
                )
            """)
            cur.execute("CREATE INDEX IF NOT EXISTS idx_stop_history_trade_id ON stop_history(trade_id)")

            # ── 5. trades index'leri ───────────────────────────────────────
            for idx_sql in [
                "CREATE INDEX IF NOT EXISTS idx_trades_symbol     ON trades(symbol)",
                "CREATE INDEX IF NOT EXISTS idx_trades_status     ON trades(status)",
                "CREATE INDEX IF NOT EXISTS idx_trades_entry_date ON trades(entry_date)",
                "CREATE INDEX IF NOT EXISTS idx_trades_portfolio  ON trades(portfolio_id)",
            ]:
                cur.execute(idx_sql)

            # ── 6. journal_entries: orphan cleanup + FK constraint ─────────
            cur.execute("""
                UPDATE journal_entries
                SET linked_trade_id = NULL
                WHERE linked_trade_id IS NOT NULL
                  AND linked_trade_id NOT IN (SELECT id FROM trades)
            """)
            orphan_count = cur.rowcount
            if orphan_count:
                log.warning(
                    f"init_trade_journal_tables: {orphan_count} orphan "
                    "journal_entries satırı NULL'a çekildi"
                )

            cur.execute("""
                DO $$
                BEGIN
                    IF NOT EXISTS (
                        SELECT 1 FROM information_schema.table_constraints
                        WHERE constraint_name = 'fk_journal_trade'
                          AND table_name = 'journal_entries'
                    ) THEN
                        ALTER TABLE journal_entries
                        ADD CONSTRAINT fk_journal_trade
                        FOREIGN KEY (linked_trade_id) REFERENCES trades(id) ON DELETE SET NULL;
                    END IF;
                END $$
            """)

            # ── 7. trade_codes ────────────────────────────────────────────
            cur.execute("""
                CREATE TABLE IF NOT EXISTS trade_codes (
                    id         SERIAL PRIMARY KEY,
                    code       TEXT UNIQUE NOT NULL,
                    label      TEXT NOT NULL,
                    color      TEXT,
                    sort_order INTEGER,
                    created_at TIMESTAMP DEFAULT NOW()
                )
            """)
            cur.execute("""
                INSERT INTO trade_codes (code, label, color, sort_order) VALUES
                ('PB', 'Perfect Buy',  '#10b981', 1),
                ('EB', 'Early Buy',    '#f59e0b', 2),
                ('LB', 'Late Buy',     '#ef4444', 3),
                ('PS', 'Perfect Sell', '#10b981', 4),
                ('ES', 'Early Sell',   '#f59e0b', 5),
                ('LS', 'Late Sell',    '#ef4444', 6)
                ON CONFLICT (code) DO NOTHING
            """)

            # ── 8. setup_types ────────────────────────────────────────────
            cur.execute("""
                CREATE TABLE IF NOT EXISTS setup_types (
                    id          SERIAL PRIMARY KEY,
                    name        TEXT UNIQUE NOT NULL,
                    description TEXT,
                    sort_order  INTEGER,
                    created_at  TIMESTAMP DEFAULT NOW()
                )
            """)
            cur.execute("""
                INSERT INTO setup_types (name, description, sort_order) VALUES
                ('VCP',          'Volatility Contraction Pattern',          1),
                ('CWH',          'Cup with Handle',                         2),
                ('Power Play',   'High Tight Flag / HTF',                   3),
                ('Cheat',        '3-C / Low Cheat / Mid Cheat / High Cheat',4),
                ('Pocket Pivot', 'Kacher/Morales early entry',              5),
                ('Other',        'Genel/sınıflandırılmamış',               99)
                ON CONFLICT (name) DO NOTHING
            """)

            # ── 9. list_types ─────────────────────────────────────────────
            cur.execute("""
                CREATE TABLE IF NOT EXISTS list_types (
                    code                 TEXT PRIMARY KEY,
                    label                TEXT NOT NULL,
                    default_column_count INTEGER NOT NULL,
                    sort_order           INTEGER NOT NULL
                )
            """)
            cur.execute("""
                INSERT INTO list_types (code, label, default_column_count, sort_order) VALUES
                ('watch',    'Watch (100+ hisse)',  7,  1),
                ('on_deck',  'On Deck (30-40)',     10, 2),
                ('focus',    'Focus (5-15)',         13, 3),
                ('buy',      'Buy (3-5)',            17, 4)
                ON CONFLICT (code) DO NOTHING
            """)

            # ── 10. users ─────────────────────────────────────────────────
            cur.execute("""
                CREATE TABLE IF NOT EXISTS users (
                    id            SERIAL PRIMARY KEY,
                    email         TEXT UNIQUE NOT NULL,
                    name          TEXT,
                    timezone      TEXT DEFAULT 'Europe/Istanbul',
                    preferences   JSONB DEFAULT '{}'::jsonb,
                    created_at    TIMESTAMP DEFAULT NOW(),
                    last_login_at TIMESTAMP
                )
            """)
            cur.execute("""
                INSERT INTO users (email, name)
                VALUES ('ferit@quanfina.local', 'Sn. Ferit')
                ON CONFLICT (email) DO NOTHING
            """)

            # ── 11. symbol_lists ──────────────────────────────────────────
            cur.execute("""
                CREATE TABLE IF NOT EXISTS symbol_lists (
                    id              SERIAL PRIMARY KEY,
                    user_id         INTEGER REFERENCES users(id) ON DELETE CASCADE,
                    symbol          TEXT NOT NULL,
                    list_type       TEXT REFERENCES list_types(code),
                    strategy        TEXT DEFAULT 'minervini',
                    setup_type_id   INTEGER REFERENCES setup_types(id),
                    pivot_price     NUMERIC,
                    pullback_health NUMERIC,
                    tt_score        TEXT,
                    alarm_setup_at  TIMESTAMP,
                    day_added       DATE NOT NULL DEFAULT CURRENT_DATE,
                    note            TEXT,
                    UNIQUE(user_id, symbol, list_type, strategy)
                )
            """)
            cur.execute("CREATE INDEX IF NOT EXISTS idx_symbol_lists_user_list ON symbol_lists(user_id, list_type)")
            cur.execute("CREATE INDEX IF NOT EXISTS idx_symbol_lists_symbol    ON symbol_lists(symbol)")

            # ── 12. user_column_preferences ───────────────────────────────
            cur.execute("""
                CREATE TABLE IF NOT EXISTS user_column_preferences (
                    id              SERIAL PRIMARY KEY,
                    user_id         INTEGER REFERENCES users(id) ON DELETE CASCADE,
                    list_name       TEXT REFERENCES list_types(code),
                    strategy        TEXT DEFAULT 'minervini',
                    visible_columns JSONB NOT NULL,
                    column_order    INTEGER[] DEFAULT '{}',
                    column_widths   JSONB DEFAULT '{}'::jsonb,
                    updated_at      TIMESTAMP DEFAULT NOW(),
                    UNIQUE(user_id, list_name, strategy)
                )
            """)

            # ── 13. alert_events ──────────────────────────────────────────
            cur.execute("""
                CREATE TABLE IF NOT EXISTS alert_events (
                    id               SERIAL PRIMARY KEY,
                    user_id          INTEGER REFERENCES users(id) ON DELETE CASCADE,
                    symbol           TEXT NOT NULL,
                    alert_name       TEXT NOT NULL,
                    trigger_value    NUMERIC,
                    actual_value     NUMERIC,
                    triggered_at     TIMESTAMP DEFAULT NOW(),
                    source           TEXT DEFAULT 'tv_webhook',
                    raw_payload      JSONB,
                    related_trade_id INTEGER REFERENCES trades(id) ON DELETE SET NULL,
                    user_action_at   TIMESTAMP,
                    delay_seconds    INTEGER GENERATED ALWAYS AS (
                        EXTRACT(EPOCH FROM (user_action_at - triggered_at))::INTEGER
                    ) STORED
                )
            """)
            cur.execute("CREATE INDEX IF NOT EXISTS idx_alert_events_user_symbol ON alert_events(user_id, symbol)")
            cur.execute("CREATE INDEX IF NOT EXISTS idx_alert_events_triggered   ON alert_events(triggered_at DESC)")

            # ── 14. trade_stop_losses ─────────────────────────────────────
            cur.execute("""
                CREATE TABLE IF NOT EXISTS trade_stop_losses (
                    id           SERIAL PRIMARY KEY,
                    trade_id     INTEGER REFERENCES trades(id) ON DELETE CASCADE,
                    sl_idx       INTEGER NOT NULL,
                    price        NUMERIC NOT NULL,
                    prefix       TEXT CHECK (prefix IS NULL OR prefix = 'Sell half'),
                    suffix       TEXT CHECK (suffix IS NULL OR suffix IN ('breakeven', 'buy to cover')),
                    triggered_at TIMESTAMP,
                    set_at       TIMESTAMP DEFAULT NOW(),
                    note         TEXT,
                    UNIQUE(trade_id, sl_idx)
                )
            """)

            # ── 15. leg_exits ─────────────────────────────────────────────
            cur.execute("""
                CREATE TABLE IF NOT EXISTS leg_exits (
                    id           SERIAL PRIMARY KEY,
                    leg_id       INTEGER REFERENCES trade_legs(id) ON DELETE CASCADE,
                    exit_idx     INTEGER NOT NULL,
                    shares       NUMERIC NOT NULL,
                    price        NUMERIC NOT NULL,
                    exit_date    DATE NOT NULL,
                    commission   NUMERIC DEFAULT 0,
                    note         TEXT,
                    action_label TEXT,
                    reason       TEXT CHECK (reason IS NULL OR reason IN ('sbe', 'stop', 'target', 'manual', 'trailing')),
                    created_at   TIMESTAMP DEFAULT NOW(),
                    UNIQUE(leg_id, exit_idx)
                )
            """)

            # ── 16. trades: Asama 2 kolonları ────────────────────────────
            for stmt in [
                "ALTER TABLE trades ADD COLUMN IF NOT EXISTS user_id              INTEGER REFERENCES users(id)",
                "ALTER TABLE trades ADD COLUMN IF NOT EXISTS code_id              INTEGER REFERENCES trade_codes(id)",
                "ALTER TABLE trades ADD COLUMN IF NOT EXISTS setup_type_id        INTEGER REFERENCES setup_types(id)",
                "ALTER TABLE trades ADD COLUMN IF NOT EXISTS parked_at            TIMESTAMP",
                "ALTER TABLE trades ADD COLUMN IF NOT EXISTS park_reason          TEXT",
                "ALTER TABLE trades ADD COLUMN IF NOT EXISTS exit_reason          TEXT",
                "ALTER TABLE trades ADD COLUMN IF NOT EXISTS lesson_learned       TEXT",
                "ALTER TABLE trades ADD COLUMN IF NOT EXISTS chart_screenshot_url TEXT",
                "ALTER TABLE trades ADD COLUMN IF NOT EXISTS mistake_type         TEXT CHECK (mistake_type IS NULL OR mistake_type IN ('execution', 'strategy', 'market_change', 'none'))",
            ]:
                cur.execute(stmt)

            # ── 17. invest_type backfill ──────────────────────────────────
            cur.execute("""
                UPDATE trades
                SET invest_type = CASE
                    WHEN trade_type = 'Long'  THEN 1
                    WHEN trade_type = 'Short' THEN 2
                    ELSE invest_type
                END
                WHERE invest_type IS NULL
            """)

            # ── 18. user_id backfill ───────────────────────────────────────
            cur.execute("""
                UPDATE trades
                SET user_id = (SELECT id FROM users WHERE email = 'ferit@quanfina.local' LIMIT 1)
                WHERE user_id IS NULL
            """)

            # ── 19. trades: Asama 2 index'leri ───────────────────────────
            cur.execute("CREATE INDEX IF NOT EXISTS idx_trades_user_id       ON trades(user_id)")
            cur.execute("CREATE INDEX IF NOT EXISTS idx_trades_code_id       ON trades(code_id)")
            cur.execute("CREATE INDEX IF NOT EXISTS idx_trades_setup_type_id ON trades(setup_type_id)")

            # ── 20. trade_grade_categories (Sprint 4.7c.2 — EK 10 TradeGrader) ──
            cur.execute("""
                CREATE TABLE IF NOT EXISTS trade_grade_categories (
                    id          SERIAL PRIMARY KEY,
                    code        VARCHAR(8)  UNIQUE NOT NULL,
                    name        TEXT        NOT NULL,
                    short_name  VARCHAR(16) NOT NULL,
                    leg_type    VARCHAR(8)  NOT NULL,
                    target_pct  NUMERIC,
                    sort_order  INTEGER     NOT NULL,
                    created_at  TIMESTAMP   DEFAULT NOW(),
                    CONSTRAINT chk_tgc_leg_type CHECK (leg_type IN ('ENTRY', 'EXIT', 'LOSS'))
                )
            """)
            cur.execute("""
                INSERT INTO trade_grade_categories
                    (code, name, short_name, leg_type, target_pct, sort_order) VALUES
                ('BP',  'Bought perfect',          'Perfect',   'ENTRY',  80.0,  10),
                ('BE',  'Bought early',            'Early',     'ENTRY', -10.0,  20),
                ('BL',  'Bought late',             'Late',      'ENTRY', -10.0,  30),
                ('FS',  'Faulty setup',            'Faulty',    'ENTRY', -10.0,  40),
                ('BB',  'Bad buy',                 'Bad',       'ENTRY', -10.0,  50),
                ('EB',  'Emotional buy',           'Emotional', 'ENTRY',  -2.0,  60),
                ('CE',  'Chased extended',         'Chased',    'ENTRY',  -2.0,  70),
                ('SP',  'Sold perfect',            'Perfect',   'EXIT',   30.0, 110),
                ('SE',  'Sold early',              'Early',     'EXIT',  -50.0, 120),
                ('SL',  'Sold late',               'Late',      'EXIT',  -20.0, 130),
                ('RFR', 'Reduced to finance risk', 'RFR',       'EXIT',   NULL, 140),
                ('BS',  'Bad sale',                'Bad',       'EXIT',  -10.0, 150),
                ('CLP', 'Cut loss perfect',        'Perfect',   'LOSS',   95.0, 210),
                ('CLE', 'Cut loss early',          'Early',     'LOSS',  -30.0, 220),
                ('CLL', 'Cut loss late',           'Late',      'LOSS',   -5.0, 230),
                ('COT', 'Choked off trade',        'Choked',    'LOSS',  -30.0, 240),
                ('ES',  'Emotional sale',          'Emotional', 'LOSS',   -5.0, 250)
                ON CONFLICT (code) DO NOTHING
            """)

            # ── 21. trade_legs / trade_exits: grade_category_id FK + annotation ─
            for stmt in [
                "ALTER TABLE trade_legs  ADD COLUMN IF NOT EXISTS grade_category_id INTEGER REFERENCES trade_grade_categories(id)",
                "ALTER TABLE trade_legs  ADD COLUMN IF NOT EXISTS annotation TEXT",
                "ALTER TABLE trade_exits ADD COLUMN IF NOT EXISTS grade_category_id INTEGER REFERENCES trade_grade_categories(id)",
                "ALTER TABLE trade_exits ADD COLUMN IF NOT EXISTS annotation TEXT",
            ]:
                cur.execute(stmt)
            cur.execute("CREATE INDEX IF NOT EXISTS idx_trade_legs_grade_category  ON trade_legs(grade_category_id)")
            cur.execute("CREATE INDEX IF NOT EXISTS idx_trade_exits_grade_category ON trade_exits(grade_category_id)")

            # ── 22. leg_exits: grade_category_id FK + annotation (Sprint 4.7d.1) ─
            # close_trade() leg_exits'e yazıyor — grade kayıt akışı için gerekli
            for stmt in [
                "ALTER TABLE leg_exits ADD COLUMN IF NOT EXISTS grade_category_id INTEGER REFERENCES trade_grade_categories(id)",
                "ALTER TABLE leg_exits ADD COLUMN IF NOT EXISTS annotation TEXT",
            ]:
                cur.execute(stmt)
            cur.execute("CREATE INDEX IF NOT EXISTS idx_leg_exits_grade_category ON leg_exits(grade_category_id)")

            conn.commit()
            log.info("init_trade_journal_tables: tamamlandı")
            return orphan_count

    except Exception as e:
        conn.rollback()
        log.error(f"init_trade_journal_tables error: {e}")
        raise
    finally:
        conn.close()
