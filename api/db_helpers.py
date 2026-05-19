"""
Quanfina POC sonrası DB helpers.
Faz 1: web_watchlist + web_trades CRUD.
"""
import json
import os
from pathlib import Path
from typing import Optional
from urllib.parse import quote_plus

from dotenv import load_dotenv
from sqlalchemy import create_engine, text

load_dotenv(Path(__file__).parent.parent / ".env")

_HOST = os.getenv("PG_HOST", "")
_PORT = os.getenv("PG_PORT", "5432")
_DB   = os.getenv("PG_DATABASE", "")
_USER = os.getenv("PG_USER", "")
_PASS = quote_plus(os.getenv("PG_PASSWORD", ""))

if _HOST.startswith("/"):
    _URL = f"postgresql+psycopg2://{_USER}:{_PASS}@/{_DB}?host={_HOST}"
else:
    _URL = f"postgresql+psycopg2://{_USER}:{_PASS}@{_HOST}:{_PORT}/{_DB}?sslmode=require"

engine = create_engine(_URL, pool_pre_ping=True, pool_size=5, max_overflow=10)

# =============================================================
# web_watchlist CRUD
# =============================================================

def watchlist_get_all() -> list[dict]:
    with engine.connect() as conn:
        result = conn.execute(text("""
            SELECT symbol, strategy, status, price, added_date,
                   setup_type, pivot_price, note, rs_rating,
                   consensus_count, consensus_strategies
            FROM web_watchlist
            ORDER BY added_date DESC, symbol ASC
        """))
        rows = []
        for row in result:
            d = dict(row._mapping)
            d["added_date"] = d["added_date"].isoformat() if d["added_date"] else None
            cs = d["consensus_strategies"]
            d["consensus_strategies"] = cs if isinstance(cs, list) else json.loads(cs)
            rows.append(d)
        return rows


def watchlist_insert(row: dict) -> None:
    with engine.begin() as conn:
        conn.execute(text("""
            INSERT INTO web_watchlist (
                symbol, strategy, status, price, added_date,
                setup_type, pivot_price, note, rs_rating,
                consensus_count, consensus_strategies
            ) VALUES (
                :symbol, :strategy, :status, :price, :added_date,
                :setup_type, :pivot_price, :note, :rs_rating,
                :consensus_count, CAST(:consensus_strategies AS JSONB)
            )
        """), {
            **{k: v for k, v in row.items() if k in (
                "symbol", "strategy", "status", "price", "added_date",
                "setup_type", "pivot_price", "note", "rs_rating",
                "consensus_count",
            )},
            "consensus_strategies": json.dumps(row.get("consensus_strategies", [])),
        })


def watchlist_update(symbol: str, strategy: str, updates: dict) -> None:
    if not updates:
        return
    clean = {k: v for k, v in updates.items() if k not in ("symbol", "strategy")}
    if not clean:
        return
    set_clauses = ", ".join(f"{k} = :{k}" for k in clean)
    with engine.begin() as conn:
        conn.execute(text(f"""
            UPDATE web_watchlist
            SET {set_clauses}
            WHERE symbol = :_sym AND strategy = :_strat
        """), {**clean, "_sym": symbol, "_strat": strategy})


def watchlist_get_one(symbol: str, strategy: str) -> Optional[dict]:
    with engine.connect() as conn:
        result = conn.execute(text("""
            SELECT symbol, strategy, status, price, added_date,
                   setup_type, pivot_price, note, rs_rating,
                   consensus_count, consensus_strategies
            FROM web_watchlist
            WHERE symbol = :symbol AND strategy = :strategy
        """), {"symbol": symbol, "strategy": strategy})
        row = result.first()
        if not row:
            return None
        d = dict(row._mapping)
        d["added_date"] = d["added_date"].isoformat() if d["added_date"] else None
        cs = d["consensus_strategies"]
        d["consensus_strategies"] = cs if isinstance(cs, list) else json.loads(cs)
        return d


def watchlist_exists(symbol: str, strategy: str) -> bool:
    with engine.connect() as conn:
        result = conn.execute(text("""
            SELECT 1 FROM web_watchlist WHERE symbol = :symbol AND strategy = :strategy
        """), {"symbol": symbol, "strategy": strategy})
        return result.first() is not None


def watchlist_delete(symbol: str, strategy: str) -> bool:
    with engine.begin() as conn:
        result = conn.execute(text("""
            DELETE FROM web_watchlist
            WHERE symbol = :symbol AND strategy = :strategy
        """), {"symbol": symbol, "strategy": strategy})
        return result.rowcount > 0


def watchlist_recompute_consensus() -> None:
    """Her sembol için consensus_count + consensus_strategies'i yeniden hesaplar."""
    with engine.begin() as conn:
        conn.execute(text("""
            WITH symbol_consensus AS (
                SELECT
                    symbol,
                    COUNT(DISTINCT strategy)::SMALLINT            AS cnt,
                    jsonb_agg(DISTINCT strategy ORDER BY strategy) AS strats
                FROM web_watchlist
                GROUP BY symbol
            )
            UPDATE web_watchlist w
            SET consensus_count       = sc.cnt,
                consensus_strategies  = sc.strats
            FROM symbol_consensus sc
            WHERE w.symbol = sc.symbol
        """))


# =============================================================
# web_trades CRUD
# =============================================================

def trades_get_all() -> list[dict]:
    with engine.connect() as conn:
        result = conn.execute(text("""
            SELECT id, symbol, strategy, setup_type,
                   entry_date, entry_price, exit_date, exit_price,
                   shares, status, pl_dollar, pl_pct,
                   grade, exit_reason, lessons
            FROM web_trades
            ORDER BY id DESC
        """))
        rows = []
        for row in result:
            d = dict(row._mapping)
            for df in ("entry_date", "exit_date"):
                if d[df] is not None:
                    d[df] = d[df].isoformat()
            for nf in ("entry_price", "exit_price", "pl_dollar", "pl_pct"):
                if d[nf] is not None:
                    d[nf] = float(d[nf])
            rows.append(d)
        return rows


def trades_insert(trade: dict) -> int:
    with engine.begin() as conn:
        result = conn.execute(text("""
            INSERT INTO web_trades (
                symbol, strategy, setup_type,
                entry_date, entry_price, exit_date, exit_price,
                shares, status, pl_dollar, pl_pct,
                grade, exit_reason, lessons
            ) VALUES (
                :symbol, :strategy, :setup_type,
                :entry_date, :entry_price, :exit_date, :exit_price,
                :shares, :status, :pl_dollar, :pl_pct,
                :grade, :exit_reason, :lessons
            )
            RETURNING id
        """), trade)
        return result.scalar()


def trades_update(trade_id: int, updates: dict) -> bool:
    if not updates:
        return False
    set_clauses = ", ".join(f"{k} = :{k}" for k in updates)
    with engine.begin() as conn:
        result = conn.execute(text(f"""
            UPDATE web_trades SET {set_clauses} WHERE id = :_id
        """), {**updates, "_id": trade_id})
        return result.rowcount > 0


def trades_delete(trade_id: int) -> bool:
    with engine.begin() as conn:
        result = conn.execute(
            text("DELETE FROM web_trades WHERE id = :id"), {"id": trade_id}
        )
        return result.rowcount > 0


def trades_get_by_id(trade_id: int) -> Optional[dict]:
    with engine.connect() as conn:
        result = conn.execute(
            text("""
                SELECT id, symbol, strategy, setup_type,
                       entry_date, entry_price, exit_date, exit_price,
                       shares, status, pl_dollar, pl_pct,
                       grade, exit_reason, lessons
                FROM web_trades WHERE id = :id
            """),
            {"id": trade_id},
        )
        row = result.first()
        if not row:
            return None
        d = dict(row._mapping)
        for df in ("entry_date", "exit_date"):
            if d[df] is not None:
                d[df] = d[df].isoformat()
        for nf in ("entry_price", "exit_price", "pl_dollar", "pl_pct"):
            if d[nf] is not None:
                d[nf] = float(d[nf])
        return d


# =============================================================
# Sprint 4-bis.1b: 8 Ready Screen — saf SQL filtreleri
# Kaynak: notebook/Notebook_C1_Sprint_QuickStart.md
# Kural: "Notebook = Hedef Vizyon" (Kalıcı İlke #4) — Notebook neyse SQL onu uygular
# =============================================================

SCREENS_READY_8 = {
    "tpr_a":           {"label": "TPR A",                     "filter": "grade = 'A'"},
    "tpr_a_b":         {"label": "TPR A & B",                 "filter": "grade IN ('A','B')"},
    "rpr_89_tpr_c":    {"label": "RPR 89+ TPR C+",            "filter": "rs_ibd >= 89 AND grade IN ('A','B','C')"},
    "stage2_10p":      {"label": "Stage 2 ($10+)",            "filter": "passed = 1 AND price >= 10"},
    "stage2_below_10": {"label": "Stage 2 (Below $10)",       "filter": "passed = 1 AND price < 10"},
    "top5_rpr":        {"label": "Top 5% RPR",                "filter": "rs_ibd >= 95"},
    "mom_10p":         {"label": "Minervini Momentum ($10+)", "filter": "passed = 1 AND price >= 10"},
    "mom_below_10":    {"label": "Momentum (Below $10)",      "filter": "passed = 1 AND price < 10"},
}


def screen_get_results(slug: str, limit: int = 500) -> list[dict]:
    """
    Sprint 4-bis.1b — Verilen ready screen slug'una göre minervini_scans tablosundan
    en son scan_date için filtrelenmiş hisseleri döner.

    Args:
        slug: SCREENS_READY_8 anahtar (ör. "tpr_a", "rpr_89_tpr_c")
        limit: Sonuç limit (default 500)

    Returns:
        [{symbol, grade, rs_ibd, price, passed, scan_date}, ...]

    Raises:
        ValueError: slug SCREENS_READY_8'de yoksa
    """
    if slug not in SCREENS_READY_8:
        raise ValueError(
            f"Bilinmeyen screen slug: '{slug}'. "
            f"Gecerli: {list(SCREENS_READY_8.keys())}"
        )

    sql_filter = SCREENS_READY_8[slug]["filter"]

    # Idempotent + parametre safety:
    # - filter SCREENS_READY_8 sabit dict'ten (SQL injection riski yok)
    # - LIMIT parametrize edildi
    query = f"""
        SELECT symbol, grade, rs_ibd, price, passed, scan_date
        FROM minervini_scans
        WHERE scan_date = (SELECT MAX(scan_date) FROM minervini_scans)
          AND {sql_filter}
        ORDER BY rs_ibd DESC NULLS LAST, symbol ASC
        LIMIT :limit
    """

    with engine.connect() as conn:
        result = conn.execute(text(query), {"limit": limit})
        rows = []
        for row in result:
            d = dict(row._mapping)
            if d.get("scan_date") is not None:
                d["scan_date"] = d["scan_date"].isoformat()
            if d.get("price") is not None:
                d["price"] = float(d["price"])
            if d.get("rs_ibd") is not None:
                d["rs_ibd"] = int(d["rs_ibd"]) if d["rs_ibd"] is not None else None
            rows.append(d)
        return rows


def screen_list_available() -> list[dict]:
    """Mevcut 8 ready screen meta listesini döner (frontend dropdown icin)."""
    return [
        {"slug": slug, "label": meta["label"], "filter_summary": meta["filter"]}
        for slug, meta in SCREENS_READY_8.items()
    ]


# =============================================================
# Health check
# =============================================================

def db_health_check() -> bool:
    try:
        with engine.connect() as conn:
            conn.execute(text("SELECT 1"))
        return True
    except Exception:
        return False
