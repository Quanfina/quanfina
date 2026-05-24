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

# connect_timeout=5: Cloud SQL erisilemez (paused / IP whitelist) durumlarinda
# OS default TCP timeout (~75-130s) yerine 5sn'de hizli fail.
# pool_recycle=300: 5 dakikalik baglantilar Cloud SQL idle drop'tan kacinir.
engine = create_engine(
    _URL,
    pool_pre_ping=True,
    pool_size=5,
    max_overflow=10,
    pool_recycle=300,
    connect_args={"connect_timeout": 5},
)

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
                   grade, exit_reason, lessons,
                   plan_entry_trigger, plan_stop, plan_target,
                   plan_size_pct, plan_exit_strategy, plan_time_horizon
            FROM web_trades
            ORDER BY id DESC
        """))
        rows = []
        for row in result:
            d = dict(row._mapping)
            for df in ("entry_date", "exit_date"):
                if d[df] is not None:
                    d[df] = d[df].isoformat()
            for nf in (
                "entry_price", "exit_price", "pl_dollar", "pl_pct",
                "plan_stop", "plan_target", "plan_size_pct",
            ):
                if d.get(nf) is not None:
                    d[nf] = float(d[nf])
            rows.append(d)
        return rows


def trades_insert(trade: dict) -> int:
    # KARAR ADAY #717 — Mark TTLC Sec 1 6 zorunlu plan alani
    # API katmaninda Pydantic Field required; eski testler icin geriye uyum
    # plan_* anahtarlari yoksa None default'la doldur
    trade_with_plan = {
        "plan_entry_trigger": None,
        "plan_stop": None,
        "plan_target": None,
        "plan_size_pct": None,
        "plan_exit_strategy": None,
        "plan_time_horizon": None,
        **trade,
    }
    with engine.begin() as conn:
        result = conn.execute(text("""
            INSERT INTO web_trades (
                symbol, strategy, setup_type,
                entry_date, entry_price, exit_date, exit_price,
                shares, status, pl_dollar, pl_pct,
                grade, exit_reason, lessons,
                plan_entry_trigger, plan_stop, plan_target,
                plan_size_pct, plan_exit_strategy, plan_time_horizon
            ) VALUES (
                :symbol, :strategy, :setup_type,
                :entry_date, :entry_price, :exit_date, :exit_price,
                :shares, :status, :pl_dollar, :pl_pct,
                :grade, :exit_reason, :lessons,
                :plan_entry_trigger, :plan_stop, :plan_target,
                :plan_size_pct, :plan_exit_strategy, :plan_time_horizon
            )
            RETURNING id
        """), trade_with_plan)
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
                       grade, exit_reason, lessons,
                       plan_entry_trigger, plan_stop, plan_target,
                       plan_size_pct, plan_exit_strategy, plan_time_horizon
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
        for nf in (
            "entry_price", "exit_price", "pl_dollar", "pl_pct",
            "plan_stop", "plan_target", "plan_size_pct",
        ):
            if d[nf] is not None:
                d[nf] = float(d[nf])
        return d


# =============================================================
# Sprint 4-bis.1b: 8 Ready Screen — saf SQL filtreleri
# Kaynak: notebook/Notebook_C1_Sprint_QuickStart.md
# Kural: "Notebook = Hedef Vizyon" (Kalıcı İlke #4) — Notebook neyse SQL onu uygular
# =============================================================

# SCREENS_READY_9: 8 orijinal + tight_low_volume (Sprint 4-bis.4, KARAR #461
# Master pre-compute kararı — scanner.py'nin yazdığı BOOLEAN okunur).
# Eski "SCREENS_READY_8" referansı bilinçli korundu (geriye uyumluluk),
# yeni isim aşağıda alias olarak set edilir.
SCREENS_READY_9 = {
    "tpr_a":            {"label": "TPR A",                     "filter": "grade = 'A'"},
    "tpr_a_b":          {"label": "TPR A & B",                 "filter": "grade IN ('A','B')"},
    "rpr_89_tpr_c":     {"label": "RPR 89+ TPR C+",            "filter": "rs_ibd >= 89 AND grade IN ('A','B','C')"},
    # KARAR #484 (22 May 2026) — RS >= 70 (IBD) backend filter eklendi.
    # passed=1 sadece 8 DMA/52W kosulunu kontrol ediyor. UI'de gosterilen 9. Mark
    # Resmi Kural (RS Rating >= 70 IBD, Trade Like Wizard s.79) backend'de UYGULANMIYORDU.
    # Sn. Ferit talimati (22 May 2026): "A yap" — filter'a rs_ibd >= 70 ekle.
    # Etki: 724 satir -> ~225 satir (RS<70 olan 499 hisse cikar). Kitap birebir.
    "stage2_10p":       {"label": "Stage 2 ($10+)",            "filter": "passed = 1 AND price >= 10 AND rs_ibd >= 70"},
    "stage2_below_10":  {"label": "Stage 2 (Below $10)",       "filter": "passed = 1 AND price < 10"},
    "top5_rpr":         {"label": "Top 5% RPR",                "filter": "rs_ibd >= 95"},
    "mom_10p":          {"label": "Minervini Momentum ($10+)", "filter": "passed = 1 AND price >= 10"},
    "mom_below_10":     {"label": "Momentum (Below $10)",      "filter": "passed = 1 AND price < 10"},
    # KARAR #461 — Master Pre-Compute (VCP scanner.py'de hesap, DB sade BOOLEAN okur)
    "tight_low_volume": {"label": "Tight Price Low Vol (VCP)", "filter": "tight_low_vol_pass = TRUE"},
    # KARAR #466 (20 May 2026) — VCP A+ Kalite (Mark canon "%50 alti en siki" + Bonus FMP altin)
    "tight_low_vol_excellent": {
        "label": "Tight Price Low Vol — EXCELLENT (A+)",
        "filter": "vcp_quality_score = 'EXCELLENT'",
    },
    # KARAR #465 (20 May 2026) — Minervini Uzmani: VCP Ready Score 70+ Inside Day + V-Dry + Tight
    "vcp_ready_high": {
        "label": "VCP Ready Score 70+",
        "filter": "vcp_ready_score >= 70",
    },
    # KARAR #467 (20 May 2026) — Power Play (HTF) Mark canon: POLE %100+ FLAG %10-25
    "power_play_ready": {
        "label": "⚡ Power Play (HTF)",
        "filter": "power_play_pass = TRUE",
    },
    # KARAR ADAY #893 (24 May 2026) — Mark Tennis Ball Detector (TLSMW s.253)
    # Mark William M. B. Berger: "I want to own tennis balls, not eggs."
    # Saglikli pullback + recovery = breakout sonrasi institutional kabul
    "tennis_ball_active": {
        "label": "🎾 Tennis Ball Aktif",
        "filter": "tennis_ball_pattern = 'TENNIS_BALL'",
    },
    # KARAR ADAY #882 (24 May 2026) — Mark Volume Asymmetry (TLSMW s.234)
    # Up volume >> Down volume = institutional accumulation
    "healthy_accumulation": {
        "label": "📈 Healthy Accumulation",
        "filter": "volume_asymmetry_tier = 'healthy'",
    },
    # KARAR ADAY #486 (23 May 2026) — Temel Eleme: 4 kosul (24 May revize)
    # 24 May 2026 — Gem + Quanfina Notebook Çift Danışma sonucu:
    #   Mark felsefesi gereği Fundamental "Hard Filter" DEĞİL, "Soft Score"
    #   ROE filtresi (fa_roe_pos15) scanner'dan kaldırıldı (Mark Ekstra olarak UI'da)
    # Tablo: minervini_fundamental_only (Quanfina pratik Hard Cut 4 kosul)
    "temel_eleme": {
        "label": "Temel Eleme (Mark Fundamental Soft Score)",
        "filter": "1 = 1",
        "table": "minervini_fundamental_only",
    },
    # KARAR ADAY #495 (24 May 2026) — Tam Minervini Tarama (Hibrit 15 koşul)
    # Sn. Ferit talimat: "tam minervini tarama listesi hem teknik hem temel"
    # Çift Danışma (Gem + Quanfina Notebook):
    #   AŞAMA 1 SCREEN (Hard Filter, 10): 2 Quanfina + 8 Mark Resmi Teknik (Trend Template)
    #   AŞAMA 2 RECIPE (Soft Score, 5):   EPS Q/Q + Sales Q/Q + ROE + Yıllık EPS + Margin
    # Tablo: minervini_fundamental_scans (Trend Template + EPS + Sales filtreli mevcut)
    # filter "1 = 1" — tablo Trend Template + Fundamental Hard Cut geçmişleri tutar
    "tam_minervini": {
        "label": "Tam Minervini Tarama (Trend Template + Fundamentals)",
        "filter": "1 = 1",
        "table": "minervini_fundamental_scans",
    },
}

# Geriye dönük alias — Sprint 4-bis.1b'de SCREENS_READY_8 ismiyle bilinen
# dict artık 10 entry. Eski referansları kırmamak için yeniden bağlanır.
SCREENS_READY_8 = SCREENS_READY_9


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

    meta = SCREENS_READY_8[slug]
    sql_filter = meta["filter"]
    # Tablo override: temel_eleme gibi slug'lar farkli tablodan okur (minervini_fundamental_only)
    # KARAR ADAY #486 (23 May 2026) — Sn. Ferit "5 kosulu yapalim trend template gibi"
    table_name = meta.get("table", "minervini_scans")
    # minervini_fundamental_only'da passed kolonu YOK (tablo Fundamental gecenleri tutar)
    # minervini_fundamental_scans Trend Template + Fundamental Hard Cut gecmis hisseleri tutar
    # Her ikisi de "geçti" varsayilir — Trend Template kontrolu olmadigi icin
    # "1 AS passed" hard-coded (gecti varsayilir)
    fundamental_tables = ("minervini_fundamental_only", "minervini_fundamental_scans")
    passed_expr = "1 AS passed" if table_name in fundamental_tables else "passed"
    # minervini_fundamental_scans Cloud SQL'de grade kolonu YOK olabilir
    # (scanner.py ALTER TABLE ile ekler, ama Cloud SQL'e deploy edilmemis)
    # Defensive: NULL hard-coded, scanner deploy sonrasi grade SELECT edilir
    grade_expr = "NULL::TEXT AS grade" if table_name == "minervini_fundamental_scans" else "grade"

    # Idempotent + parametre safety:
    # - filter SCREENS_READY_8 sabit dict'ten (SQL injection riski yok)
    # - LIMIT parametrize edildi
    # NOT: minervini_scans tablosunda kolon adi 'ticker', frontend uyumu icin 'symbol' alias
    # rs_ibd double precision (TEXT degil) - Python tarafinda float kabul edilir
    # scan_date TEXT (TIMESTAMP degil) - .isoformat() cagrilmaz
    #
    # KARAR ADAY (22 May 2026): Migration 004-006 (vcp_quality_score, vcp_ready_score,
    # power_play_pass) Cloud SQL'e UYGULANMAMIS. ACIK KONU #70 ile baglantili.
    # Gecici patch: bu 3 kolon SELECT'ten cikarildi. Frontend NULL/undefined olarak goruyor
    # (ScreenResultRow tipinde optional). Migration uygulandiginda geri eklenecek.
    # Sn. Ferit talimat (22 May 2026): "Adim 1 + Cup-Handle + Pocket Pivot tumu calisir...
    # bunlarida yapicaz ama hemen degil" -> B patch.
    query = f"""
        SELECT ticker AS symbol, {grade_expr}, rs_ibd, price, {passed_expr}, scan_date
        FROM {table_name}
        WHERE scan_date = (SELECT MAX(scan_date) FROM {table_name})
          AND {sql_filter}
        ORDER BY rs_ibd DESC NULLS LAST, ticker ASC
        LIMIT :limit
    """

    with engine.connect() as conn:
        result = conn.execute(text(query), {"limit": limit})
        rows = []
        for row in result:
            d = dict(row._mapping)
            # scan_date TEXT olarak gelir, isoformat gerekmez
            if d.get("price") is not None:
                d["price"] = float(d["price"])
            if d.get("rs_ibd") is not None:
                d["rs_ibd"] = int(round(float(d["rs_ibd"])))
            rows.append(d)
        return rows


def screen_list_available() -> list[dict]:
    """Screen meta listesini doner (frontend dropdown icin).
    Sprint 4-bis.5 KARAR #467 sonrasi kategori sayim: 12 ready + 7 parse + 6 diff + 0 deferred = 25 ekran.
    """
    out = [
        {"slug": slug, "label": meta["label"], "filter_summary": meta["filter"],
         "category": "ready"}
        for slug, meta in SCREENS_READY_9.items()
    ]
    out.extend([
        {"slug": slug, "label": meta["label"], "filter_summary": meta["filter"],
         "category": "parse"}
        for slug, meta in SCREENS_PARSE_7.items()
    ])
    # Sprint 4-bis.3 — scan_diff (Master danisma: self-JOIN + 6'li grade + tolerant 7g)
    out.extend([
        {"slug": slug, "label": meta["label"], "filter_summary": meta["filter"],
         "category": "diff"}
        for slug, meta in SCREENS_DIFF_6.items()
    ])
    # KARAR #461 — Deferred kategorisi bosaldi (tight_low_volume Ready'ye tasindi).
    # 30 gun sonra Kural #18 pasif oge cikarma adayi (ScreenCategory union'dan da silinebilir).
    return out


# =============================================================
# Sprint 4-bis.2: 7 Parse Screen — confirmations/violations text parse
# Kaynak: notebook/Notebook_C1_Sprint_QuickStart.md SCREENS tuple
#         notebook/Sprint_4_bis_Mimari_Kararlar.md KARAR ADAY #457
# DB format: confirmations/violations virgülle ayrilmis TEXT
#   ornek conf = 'Inside Day,Volume Surge,Up on Volume'
# Pattern: string_to_array + array_length (PostgreSQL native)
# Clean-room: Notebook_C1 satir 482 yasakli isim (tescilli marka)
#             -> Quanfina-ozgu "momentum_5x_rpr_70" (KARAR #459 Kural #20 uyumu)
# =============================================================

SCREENS_PARSE_7 = {
    "stage2_loose_10p": {
        "label": "Stage 2 Loose ($10+)",
        "filter": "passed=1 AND price>=10 AND confirmations>=6",
        "sql": """
            passed = 1
            AND price >= 10
            AND COALESCE(array_length(string_to_array(NULLIF(confirmations, ''), ','), 1), 0) >= 6
        """,
    },
    "stage2_loose_below": {
        "label": "Stage 2 Loose (Below $10)",
        "filter": "passed=1 AND price<10 AND confirmations>=6",
        "sql": """
            passed = 1
            AND price < 10
            AND COALESCE(array_length(string_to_array(NULLIF(confirmations, ''), ','), 1), 0) >= 6
        """,
    },
    "stage2_vloose_10p": {
        "label": "Stage 2 Very Loose ($10+)",
        "filter": "passed=1 AND price>=10 AND confirmations>=4",
        "sql": """
            passed = 1
            AND price >= 10
            AND COALESCE(array_length(string_to_array(NULLIF(confirmations, ''), ','), 1), 0) >= 4
        """,
    },
    "stage2_vloose_below": {
        "label": "Stage 2 Very Loose (Below $10)",
        "filter": "passed=1 AND price<10 AND confirmations>=4",
        "sql": """
            passed = 1
            AND price < 10
            AND COALESCE(array_length(string_to_array(NULLIF(confirmations, ''), ','), 1), 0) >= 4
        """,
    },
    "buy_risk_green": {
        "label": "Buy Risk Green",
        "filter": "(conf_count - viol_count) >= 2",
        "sql": """
            COALESCE(array_length(string_to_array(NULLIF(confirmations, ''), ','), 1), 0) -
            COALESCE(array_length(string_to_array(NULLIF(violations, ''), ','), 1), 0) >= 2
        """,
    },
    "momentum_5x_rpr_70": {
        "label": "Momentum 5x (RPR 70+)",  # Quanfina-ozgu adlandirma (clean-room)
        "filter": "passed=1 AND rs_ibd>=70 AND conf>=3 AND price>=10",
        "sql": """
            passed = 1
            AND rs_ibd >= 70
            AND price >= 10
            AND COALESCE(array_length(string_to_array(NULLIF(confirmations, ''), ','), 1), 0) >= 3
        """,
    },
    "mom_qualifier": {
        "label": "Minervini Qualifier",
        "filter": "passed=1 AND rs_ibd>=80 (siki momentum)",
        "sql": """
            passed = 1
            AND rs_ibd >= 80
        """,
    },
}


def screen_parse_get_results(slug: str, limit: int = 500) -> list[dict]:
    """Sprint 4-bis.2 parse screen — confirmations/violations text-parse SQL."""
    if slug not in SCREENS_PARSE_7:
        raise ValueError(
            f"Bilinmeyen parse screen slug: '{slug}'. "
            f"Gecerli: {list(SCREENS_PARSE_7.keys())}"
        )

    sql_filter = SCREENS_PARSE_7[slug]["sql"].strip()

    query = f"""
        SELECT ticker AS symbol, grade, rs_ibd, price, passed, scan_date,
               confirmations, violations
        FROM minervini_scans
        WHERE scan_date = (SELECT MAX(scan_date) FROM minervini_scans)
          AND {sql_filter}
        ORDER BY rs_ibd DESC NULLS LAST, ticker ASC
        LIMIT :limit
    """

    with engine.connect() as conn:
        result = conn.execute(text(query), {"limit": limit})
        rows = []
        for row in result:
            d = dict(row._mapping)
            if d.get("price") is not None:
                d["price"] = float(d["price"])
            if d.get("rs_ibd") is not None:
                d["rs_ibd"] = int(round(float(d["rs_ibd"])))
            rows.append(d)
        return rows


# =============================================================
# Sprint 4-bis.3: 6 scan_diff Screen — onceki scan karsilastirma
# Kaynak: notebook/Notebook_C1_Sprint_QuickStart.md SCREENS tuple
#         Master NotebookLM 19 May 2026 ~08:45 danışma cevabi
#         (Kural #20 cift danisma + KARAR ADAY #457 ailesi)
#
# Master kararlari:
# 1. SQL strateji: Self-JOIN (LAG degil) - performans + index kullanim
# 2. 7 gun toleransi: ±1 gun BETWEEN + DISTINCT ON sirali fallback
# 3. Grade ordering: 6'li A+/A/B/C/D/F = 5/4/3/2/1/0 (KARAR #403 + TradeGrader 17 kategori)
# 4. rs_ibd delta esigi YOK - saf gecis yeterli (Minervini hard cut felsefesi)
# 5. Mevcut Notebook_B6 s.1725-1734 self-JOIN pattern miras
# =============================================================

SCREENS_DIFF_6 = {
    "tpr_moving_up": {
        "label": "TPR Moving Up",
        "filter": "grade_rank > prev_grade_rank",
        "where": "g.grade_rank > p.prev_grade_rank AND p.prev_grade IS NOT NULL",
    },
    "tpr_jump_2": {
        "label": "TPR Jump 2+ Grades",
        "filter": "grade yukseli si >= 2",
        "where": "(g.grade_rank - p.prev_grade_rank) >= 2 AND p.prev_grade IS NOT NULL",
    },
    "tpr_d_to_b": {
        "label": "TPR D to B+",
        "filter": "Onceki D, simdi A+/A/B",
        "where": "p.prev_grade = 'D' AND g.grade IN ('A+','A','B')",
    },
    "new_top5_rpr": {
        "label": "New Top 5% RPR",
        "filter": "Onceki rs<95, simdi rs>=95",
        "where": "p.prev_rs_ibd < 95 AND g.rs_ibd >= 95 AND p.prev_rs_ibd IS NOT NULL",
    },
    "new_7d_rpr_10p": {
        "label": "New 7D RPR ($10+)",
        "filter": "7g onceki rs<95, simdi rs>=95, $10+",
        "where": "g.price >= 10 AND p7.prev_7d_rs_ibd < 95 AND g.rs_ibd >= 95 AND p7.prev_7d_rs_ibd IS NOT NULL",
    },
    "new_7d_rpr_below": {
        "label": "New 7D RPR (Below $10)",
        "filter": "Ayni, $10 alti",
        "where": "g.price < 10 AND p7.prev_7d_rs_ibd < 95 AND g.rs_ibd >= 95 AND p7.prev_7d_rs_ibd IS NOT NULL",
    },
}


def screen_diff_get_results(slug: str, limit: int = 500) -> list[dict]:
    """Sprint 4-bis.3 scan_diff — Self-JOIN ile onceki scan karsilastirma.

    Master danisma (Kural #20):
    - Self-JOIN (LAG performans degil, B-Tree index dostu)
    - 6'li grade rank (A+/A/B/C/D/F = 5/4/3/2/1/0)
    - 7 gun toleransi ±1 (haftasonu + tatil fallback)
    """
    if slug not in SCREENS_DIFF_6:
        raise ValueError(
            f"Bilinmeyen scan_diff slug: '{slug}'. "
            f"Gecerli: {list(SCREENS_DIFF_6.keys())}"
        )

    sql_filter = SCREENS_DIFF_6[slug]["where"]

    # Grade rank CASE — A+/A/B/C/D/F = 5/4/3/2/1/0
    grade_rank_expr = """
        CASE grade
            WHEN 'A+' THEN 5 WHEN 'A' THEN 4 WHEN 'B' THEN 3
            WHEN 'C' THEN 2  WHEN 'D' THEN 1 WHEN 'F' THEN 0
            ELSE NULL
        END
    """

    # Self-JOIN pattern (Master Notebook_B6 s.1725-1734 miras)
    query = f"""
        WITH current_scan AS (
            SELECT ticker, scan_date, grade, rs_ibd, price, passed,
                   ({grade_rank_expr}) AS grade_rank
            FROM minervini_scans
            WHERE scan_date = (SELECT MAX(scan_date) FROM minervini_scans)
        ),
        previous_scan AS (
            SELECT DISTINCT ON (ticker)
                   ticker,
                   grade AS prev_grade,
                   rs_ibd AS prev_rs_ibd,
                   ({grade_rank_expr}) AS prev_grade_rank,
                   scan_date AS prev_scan_date
            FROM minervini_scans
            WHERE scan_date < (SELECT MAX(scan_date) FROM minervini_scans)
            ORDER BY ticker, scan_date DESC
        ),
        prev_7d AS (
            -- 7 gun once ±1 toleransi (Master karar: Tolerant + Fallback)
            SELECT DISTINCT ON (ticker)
                   ticker,
                   rs_ibd AS prev_7d_rs_ibd,
                   scan_date AS prev_7d_scan_date
            FROM minervini_scans
            WHERE scan_date::date BETWEEN
                  ((SELECT MAX(scan_date)::date FROM minervini_scans) - INTERVAL '8 days')
              AND ((SELECT MAX(scan_date)::date FROM minervini_scans) - INTERVAL '6 days')
            ORDER BY ticker, scan_date DESC
        )
        SELECT g.ticker AS symbol, g.grade, g.rs_ibd, g.price, g.passed, g.scan_date
        FROM current_scan g
        LEFT JOIN previous_scan p ON g.ticker = p.ticker
        LEFT JOIN prev_7d p7 ON g.ticker = p7.ticker
        WHERE {sql_filter}
        ORDER BY g.rs_ibd DESC NULLS LAST, g.ticker ASC
        LIMIT :limit
    """

    with engine.connect() as conn:
        result = conn.execute(text(query), {"limit": limit})
        rows = []
        for row in result:
            d = dict(row._mapping)
            if d.get("price") is not None:
                d["price"] = float(d["price"])
            if d.get("rs_ibd") is not None:
                d["rs_ibd"] = int(round(float(d["rs_ibd"])))
            rows.append(d)
        return rows


def screen_get_results_dispatch(slug: str, limit: int = 500) -> list[dict]:
    """Dispatch: ready / parse / diff slug'a gore otomatik."""
    if slug in SCREENS_READY_8:
        return screen_get_results(slug, limit)
    if slug in SCREENS_PARSE_7:
        return screen_parse_get_results(slug, limit)
    if slug in SCREENS_DIFF_6:
        return screen_diff_get_results(slug, limit)
    raise ValueError(
        f"Bilinmeyen screen slug: '{slug}'. "
        f"Gecerli ready: {list(SCREENS_READY_8.keys())}; "
        f"parse: {list(SCREENS_PARSE_7.keys())}; "
        f"diff: {list(SCREENS_DIFF_6.keys())}"
    )


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
