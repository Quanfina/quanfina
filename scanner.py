import os
import sys
import time
import json
import requests
import pandas as pd
import yfinance as yf
from io import StringIO
from dotenv import load_dotenv
from datetime import date, datetime, timedelta

# Windows cp1254 console "→" gibi non-ASCII karakteri encode edemez (UnicodeEncodeError).
# Cloud Run UTF-8 default ama lokal calistirmada lazim. 22 May 2026 manuel scan trigger
# sirasinda yakalandi (Sn. Ferit Tarama 14 gun eski veri sorgu -> manuel scan baslat).
if sys.stdout and hasattr(sys.stdout, "reconfigure"):
    try:
        sys.stdout.reconfigure(encoding="utf-8")
    except Exception:
        pass
if sys.stderr and hasattr(sys.stderr, "reconfigure"):
    try:
        sys.stderr.reconfigure(encoding="utf-8")
    except Exception:
        pass


from db_connection import get_connection
# Sprint 4-bis.4 KARAR #461 — Pre-Compute tek motor felsefesi (Kural #22 lokal birikim kullan)
# Sprint 4-bis.5 KARAR #466 — VCP Kalite Skoru (3 kanal sentezi)
# Sprint 4-bis.5 KARAR #465 — Inside Day + Outside Day Negative Reversal + Ready Score
# Sprint 4-bis.5 KARAR #467 — Power Play (HTF) Mark canon
from quanfina_math import (
    compute_vcp_pass, compute_vcp_quality, compute_vcp_ready_score,
    compute_power_play_pass,
    compute_cup_with_handle, compute_flat_base, compute_double_bottom,
    # Sprint 4-bis.7 Faz 2 (Vizyon v22.03 + Migration 007)
    detect_tennis_ball, find_recent_breakout_idx, compute_volume_asymmetry,
    # KARAR #733 (Paket 272 — 28 May 2026): Carr Stage 4-Stage detector
    compute_carr_stage,
)
# Paket 373: SAF yardimci fonksiyonlar scanner_helpers'a tasindi (test-first
# guvenli refactor, tests/test_scanner_pure.py 24 test net). Cagrilar degismez.
from scanner_helpers import (
    parse_earnings_date, _parse_pct, _compute_grade,
    detect_signals, calculate_rs_ratings,
)

load_dotenv()
FINVIZ_KEY = os.getenv("FINVIZ_API_KEY")


# ─── HTTP Session with Retry (Sprint 4-bis.7, 22 May 2026) ───────────────────
# Finviz Elite SSL kopmalarına karşı otomatik retry. 22 May 2026'da scanner manuel
# 3 kez fail oldu (SSLError: UNEXPECTED_EOF_WHILE_READING) — Sn. Ferit talimat:
# "veri çekme sistemini geliştirelim". Çözüm: Session + Retry adapter, exponential
# backoff (1s, 2s, 4s, 8s, 16s) max 5 deneme. SSL + connection error + 429/503'a uygulanır.
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry


def _make_finviz_session() -> requests.Session:
    """Finviz çağrıları için retry'lı session — SSL/connection/rate-limit dayanıklı."""
    session = requests.Session()
    retry_strategy = Retry(
        total=5,
        backoff_factor=1.0,  # 1s, 2s, 4s, 8s, 16s exponential
        status_forcelist=[429, 500, 502, 503, 504],  # rate-limit + server hata
        allowed_methods=["GET", "HEAD"],
        raise_on_status=False,
    )
    adapter = HTTPAdapter(max_retries=retry_strategy, pool_connections=10, pool_maxsize=20)
    session.mount("https://", adapter)
    session.mount("http://", adapter)
    return session


# Modül-seviyesi session — tüm Finviz çağrılarında reuse (connection pool + retry)
FINVIZ_SESSION = _make_finviz_session()


class ScannerHealthError(Exception):
    """Scanner çalışma öncesi/sırasında veri sağlığı kontrolü başarısız oldu."""
    pass


# parse_earnings_date -> scanner_helpers.py (Paket 373 extraction, import yukarida)


# --- VERİTABANI KURULUM ---
def init_db():
    conn = get_connection()
    c = conn.cursor()
    c.execute("""
        CREATE TABLE IF NOT EXISTS minervini_scans (
            id            SERIAL PRIMARY KEY,
            scan_date     TEXT NOT NULL,
            ticker        TEXT NOT NULL,
            company       TEXT,
            sector        TEXT,
            industry      TEXT,
            price         DOUBLE PRECISION,
            change_pct    TEXT,
            volume        INTEGER,
            market_cap    DOUBLE PRECISION,
            pe            DOUBLE PRECISION,
            eps_qoq       TEXT,
            sales_qoq     TEXT,
            ma200_slope   DOUBLE PRECISION,
            passed        INTEGER DEFAULT 1,
            grade         TEXT,
            UNIQUE(scan_date, ticker)
        )
    """)
    for col_sql in [
        "ALTER TABLE minervini_scans ADD COLUMN IF NOT EXISTS earnings_date TEXT",
        "ALTER TABLE minervini_scans ADD COLUMN IF NOT EXISTS eps_last_updated TEXT",
        "ALTER TABLE minervini_scans ADD COLUMN IF NOT EXISTS sales_last_updated TEXT",
        "ALTER TABLE minervini_scans ADD COLUMN IF NOT EXISTS high52 DOUBLE PRECISION",
        "ALTER TABLE minervini_52w_high ADD COLUMN IF NOT EXISTS high52 DOUBLE PRECISION",
        "ALTER TABLE minervini_fundamental_only ADD COLUMN IF NOT EXISTS high52 DOUBLE PRECISION",
        "ALTER TABLE minervini_fundamental_scans ADD COLUMN IF NOT EXISTS high52 DOUBLE PRECISION",
        "ALTER TABLE minervini_fundamental_only ADD COLUMN IF NOT EXISTS ma200_slope DOUBLE PRECISION",
        "ALTER TABLE minervini_fundamental_only ADD COLUMN IF NOT EXISTS eps_qoq TEXT",
        "ALTER TABLE minervini_fundamental_only ADD COLUMN IF NOT EXISTS sales_qoq TEXT",
        "ALTER TABLE minervini_fundamental_only ADD COLUMN IF NOT EXISTS grade TEXT",
        # Sprint 4.7e.1 — sma50 + atr14 teknik göstergeler; perf_year/roe şema tutarsızlık düzeltmesi
        "ALTER TABLE minervini_scans ADD COLUMN IF NOT EXISTS sma50 DOUBLE PRECISION",
        "ALTER TABLE minervini_scans ADD COLUMN IF NOT EXISTS atr14 DOUBLE PRECISION",
        "ALTER TABLE minervini_scans ADD COLUMN IF NOT EXISTS perf_year DOUBLE PRECISION",
        "ALTER TABLE minervini_scans ADD COLUMN IF NOT EXISTS roe DOUBLE PRECISION",
        # Sprint 4.7e.3 — son 25 günlük fiyat/hacim geçmişi (count_distribution_days için)
        "ALTER TABLE minervini_scans ADD COLUMN IF NOT EXISTS price_volume_history JSONB",
        # Sprint 4-bis.4 KARAR #461 — VCP pre-compute (scanner.py hesaplar, SQL sade WHERE okur)
        "ALTER TABLE minervini_scans ADD COLUMN IF NOT EXISTS tight_low_vol_pass BOOLEAN DEFAULT FALSE",
        # Sprint 4-bis.5 KARAR #466 — VCP Kalite Skoru (EXCELLENT/PASS/NULL, 3 kanal sentezi)
        "ALTER TABLE minervini_scans ADD COLUMN IF NOT EXISTS vcp_quality_score TEXT DEFAULT NULL",
        # Sprint 4-bis.5 KARAR #465 — VCP Ready Score 0-100 (Inside Day + V-Dry + Tight)
        "ALTER TABLE minervini_scans ADD COLUMN IF NOT EXISTS vcp_ready_score INTEGER DEFAULT NULL",
        # Sprint 4-bis.5 KARAR #467 — Power Play (HTF) Mark canon: POLE %100+ FLAG %10-25
        "ALTER TABLE minervini_scans ADD COLUMN IF NOT EXISTS power_play_pass BOOLEAN DEFAULT FALSE",
        # Sprint 4-bis.7 KARAR ADAY #893 — Tennis Ball Detector (TLSMW s.253)
        "ALTER TABLE minervini_scans ADD COLUMN IF NOT EXISTS tennis_ball_pattern TEXT DEFAULT NULL",
        # Sprint 4-bis.7 KARAR ADAY #882 — Volume Asymmetry Tracker (TLSMW s.234)
        "ALTER TABLE minervini_scans ADD COLUMN IF NOT EXISTS volume_asymmetry_ratio NUMERIC(8,3) DEFAULT NULL",
        "ALTER TABLE minervini_scans ADD COLUMN IF NOT EXISTS volume_asymmetry_tier TEXT DEFAULT NULL",
        # KARAR #733 (Paket 272 — 28 May 2026): Carr Stage 4-Stage detector (Migration 009)
        "ALTER TABLE minervini_scans ADD COLUMN IF NOT EXISTS carr_stage SMALLINT",
        "ALTER TABLE minervini_scans ADD COLUMN IF NOT EXISTS carr_stage_label TEXT",
        "ALTER TABLE minervini_scans ADD COLUMN IF NOT EXISTS carr_slope_pct_per_year NUMERIC(8,2)",
        "ALTER TABLE minervini_scans ADD COLUMN IF NOT EXISTS carr_ma_value NUMERIC(12,4)",
        "ALTER TABLE minervini_scans ADD COLUMN IF NOT EXISTS carr_price_vs_ma_pct NUMERIC(8,2)",
        # Paket 465 (12 Haz 2026): O'Neil base detector ailesi (cup/flat/double quality)
        "ALTER TABLE minervini_scans ADD COLUMN IF NOT EXISTS cup_handle_quality TEXT DEFAULT NULL",
        "ALTER TABLE minervini_scans ADD COLUMN IF NOT EXISTS flat_base_quality TEXT DEFAULT NULL",
        "ALTER TABLE minervini_scans ADD COLUMN IF NOT EXISTS double_bottom_quality TEXT DEFAULT NULL",
    ]:
        c.execute(col_sql)
    c.execute("""
        CREATE TABLE IF NOT EXISTS minervini_52w_high (
            id          SERIAL PRIMARY KEY,
            scan_date   TEXT NOT NULL,
            ticker      TEXT NOT NULL,
            company     TEXT,
            sector      TEXT,
            industry    TEXT,
            price       DOUBLE PRECISION,
            change_pct  TEXT,
            volume      INTEGER,
            market_cap  DOUBLE PRECISION,
            ma200_slope DOUBLE PRECISION,
            eps_qoq     DOUBLE PRECISION,
            sales_qoq   DOUBLE PRECISION,
            grade       TEXT,
            UNIQUE(scan_date, ticker)
        )
    """)
    c.execute("""
        CREATE TABLE IF NOT EXISTS minervini_watchlist (
            id         SERIAL PRIMARY KEY,
            ticker     TEXT NOT NULL UNIQUE,
            added_date TEXT NOT NULL
        )
    """)
    c.execute("""
        CREATE TABLE IF NOT EXISTS sector_rotation (
            id          SERIAL PRIMARY KEY,
            scan_date   DATE NOT NULL,
            ticker      VARCHAR(10) NOT NULL,
            sector_name VARCHAR(50) NOT NULL,
            perf_1w     DOUBLE PRECISION,
            perf_1m     DOUBLE PRECISION,
            perf_3m     DOUBLE PRECISION,
            perf_6m     DOUBLE PRECISION,
            perf_1y     DOUBLE PRECISION,
            rs_score    DOUBLE PRECISION,
            rs_rank     INTEGER,
            UNIQUE(scan_date, ticker)
        )
    """)
    c.execute("ALTER TABLE sector_rotation ADD COLUMN IF NOT EXISTS perf_1w DOUBLE PRECISION")
    c.execute("""
        CREATE TABLE IF NOT EXISTS trades (
            id                SERIAL PRIMARY KEY,
            symbol            VARCHAR(20) NOT NULL,
            trade_type        VARCHAR(10) NOT NULL DEFAULT 'Long',
            strategy          VARCHAR(50),
            entry_date        TIMESTAMP NOT NULL,
            entry_price       NUMERIC(12,4) NOT NULL,
            stop_loss         NUMERIC(12,4) NOT NULL,
            quantity          INTEGER NOT NULL,
            risk_amount       NUMERIC(12,2),
            risk_pct          NUMERIC(8,4),
            risk_equity_pct   NUMERIC(8,4),
            position_size_pct NUMERIC(8,4),
            breakeven         NUMERIC(12,4),
            sbe_pct           NUMERIC(8,4),
            sbe_shares        INTEGER,
            r_multiple        NUMERIC(8,2),
            status            VARCHAR(20) DEFAULT 'Open',
            exit_date         TIMESTAMP,
            exit_price        NUMERIC(12,4),
            profit_loss       NUMERIC(12,2),
            pnl_pct           NUMERIC(8,4),
            commission            NUMERIC(8,2) DEFAULT 0,
            portfolio_id          INTEGER DEFAULT 1,
            position_size_dollars NUMERIC(14,2),
            notes                 TEXT,
            created_at        TIMESTAMP DEFAULT NOW(),
            updated_at        TIMESTAMP DEFAULT NOW()
        )
    """)
    c.execute("ALTER TABLE trades ADD COLUMN IF NOT EXISTS position_size_dollars NUMERIC(14,2)")
    try:
        c.execute("""
            ALTER TABLE trades
            ALTER COLUMN entry_date TYPE TIMESTAMP USING entry_date::timestamp,
            ALTER COLUMN exit_date  TYPE TIMESTAMP USING exit_date::timestamp
        """)
        conn.commit()
    except Exception as _e:
        conn.rollback()
        print(f"entry/exit_date already TIMESTAMP or alter skipped: {_e}", flush=True)
    c.execute("CREATE INDEX IF NOT EXISTS idx_trades_status     ON trades(status)")
    c.execute("CREATE INDEX IF NOT EXISTS idx_trades_symbol     ON trades(symbol)")
    c.execute("CREATE INDEX IF NOT EXISTS idx_trades_entry_date ON trades(entry_date)")
    c.execute("CREATE INDEX IF NOT EXISTS idx_trades_portfolio  ON trades(portfolio_id)")
    c.execute("""
        CREATE TABLE IF NOT EXISTS portfolios (
            id              SERIAL PRIMARY KEY,
            name            VARCHAR(50) DEFAULT 'Main',
            starting_value  NUMERIC(14,2) NOT NULL,
            current_value   NUMERIC(14,2) NOT NULL,
            created_at      TIMESTAMP DEFAULT NOW(),
            updated_at      TIMESTAMP DEFAULT NOW()
        )
    """)
    c.execute("""
        INSERT INTO portfolios (id, name, starting_value, current_value)
        VALUES (1, 'Main', 10000, 10000)
        ON CONFLICT (id) DO NOTHING
    """)
    c.execute("""
        CREATE TABLE IF NOT EXISTS journal_entries (
            id               SERIAL PRIMARY KEY,
            date             DATE NOT NULL,
            category         TEXT,
            content          TEXT,
            linked_trade_id  INTEGER REFERENCES trades(id)
        )
    """)
    resync_serial_sequences(c)
    conn.commit()
    conn.close()


# Scanner SERIAL id tablolari — sequence resync hedefi (asagidaki helper kullanir)
_SERIAL_ID_TABLES = [
    "minervini_scans",
    "minervini_fundamental_scans",
    "minervini_fundamental_only",
    "minervini_52w_high",
    "sector_rotation",
]


def resync_serial_sequences(c, tables=None):
    """SERIAL `id` sequence'lerini tablonun max(id)'sine senkronla.

    Neden: DB'ye explicit-id ile satir eklenince (pg_dump/restore, kopya, manuel
    import) sequence geride kalir; sonraki auto-increment INSERT'in urettigi id
    zaten mevcut -> "duplicate key value violates unique constraint *_pkey". run_scan
    ilk hatada `break` ettigi icin tum tarama sessizce 0 kayit yazar (17 Haz 2026'da
    06-11..06-17 prod hisse taramasi 6 gun bu yuzden olu kaldi). Bu defansif resync
    her init_db + her run_scan yazma oncesi calisir; idempotent + non-destructive
    (satir silmez/degistirmez, yalniz sayaci ilerletir).
    """
    if tables is None:
        tables = _SERIAL_ID_TABLES
    for t in tables:
        try:
            c.execute("SELECT pg_get_serial_sequence(%s, 'id')", (t,))
            row = c.fetchone()
            seq = row[0] if row else None
            if not seq:
                continue
            # is_called=false (empty tablo) -> next nextval = 1; aksi next = max(id)+1
            c.execute(
                "SELECT setval(%s, COALESCE((SELECT MAX(id) FROM {t}), 1), "
                "(SELECT MAX(id) FROM {t}) IS NOT NULL)".format(t=t),
                (seq,),
            )
        except Exception as e:
            print(f"  [seq-resync] {t} atlandi: {e}")


# --- FİNVİZ API SAĞLIK KONTROLLERI ---

_SCREENER_REQUIRED_COLS = [
    'Ticker', 'Company', 'Sector', 'Industry',
    'Market Cap', 'P/E', 'Price', 'Change', 'Volume',
]

# AÇIK KONU #71 (20 May 2026) — Finviz Column Drift Guard
# Finviz yeni kolon eklerse `c=...` ID'leri kayar -> yanlis veriler DB'ye yazilir.
# Bu sabit listeler "beklenen header isimleri"dir. Finviz CSV export'unda
# header adlari gercek kolon adlaridir; ID kaymasi olursa header eslesmesi
# fail eder ve validate_finviz_response ScannerHealthError firlatir.
#
# Kayit: notebook/Notebook_C2_EK1-8.md EK 7 (151 ID Tam Haritasi) +
#         notebook/Notebook_A_Vizyon.md AÇIK KONU #71 (drift kontrol)

# get_finviz_extras: c=1,22,23,46,33,68
# Finviz Elite CSV header'lari TAM uzun adlar (rename'den onceki orijinal)
_EXTRAS_REQUIRED_COLS = [
    'Ticker',
    'EPS Growth Quarter Over Quarter',  # c=22
    'Sales Growth Quarter Over Quarter',  # c=23
    'Performance (Year)',  # c=46
    'Return on Equity',  # c=33
    'Earnings Date',  # c=68
]

# scan_sectors: c=1,42,43,44,45,46 (Performance haftalik/aylik/quarter/half/yearly)
# Finviz orijinal header adlari (scan_sectors rename satirini referans alindi)
_SECTOR_REQUIRED_COLS = [
    'Ticker',
    'Performance (Week)',    # c=42
    'Performance (Month)',   # c=43
    'Performance (Quarter)', # c=44
    'Performance (Half Year)',  # c=45
    'Performance (Year)',    # c=46
]


def health_check_finviz() -> None:
    """Scanner çalışmadan önce Finviz API'sının beklenen kolonları döndürdüğünü
    doğrula. Tek ticker (AAPL) ile minimum maliyetli test çağrısı yapar.

    Raises:
        ScannerHealthError: API formatı değişmiş veya auth sorunu var.
    """
    print("Finviz health check basliyor...")

    test_url = (
        f"https://elite.finviz.com/export.ashx?"
        f"v=152&t=AAPL&c=1,2,3,4,6,7,65,66,67"
        f"&auth={FINVIZ_KEY}&ft=4"
    )

    try:
        r = requests.get(test_url, headers={"User-Agent": "Mozilla/5.0"}, timeout=15)
        r.raise_for_status()
    except requests.RequestException as e:
        raise ScannerHealthError(
            f"Finviz health check HTTP hatasi: {e}\n"
            f"  Olasi sebep: Network sorunu, API key bozuk, veya endpoint kapali."
        )

    try:
        df = pd.read_csv(StringIO(r.text))
    except Exception as e:
        raise ScannerHealthError(
            f"Finviz cevabi CSV olarak parse edilemedi: {e}\n"
            f"  Ilk 200 char: {r.text[:200]}"
        )

    missing = [c for c in _SCREENER_REQUIRED_COLS if c not in df.columns]
    if missing:
        raise ScannerHealthError(
            f"Finviz API formati degismis!\n"
            f"  Eksik kolonlar: {missing}\n"
            f"  Gelen kolonlar: {list(df.columns)}\n"
            f"  Cozum: scanner.py URL'lerinde c= parametresini guncelle."
        )

    if df.empty:
        raise ScannerHealthError("Finviz health check: AAPL test bos dondü.")

    aapl_price = df.iloc[0].get('Price', 0)
    aapl_company = str(df.iloc[0].get('Company', '')).strip()

    if aapl_price <= 0:
        raise ScannerHealthError(
            f"Finviz health check: AAPL price={aapl_price} (sifir veya negatif).\n"
            f"  API key gecersiz olabilir, abonelik kontrol et."
        )

    if not aapl_company:
        raise ScannerHealthError(
            f"Finviz health check: AAPL Company bos.\n"
            f"  v=152 view formati bozuk olabilir."
        )

    print(f"[OK] Finviz health check (screener) OK")
    print(f"   AAPL: price=${aapl_price}, company={aapl_company!r}")
    print(f"   Kolonlar tam ({len(df.columns)} kolon)")

    # AÇIK KONU #71 — Drift Guard: 3 endpoint preflight probe
    # Sn. Ferit dikkat: Finviz yeni kolon eklerse `c=...` ID'leri kayar.
    # Her scan oncesi 3 endpoint icin tek-ticker probe + header eslesmesi.

    # Probe 2 — extras endpoint (c=1,22,23,46,33,68)
    print("Finviz health check — extras endpoint probe...")
    try:
        probe2_url = (
            f"https://elite.finviz.com/export.ashx?"
            f"v=152&t=AAPL&c=1,22,23,46,33,68&auth={FINVIZ_KEY}&ft=4"
        )
        r2 = requests.get(probe2_url, headers={"User-Agent": "Mozilla/5.0"}, timeout=15)
        r2.raise_for_status()
        df2 = pd.read_csv(StringIO(r2.text))
        missing2 = [c for c in _EXTRAS_REQUIRED_COLS if c not in df2.columns]
        if missing2:
            unexpected2 = [c for c in df2.columns if c not in _EXTRAS_REQUIRED_COLS]
            raise ScannerHealthError(
                f"Finviz EXTRAS endpoint drift! Eksik: {missing2}\n"
                f"  Beklenmeyen: {unexpected2}\n"
                f"  Notebook_C2_EK1-8.md EK 7 ID haritası güncel mi kontrol et.\n"
                f"  scanner.py `get_finviz_extras` URL'ini güncelle (c=1,22,23,46,33,68)."
            )
        print(f"[OK] Finviz health check (extras) OK — {len(df2.columns)} kolon")
    except ScannerHealthError:
        raise
    except Exception as e:
        raise ScannerHealthError(f"Finviz extras probe hatası: {e}")

    # Probe 3 — sector endpoint (c=1,42,43,44,45,46)
    print("Finviz health check — sector endpoint probe...")
    try:
        probe3_url = (
            f"https://elite.finviz.com/export.ashx?"
            f"v=152&t=XLK&c=1,42,43,44,45,46&auth={FINVIZ_KEY}&ft=4"
        )
        r3 = requests.get(probe3_url, headers={"User-Agent": "Mozilla/5.0"}, timeout=15)
        r3.raise_for_status()
        df3 = pd.read_csv(StringIO(r3.text))
        missing3 = [c for c in _SECTOR_REQUIRED_COLS if c not in df3.columns]
        if missing3:
            unexpected3 = [c for c in df3.columns if c not in _SECTOR_REQUIRED_COLS]
            raise ScannerHealthError(
                f"Finviz SECTOR endpoint drift! Eksik: {missing3}\n"
                f"  Beklenmeyen: {unexpected3}\n"
                f"  Notebook_C2_EK1-8.md EK 7 ID haritası güncel mi kontrol et.\n"
                f"  scanner.py `scan_sectors` URL'ini güncelle (c=1,42,43,44,45,46)."
            )
        print(f"[OK] Finviz health check (sector) OK — {len(df3.columns)} kolon")
    except ScannerHealthError:
        raise
    except Exception as e:
        raise ScannerHealthError(f"Finviz sector probe hatası: {e}")

    print("[OK] Finviz Drift Guard: 3 endpoint preflight tamam.")


def validate_finviz_response(df: pd.DataFrame, source: str,
                              required_cols: list = None,
                              min_filled_ratio: float = 0.50) -> None:
    """Her screener API cevabini runtime'da dogrular.

    Args:
        df: Finviz CSV parse sonucu DataFrame
        source: Hangi fonksiyondan geldigini belirtir (log icin)
        required_cols: Beklenen kolon isimleri. None ise standart 9 kolon.
        min_filled_ratio: Price/Company minimum doluluk orani (varsayilan %50)

    Raises:
        ScannerHealthError: Kolon eksik veya kritik veri yetersiz dolu.
    """
    if required_cols is None:
        required_cols = _SCREENER_REQUIRED_COLS

    actual_cols = list(df.columns)
    missing = [c for c in required_cols if c not in actual_cols]
    # AÇIK KONU #71 (20 May 2026) — Drift Guard:
    # Finviz yeni kolon eklerse `c=...` ID kaymasi olur, header degisir.
    # "Fazla" kolon: c= talebinde olmayan bir kolon geldiyse drift kaniti.
    unexpected = [c for c in actual_cols if c not in required_cols]
    if missing:
        # Drift teshisi — eksik VEYA fazla kolon = ID kaymasi sinyali
        drift_msg = ""
        if unexpected:
            drift_msg = (
                f"\n  DRIFT TESHIS: Beklenmeyen kolonlar geldi: {unexpected}\n"
                f"  Finviz `c=...` ID'leri kaymis olabilir (yeni kolon eklenmis).\n"
                f"  Çözüm: notebook/Notebook_C2_EK1-8.md EK 7 (151 ID Haritasi) güncel mi kontrol et,\n"
                f"  scanner.py URL'lerinde c= parametresini yeni ID'lerle değiştir."
            )
        raise ScannerHealthError(
            f"[{source}] Kolonlar eksik: {missing}\n"
            f"  Gelen: {actual_cols}\n"
            f"  c= parametresi URL'de eksik veya yanlis olabilir.{drift_msg}"
        )

    if df.empty:
        print(f"  [{source}] DataFrame bos dondü (filtreden hic ticker gecmedi).")
        return

    if 'Price' in df.columns:
        try:
            price_filled = pd.to_numeric(df['Price'], errors='coerce').gt(0).sum()
            ratio = price_filled / len(df)
            if ratio < min_filled_ratio:
                raise ScannerHealthError(
                    f"[{source}] Price kolonu yetersiz dolu: "
                    f"{price_filled}/{len(df)} ({ratio*100:.0f}%) > 0.\n"
                    f"  Beklenen: en az %{min_filled_ratio*100:.0f}. Scan abort."
                )
        except ScannerHealthError:
            raise
        except Exception as e:
            print(f"  [{source}] Price doluluk kontrolü hata: {e}")

    if 'Company' in df.columns:
        try:
            company_filled = (df['Company'].astype(str).str.strip() != '').sum()
            ratio = company_filled / len(df)
            if ratio < min_filled_ratio:
                raise ScannerHealthError(
                    f"[{source}] Company kolonu yetersiz dolu: "
                    f"{company_filled}/{len(df)} ({ratio*100:.0f}%). Scan abort."
                )
        except ScannerHealthError:
            raise
        except Exception as e:
            print(f"  [{source}] Company doluluk kontrolü hata: {e}")


# --- FİNVİZ TARAMASI (8 KURAL FİLTRELİ) ---
def get_finviz_screener():
    """
    Finviz Elite filtreler (8 kuralın 7'si burada):
    - sh_price_o10     : Fiyat > $10
    - sh_avgvol_o500   : Ort. Hacim > 500K
    - ta_sma50_pa      : Fiyat > MA50
    - ta_sma200_pa     : Fiyat > MA200
    - ta_sma50_sa150   : MA50 > MA150
    - ta_sma50_sa200   : MA50 > MA200
    - ta_sma150_sa200  : MA150 > MA200
    - ta_highlow52w_a25h : 52W High'tan en fazla %25 altta (Mark canon koşul 8)
    - ta_highlow52w_b75l : 52W Low'dan en az %75 yukarıda (KARAR ADAY #485, 22 May 2026 —
                           Mark canon ≥%25 ama Finviz parameter ismi %75. Quanfina Notebook +
                           Quanfina Minervini onayı: pozitif yön tolerans, gerçek hisseler %1000+
                           dipten, pratik etki yok. Risk düşük. _HATALAR.md H#13)
    - ta_rsi_o70       : RS > 70 (yaklaşık)
    """
    filters = ",".join([
        "sh_price_o10",
        "sh_avgvol_o500",
        "ta_sma50_pa",
        "ta_sma200_pa",
        "ta_sma50_sa150",
        "ta_sma50_sa200",
        "ta_sma150_sa200",
        "ta_highlow52w_a25h",
        "ta_highlow52w_b75l",
        "sec_etf_false",
        "geo_usa",
        "ind_stocksonly",
    ])
    
    url = (
        f"https://elite.finviz.com/export.ashx?"
        f"v=152&f={filters}&c=1,2,3,4,6,7,65,66,67&auth={FINVIZ_KEY}&ft=4"
    )
    
    r = FINVIZ_SESSION.get(url, headers={"User-Agent": "Mozilla/5.0"}, timeout=15)
    df = pd.read_csv(StringIO(r.text))
    validate_finviz_response(df, source="get_finviz_screener")
    print(f"Finviz filtresi geçti: {len(df)} hisse")
    return df

def get_finviz_fundamental():
    """
    Teknik + Temel filtreler:
    - Trend Template (7 teknik kural)
    - EPS Q/Q > %25
    - Sales Q/Q > %25
    - Fiyat > $10, Hacim > 500K
    """
    filters = ",".join([
        "sh_price_o10",
        "sh_avgvol_o500",
        "ta_sma50_pa",
        "ta_sma200_pa",
        "ta_sma50_sa150",
        "ta_sma50_sa200",
        "ta_sma150_sa200",
        "ta_highlow52w_a25h",
        "ta_highlow52w_b75l",
        "fa_epsqoq_o25",
        "fa_salesqoq_o25",
        "sec_etf_false",
        "geo_usa",
        "ind_stocksonly",
    ])

    url = (
        f"https://elite.finviz.com/export.ashx?"
        f"v=152&f={filters}&c=1,2,3,4,6,7,65,66,67&auth={FINVIZ_KEY}&ft=4"
    )

    r = FINVIZ_SESSION.get(url, headers={"User-Agent": "Mozilla/5.0"}, timeout=15)
    df = pd.read_csv(StringIO(r.text))
    validate_finviz_response(df, source="get_finviz_fundamental")
    print(f"Fundamental filtresi geçti: {len(df)} hisse")
    return df

def get_finviz_fundamental_only():
    """
    Sadece temel filtreler — teknik kural YOK.

    24 May 2026 revize (Gem + Quanfina Notebook Çift Danışma):
      Mark felsefesi gereği Fundamental kuralları "Hard Filter" DEĞİL,
      "Soft Score / Relative Prioritizing" katmanına aittir (Mark Recipe).
      Yeni IPO + biotech + Story Stocks "Açıklanamayan Güç" hisseleri
      Hard Filter'a takılırsa Quanfina patlayıcı fırsatları eler.

    ROE filtresi (fa_roe_pos15) 24 May 2026'da KALDIRILDI — Hard Filter
    değil. ROE Finviz extras (c=33) ile DB'ye kolon olarak çekilir,
    UI tarafında Soft Score puanlama olarak uygulanır.

    4 koşul:
      - Quanfina Ek (evren daraltma):
        - sh_price_o10   : Fiyat > $10
        - sh_avgvol_o500 : Ortalama hacim > 500K
      - Mark Soft Score (UI turuncu Mark Ekstra):
        - fa_epsqoq_o25  : EPS Q/Q > %25 (TLSMW s.127, Quanfina pratik Hard Cut)
        - fa_salesqoq_o25: Sales Q/Q > %25 (TLSMW s.132, Quanfina pratik Hard Cut)

    KALICI İLKE #4: Eşikler kitap birebir kaynak gösterir.
    """
    filters = ",".join([
        "sh_price_o10",
        "sh_avgvol_o500",
        "fa_epsqoq_o25",
        "fa_salesqoq_o25",
        # fa_roe_pos15 KALDIRILDI 24 May 2026 — Mark felsefesi gereği Soft Score
        # (Gem + Quanfina Notebook Çift Danışma onayı, ROE Hard Filter değil)
        "sec_etf_false",
        "geo_usa",
        "ind_stocksonly",
    ])

    url = (
        f"https://elite.finviz.com/export.ashx?"
        f"v=152&f={filters}&c=1,2,3,4,6,7,65,66,67&auth={FINVIZ_KEY}&ft=4"
    )

    r = FINVIZ_SESSION.get(url, headers={"User-Agent": "Mozilla/5.0"}, timeout=15)
    df = pd.read_csv(StringIO(r.text))
    validate_finviz_response(df, source="get_finviz_fundamental_only")
    print(f"Temel filtresi geçti: {len(df)} hisse")
    return df

def get_finviz_52w_high():
    """
    52 Hafta Yüksek filtresi — sadece fiyat/hacim + 52W yeni yüksek:
    - Fiyat > $10
    - Hacim > 500K
    - 52W yeni yüksek yapıyor (ta_highlow52w_nh)
    """
    filters = ",".join([
        "sh_price_o10",
        "sh_avgvol_o500",
        "geo_usa",
        "ind_stocksonly",
        "ta_highlow52w_nh",
    ])
    url = (
        f"https://elite.finviz.com/export.ashx?"
        f"v=152&f={filters}&c=1,2,3,4,6,7,65,66,67&auth={FINVIZ_KEY}&ft=4"
    )
    r = FINVIZ_SESSION.get(url, headers={"User-Agent": "Mozilla/5.0"}, timeout=15)
    df = pd.read_csv(StringIO(r.text))
    validate_finviz_response(df, source="get_finviz_52w_high")
    print(f"52W Yüksek filtresi geçti: {len(df)} hisse")
    return df


def get_finviz_extras(tickers: list) -> "pd.DataFrame":
    """
    Ticker listesi için EPS Q/Q, Sales Q/Q, Perf Year, ROE, Earnings Date toplu çeker.
    scan_sectors() ile aynı batch pattern — tek API çağrısı, sleep yok.
    Returns: ticker-indexed DataFrame (eps_qoq, sales_qoq, perf_year, roe, earnings_date)
    """
    url = (
        f"https://elite.finviz.com/export.ashx?"
        f"v=152&t={','.join(tickers)}&c=1,22,23,46,33,68&auth={FINVIZ_KEY}&ft=4"
    )
    r = FINVIZ_SESSION.get(url, headers={"User-Agent": "Mozilla/5.0"}, timeout=30)
    r.raise_for_status()
    df = pd.read_csv(StringIO(r.text))
    # AÇIK KONU #71 — Drift Guard: extras endpoint icin validation
    # (orijinal header'lar — rename'den ONCE)
    validate_finviz_response(df, source="get_finviz_extras",
                              required_cols=_EXTRAS_REQUIRED_COLS)
    df = df.rename(columns={
        "Ticker": "ticker",
        "EPS Growth Quarter Over Quarter": "eps_qoq",
        "Sales Growth Quarter Over Quarter": "sales_qoq",
        "Performance (Year)": "perf_year",
        "Return on Equity": "roe",
        "Earnings Date": "earnings_date",
    })
    return df.set_index("ticker")


# _parse_pct -> scanner_helpers.py (Paket 373 extraction, import yukarida)


# Sprint 4-bis.4 KARAR #461 — Yerel VCP fonksiyonu quanfina_math'e tasindi.
# Brandon "Begrudgingly Pull Back" formulu (Minervini_Video.md sat. 2823-2867).
# Tek motor felsefesi: scanner.py import quanfina_math.compute_vcp_pass


# _compute_grade -> scanner_helpers.py (Paket 373 extraction, import yukarida)


SECTOR_ETFS = {
    "XLK":  "Technology",
    "XLF":  "Financials",
    "XLE":  "Energy",
    "XLV":  "Health Care",
    "XLI":  "Industrials",
    "XLY":  "Consumer Discretionary",
    "XLP":  "Consumer Staples",
    "XLU":  "Utilities",
    "XLB":  "Materials",
    "XLRE": "Real Estate",
    "XLC":  "Communication Services",
}


def scan_sectors(scan_date):
    """
    11 SPDR sektör ETF'i için Finviz'den performance verilerini çeker,
    çoklu periyot ağırlıklı RS Score hesaplar ve sector_rotation tablosuna yazar.

    RS Score = (perf_1m * 0.4) + (perf_3m * 0.2) + (perf_6m * 0.2) + (perf_1y * 0.2)
    """
    conn = get_connection()
    try:
        cursor = conn.cursor()
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS sector_rotation (
                id          SERIAL PRIMARY KEY,
                scan_date   DATE NOT NULL,
                ticker      VARCHAR(10) NOT NULL,
                sector_name VARCHAR(50) NOT NULL,
                perf_1w     DOUBLE PRECISION,
                perf_1m     DOUBLE PRECISION,
                perf_3m     DOUBLE PRECISION,
                perf_6m     DOUBLE PRECISION,
                perf_1y     DOUBLE PRECISION,
                rs_score    DOUBLE PRECISION,
                rs_rank     INTEGER,
                UNIQUE(scan_date, ticker)
            )
        """)
        cursor.execute("ALTER TABLE sector_rotation ADD COLUMN IF NOT EXISTS perf_1w DOUBLE PRECISION")
        conn.commit()
    finally:
        conn.close()

    tickers = ",".join(SECTOR_ETFS.keys())
    url = (
        f"https://elite.finviz.com/export.ashx?"
        f"v=152&t={tickers}&c=1,42,43,44,45,46&auth={FINVIZ_KEY}&ft=4"
    )

    try:
        r = FINVIZ_SESSION.get(url, headers={"User-Agent": "Mozilla/5.0"}, timeout=30)
        r.raise_for_status()
    except Exception as e:
        print(f"Finviz API hatası (sektör): {e}")
        return None

    df = pd.read_csv(StringIO(r.text))

    # AÇIK KONU #71 — Drift Guard: scan_sectors icin de validation
    validate_finviz_response(df, source="scan_sectors",
                              required_cols=_SECTOR_REQUIRED_COLS)

    if df.empty:
        print("Sektör API'den boş cevap")
        return None

    def parse_pct(val):
        """'12.34%' -> 12.34, '-' veya boş -> None"""
        if val is None or str(val).strip() in ("-", "", "nan"):
            return None
        try:
            return float(str(val).replace("%", "").strip())
        except (ValueError, AttributeError):
            return None

    sectors_data = []
    for _, row in df.iterrows():
        ticker = str(row.get("Ticker", "")).strip()
        if ticker not in SECTOR_ETFS:
            continue

        perf_1w = parse_pct(row.get("Performance (Week)"))
        perf_1m = parse_pct(row.get("Performance (Month)"))
        perf_3m = parse_pct(row.get("Performance (Quarter)"))
        perf_6m = parse_pct(row.get("Performance (Half Year)"))
        perf_1y = parse_pct(row.get("Performance (Year)"))

        rs_score = (
            (perf_1m or 0) * 0.4 +
            (perf_3m or 0) * 0.2 +
            (perf_6m or 0) * 0.2 +
            (perf_1y or 0) * 0.2
        )

        sectors_data.append({
            "ticker":      ticker,
            "sector_name": SECTOR_ETFS[ticker],
            "perf_1w":     perf_1w,
            "perf_1m":     perf_1m,
            "perf_3m":     perf_3m,
            "perf_6m":     perf_6m,
            "perf_1y":     perf_1y,
            "rs_score":    rs_score,
        })

    sectors_data.sort(key=lambda x: x["rs_score"], reverse=True)
    for rank, s in enumerate(sectors_data, start=1):
        s["rs_rank"] = rank

    conn = get_connection()
    try:
        cursor = conn.cursor()
        for s in sectors_data:
            cursor.execute(
                """
                INSERT INTO sector_rotation
                    (scan_date, ticker, sector_name, perf_1w, perf_1m, perf_3m, perf_6m, perf_1y, rs_score, rs_rank)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (scan_date, ticker) DO UPDATE SET
                    sector_name = EXCLUDED.sector_name,
                    perf_1w     = EXCLUDED.perf_1w,
                    perf_1m     = EXCLUDED.perf_1m,
                    perf_3m     = EXCLUDED.perf_3m,
                    perf_6m     = EXCLUDED.perf_6m,
                    perf_1y     = EXCLUDED.perf_1y,
                    rs_score    = EXCLUDED.rs_score,
                    rs_rank     = EXCLUDED.rs_rank
                """,
                (
                    scan_date,
                    s["ticker"],
                    s["sector_name"],
                    s["perf_1w"],
                    s["perf_1m"],
                    s["perf_3m"],
                    s["perf_6m"],
                    s["perf_1y"],
                    s["rs_score"],
                    s["rs_rank"],
                ),
            )
        conn.commit()
        print(f"Sektör rotasyonu kaydedildi: {len(sectors_data)} sektör, scan_date={scan_date}")
        return len(sectors_data)
    except Exception as e:
        conn.rollback()
        print(f"Sektör DB yazma hatası: {e}")
        return None
    finally:
        conn.close()


# --- TEKNİK SİNYAL TESPİTİ ---
# detect_signals + calculate_rs_ratings -> scanner_helpers.py (Paket 374 increment 2,
# import yukarida). Saf fonksiyonlar; cagrilar (check_ma200_slope vb.) degismez.

# --- MA200 SLOPE KONTROLÜ (Kural 3) ---
def check_ma200_slope(tickers):
    """
    MA200 slope, high52, sinyaller ve RS rating hesabı.
    SPY her zaman download'a eklenir (RS için referans).
    """
    results = {}
    closes  = {}
    print(f"MA200 slope kontrolü: {len(tickers)} hisse...")

    _null_rs = {"rs_ibd": None, "rs_12m": None, "rs_20d": None,
                "rs_50d": None, "rs_200d": None, "rs_mansfield": None}

    tickers_dl = list(set(list(tickers) + ["SPY"]))

    try:
        # 420 takvim günü ≈ 300 işlem günü → p12 (252 gün) için yeterli tampon
        start_str = str(date.today() - timedelta(days=420))
        data = yf.download(tickers_dl, start=start_str, progress=False, auto_adjust=True, group_by="ticker")

        for ticker in tickers:
            try:
                close  = data[ticker]["Close"].squeeze()
                high   = data[ticker]["High"].squeeze()
                open_  = data[ticker]["Open"].squeeze()
                low    = data[ticker]["Low"].squeeze()
                volume = data[ticker]["Volume"].squeeze()

                closes[ticker] = close

                ma200_today = float(close.rolling(200).mean().iloc[-1])
                ma200_1m    = float(close.rolling(200).mean().iloc[-21])
                slope       = round(ma200_today - ma200_1m, 4)
                high52      = round(float(high.max()), 4)

                # Sprint 4.7e.1 — sma50 + atr14 (mevcut OHLCV serisinden, ek API yok)
                try:
                    sma50_val = float(close.rolling(50).mean().iloc[-1])
                except Exception:
                    sma50_val = None
                try:
                    prev_close = close.shift(1)
                    tr = pd.concat([
                        (high - low),
                        (high - prev_close).abs(),
                        (low  - prev_close).abs(),
                    ], axis=1).max(axis=1)
                    atr14_val = float(tr.rolling(14).mean().iloc[-1])
                except Exception:
                    atr14_val = None

                # Sprint 4-bis.4 KARAR #464 (19 May 2026) — PVH OHLC genişlemesi
                # ESKI: {date, close, volume}
                # YENI: {date, open, high, low, close, volume}
                # Sebep: Brandon range_pct gun-ici, Mark canon Inside/Outside Day +
                # Pivot intraday high/low gerektirir (3 kanal onay).
                # Sprint 4-bis.5 KARAR #467 — Power Play icin POLE 40 + FLAG 30 = 70
                # gun gerekli, 80 gun marjli (compute_vcp_pass + power_play_pass yeter)
                # P516 (18 Haz 2026): 80 -> 280 bar. Carr bulk /screens icin: Blue Sky 261g
                # (52-hafta), Pullback/Bullish Base/Divergence 200g (SMA200) gerektirir. Kaynak
                # ~300 islem gunu (420 takvim) -> 280 guvenli. MR(21)/Coiled(60)/Power Play(70)
                # tail-slice ile etkilenmez. ~+10MB/scan storage (kabul, tek-kullanici).
                try:
                    tail_o = open_.tail(280)
                    tail_h = high.tail(280)
                    tail_l = low.tail(280)
                    tail_c = close.tail(280)
                    tail_v = volume.tail(280)
                    pvh_val = [
                        {"date": str(d.date()),
                         "open": float(o_), "high": float(h_), "low": float(l_),
                         "close": float(c_), "volume": float(v_)}
                        for d, o_, h_, l_, c_, v_ in zip(
                            tail_c.index, tail_o, tail_h, tail_l, tail_c, tail_v
                        )
                    ]
                except Exception:
                    pvh_val = None

                ohlcv = pd.DataFrame({
                    "Open": open_, "High": high, "Low": low,
                    "Close": close, "Volume": volume,
                }).dropna()
                confs, viols = detect_signals(ohlcv)
                # P469 (15 Haz 2026): tam OHLCV listeleri slope_info'da saklanir -> run_scan
                # canli yolu carr_stage (>=150 bar) + base detector (cup/flat/double, >=60 bar)
                # hesaplayabilsin. pvh tail-80 carr icin yetersiz; eski save_results bunlari
                # scope-disi 'close'/'volume'/'data[ticker]' ile okuyordu (olu kod bug). Artik
                # tek kaynak burasi (dropna ile hizali Series'ten).
                results[ticker] = {
                    "slope":                slope,
                    "high52":               high52,
                    "sma50":                sma50_val,
                    "atr14":                atr14_val,
                    "price_volume_history": pvh_val,
                    "ohlcv_high":           [float(x) for x in ohlcv["High"].tolist()],
                    "ohlcv_low":            [float(x) for x in ohlcv["Low"].tolist()],
                    "ohlcv_close":          [float(x) for x in ohlcv["Close"].tolist()],
                    "ohlcv_volume":         [float(x) for x in ohlcv["Volume"].tolist()],
                    "confirmations":        ",".join(confs),
                    "violations":           ",".join(viols),
                }
            except Exception:  # P481 (#6): ciplak 'except:' -> Exception (KeyboardInterrupt/SystemExit maskelemesin; alttaki try zaten bu konvansiyonu kullaniyor)
                results[ticker] = {"slope": None, "high52": None, "sma50": None, "atr14": None,
                                   "price_volume_history": None,
                                   "confirmations": "", "violations": "", **_null_rs}

        try:
            spy_close = data["SPY"]["Close"].squeeze().dropna()
        except Exception:
            spy_close = None

        spy_actual_date = str(spy_close.index[-1].date()) if spy_close is not None and len(spy_close) > 0 else None

        rs_ratings = calculate_rs_ratings(closes, spy_close)

        for ticker, info in results.items():
            if info.get("slope") is not None:
                info.update(rs_ratings.get(ticker, _null_rs))

    except Exception as e:
        print(f"Toplu indirme hatası: {e}")
        return results, None

    return results, spy_actual_date

# --- VERİTABANINA KAYDET ---
def _pvh_nan_safe(pvh):
    """price_volume_history icindeki NaN/Inf degerleri -> None.

    json.dumps NaN/Inf icin gecersiz JSON token ('NaN'/'Infinity') uretir; PostgreSQL
    JSONB bunu reddeder ('invalid input syntax for type json: Token NaN is invalid').
    GENB gibi eksik-OHLCV tickerda bu INSERT'i patlatip tum ana taramayi dusuruyordu
    (17 Haz 2026 kok neden #2). NaN/Inf -> null cevrilir; yapi (list[dict]) korunur.
    """
    if not pvh:
        return pvh
    safe = []
    for bar in pvh:
        if isinstance(bar, dict):
            safe.append({
                k: (None if isinstance(v, float) and (v != v or v == float("inf") or v == float("-inf")) else v)
                for k, v in bar.items()
            })
        else:
            safe.append(bar)
    return safe


def _upsert_minervini_scan_row(c, scan_date, ticker, row, slope_info):
    """P469 (15 Haz 2026): minervini_scans tek-satir UPSERT — DRY tek yazma yolu.

    save_results (eski) ve run_scan ayni 39-kolon INSERT'i buradan cagirir; boylece
    kolon/placeholder/tuple drift'i + olu-kod bug'i biter. Onceki durum: 39-kolon
    INSERT yalniz save_results'taydi (HIC cagrilmiyordu) + carr/base'i scope-disi
    'close'/'volume'/'data[ticker]' ile okuyordu; canli run_scan inline 21-kolon INSERT
    base/carr/vcp kolonlarini hic yazmiyordu. Artik tum hesaplama row (Finviz) + slope_info
    (check_ma200_slope: pvh + ohlcv_*) uzerinden, tek kaynak.
    """
    slope_info = slope_info or {}
    slope   = slope_info.get("slope")
    high52  = slope_info.get("high52")
    confs   = slope_info.get("confirmations", "")
    viols   = slope_info.get("violations", "")
    rs_ibd  = slope_info.get("rs_ibd")
    rs_12m  = slope_info.get("rs_12m")
    rs_20d  = slope_info.get("rs_20d")
    rs_50d  = slope_info.get("rs_50d")
    rs_200d = slope_info.get("rs_200d")
    rs_mf   = slope_info.get("rs_mansfield")
    sma50   = slope_info.get("sma50")
    atr14   = slope_info.get("atr14")
    pvh     = slope_info.get("price_volume_history")
    pvh_json = json.dumps(_pvh_nan_safe(pvh)) if pvh else None

    # VCP / Power Play (pvh'den) — KARAR #461/#465/#466/#467
    tight_low_vol_pass = compute_vcp_pass(pvh)
    vcp_quality_score = compute_vcp_quality(pvh)
    vcp_ready_score = compute_vcp_ready_score(pvh)
    power_play_pass = compute_power_play_pass(pvh)

    # Tennis Ball (pvh'den) — KARAR ADAY #893
    # P578 (21 Haz 2026): breakout anchor FIX. Eski heuristik (son 10 barin max-close)
    # recovery'yi (close > breakout_high) matematiksel imkansiz kiliyordu -> TENNIS_BALL
    # asla uretilmiyordu, tennis_ball_active kalici bos (P577 bug). find_recent_breakout_idx
    # pullback'i ONCELEYEN ilk pivot kirilimini bulur (Mark X/TLSMW s.253 kanon). Anchor
    # None ise pattern None (recent advance'ta breakout yok).
    tennis_ball_pattern = None
    if pvh:
        try:
            closes_tb = [b.get('close', 0) for b in pvh]
            breakout_idx = find_recent_breakout_idx(closes_tb)
            if breakout_idx is not None:
                tb_result = detect_tennis_ball(breakout_idx, pvh)
                tennis_ball_pattern = tb_result.get('pattern')
        except Exception:
            tennis_ball_pattern = None

    # Volume Asymmetry (pvh'den) — KARAR ADAY #882
    volume_asymmetry_ratio = None
    volume_asymmetry_tier = None
    if pvh and len(pvh) >= 5:
        try:
            va_result = compute_volume_asymmetry(pvh, lookback_days=20)
            volume_asymmetry_ratio = va_result.get('asymmetry_ratio')
            volume_asymmetry_tier = va_result.get('tier')
        except Exception:
            pass

    # Carr Stage (tam OHLCV — slope_info ohlcv_close/volume) — KARAR #733
    carr_stage = carr_stage_label = carr_slope_pct_per_year = None
    carr_ma_value = carr_price_vs_ma_pct = None
    _oc = slope_info.get("ohlcv_close")
    _ov = slope_info.get("ohlcv_volume")
    try:
        if _oc and _ov and len(_oc) >= 150 and len(_oc) == len(_ov):
            cs_result = compute_carr_stage(_oc, _ov)
            carr_stage = cs_result.get('stage')
            carr_stage_label = cs_result.get('stage_label')
            carr_slope_pct_per_year = cs_result.get('slope_pct_per_year')
            carr_ma_value = cs_result.get('ma_value')
            carr_price_vs_ma_pct = cs_result.get('price_vs_ma_pct')
    except Exception:
        pass

    # O'Neil base detector (tam OHLCV — slope_info ohlcv_*) — Paket 465
    cup_handle_quality = flat_base_quality = double_bottom_quality = None
    _oh = slope_info.get("ohlcv_high")
    _ol = slope_info.get("ohlcv_low")
    try:
        if (_oh and _ol and _oc and _ov and len(_oc) >= 60
                and len({len(_oh), len(_ol), len(_oc), len(_ov)}) == 1):
            cup_handle_quality = compute_cup_with_handle(_oh, _ol, _oc, _ov).get("quality")
            flat_base_quality = compute_flat_base(_oh, _ol, _oc, _ov).get("quality")
            double_bottom_quality = compute_double_bottom(_oh, _ol, _oc, _ov).get("quality")
    except Exception:
        pass

    passed = 1 if slope is not None and slope > 0 else 0

    c.execute("""
        INSERT INTO minervini_scans
        (scan_date, ticker, company, sector, industry,
         price, change_pct, volume, market_cap, pe,
         ma200_slope, passed, high52, sma50, atr14,
         price_volume_history, tight_low_vol_pass, vcp_quality_score,
         vcp_ready_score, power_play_pass,
         tennis_ball_pattern, volume_asymmetry_ratio, volume_asymmetry_tier,
         confirmations, violations,
         rs_ibd, rs_12m, rs_20d, rs_50d, rs_200d, rs_mansfield,
         carr_stage, carr_stage_label, carr_slope_pct_per_year,
         carr_ma_value, carr_price_vs_ma_pct,
         cup_handle_quality, flat_base_quality, double_bottom_quality)
        VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
        ON CONFLICT(scan_date, ticker) DO UPDATE SET
            company               = EXCLUDED.company,
            sector                = EXCLUDED.sector,
            industry              = EXCLUDED.industry,
            price                 = EXCLUDED.price,
            change_pct            = EXCLUDED.change_pct,
            volume                = EXCLUDED.volume,
            market_cap            = EXCLUDED.market_cap,
            pe                    = EXCLUDED.pe,
            ma200_slope           = EXCLUDED.ma200_slope,
            passed                = EXCLUDED.passed,
            high52                = EXCLUDED.high52,
            sma50                 = EXCLUDED.sma50,
            atr14                 = EXCLUDED.atr14,
            price_volume_history  = EXCLUDED.price_volume_history,
            tight_low_vol_pass    = EXCLUDED.tight_low_vol_pass,
            vcp_quality_score     = EXCLUDED.vcp_quality_score,
            vcp_ready_score       = EXCLUDED.vcp_ready_score,
            power_play_pass       = EXCLUDED.power_play_pass,
            tennis_ball_pattern   = EXCLUDED.tennis_ball_pattern,
            volume_asymmetry_ratio = EXCLUDED.volume_asymmetry_ratio,
            volume_asymmetry_tier  = EXCLUDED.volume_asymmetry_tier,
            confirmations         = EXCLUDED.confirmations,
            violations            = EXCLUDED.violations,
            rs_ibd                = EXCLUDED.rs_ibd,
            rs_12m                = EXCLUDED.rs_12m,
            rs_20d                = EXCLUDED.rs_20d,
            rs_50d                = EXCLUDED.rs_50d,
            rs_200d               = EXCLUDED.rs_200d,
            rs_mansfield          = EXCLUDED.rs_mansfield,
            carr_stage            = EXCLUDED.carr_stage,
            carr_stage_label      = EXCLUDED.carr_stage_label,
            carr_slope_pct_per_year = EXCLUDED.carr_slope_pct_per_year,
            carr_ma_value         = EXCLUDED.carr_ma_value,
            carr_price_vs_ma_pct  = EXCLUDED.carr_price_vs_ma_pct,
            cup_handle_quality    = EXCLUDED.cup_handle_quality,
            flat_base_quality     = EXCLUDED.flat_base_quality,
            double_bottom_quality = EXCLUDED.double_bottom_quality
    """, (
        scan_date, ticker,
        row.get("Company", ""), row.get("Sector", ""), row.get("Industry", ""),
        row.get("Price", 0), row.get("Change", ""), row.get("Volume", 0),
        row.get("Market Cap", 0), row.get("P/E", 0),
        slope, passed, high52, sma50, atr14,
        pvh_json, tight_low_vol_pass, vcp_quality_score, vcp_ready_score,
        power_play_pass,
        tennis_ball_pattern, volume_asymmetry_ratio, volume_asymmetry_tier,
        confs, viols,
        rs_ibd, rs_12m, rs_20d, rs_50d, rs_200d, rs_mf,
        carr_stage, carr_stage_label, carr_slope_pct_per_year,
        carr_ma_value, carr_price_vs_ma_pct,
        cup_handle_quality, flat_base_quality, double_bottom_quality,
    ))


def save_results(df_finviz, slopes, scan_date):
    conn = get_connection()
    c = conn.cursor()
    saved = 0
    
    for _, row in df_finviz.iterrows():
        try:
            _upsert_minervini_scan_row(c, scan_date, row["Ticker"], row, slopes.get(row["Ticker"]))
            saved += 1
        except Exception as e:
            print(f"  Kayit hatasi {row['Ticker']}: {e}")
            break  # ilk hatada dur

    conn.commit()
    conn.close()
    return saved

# --- ANA AKIŞ ---
def run_scan(scan_date_override: str = None, force: bool = False):
    """
    Ana scan fonksiyonu.

    Args:
        scan_date_override: Belirli bir tarih için scan (test/backfill). Verilirse
            takvim + intraday guard atlanır (belirtilen tarih EOD kabul edilir).
        force: True ise BİLEREK ZORLAMA. Tam kapsam (semantik genişleme — B6-01, 05 Tem 2026):
            (1) mevcut günün verisini SİL + yeniden ÇALIŞTIR (HTTP /scan?force=1 yolunda
                server tarafı — bkz. scanner_server.py),
            (2) takvim guard bypass (hafta sonu/tatil kontrolü atlanır),
            (3) intraday guard bypass (ABD seansı AÇIKKEN partial bar YAZILIR).
            Sadece ne yaptığını bilerek kullan — partial/yanlış EOD verisi riski var.

    Sprint 4-bis.7 (22 May 2026): ABD borsa takvim entegrasyonu —
    Hafta sonu + ABD tatil günlerinde scan ATLANIR (Sn. Ferit talimat:
    "veri çekme saati ABD borsa saatleri ABD tatiller").
    B6-01 (05 Tem 2026): intraday partial-bar guard — seans açıkken (9:30-16:00 ET)
    bugünü taramak partial bar'ı EOD sanıp yazar (H#17 ailesi); is_us_market_open ile bloklanır.
    Manuel override için force=True veya scan_date_override kullan.
    """
    # ABD borsa takvim kısa devre (Sprint 4-bis.7 — market_calendar.py)
    if scan_date_override is None and not force:
        try:
            from market_calendar import should_scan_today, now_tr, now_et
            ok, reason = should_scan_today()
            if not ok:
                tr = now_tr().strftime("%Y-%m-%d %H:%M %Z")
                et = now_et().strftime("%Y-%m-%d %H:%M %Z")
                print("=== QUANFINA SCANNER — SKIP ===")
                print(f"TR: {tr}")
                print(f"ET: {et}")
                print(f"[SKIP] Bugun scan ATLANDI. Sebep: {reason}")
                print("[INFO] Manuel zorlama icin: run_scan(force=True)")
                print("[INFO] Belirli tarih icin: run_scan(scan_date_override='YYYY-MM-DD')")
                return
        except ImportError:
            print("[WARN] market_calendar modulu bulunamadi - takvim kontrolu atlandi.")

    # B6-01 (05 Tem 2026): Intraday partial-bar korumasi. Bugunu tararken ABD ana seansi
    # (9:30-16:00 ET) ACIKSA gunun bar'i henuz kapanmadi (partial); EOD sanip yazmak
    # point-in-time butunlugunu bozar (H#17 ailesi — sessiz bozuk veri). force ile bilerek
    # atlanir; belirli tarih (scan_date_override) EOD kabul edildigi icin muaf.
    if scan_date_override is None and not force:
        try:
            from market_calendar import is_us_market_open, now_et
            if is_us_market_open():
                et = now_et().strftime("%Y-%m-%d %H:%M %Z")
                print("=== QUANFINA SCANNER — INTRADAY BLOCK ===")
                print(f"ET: {et}")
                print("[BLOCK] ABD seansi acik — bugunun bar'i partial, EOD scan atlandi.")
                print("[INFO] Kapanistan (16:00 ET) sonra calistir VEYA run_scan(force=True) ile zorla.")
                return
        except ImportError:
            print("[WARN] market_calendar modulu bulunamadi - intraday kontrolu atlandi.")

    try:
        health_check_finviz()
    except ScannerHealthError as e:
        print(f"[FAIL] FINVIZ HEALTH CHECK BASARISIZ:\n{e}")
        print(f"\nScan iptal edildi. Sorunu coz, tekrar dene.")
        return

    if scan_date_override:
        scan_date = scan_date_override
        print("=== QUANFINA SCANNER v2 (Geçmiş Tarih) ===")
    else:
        today = date.today()
        if today.weekday() == 5:
            scan_date = str(today - timedelta(days=1))
        elif today.weekday() == 6:
            scan_date = str(today - timedelta(days=2))
        else:
            scan_date = str(today)
        print("=== QUANFINA SCANNER v2 (Hızlı) ===")

    print(f"Tarih: {scan_date}")

    # Tek bağlantı kullan - database lock önle
    conn = get_connection()
    c = conn.cursor()
    resume_partial = False

    # Aynı tarih kontrolü
    c.execute(
        "SELECT COUNT(*) FROM information_schema.tables WHERE table_name = 'minervini_scans'"
    )
    if c.fetchone()[0]:
        c.execute("SELECT COUNT(*) FROM minervini_scans WHERE scan_date = %s", (scan_date,))
        count = c.fetchone()[0]
        if count > 0:
            c.execute("SELECT COUNT(*) FROM minervini_fundamental_scans WHERE scan_date = %s", (scan_date,))
            fund_c = c.fetchone()[0]
            c.execute("SELECT COUNT(*) FROM minervini_52w_high WHERE scan_date = %s", (scan_date,))
            w52_c = c.fetchone()[0]
            c.execute("SELECT COUNT(*) FROM minervini_fundamental_only WHERE scan_date = %s", (scan_date,))
            fo_c = c.fetchone()[0]
            if fund_c == 0 or w52_c == 0 or fo_c == 0:
                print(f"\n[!] Eksik tarama tespit edildi (fund:{fund_c}, 52w:{w52_c}, fo:{fo_c}).")
                print("[!] Ana tarama atlanıyor — sadece eksik tablolar tamamlanacak.")
                resume_partial = True
            else:
                print(f"\n[!] Bugün ({scan_date}) zaten {count} kayıt mevcut.")
                noninteractive = os.getenv("QUANFINA_NONINTERACTIVE", "")
                if noninteractive == "force":
                    answer = "e"
                elif noninteractive:
                    answer = "h"
                else:
                    answer = input("Yeniden tara? (e/h, varsayılan: h): ").strip().lower()
                if answer != "e":
                    print("Tarama iptal edildi. Mevcut veriler kullanılabilir.")
                    conn.close()
                    sys.exit(0)
                else:
                    print("Mevcut kayıtlar siliniyor...")
                    for tbl in ["minervini_scans", "minervini_52w_high",
                                "minervini_fundamental_scans", "minervini_fundamental_only"]:
                        c.execute(f"DELETE FROM {tbl} WHERE scan_date = %s", (scan_date,))
                    conn.commit()
                    print("Silindi. Tarama başlıyor...\n")

    # Tabloları oluştur
    c.execute("""
        CREATE TABLE IF NOT EXISTS minervini_scans (
            id            SERIAL PRIMARY KEY,
            scan_date     TEXT NOT NULL,
            ticker        TEXT NOT NULL,
            company       TEXT,
            sector        TEXT,
            industry      TEXT,
            price         DOUBLE PRECISION,
            change_pct    TEXT,
            volume        INTEGER,
            market_cap    DOUBLE PRECISION,
            pe            DOUBLE PRECISION,
            eps_qoq       TEXT,
            sales_qoq     TEXT,
            ma200_slope   DOUBLE PRECISION,
            passed        INTEGER DEFAULT 1,
            grade         TEXT,
            UNIQUE(scan_date, ticker)
        )
    """)
    for col_sql in [
        "ALTER TABLE minervini_scans ADD COLUMN IF NOT EXISTS earnings_date TEXT",
        "ALTER TABLE minervini_scans ADD COLUMN IF NOT EXISTS eps_last_updated TEXT",
        "ALTER TABLE minervini_scans ADD COLUMN IF NOT EXISTS sales_last_updated TEXT",
        "ALTER TABLE minervini_scans ADD COLUMN IF NOT EXISTS high52 DOUBLE PRECISION",
        "ALTER TABLE minervini_52w_high ADD COLUMN IF NOT EXISTS high52 DOUBLE PRECISION",
        "ALTER TABLE minervini_fundamental_only ADD COLUMN IF NOT EXISTS high52 DOUBLE PRECISION",
        "ALTER TABLE minervini_fundamental_scans ADD COLUMN IF NOT EXISTS high52 DOUBLE PRECISION",
        "ALTER TABLE minervini_scans ADD COLUMN IF NOT EXISTS confirmations TEXT",
        "ALTER TABLE minervini_scans ADD COLUMN IF NOT EXISTS violations TEXT",
        "ALTER TABLE minervini_52w_high ADD COLUMN IF NOT EXISTS confirmations TEXT",
        "ALTER TABLE minervini_52w_high ADD COLUMN IF NOT EXISTS violations TEXT",
        "ALTER TABLE minervini_fundamental_only ADD COLUMN IF NOT EXISTS confirmations TEXT",
        "ALTER TABLE minervini_fundamental_only ADD COLUMN IF NOT EXISTS violations TEXT",
        "ALTER TABLE minervini_fundamental_scans ADD COLUMN IF NOT EXISTS confirmations TEXT",
        "ALTER TABLE minervini_fundamental_scans ADD COLUMN IF NOT EXISTS violations TEXT",
        "ALTER TABLE minervini_scans ADD COLUMN IF NOT EXISTS rs_ibd DOUBLE PRECISION",
        "ALTER TABLE minervini_scans ADD COLUMN IF NOT EXISTS rs_12m DOUBLE PRECISION",
        "ALTER TABLE minervini_scans ADD COLUMN IF NOT EXISTS rs_20d DOUBLE PRECISION",
        "ALTER TABLE minervini_scans ADD COLUMN IF NOT EXISTS rs_50d DOUBLE PRECISION",
        "ALTER TABLE minervini_scans ADD COLUMN IF NOT EXISTS rs_200d DOUBLE PRECISION",
        "ALTER TABLE minervini_scans ADD COLUMN IF NOT EXISTS rs_mansfield DOUBLE PRECISION",
        "ALTER TABLE minervini_52w_high ADD COLUMN IF NOT EXISTS rs_ibd DOUBLE PRECISION",
        "ALTER TABLE minervini_52w_high ADD COLUMN IF NOT EXISTS rs_12m DOUBLE PRECISION",
        "ALTER TABLE minervini_52w_high ADD COLUMN IF NOT EXISTS rs_20d DOUBLE PRECISION",
        "ALTER TABLE minervini_52w_high ADD COLUMN IF NOT EXISTS rs_50d DOUBLE PRECISION",
        "ALTER TABLE minervini_52w_high ADD COLUMN IF NOT EXISTS rs_200d DOUBLE PRECISION",
        "ALTER TABLE minervini_52w_high ADD COLUMN IF NOT EXISTS rs_mansfield DOUBLE PRECISION",
        "ALTER TABLE minervini_fundamental_only ADD COLUMN IF NOT EXISTS rs_ibd DOUBLE PRECISION",
        "ALTER TABLE minervini_fundamental_only ADD COLUMN IF NOT EXISTS rs_12m DOUBLE PRECISION",
        "ALTER TABLE minervini_fundamental_only ADD COLUMN IF NOT EXISTS rs_20d DOUBLE PRECISION",
        "ALTER TABLE minervini_fundamental_only ADD COLUMN IF NOT EXISTS rs_50d DOUBLE PRECISION",
        "ALTER TABLE minervini_fundamental_only ADD COLUMN IF NOT EXISTS rs_200d DOUBLE PRECISION",
        "ALTER TABLE minervini_fundamental_only ADD COLUMN IF NOT EXISTS rs_mansfield DOUBLE PRECISION",
        "ALTER TABLE minervini_fundamental_scans ADD COLUMN IF NOT EXISTS rs_ibd DOUBLE PRECISION",
        "ALTER TABLE minervini_fundamental_scans ADD COLUMN IF NOT EXISTS rs_12m DOUBLE PRECISION",
        "ALTER TABLE minervini_fundamental_scans ADD COLUMN IF NOT EXISTS rs_20d DOUBLE PRECISION",
        "ALTER TABLE minervini_fundamental_scans ADD COLUMN IF NOT EXISTS rs_50d DOUBLE PRECISION",
        "ALTER TABLE minervini_fundamental_scans ADD COLUMN IF NOT EXISTS rs_200d DOUBLE PRECISION",
        "ALTER TABLE minervini_fundamental_scans ADD COLUMN IF NOT EXISTS rs_mansfield DOUBLE PRECISION",
        # Sprint 4.7e.1 — sma50 + atr14 teknik göstergeler; perf_year/roe şema tutarsızlık düzeltmesi
        "ALTER TABLE minervini_scans ADD COLUMN IF NOT EXISTS sma50 DOUBLE PRECISION",
        "ALTER TABLE minervini_scans ADD COLUMN IF NOT EXISTS atr14 DOUBLE PRECISION",
        "ALTER TABLE minervini_scans ADD COLUMN IF NOT EXISTS perf_year DOUBLE PRECISION",
        "ALTER TABLE minervini_scans ADD COLUMN IF NOT EXISTS roe DOUBLE PRECISION",
        # Sprint 4.7e.3 — son 25 günlük fiyat/hacim geçmişi (count_distribution_days için)
        "ALTER TABLE minervini_scans ADD COLUMN IF NOT EXISTS price_volume_history JSONB",
        # Sprint 4-bis.4 KARAR #461 — VCP pre-compute (scanner.py hesaplar, SQL sade WHERE okur)
        "ALTER TABLE minervini_scans ADD COLUMN IF NOT EXISTS tight_low_vol_pass BOOLEAN DEFAULT FALSE",
        # Sprint 4-bis.5 KARAR #466 — VCP Kalite Skoru (EXCELLENT/PASS/NULL, 3 kanal sentezi)
        "ALTER TABLE minervini_scans ADD COLUMN IF NOT EXISTS vcp_quality_score TEXT DEFAULT NULL",
        # Sprint 4-bis.5 KARAR #465 — VCP Ready Score 0-100 (Inside Day + V-Dry + Tight)
        "ALTER TABLE minervini_scans ADD COLUMN IF NOT EXISTS vcp_ready_score INTEGER DEFAULT NULL",
        # Sprint 4-bis.5 KARAR #467 — Power Play (HTF) Mark canon: POLE %100+ FLAG %10-25
        "ALTER TABLE minervini_scans ADD COLUMN IF NOT EXISTS power_play_pass BOOLEAN DEFAULT FALSE",
        # KARAR #733 (Paket 272 — 28 May 2026): Carr Stage 4-Stage detector (Migration 009)
        "ALTER TABLE minervini_scans ADD COLUMN IF NOT EXISTS carr_stage SMALLINT",
        "ALTER TABLE minervini_scans ADD COLUMN IF NOT EXISTS carr_stage_label TEXT",
        "ALTER TABLE minervini_scans ADD COLUMN IF NOT EXISTS carr_slope_pct_per_year NUMERIC(8,2)",
        "ALTER TABLE minervini_scans ADD COLUMN IF NOT EXISTS carr_ma_value NUMERIC(12,4)",
        "ALTER TABLE minervini_scans ADD COLUMN IF NOT EXISTS carr_price_vs_ma_pct NUMERIC(8,2)",
        # Paket 465 (12 Haz 2026): O'Neil base detector ailesi (cup/flat/double quality)
        "ALTER TABLE minervini_scans ADD COLUMN IF NOT EXISTS cup_handle_quality TEXT DEFAULT NULL",
        "ALTER TABLE minervini_scans ADD COLUMN IF NOT EXISTS flat_base_quality TEXT DEFAULT NULL",
        "ALTER TABLE minervini_scans ADD COLUMN IF NOT EXISTS double_bottom_quality TEXT DEFAULT NULL",
    ]:
        c.execute(col_sql)

    c.execute("""
        CREATE TABLE IF NOT EXISTS minervini_fundamental_scans (
            id          SERIAL PRIMARY KEY,
            scan_date   TEXT NOT NULL,
            ticker      TEXT NOT NULL,
            company     TEXT,
            sector      TEXT,
            industry    TEXT,
            price       DOUBLE PRECISION,
            change_pct  TEXT,
            volume      INTEGER,
            market_cap  DOUBLE PRECISION,
            pe          DOUBLE PRECISION,
            ma200_slope DOUBLE PRECISION,
            high52      DOUBLE PRECISION,
            UNIQUE(scan_date, ticker)
        )
    """)

    c.execute("""
        CREATE TABLE IF NOT EXISTS minervini_fundamental_only (
            id          SERIAL PRIMARY KEY,
            scan_date   TEXT NOT NULL,
            ticker      TEXT NOT NULL,
            company     TEXT,
            sector      TEXT,
            industry    TEXT,
            price       DOUBLE PRECISION,
            change_pct  TEXT,
            volume      INTEGER,
            market_cap  DOUBLE PRECISION,
            pe          DOUBLE PRECISION,
            UNIQUE(scan_date, ticker)
        )
    """)
    for col_sql in [
        "ALTER TABLE minervini_fundamental_only ADD COLUMN IF NOT EXISTS ma200_slope DOUBLE PRECISION",
        "ALTER TABLE minervini_fundamental_only ADD COLUMN IF NOT EXISTS eps_qoq TEXT",
        "ALTER TABLE minervini_fundamental_only ADD COLUMN IF NOT EXISTS sales_qoq TEXT",
        "ALTER TABLE minervini_fundamental_only ADD COLUMN IF NOT EXISTS grade TEXT",
        # P481 (#5): INSERT (asagida) perf_year/roe yaziyor ama tabloda yoktu ->
        # her ticker INSERT "column does not exist" ile fail (1929 except: print) ->
        # fresh DB'de minervini_fundamental_only BOS kaliyordu. Additive (Kodlama #2).
        "ALTER TABLE minervini_fundamental_only ADD COLUMN IF NOT EXISTS perf_year DOUBLE PRECISION",
        "ALTER TABLE minervini_fundamental_only ADD COLUMN IF NOT EXISTS roe DOUBLE PRECISION",
    ]:
        c.execute(col_sql)

    c.execute("""
        CREATE TABLE IF NOT EXISTS minervini_52w_high (
            id          SERIAL PRIMARY KEY,
            scan_date   TEXT NOT NULL,
            ticker      TEXT NOT NULL,
            company     TEXT,
            sector      TEXT,
            industry    TEXT,
            price       DOUBLE PRECISION,
            change_pct  TEXT,
            volume      INTEGER,
            market_cap  DOUBLE PRECISION,
            ma200_slope DOUBLE PRECISION,
            eps_qoq     DOUBLE PRECISION,
            sales_qoq   DOUBLE PRECISION,
            grade       TEXT,
            UNIQUE(scan_date, ticker)
        )
    """)
    for col_sql in [
        # P481 (#4): INSERT (1894) perf_year/roe yaziyor ama 52w_high tablosunda yoktu ->
        # her ticker INSERT sessizce fail -> fresh DB'de minervini_52w_high BOS kaliyordu.
        # Additive ALTER (Kodlama #2, idempotent IF NOT EXISTS).
        "ALTER TABLE minervini_52w_high ADD COLUMN IF NOT EXISTS perf_year DOUBLE PRECISION",
        "ALTER TABLE minervini_52w_high ADD COLUMN IF NOT EXISTS roe DOUBLE PRECISION",
    ]:
        c.execute(col_sql)

    c.execute("""
        CREATE TABLE IF NOT EXISTS sector_rotation (
            id          SERIAL PRIMARY KEY,
            scan_date   DATE NOT NULL,
            ticker      VARCHAR(10) NOT NULL,
            sector_name VARCHAR(50) NOT NULL,
            perf_1w     DOUBLE PRECISION,
            perf_1m     DOUBLE PRECISION,
            perf_3m     DOUBLE PRECISION,
            perf_6m     DOUBLE PRECISION,
            perf_1y     DOUBLE PRECISION,
            rs_score    DOUBLE PRECISION,
            rs_rank     INTEGER,
            UNIQUE(scan_date, ticker)
        )
    """)
    c.execute("ALTER TABLE sector_rotation ADD COLUMN IF NOT EXISTS perf_1w DOUBLE PRECISION")

    # SERIAL id sequence'lerini yazma oncesi senkronla (desync -> 'duplicate key id'
    # -> tum tarama sessizce 0 kayit; 17 Haz 2026 kok neden). init_db da yapar ama
    # CLI/local yolu init_db cagirmaz -> burada da garanti. Idempotent + non-destructive.
    resync_serial_sequences(c)

    conn.commit()

    if not resume_partial:
        # 1. Finviz filtresi
        print("\n1. Finviz Elite filtresi çalışıyor...")
        df = get_finviz_screener()

        if df.empty:
            print("Hiç hisse bulunamadı.")
            conn.close()
            return

        tickers = df["Ticker"].tolist()

        # 2. Sadece geçenler için MA200 slope
        print("\n2. MA200 slope kontrolü (yfinance toplu indirme)...")
        slopes, spy_actual_date = check_ma200_slope(tickers)

        if spy_actual_date and spy_actual_date != scan_date:
            print(f"[!] Manuel tarih: {scan_date} → Gerçek piyasa günü: {spy_actual_date} (tatil/hafta sonu)")
            scan_date = spy_actual_date
            c.execute("SELECT COUNT(*) FROM minervini_scans WHERE scan_date = %s", (scan_date,))
            count2 = c.fetchone()[0]
            if count2 > 0:
                print(f"\n[!] {scan_date} için zaten {count2} kayıt mevcut.")
                noninteractive = os.getenv("QUANFINA_NONINTERACTIVE", "")
                if noninteractive == "force":
                    answer = "e"
                elif noninteractive:
                    answer = "h"
                else:
                    answer = input("Yeniden tara? (e/h, varsayılan: h): ").strip().lower()
                if answer != "e":
                    print("Tarama iptal edildi. Mevcut veriler kullanılabilir.")
                    conn.close()
                    sys.exit(0)
                else:
                    print("Mevcut kayıtlar siliniyor...")
                    for tbl in ["minervini_scans", "minervini_52w_high",
                                "minervini_fundamental_scans", "minervini_fundamental_only"]:
                        c.execute(f"DELETE FROM {tbl} WHERE scan_date = %s", (scan_date,))
                    conn.commit()
                    print("Silindi. Tarama başlıyor...\n")

        # 3. Kaydet
        print("\n3. Veritabanına kaydediliyor...")
        saved = 0

        for _, row in df.iterrows():
            # P469 (15 Haz 2026): canli yazma yolu artik tek _upsert_minervini_scan_row
            # helper'i (DRY). Eskiden bu inline INSERT yalniz 21 kolon yaziyordu -> base
            # (cup/flat/double) + carr + vcp/power_play kolonlari NULL kaliyordu. Helper
            # 39 kolonu (slope_info pvh + ohlcv_*) yazar.
            # Per-ticker SAVEPOINT (17 Haz 2026 — eski 'break' tek bozuk ticker'da
            # tum transaction'i abort edip TUM taramayi 0 kayda dusuruyordu; GENB NaN-JSON
            # bunu tetikledi). Savepoint ile yalniz bozuk satir rollback, gerisi devam.
            try:
                c.execute("SAVEPOINT row_sp")
                _upsert_minervini_scan_row(c, scan_date, row["Ticker"], row, slopes.get(row["Ticker"]))
                c.execute("RELEASE SAVEPOINT row_sp")
                saved += 1
            except Exception as e:
                c.execute("ROLLBACK TO SAVEPOINT row_sp")
                print(f"  Kayıt hatası {row['Ticker']}: {e}")
                continue  # tek bozuk ticker tarama butununu dusurmesin

        conn.commit()

        passed = sum(1 for s in slopes.values() if s and s.get("slope") is not None and s.get("slope") > 0)

        print(f"\n[OK] TARAMA TAMAMLANDI!")
        print(f"   Finviz filtresi geçen : {len(tickers)}")
        print(f"   MA200 slope geçen     : {passed}")
        print(f"   Toplam kayıt          : {saved}")
        print(f"   Tarih                 : {scan_date}")
    else:
        print("\n[DEVAM] Ana tarama zaten tamamlanmış — yardımcı tablolar işlenecek.")

    # --- EPS/SALES SCRAPING VE GRADE ---
    print("\n4. EPS/Sales Q/Q scraping ve grade hesaplaması...")
    c.execute("""
        SELECT ticker, earnings_date, eps_last_updated
        FROM minervini_scans WHERE scan_date = %s
    """, (scan_date,))
    tickers_data = c.fetchall()

    # Batch Finviz Elite API çağrısı (HTML scraping'in yerini aldı)
    ticker_list_blok1 = [t[0] for t in tickers_data]
    extras = get_finviz_extras(ticker_list_blok1) if ticker_list_blok1 else None
    print(f"  → Finviz extras alındı: {len(extras) if extras is not None else 0} ticker")

    print(f"EPS/Sales işleniyor: {len(tickers_data)} hisse...")

    stats = {"skipped": 0, "scraped": 0, "post_earnings": 0}
    today = date.today()

    for i, (ticker, stored_earnings_date, eps_last_updated) in enumerate(tickers_data, 1):
        reason = "scrape"

        if eps_last_updated:
            last_upd = date.fromisoformat(eps_last_updated)
            days_old = (today - last_upd).days
            ed = parse_earnings_date(stored_earnings_date)

            if ed is None:
                if days_old < 30:
                    stats["skipped"] += 1
                    continue
            elif today < ed + timedelta(days=2):
                if days_old < 30:
                    stats["skipped"] += 1
                    continue
            else:
                reason = "post_earnings"

        if i % 50 == 0 or i == 1:
            print(f"  [{i}/{len(tickers_data)}] {ticker} işleniyor...")
        try:
            c.execute("SAVEPOINT row_sp")
            # Finviz Elite batch API'den veri oku (HTML scraping'in yerini aldı)
            row = extras.loc[ticker] if (extras is not None and ticker in extras.index) else None
            if row is not None:
                eps_qoq           = _parse_pct(row["eps_qoq"])
                sales_qoq         = _parse_pct(row["sales_qoq"])
                perf_year         = _parse_pct(row["perf_year"])
                roe               = _parse_pct(row["roe"])
                earnings_date_raw = str(row["earnings_date"]) if row["earnings_date"] else None
            else:
                eps_qoq = sales_qoq = perf_year = roe = None
                earnings_date_raw = None

            grade = _compute_grade(eps_qoq, sales_qoq)

            today_str = str(today)
            c.execute("""
                UPDATE minervini_scans
                SET eps_qoq = %s, sales_qoq = %s, grade = %s,
                    earnings_date = %s, eps_last_updated = %s, sales_last_updated = %s,
                    perf_year = %s, roe = %s
                WHERE scan_date = %s AND ticker = %s
            """, (eps_qoq, sales_qoq, grade, earnings_date_raw, today_str, today_str,
                  perf_year, roe, scan_date, ticker))

            c.execute("RELEASE SAVEPOINT row_sp")
            stats["scraped"] += 1
            if reason == "post_earnings":
                stats["post_earnings"] += 1

        except Exception as e:
            c.execute("ROLLBACK TO SAVEPOINT row_sp")
            print(f"  Extras okuma hatası {ticker}: {e}")
            continue

    conn.commit()
    print(f"\n--- Scraping İstatistikleri ---")
    print(f"   Atlanan (güncel veri)     : {stats['skipped']}")
    print(f"   Scraping yapılan          : {stats['scraped']}")
    print(f"   Bilanço sonrası güncelle  : {stats['post_earnings']}")

    # --- FUNDAMENTAL TARAMA ---
    print("\n=== FUNDAMENTAL TARAMA BAŞLIYOR ===")
    df_fund = get_finviz_fundamental()

    if not df_fund.empty:
        tickers_fund = df_fund["Ticker"].tolist()
        print(f"MA200 slope kontrolü: {len(tickers_fund)} hisse...")
        slopes_fund, _ = check_ma200_slope(tickers_fund)
        saved_fund = 0
        
        for _, row in df_fund.iterrows():
            ticker     = row["Ticker"]
            slope_info = slopes_fund.get(ticker) or {}
            slope      = slope_info.get("slope")
            passed     = 1 if slope is not None and slope > 0 else 0

            if passed == 0:
                continue  # MA200 slope geçemeyenleri kaydetme

            try:
                c.execute("SAVEPOINT row_sp")
                c.execute("""
                    INSERT INTO minervini_fundamental_scans
                    (scan_date, ticker, company, sector, industry,
                     price, change_pct, volume, market_cap, pe, ma200_slope, high52,
                     confirmations, violations,
                     rs_ibd, rs_12m, rs_20d, rs_50d, rs_200d, rs_mansfield)
                    VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
                    ON CONFLICT (scan_date, ticker) DO UPDATE SET
                        company       = EXCLUDED.company,
                        sector        = EXCLUDED.sector,
                        industry      = EXCLUDED.industry,
                        price         = EXCLUDED.price,
                        change_pct    = EXCLUDED.change_pct,
                        volume        = EXCLUDED.volume,
                        market_cap    = EXCLUDED.market_cap,
                        pe            = EXCLUDED.pe,
                        ma200_slope   = EXCLUDED.ma200_slope,
                        high52        = EXCLUDED.high52,
                        confirmations = EXCLUDED.confirmations,
                        violations    = EXCLUDED.violations,
                        rs_ibd        = EXCLUDED.rs_ibd,
                        rs_12m        = EXCLUDED.rs_12m,
                        rs_20d        = EXCLUDED.rs_20d,
                        rs_50d        = EXCLUDED.rs_50d,
                        rs_200d       = EXCLUDED.rs_200d,
                        rs_mansfield  = EXCLUDED.rs_mansfield
                """, (
                    scan_date, ticker,
                    row.get("Company", ""), row.get("Sector", ""), row.get("Industry", ""),
                    row.get("Price", 0), row.get("Change", ""), row.get("Volume", 0),
                    row.get("Market Cap", 0), row.get("P/E", 0),
                    slope, slope_info.get("high52"),
                    slope_info.get("confirmations", ""), slope_info.get("violations", ""),
                    slope_info.get("rs_ibd"), slope_info.get("rs_12m"),
                    slope_info.get("rs_20d"), slope_info.get("rs_50d"),
                    slope_info.get("rs_200d"), slope_info.get("rs_mansfield"),
                ))
                c.execute("RELEASE SAVEPOINT row_sp")
                saved_fund += 1
            except Exception as e:
                c.execute("ROLLBACK TO SAVEPOINT row_sp")
                print(f"  Kayıt hatası {ticker}: {e}")
        
        conn.commit()
        passed_fund = sum(1 for s in slopes_fund.values() if s and s.get("slope") is not None and s["slope"] > 0)
        print(f"\n[OK] FUNDAMENTAL TARAMA TAMAMLANDI!")
        print(f"   Finviz filtresi geçen : {len(tickers_fund)}")
        print(f"   MA200 slope geçen     : {passed_fund}")
        print(f"   Toplam kayıt          : {saved_fund}")

    # --- SADECE TEMEL TARAMA ---
    print("\n=== SADECE TEMEL TARAMA BAŞLIYOR ===")
    df_fund_only = get_finviz_fundamental_only()

    if not df_fund_only.empty:
        tickers_fo = df_fund_only["Ticker"].tolist()

        # minervini_scans'da zaten olan tickerların verilerini al (tekrar çekme)
        placeholders = ','.join(['%s'] * len(tickers_fo))
        c.execute(f"""
            SELECT ticker, ma200_slope, high52, eps_qoq, sales_qoq, grade,
                   confirmations, violations,
                   rs_ibd, rs_12m, rs_20d, rs_50d, rs_200d, rs_mansfield,
                   perf_year, roe
            FROM minervini_scans
            WHERE scan_date = %s AND ticker IN ({placeholders})
        """, [scan_date] + tickers_fo)
        cached = {
            row[0]: {"slope": row[1], "high52": row[2], "eps_qoq": row[3], "sales_qoq": row[4],
                     "grade": row[5], "confirmations": row[6] or "", "violations": row[7] or "",
                     "rs_ibd": row[8], "rs_12m": row[9], "rs_20d": row[10],
                     "rs_50d": row[11], "rs_200d": row[12], "rs_mansfield": row[13],
                     "perf_year": row[14], "roe": row[15]}
            for row in c.fetchall()
        }

        need_new_data = [t for t in tickers_fo if t not in cached]
        print(f"  minervini_scans'dan alinan : {len(cached)}")
        print(f"  Yeni veri cekilecek        : {len(need_new_data)}")

        # Yeni tickerlar icin MA200 slope (yfinance batch)
        fresh_slopes, _ = check_ma200_slope(need_new_data) if need_new_data else ({}, None)

        # Yeni tickerlar icin Finviz Elite batch API
        fresh_eps = {}
        if need_new_data:
            print(f"  Finviz extras (batch): {len(need_new_data)} ticker...")
            try:
                extras_fo = get_finviz_extras(need_new_data)
            except Exception as e:
                print(f"  get_finviz_extras hatasi: {e}")
                extras_fo = None
            for ticker in need_new_data:
                erow = extras_fo.loc[ticker] if (extras_fo is not None and ticker in extras_fo.index) else None
                if erow is not None:
                    eps_q    = _parse_pct(erow["eps_qoq"])
                    sales_q  = _parse_pct(erow["sales_qoq"])
                    perf_y   = _parse_pct(erow["perf_year"])
                    roe_v    = _parse_pct(erow["roe"])
                else:
                    eps_q = sales_q = perf_y = roe_v = None
                fresh_eps[ticker] = {
                    "eps_qoq": eps_q, "sales_qoq": sales_q,
                    "grade": _compute_grade(eps_q, sales_q),
                    "perf_year": perf_y, "roe": roe_v,
                }

        # INSERT — tum veriyle
        saved_fund_only = 0
        for _, row in df_fund_only.iterrows():
            ticker = row["Ticker"]
            if ticker in cached:
                slope     = cached[ticker]["slope"]
                high52    = cached[ticker].get("high52")
                eps_qoq   = cached[ticker]["eps_qoq"]
                sales_qoq = cached[ticker]["sales_qoq"]
                grade     = cached[ticker]["grade"]
                confs     = cached[ticker].get("confirmations", "")
                viols     = cached[ticker].get("violations", "")
                rs_ibd    = cached[ticker].get("rs_ibd")
                rs_12m    = cached[ticker].get("rs_12m")
                rs_20d    = cached[ticker].get("rs_20d")
                rs_50d    = cached[ticker].get("rs_50d")
                rs_200d   = cached[ticker].get("rs_200d")
                rs_mf     = cached[ticker].get("rs_mansfield")
                perf_year = cached[ticker].get("perf_year")
                roe       = cached[ticker].get("roe")
            else:
                fresh_info = fresh_slopes.get(ticker) or {}
                slope      = fresh_info.get("slope")
                high52     = fresh_info.get("high52")
                confs      = fresh_info.get("confirmations", "")
                viols      = fresh_info.get("violations", "")
                rs_ibd     = fresh_info.get("rs_ibd")
                rs_12m     = fresh_info.get("rs_12m")
                rs_20d     = fresh_info.get("rs_20d")
                rs_50d     = fresh_info.get("rs_50d")
                rs_200d    = fresh_info.get("rs_200d")
                rs_mf      = fresh_info.get("rs_mansfield")
                eps_data   = fresh_eps.get(ticker, {})
                eps_qoq    = eps_data.get("eps_qoq")
                sales_qoq  = eps_data.get("sales_qoq")
                grade      = eps_data.get("grade", "D")
                perf_year  = eps_data.get("perf_year")
                roe        = eps_data.get("roe")

            try:
                c.execute("SAVEPOINT row_sp")
                c.execute("""
                    INSERT INTO minervini_fundamental_only
                    (scan_date, ticker, company, sector, industry,
                     price, change_pct, volume, market_cap, pe,
                     ma200_slope, eps_qoq, sales_qoq, grade, high52,
                     confirmations, violations,
                     rs_ibd, rs_12m, rs_20d, rs_50d, rs_200d, rs_mansfield,
                     perf_year, roe)
                    VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
                    ON CONFLICT (scan_date, ticker) DO UPDATE SET
                        company       = EXCLUDED.company,
                        sector        = EXCLUDED.sector,
                        industry      = EXCLUDED.industry,
                        price         = EXCLUDED.price,
                        change_pct    = EXCLUDED.change_pct,
                        volume        = EXCLUDED.volume,
                        market_cap    = EXCLUDED.market_cap,
                        pe            = EXCLUDED.pe,
                        ma200_slope   = EXCLUDED.ma200_slope,
                        eps_qoq       = EXCLUDED.eps_qoq,
                        sales_qoq     = EXCLUDED.sales_qoq,
                        grade         = EXCLUDED.grade,
                        high52        = EXCLUDED.high52,
                        confirmations = EXCLUDED.confirmations,
                        violations    = EXCLUDED.violations,
                        rs_ibd        = EXCLUDED.rs_ibd,
                        rs_12m        = EXCLUDED.rs_12m,
                        rs_20d        = EXCLUDED.rs_20d,
                        rs_50d        = EXCLUDED.rs_50d,
                        rs_200d       = EXCLUDED.rs_200d,
                        rs_mansfield  = EXCLUDED.rs_mansfield,
                        perf_year     = EXCLUDED.perf_year,
                        roe           = EXCLUDED.roe
                """, (
                    scan_date, ticker,
                    row.get("Company", ""), row.get("Sector", ""), row.get("Industry", ""),
                    row.get("Price", 0), row.get("Change", ""), row.get("Volume", 0),
                    row.get("Market Cap", 0), row.get("P/E", 0),
                    slope, eps_qoq, sales_qoq, grade, high52, confs, viols,
                    rs_ibd, rs_12m, rs_20d, rs_50d, rs_200d, rs_mf,
                    perf_year, roe,
                ))
                c.execute("RELEASE SAVEPOINT row_sp")
                saved_fund_only += 1
            except Exception as e:
                c.execute("ROLLBACK TO SAVEPOINT row_sp")
                print(f"  Kayit hatasi {ticker}: {e}")

        conn.commit()
        print(f"\n[OK] TEMEL TARAMA TAMAMLANDI!")
        print(f"   Temel filtresi gecen      : {len(df_fund_only)}")
        print(f"   Cache'den alinan (hizli)  : {len(cached)}")
        print(f"   Yeni scraping yapilan     : {len(need_new_data)}")
        print(f"   Toplam kayit              : {saved_fund_only}")

    # === 5. 52 HAFTA YÜKSEK TARAMA ===
    print("\n=== 52 HAFTA YÜKSEK TARAMA BAŞLIYOR ===")
    df_52w = get_finviz_52w_high()

    if not df_52w.empty:
        tickers_52w = df_52w["Ticker"].tolist()

        # minervini_scans'da zaten olan tickerların verilerini al (tekrar çekme)
        placeholders = ','.join(['%s'] * len(tickers_52w))
        c.execute(f"""
            SELECT ticker, ma200_slope, high52, eps_qoq, sales_qoq, grade,
                   confirmations, violations,
                   rs_ibd, rs_12m, rs_20d, rs_50d, rs_200d, rs_mansfield,
                   perf_year, roe
            FROM minervini_scans
            WHERE scan_date = %s AND ticker IN ({placeholders})
        """, [scan_date] + tickers_52w)
        cached_52w = {
            row[0]: {"slope": row[1], "high52": row[2], "eps_qoq": row[3], "sales_qoq": row[4],
                     "grade": row[5], "confirmations": row[6] or "", "violations": row[7] or "",
                     "rs_ibd": row[8], "rs_12m": row[9], "rs_20d": row[10],
                     "rs_50d": row[11], "rs_200d": row[12], "rs_mansfield": row[13],
                     "perf_year": row[14], "roe": row[15]}
            for row in c.fetchall()
        }

        need_new_52w = [t for t in tickers_52w if t not in cached_52w]
        print(f"  minervini_scans'dan alinan : {len(cached_52w)}")
        print(f"  Yeni veri cekilecek        : {len(need_new_52w)}")

        # Yeni tickerlar icin MA200 slope (yfinance batch)
        fresh_slopes_52w, _ = check_ma200_slope(need_new_52w) if need_new_52w else ({}, None)

        # Yeni tickerlar icin Finviz Elite batch API
        fresh_eps_52w = {}
        if need_new_52w:
            print(f"  Finviz extras (batch): {len(need_new_52w)} ticker...")
            try:
                extras_52w = get_finviz_extras(need_new_52w)
            except Exception as e:
                print(f"  get_finviz_extras hatasi: {e}")
                extras_52w = None
            for ticker in need_new_52w:
                erow = extras_52w.loc[ticker] if (extras_52w is not None and ticker in extras_52w.index) else None
                if erow is not None:
                    eps_q    = _parse_pct(erow["eps_qoq"])
                    sales_q  = _parse_pct(erow["sales_qoq"])
                    perf_y   = _parse_pct(erow["perf_year"])
                    roe_v    = _parse_pct(erow["roe"])
                else:
                    eps_q = sales_q = perf_y = roe_v = None
                fresh_eps_52w[ticker] = {
                    "eps_qoq": eps_q, "sales_qoq": sales_q,
                    "grade": _compute_grade(eps_q, sales_q),
                    "perf_year": perf_y, "roe": roe_v,
                }

        # INSERT — tum veriyle
        saved_52w = 0
        for _, row in df_52w.iterrows():
            ticker = row["Ticker"]
            if ticker in cached_52w:
                slope     = cached_52w[ticker]["slope"]
                high52    = cached_52w[ticker].get("high52")
                eps_qoq   = cached_52w[ticker]["eps_qoq"]
                sales_qoq = cached_52w[ticker]["sales_qoq"]
                grade     = cached_52w[ticker]["grade"]
                confs     = cached_52w[ticker].get("confirmations", "")
                viols     = cached_52w[ticker].get("violations", "")
                rs_ibd    = cached_52w[ticker].get("rs_ibd")
                rs_12m    = cached_52w[ticker].get("rs_12m")
                rs_20d    = cached_52w[ticker].get("rs_20d")
                rs_50d    = cached_52w[ticker].get("rs_50d")
                rs_200d   = cached_52w[ticker].get("rs_200d")
                rs_mf     = cached_52w[ticker].get("rs_mansfield")
                perf_year = cached_52w[ticker].get("perf_year")
                roe       = cached_52w[ticker].get("roe")
            else:
                fresh_info_52w = fresh_slopes_52w.get(ticker) or {}
                slope     = fresh_info_52w.get("slope")
                high52    = fresh_info_52w.get("high52")
                confs     = fresh_info_52w.get("confirmations", "")
                viols     = fresh_info_52w.get("violations", "")
                rs_ibd    = fresh_info_52w.get("rs_ibd")
                rs_12m    = fresh_info_52w.get("rs_12m")
                rs_20d    = fresh_info_52w.get("rs_20d")
                rs_50d    = fresh_info_52w.get("rs_50d")
                rs_200d   = fresh_info_52w.get("rs_200d")
                rs_mf     = fresh_info_52w.get("rs_mansfield")
                eps_data  = fresh_eps_52w.get(ticker, {})
                eps_qoq   = eps_data.get("eps_qoq")
                sales_qoq = eps_data.get("sales_qoq")
                grade     = eps_data.get("grade", "D")
                perf_year = eps_data.get("perf_year")
                roe       = eps_data.get("roe")

            try:
                c.execute("SAVEPOINT row_sp")
                c.execute("""
                    INSERT INTO minervini_52w_high
                    (scan_date, ticker, company, sector, industry,
                     price, change_pct, volume, market_cap,
                     ma200_slope, eps_qoq, sales_qoq, grade, high52,
                     confirmations, violations,
                     rs_ibd, rs_12m, rs_20d, rs_50d, rs_200d, rs_mansfield,
                     perf_year, roe)
                    VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
                    ON CONFLICT (scan_date, ticker) DO UPDATE SET
                        company       = EXCLUDED.company,
                        sector        = EXCLUDED.sector,
                        industry      = EXCLUDED.industry,
                        price         = EXCLUDED.price,
                        change_pct    = EXCLUDED.change_pct,
                        volume        = EXCLUDED.volume,
                        market_cap    = EXCLUDED.market_cap,
                        ma200_slope   = EXCLUDED.ma200_slope,
                        eps_qoq       = EXCLUDED.eps_qoq,
                        sales_qoq     = EXCLUDED.sales_qoq,
                        grade         = EXCLUDED.grade,
                        high52        = EXCLUDED.high52,
                        confirmations = EXCLUDED.confirmations,
                        violations    = EXCLUDED.violations,
                        rs_ibd        = EXCLUDED.rs_ibd,
                        rs_12m        = EXCLUDED.rs_12m,
                        rs_20d        = EXCLUDED.rs_20d,
                        rs_50d        = EXCLUDED.rs_50d,
                        rs_200d       = EXCLUDED.rs_200d,
                        rs_mansfield  = EXCLUDED.rs_mansfield,
                        perf_year     = EXCLUDED.perf_year,
                        roe           = EXCLUDED.roe
                """, (
                    scan_date, ticker,
                    row.get("Company", ""), row.get("Sector", ""), row.get("Industry", ""),
                    row.get("Price", 0), row.get("Change", ""), row.get("Volume", 0),
                    row.get("Market Cap", 0),
                    slope, eps_qoq, sales_qoq, grade, high52, confs, viols,
                    rs_ibd, rs_12m, rs_20d, rs_50d, rs_200d, rs_mf,
                    perf_year, roe,
                ))
                c.execute("RELEASE SAVEPOINT row_sp")
                saved_52w += 1
            except Exception as e:
                c.execute("ROLLBACK TO SAVEPOINT row_sp")
                print(f"  Kayit hatasi {ticker}: {e}")

        conn.commit()
        print(f"\n[OK] 52 HAFTA YÜKSEK TARAMA TAMAMLANDI!")
        print(f"   52W filtresi gecen         : {len(df_52w)}")
        print(f"   Cache'den alinan (hizli)   : {len(cached_52w)}")
        print(f"   Yeni scraping yapilan      : {len(need_new_52w)}")
        print(f"   Toplam kayit               : {saved_52w}")

    conn.close()

if __name__ == "__main__":
    run_scan()