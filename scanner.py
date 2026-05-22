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


def parse_earnings_date(raw: str):
    """'Apr 30 AMC' gibi Finviz earnings stringini date nesnesine çevirir."""
    if not raw or raw in ('-', 'N/A', ''):
        return None
    parts = raw.split()
    if len(parts) < 2:
        return None
    today = date.today()
    for year in [today.year, today.year + 1]:
        try:
            d = datetime.strptime(f"{parts[0]} {parts[1]} {year}", "%b %d %Y").date()
            if d >= today - timedelta(days=180):
                return d
        except ValueError:
            continue
    return None


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
    conn.commit()
    conn.close()

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
    Sadece temel filtreler — teknik kural YOK:
    - Fiyat > $10
    - Hacim > 500K
    - EPS Q/Q > %25
    - Sales Q/Q > %25
    """
    filters = ",".join([
        "sh_price_o10",
        "sh_avgvol_o500",
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


def _parse_pct(val):
    """
    Finviz API'den gelen yüzde stringini float'a çevirir.
    "96.65%" → 96.65
    "-12.3%" → -12.3
    "" / "-" / None → None
    Hata durumunda None döner (NaN/None koruması).
    """
    if val is None:
        return None
    s = str(val).strip()
    if s == "" or s == "-" or s.lower() == "nan":
        return None
    try:
        return float(s.rstrip("%"))
    except (ValueError, TypeError):
        return None


# Sprint 4-bis.4 KARAR #461 — Yerel VCP fonksiyonu quanfina_math'e tasindi.
# Brandon "Begrudgingly Pull Back" formulu (Minervini_Video.md sat. 2823-2867).
# Tek motor felsefesi: scanner.py import quanfina_math.compute_vcp_pass


def _compute_grade(eps_qoq, sales_qoq):
    """
    EPS Q/Q ve Sales Q/Q yüzdelerine göre Grade hesaplar.
    Eşikler değişmedi (önceki scraping mantığı ile aynı).
    None değerler → 'D' (yetersiz veri).

    A: EPS > 40 AND Sales > 25
    B: EPS > 25 AND Sales > 15
    C: EPS > 20 AND Sales > 10
    D: aksi
    """
    if eps_qoq is None or sales_qoq is None:
        return "D"
    if eps_qoq > 40 and sales_qoq > 25:
        return "A"
    if eps_qoq > 25 and sales_qoq > 15:
        return "B"
    if eps_qoq > 20 and sales_qoq > 10:
        return "C"
    return "D"


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
def detect_signals(ohlcv_df):
    """Son günün OHLCV'sinden teknik sinyal tespiti. En az 55 satır gerekli."""
    if len(ohlcv_df) < 55:
        return [], []

    confirmations = []
    violations    = []

    today     = ohlcv_df.iloc[-1]
    yesterday = ohlcv_df.iloc[-2]

    vol_sma50   = ohlcv_df["Volume"].iloc[-51:-1].mean()
    close_sma10 = ohlcv_df["Close"].iloc[-11:-1].mean()

    # CONFIRMATIONS
    if today["High"] < yesterday["High"] and today["Low"] > yesterday["Low"]:
        confirmations.append("Inside Day")

    if today["High"] >= ohlcv_df["High"].iloc[-5:].max() and \
       today["High"] > ohlcv_df["High"].iloc[-5:-1].max():
        confirmations.append("Higher High")

    if vol_sma50 > 0 and today["Volume"] > vol_sma50 * 1.5 and today["Close"] > today["Open"]:
        confirmations.append("Volume Surge")

    if today["Close"] > today["Open"] and vol_sma50 > 0 and today["Volume"] > vol_sma50:
        confirmations.append("Up on Volume")

    last_10   = ohlcv_df.iloc[-11:-1]
    down_days = last_10[last_10["Close"] < last_10["Open"]]
    if today["Close"] > today["Open"] and \
       (down_days.empty or today["Volume"] > down_days["Volume"].max()):
        confirmations.append("Pocket Pivot")

    # VIOLATIONS
    pct = ohlcv_df["Close"].pct_change().iloc[-30:]
    today_pct = pct.iloc[-1]
    if today_pct < 0 and today_pct <= pct.min():
        violations.append("Largest Down")

    if today["Close"] < today["Open"] and vol_sma50 > 0 and today["Volume"] > vol_sma50 * 1.2:
        violations.append("Down on Volume")

    if today["Open"] < yesterday["Close"] * 0.99:
        violations.append("Gap Down")

    if today["Close"] < close_sma10:
        violations.append("Below 10-MA")

    if today["Low"] <= ohlcv_df["Low"].iloc[-5:].min():
        violations.append("Lower Low")

    return confirmations, violations

# --- RS RATING HESAPLAMA ---
def calculate_rs_ratings(closes, spy_close):
    """
    closes    : {ticker: pd.Series close}
    spy_close : pd.Series SPY close (veya None)
    Döndürür  : {ticker: {rs_ibd, rs_12m, rs_20d, rs_50d, rs_200d, rs_mansfield}}
    """
    raw = {}

    for ticker, close in closes.items():
        close = close.dropna()
        n = len(close)
        if n < 20:
            continue

        p3  = float(close.iloc[-1] / close.iloc[-63]  - 1) if n >= 63  else None
        p6  = float(close.iloc[-1] / close.iloc[-126] - 1) if n >= 126 else None
        p9  = float(close.iloc[-1] / close.iloc[-189] - 1) if n >= 189 else None
        p12 = float(close.iloc[-1] / close.iloc[-252] - 1) if n >= 252 else None

        ibd_raw = (0.4 * p3 + 0.2 * p6 + 0.2 * p9 + 0.2 * p12
                   if all(x is not None for x in [p3, p6, p9, p12]) else None)

        rs_20d_raw = rs_50d_raw = rs_200d_raw = mansfield = None
        if spy_close is not None:
            spy    = spy_close.dropna()
            common = close.index.intersection(spy.index)
            tc     = close.loc[common]
            sc     = spy.loc[common]
            nc     = len(common)

            def rel(n_):
                if nc >= n_:
                    return float(tc.iloc[-1] / tc.iloc[-n_] - 1) - float(sc.iloc[-1] / sc.iloc[-n_] - 1)
                return None

            rs_20d_raw  = rel(20)
            rs_50d_raw  = rel(50)
            rs_200d_raw = rel(200)

            try:
                ratio = (tc / sc).dropna()
                if len(ratio) >= 252:
                    sma = ratio.rolling(252).mean()
                    mansfield = round(float(ratio.iloc[-1] / sma.iloc[-1]) - 1, 4)
            except Exception:
                pass

        raw[ticker] = {
            "ibd_raw":     ibd_raw,
            "p12":         p12,
            "rs_20d_raw":  rs_20d_raw,
            "rs_50d_raw":  rs_50d_raw,
            "rs_200d_raw": rs_200d_raw,
            "mansfield":   mansfield,
        }

    print(f"  [RS] closes count: {len(closes)}")
    print(f"  [RS] spy_close available: {spy_close is not None}")
    print(f"  [RS] sample raw values: {list(raw.items())[:3]}")

    def rank_1_99(field):
        pairs = [(t, v[field]) for t, v in raw.items() if v.get(field) is not None]
        if not pairs:
            return {}
        pairs.sort(key=lambda x: x[1])
        n = len(pairs)
        return {t: max(1, min(99, round((i + 1) / n * 99))) for i, (t, _) in enumerate(pairs)}

    ibd_r  = rank_1_99("ibd_raw")
    p12_r  = rank_1_99("p12")
    r20_r  = rank_1_99("rs_20d_raw")
    r50_r  = rank_1_99("rs_50d_raw")
    r200_r = rank_1_99("rs_200d_raw")

    result = {}
    for ticker in closes:
        result[ticker] = {
            "rs_ibd":       ibd_r.get(ticker),
            "rs_12m":       p12_r.get(ticker),
            "rs_20d":       r20_r.get(ticker),
            "rs_50d":       r50_r.get(ticker),
            "rs_200d":      r200_r.get(ticker),
            "rs_mansfield": raw.get(ticker, {}).get("mansfield"),
        }
    return result

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
                try:
                    tail_o = open_.tail(80)
                    tail_h = high.tail(80)
                    tail_l = low.tail(80)
                    tail_c = close.tail(80)
                    tail_v = volume.tail(80)
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
                results[ticker] = {
                    "slope":                slope,
                    "high52":               high52,
                    "sma50":                sma50_val,
                    "atr14":                atr14_val,
                    "price_volume_history": pvh_val,
                    "confirmations":        ",".join(confs),
                    "violations":           ",".join(viols),
                }
            except:
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
def save_results(df_finviz, slopes, scan_date):
    conn = get_connection()
    c = conn.cursor()
    saved = 0
    
    for _, row in df_finviz.iterrows():
        ticker     = row["Ticker"]
        slope_info = slopes.get(ticker) or {}
        slope      = slope_info.get("slope")
        high52     = slope_info.get("high52")
        confs      = slope_info.get("confirmations", "")
        viols      = slope_info.get("violations", "")
        rs_ibd     = slope_info.get("rs_ibd")
        rs_12m     = slope_info.get("rs_12m")
        rs_20d     = slope_info.get("rs_20d")
        rs_50d     = slope_info.get("rs_50d")
        rs_200d    = slope_info.get("rs_200d")
        rs_mf      = slope_info.get("rs_mansfield")
        sma50      = slope_info.get("sma50")
        atr14      = slope_info.get("atr14")
        pvh        = slope_info.get("price_volume_history")
        pvh_json   = json.dumps(pvh) if pvh else None

        # Sprint 4-bis.4 KARAR #461 — VCP pre-compute (quanfina_math motoru — tek motor)
        tight_low_vol_pass = compute_vcp_pass(pvh)
        # Sprint 4-bis.5 KARAR #466 — VCP Kalite Skoru (EXCELLENT/PASS/None)
        vcp_quality_score = compute_vcp_quality(pvh)
        # Sprint 4-bis.5 KARAR #465 — VCP Ready Score 0-100
        vcp_ready_score = compute_vcp_ready_score(pvh)
        # Sprint 4-bis.5 KARAR #467 — Power Play (HTF) Mark canon
        power_play_pass = compute_power_play_pass(pvh)

        # Kural 3: MA200 yükselişte (slope > 0)
        passed = 1 if slope is not None and slope > 0 else 0

        try:
            c.execute("""
                INSERT INTO minervini_scans
                (scan_date, ticker, company, sector, industry,
                 price, change_pct, volume, market_cap, pe,
                 ma200_slope, passed, high52, sma50, atr14,
                 price_volume_history, tight_low_vol_pass, vcp_quality_score,
                 vcp_ready_score, power_play_pass,
                 confirmations, violations,
                 rs_ibd, rs_12m, rs_20d, rs_50d, rs_200d, rs_mansfield)
                VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
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
                    confirmations         = EXCLUDED.confirmations,
                    violations            = EXCLUDED.violations,
                    rs_ibd                = EXCLUDED.rs_ibd,
                    rs_12m                = EXCLUDED.rs_12m,
                    rs_20d                = EXCLUDED.rs_20d,
                    rs_50d                = EXCLUDED.rs_50d,
                    rs_200d               = EXCLUDED.rs_200d,
                    rs_mansfield          = EXCLUDED.rs_mansfield
            """, (
                scan_date, ticker,
                row.get("Company", ""), row.get("Sector", ""), row.get("Industry", ""),
                row.get("Price", 0), row.get("Change", ""), row.get("Volume", 0),
                row.get("Market Cap", 0), row.get("P/E", 0),
                slope, passed, high52, sma50, atr14,
                pvh_json, tight_low_vol_pass, vcp_quality_score, vcp_ready_score,
                power_play_pass,
                confs, viols,
                rs_ibd, rs_12m, rs_20d, rs_50d, rs_200d, rs_mf,
            ))
            saved += 1
        except Exception as e:
            print(f"  Kayıt hatası {ticker}: {e}")
            break  # ilk hatada dur

    conn.commit()
    conn.close()
    return saved

# --- ANA AKIŞ ---
def run_scan(scan_date_override: str = None, force: bool = False):
    """
    Ana scan fonksiyonu.

    Args:
        scan_date_override: Belirli bir tarih için scan (test/backfill)
        force: True ise hafta sonu/tatil kontrolünü atla (manuel zorlama)

    Sprint 4-bis.7 (22 May 2026): ABD borsa takvim entegrasyonu —
    Hafta sonu + ABD tatil günlerinde scan ATLANIR (Sn. Ferit talimat:
    "veri çekme saati ABD borsa saatleri ABD tatiller").
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
            ticker     = row["Ticker"]
            slope_info = slopes.get(ticker) or {}
            slope      = slope_info.get("slope")
            high52     = slope_info.get("high52")
            confs      = slope_info.get("confirmations", "")
            viols      = slope_info.get("violations", "")
            rs_ibd     = slope_info.get("rs_ibd")
            rs_12m     = slope_info.get("rs_12m")
            rs_20d     = slope_info.get("rs_20d")
            rs_50d     = slope_info.get("rs_50d")
            rs_200d    = slope_info.get("rs_200d")
            rs_mf      = slope_info.get("rs_mansfield")

            # Kural 3: MA200 yükselişte (slope > 0)
            passed = 1 if slope is not None and slope > 0 else 0

            try:
                c.execute("""
                    INSERT INTO minervini_scans
                    (scan_date, ticker, company, sector, industry,
                     price, change_pct, volume, market_cap, pe,
                     ma200_slope, passed, high52, confirmations, violations,
                     rs_ibd, rs_12m, rs_20d, rs_50d, rs_200d, rs_mansfield)
                    VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
                    ON CONFLICT(scan_date, ticker) DO UPDATE SET
                        company       = EXCLUDED.company,
                        sector        = EXCLUDED.sector,
                        industry      = EXCLUDED.industry,
                        price         = EXCLUDED.price,
                        change_pct    = EXCLUDED.change_pct,
                        volume        = EXCLUDED.volume,
                        market_cap    = EXCLUDED.market_cap,
                        pe            = EXCLUDED.pe,
                        ma200_slope   = EXCLUDED.ma200_slope,
                        passed        = EXCLUDED.passed,
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
                    slope, passed, high52, confs, viols,
                    rs_ibd, rs_12m, rs_20d, rs_50d, rs_200d, rs_mf,
                ))
                saved += 1
            except Exception as e:
                print(f"  Kayıt hatası {ticker}: {e}")
                break  # ilk hatada dur

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

            stats["scraped"] += 1
            if reason == "post_earnings":
                stats["post_earnings"] += 1

        except Exception as e:
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
                saved_fund += 1
            except Exception as e:
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
                saved_fund_only += 1
            except Exception as e:
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
                saved_52w += 1
            except Exception as e:
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