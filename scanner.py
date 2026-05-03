import os
import sys
import time
import requests
import pandas as pd
import yfinance as yf
from io import StringIO
from dotenv import load_dotenv
from datetime import date, datetime, timedelta
from bs4 import BeautifulSoup

from db_connection import get_connection

load_dotenv()
FINVIZ_KEY = os.getenv("FINVIZ_API_KEY")

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
    conn.commit()
    conn.close()

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
    - ta_highlow52w_a25h : 52W High'a %25 içinde
    - ta_highlow52w_b75l : 52W Low'dan %25 yukarı
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
        f"v=152&f={filters}&auth={FINVIZ_KEY}&ft=4"
    )
    
    r = requests.get(url, headers={"User-Agent": "Mozilla/5.0"}, timeout=15)
    df = pd.read_csv(StringIO(r.text))
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
        f"v=152&f={filters}&auth={FINVIZ_KEY}&ft=4"
    )

    r = requests.get(url, headers={"User-Agent": "Mozilla/5.0"}, timeout=15)
    df = pd.read_csv(StringIO(r.text))
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
        f"v=152&f={filters}&auth={FINVIZ_KEY}&ft=4"
    )

    r = requests.get(url, headers={"User-Agent": "Mozilla/5.0"}, timeout=15)
    df = pd.read_csv(StringIO(r.text))
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
        f"v=152&f={filters}&auth={FINVIZ_KEY}&ft=4"
    )
    r = requests.get(url, headers={"User-Agent": "Mozilla/5.0"}, timeout=15)
    df = pd.read_csv(StringIO(r.text))
    print(f"52W Yüksek filtresi geçti: {len(df)} hisse")
    return df

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
                ohlcv = pd.DataFrame({
                    "Open": open_, "High": high, "Low": low,
                    "Close": close, "Volume": volume,
                }).dropna()
                confs, viols = detect_signals(ohlcv)
                results[ticker] = {
                    "slope":         slope,
                    "high52":        high52,
                    "confirmations": ",".join(confs),
                    "violations":    ",".join(viols),
                }
            except:
                results[ticker] = {"slope": None, "high52": None,
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
    conn.close()
    return saved

# --- EPS/SALES Q/Q SCRAPING VE GRADE HESAPLAMA ---
def scrape_eps_sales_and_grade(scan_date):
    """
    minervini_scans tablosundaki her ticker için Finviz'den EPS Q/Q, Sales Q/Q ve
    Earnings date çeker; akıllı skip mantığıyla gereksiz istekleri atlar.
    """
    conn = get_connection()
    c = conn.cursor()

    c.execute("""
        SELECT ticker, earnings_date, eps_last_updated
        FROM minervini_scans WHERE scan_date = %s
    """, (scan_date,))
    tickers_data = c.fetchall()

    print(f"EPS/Sales scraping: {len(tickers_data)} hisse kontrol ediliyor...")

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
            print(f"  [{i}/{len(tickers_data)}] {ticker} scraping...")
        try:
            url = f"https://finviz.com/quote.ashx?t={ticker}"
            headers = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"}

            response = requests.get(url, headers=headers, timeout=10)
            soup = BeautifulSoup(response.content, 'html.parser')

            eps_qoq = None
            sales_qoq = None
            earnings_date_raw = None

            eps_label = soup.find('td', string='EPS Q/Q')
            if eps_label:
                eps_value = eps_label.find_next_sibling('td')
                if eps_value:
                    eps_text = eps_value.get_text().strip()
                    if eps_text.endswith('%'):
                        try:
                            eps_qoq = float(eps_text[:-1])
                        except:
                            eps_qoq = None

            sales_label = soup.find('td', string='Sales Q/Q')
            if sales_label:
                sales_value = sales_label.find_next_sibling('td')
                if sales_value:
                    sales_text = sales_value.get_text().strip()
                    if sales_text.endswith('%'):
                        try:
                            sales_qoq = float(sales_text[:-1])
                        except:
                            sales_qoq = None

            earnings_label = soup.find('td', string='Earnings')
            if earnings_label:
                earnings_value = earnings_label.find_next_sibling('td')
                if earnings_value:
                    earnings_date_raw = earnings_value.get_text().strip()

            grade = "D"
            if eps_qoq is not None and sales_qoq is not None:
                if eps_qoq > 40 and sales_qoq > 25:
                    grade = "A"
                elif eps_qoq > 25 and sales_qoq > 15:
                    grade = "B"
                elif eps_qoq > 20 and sales_qoq > 10:
                    grade = "C"

            today_str = str(today)
            c.execute("""
                UPDATE minervini_scans
                SET eps_qoq = %s, sales_qoq = %s, grade = %s,
                    earnings_date = %s, eps_last_updated = %s, sales_last_updated = %s
                WHERE scan_date = %s AND ticker = %s
            """, (eps_qoq, sales_qoq, grade, earnings_date_raw, today_str, today_str, scan_date, ticker))

            stats["scraped"] += 1
            if reason == "post_earnings":
                stats["post_earnings"] += 1
            time.sleep(0.5)

        except Exception as e:
            print(f"  Scraping hatası {ticker}: {e}")
            continue

    conn.commit()
    conn.close()
    print(f"\n--- Scraping İstatistikleri ---")
    print(f"   Atlanan (güncel veri)     : {stats['skipped']}")
    print(f"   Scraping yapılan          : {stats['scraped']}")
    print(f"   Bilanço sonrası güncelle  : {stats['post_earnings']}")
    return stats["scraped"]

# --- ANA AKIŞ ---
def run_scan():
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

    # Aynı tarih kontrolü
    c.execute(
        "SELECT COUNT(*) FROM information_schema.tables WHERE table_name = 'minervini_scans'"
    )
    if c.fetchone()[0]:
        c.execute("SELECT COUNT(*) FROM minervini_scans WHERE scan_date = %s", (scan_date,))
        count = c.fetchone()[0]
        if count > 0:
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

    conn.commit()

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

    # --- EPS/SALES SCRAPING VE GRADE ---
    print("\n4. EPS/Sales Q/Q scraping ve grade hesaplaması...")
    c.execute("""
        SELECT ticker, earnings_date, eps_last_updated
        FROM minervini_scans WHERE scan_date = ?
    """, (scan_date,))
    tickers_data = c.fetchall()

    print(f"EPS/Sales scraping: {len(tickers_data)} hisse kontrol ediliyor...")

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
            print(f"  [{i}/{len(tickers_data)}] {ticker} scraping...")
        try:
            url = f"https://finviz.com/quote.ashx?t={ticker}"
            headers = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"}

            response = requests.get(url, headers=headers, timeout=10)
            soup = BeautifulSoup(response.content, 'html.parser')

            eps_qoq = None
            sales_qoq = None
            earnings_date_raw = None

            eps_label = soup.find('td', string='EPS Q/Q')
            if eps_label:
                eps_value = eps_label.find_next_sibling('td')
                if eps_value:
                    eps_text = eps_value.get_text().strip()
                    if eps_text.endswith('%'):
                        try:
                            eps_qoq = float(eps_text[:-1])
                        except:
                            eps_qoq = None

            sales_label = soup.find('td', string='Sales Q/Q')
            if sales_label:
                sales_value = sales_label.find_next_sibling('td')
                if sales_value:
                    sales_text = sales_value.get_text().strip()
                    if sales_text.endswith('%'):
                        try:
                            sales_qoq = float(sales_text[:-1])
                        except:
                            sales_qoq = None

            earnings_label = soup.find('td', string='Earnings')
            if earnings_label:
                earnings_value = earnings_label.find_next_sibling('td')
                if earnings_value:
                    earnings_date_raw = earnings_value.get_text().strip()

            grade = "D"
            if eps_qoq is not None and sales_qoq is not None:
                if eps_qoq > 40 and sales_qoq > 25:
                    grade = "A"
                elif eps_qoq > 25 and sales_qoq > 15:
                    grade = "B"
                elif eps_qoq > 20 and sales_qoq > 10:
                    grade = "C"

            today_str = str(today)
            c.execute("""
                UPDATE minervini_scans
                SET eps_qoq = %s, sales_qoq = %s, grade = %s,
                    earnings_date = %s, eps_last_updated = %s, sales_last_updated = %s
                WHERE scan_date = %s AND ticker = %s
            """, (eps_qoq, sales_qoq, grade, earnings_date_raw, today_str, today_str, scan_date, ticker))

            stats["scraped"] += 1
            if reason == "post_earnings":
                stats["post_earnings"] += 1
            time.sleep(0.5)

        except Exception as e:
            print(f"  Scraping hatası {ticker}: {e}")
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
                   rs_ibd, rs_12m, rs_20d, rs_50d, rs_200d, rs_mansfield
            FROM minervini_scans
            WHERE scan_date = %s AND ticker IN ({placeholders})
        """, [scan_date] + tickers_fo)
        cached = {
            row[0]: {"slope": row[1], "high52": row[2], "eps_qoq": row[3], "sales_qoq": row[4],
                     "grade": row[5], "confirmations": row[6] or "", "violations": row[7] or "",
                     "rs_ibd": row[8], "rs_12m": row[9], "rs_20d": row[10],
                     "rs_50d": row[11], "rs_200d": row[12], "rs_mansfield": row[13]}
            for row in c.fetchall()
        }

        need_new_data = [t for t in tickers_fo if t not in cached]
        print(f"  minervini_scans'dan alinan : {len(cached)}")
        print(f"  Yeni veri cekilecek        : {len(need_new_data)}")

        # Yeni tickerlar icin MA200 slope (yfinance batch)
        fresh_slopes, _ = check_ma200_slope(need_new_data) if need_new_data else ({}, None)

        # Yeni tickerlar icin Finviz EPS/Sales scraping
        fresh_eps = {}
        if need_new_data:
            print(f"  Finviz EPS scraping: {len(need_new_data)} ticker...")
            headers = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"}
            for i, ticker in enumerate(need_new_data, 1):
                if i % 25 == 0 or i == 1:
                    print(f"    [{i}/{len(need_new_data)}] {ticker}...")
                try:
                    url = f"https://finviz.com/quote.ashx?t={ticker}"
                    response = requests.get(url, headers=headers, timeout=10)
                    soup = BeautifulSoup(response.content, 'html.parser')

                    eps_qoq = None
                    sales_qoq = None

                    eps_label = soup.find('td', string='EPS Q/Q')
                    if eps_label:
                        eps_value = eps_label.find_next_sibling('td')
                        if eps_value:
                            eps_text = eps_value.get_text().strip()
                            if eps_text.endswith('%'):
                                try:
                                    eps_qoq = float(eps_text[:-1])
                                except:
                                    pass

                    sales_label = soup.find('td', string='Sales Q/Q')
                    if sales_label:
                        sales_value = sales_label.find_next_sibling('td')
                        if sales_value:
                            sales_text = sales_value.get_text().strip()
                            if sales_text.endswith('%'):
                                try:
                                    sales_qoq = float(sales_text[:-1])
                                except:
                                    pass

                    grade = "D"
                    if eps_qoq is not None and sales_qoq is not None:
                        if eps_qoq > 40 and sales_qoq > 25:
                            grade = "A"
                        elif eps_qoq > 25 and sales_qoq > 15:
                            grade = "B"
                        elif eps_qoq > 20 and sales_qoq > 10:
                            grade = "C"

                    fresh_eps[ticker] = {"eps_qoq": eps_qoq, "sales_qoq": sales_qoq, "grade": grade}
                    time.sleep(0.5)
                except Exception as e:
                    print(f"  Scraping hatasi {ticker}: {e}")
                    fresh_eps[ticker] = {"eps_qoq": None, "sales_qoq": None, "grade": "D"}

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

            try:
                c.execute("""
                    INSERT INTO minervini_fundamental_only
                    (scan_date, ticker, company, sector, industry,
                     price, change_pct, volume, market_cap, pe,
                     ma200_slope, eps_qoq, sales_qoq, grade, high52,
                     confirmations, violations,
                     rs_ibd, rs_12m, rs_20d, rs_50d, rs_200d, rs_mansfield)
                    VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
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
                        rs_mansfield  = EXCLUDED.rs_mansfield
                """, (
                    scan_date, ticker,
                    row.get("Company", ""), row.get("Sector", ""), row.get("Industry", ""),
                    row.get("Price", 0), row.get("Change", ""), row.get("Volume", 0),
                    row.get("Market Cap", 0), row.get("P/E", 0),
                    slope, eps_qoq, sales_qoq, grade, high52, confs, viols,
                    rs_ibd, rs_12m, rs_20d, rs_50d, rs_200d, rs_mf,
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
                   rs_ibd, rs_12m, rs_20d, rs_50d, rs_200d, rs_mansfield
            FROM minervini_scans
            WHERE scan_date = %s AND ticker IN ({placeholders})
        """, [scan_date] + tickers_52w)
        cached_52w = {
            row[0]: {"slope": row[1], "high52": row[2], "eps_qoq": row[3], "sales_qoq": row[4],
                     "grade": row[5], "confirmations": row[6] or "", "violations": row[7] or "",
                     "rs_ibd": row[8], "rs_12m": row[9], "rs_20d": row[10],
                     "rs_50d": row[11], "rs_200d": row[12], "rs_mansfield": row[13]}
            for row in c.fetchall()
        }

        need_new_52w = [t for t in tickers_52w if t not in cached_52w]
        print(f"  minervini_scans'dan alinan : {len(cached_52w)}")
        print(f"  Yeni veri cekilecek        : {len(need_new_52w)}")

        # Yeni tickerlar icin MA200 slope (yfinance batch)
        fresh_slopes_52w, _ = check_ma200_slope(need_new_52w) if need_new_52w else ({}, None)

        # Yeni tickerlar icin Finviz EPS/Sales scraping
        fresh_eps_52w = {}
        if need_new_52w:
            print(f"  Finviz EPS scraping: {len(need_new_52w)} ticker...")
            headers = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"}
            for i, ticker in enumerate(need_new_52w, 1):
                if i % 25 == 0 or i == 1:
                    print(f"    [{i}/{len(need_new_52w)}] {ticker}...")
                try:
                    url = f"https://finviz.com/quote.ashx?t={ticker}"
                    response = requests.get(url, headers=headers, timeout=10)
                    soup = BeautifulSoup(response.content, 'html.parser')

                    eps_qoq = None
                    sales_qoq = None

                    eps_label = soup.find('td', string='EPS Q/Q')
                    if eps_label:
                        eps_value = eps_label.find_next_sibling('td')
                        if eps_value:
                            eps_text = eps_value.get_text().strip()
                            if eps_text.endswith('%'):
                                try:
                                    eps_qoq = float(eps_text[:-1])
                                except:
                                    pass

                    sales_label = soup.find('td', string='Sales Q/Q')
                    if sales_label:
                        sales_value = sales_label.find_next_sibling('td')
                        if sales_value:
                            sales_text = sales_value.get_text().strip()
                            if sales_text.endswith('%'):
                                try:
                                    sales_qoq = float(sales_text[:-1])
                                except:
                                    pass

                    grade = "D"
                    if eps_qoq is not None and sales_qoq is not None:
                        if eps_qoq > 40 and sales_qoq > 25:
                            grade = "A"
                        elif eps_qoq > 25 and sales_qoq > 15:
                            grade = "B"
                        elif eps_qoq > 20 and sales_qoq > 10:
                            grade = "C"

                    fresh_eps_52w[ticker] = {"eps_qoq": eps_qoq, "sales_qoq": sales_qoq, "grade": grade}
                    time.sleep(0.5)
                except Exception as e:
                    print(f"  Scraping hatasi {ticker}: {e}")
                    fresh_eps_52w[ticker] = {"eps_qoq": None, "sales_qoq": None, "grade": "D"}

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

            try:
                c.execute("""
                    INSERT INTO minervini_52w_high
                    (scan_date, ticker, company, sector, industry,
                     price, change_pct, volume, market_cap,
                     ma200_slope, eps_qoq, sales_qoq, grade, high52,
                     confirmations, violations,
                     rs_ibd, rs_12m, rs_20d, rs_50d, rs_200d, rs_mansfield)
                    VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
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
                        rs_mansfield  = EXCLUDED.rs_mansfield
                """, (
                    scan_date, ticker,
                    row.get("Company", ""), row.get("Sector", ""), row.get("Industry", ""),
                    row.get("Price", 0), row.get("Change", ""), row.get("Volume", 0),
                    row.get("Market Cap", 0),
                    slope, eps_qoq, sales_qoq, grade, high52, confs, viols,
                    rs_ibd, rs_12m, rs_20d, rs_50d, rs_200d, rs_mf,
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