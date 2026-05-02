import os
import sqlite3
import time
import requests
import pandas as pd
import yfinance as yf
from io import StringIO
from dotenv import load_dotenv
from datetime import date, datetime, timedelta
from bs4 import BeautifulSoup

load_dotenv()
FINVIZ_KEY = os.getenv("FINVIZ_API_KEY")
DB_PATH = os.path.join(os.path.dirname(__file__), "quanfina.db")

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
    conn = sqlite3.connect(DB_PATH)
    c = conn.cursor()
    c.execute("""
        CREATE TABLE IF NOT EXISTS minervini_scans (
            id            INTEGER PRIMARY KEY AUTOINCREMENT,
            scan_date     TEXT NOT NULL,
            ticker        TEXT NOT NULL,
            company       TEXT,
            sector        TEXT,
            industry      TEXT,
            price         REAL,
            change_pct    TEXT,
            volume        INTEGER,
            market_cap    REAL,
            pe            REAL,
            eps_qoq       TEXT,
            sales_qoq     TEXT,
            ma200_slope   REAL,
            passed        INTEGER DEFAULT 1,
            grade         TEXT,
            UNIQUE(scan_date, ticker)
        )
    """)
    for col_sql in [
        "ALTER TABLE minervini_scans ADD COLUMN earnings_date TEXT",
        "ALTER TABLE minervini_scans ADD COLUMN eps_last_updated TEXT",
        "ALTER TABLE minervini_scans ADD COLUMN sales_last_updated TEXT",
    ]:
        try:
            c.execute(col_sql)
        except sqlite3.OperationalError:
            pass
    for col_sql in [
        "ALTER TABLE minervini_fundamental_only ADD COLUMN ma200_slope REAL",
        "ALTER TABLE minervini_fundamental_only ADD COLUMN eps_qoq TEXT",
        "ALTER TABLE minervini_fundamental_only ADD COLUMN sales_qoq TEXT",
        "ALTER TABLE minervini_fundamental_only ADD COLUMN grade TEXT",
    ]:
        try:
            c.execute(col_sql)
        except sqlite3.OperationalError:
            pass
    c.execute("""
        CREATE TABLE IF NOT EXISTS minervini_52w_high (
            id          INTEGER PRIMARY KEY AUTOINCREMENT,
            scan_date   TEXT NOT NULL,
            ticker      TEXT NOT NULL,
            company     TEXT,
            sector      TEXT,
            industry    TEXT,
            price       REAL,
            change_pct  TEXT,
            volume      INTEGER,
            market_cap  REAL,
            ma200_slope REAL,
            eps_qoq     REAL,
            sales_qoq   REAL,
            grade       TEXT,
            UNIQUE(scan_date, ticker)
        )
    """)
    c.execute("""
        CREATE TABLE IF NOT EXISTS minervini_watchlist (
            id         INTEGER PRIMARY KEY AUTOINCREMENT,
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

# --- MA200 SLOPE KONTROLÜ (Kural 3) ---
def check_ma200_slope(tickers):
    """
    Tek kalan kural: MA200 en az 1 aydır yükselişte.
    Sadece geçen hisseler için yfinance'a bakıyoruz.
    """
    results = {}
    print(f"MA200 slope kontrolü: {len(tickers)} hisse...")
    
    # Toplu indirme - çok daha hızlı!
    try:
        data = yf.download(tickers, period="1y", progress=False, auto_adjust=True, group_by="ticker")
        
        for ticker in tickers:
            try:
                if len(tickers) == 1:
                    close = data["Close"].squeeze()
                else:
                    close = data[ticker]["Close"].squeeze()
                
                ma200_today = float(close.rolling(200).mean().iloc[-1])
                ma200_1m    = float(close.rolling(200).mean().iloc[-21])
                slope       = round(ma200_today - ma200_1m, 4)
                results[ticker] = slope
            except:
                results[ticker] = None
    except Exception as e:
        print(f"Toplu indirme hatası: {e}")
    
    return results

# --- VERİTABANINA KAYDET ---
def save_results(df_finviz, slopes, scan_date):
    conn = sqlite3.connect(DB_PATH)
    c = conn.cursor()
    saved = 0
    
    for _, row in df_finviz.iterrows():
        ticker = row["Ticker"]
        slope  = slopes.get(ticker, None)
        
        # Kural 3: MA200 yükselişte (slope > 0)
        passed = 1 if slope is not None and slope > 0 else 0
        
        try:
            c.execute("""
                INSERT INTO minervini_scans
                (scan_date, ticker, company, sector, industry,
                 price, change_pct, volume, market_cap, pe,
                 ma200_slope, passed)
                VALUES (?,?,?,?,?,?,?,?,?,?,?,?)
                ON CONFLICT(scan_date, ticker) DO UPDATE SET
                    company     = excluded.company,
                    sector      = excluded.sector,
                    industry    = excluded.industry,
                    price       = excluded.price,
                    change_pct  = excluded.change_pct,
                    volume      = excluded.volume,
                    market_cap  = excluded.market_cap,
                    pe          = excluded.pe,
                    ma200_slope = excluded.ma200_slope,
                    passed      = excluded.passed
            """, (
                scan_date,
                ticker,
                row.get("Company", ""),
                row.get("Sector", ""),
                row.get("Industry", ""),
                row.get("Price", 0),
                row.get("Change", ""),
                row.get("Volume", 0),
                row.get("Market Cap", 0),
                row.get("P/E", 0),
                slope,
                passed
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
    conn = sqlite3.connect(DB_PATH)
    c = conn.cursor()

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
                SET eps_qoq = ?, sales_qoq = ?, grade = ?,
                    earnings_date = ?, eps_last_updated = ?, sales_last_updated = ?
                WHERE scan_date = ? AND ticker = ?
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
    print("=== QUANFINA SCANNER v2 (Hızlı) ===")
    
    # Tek bağlantı kullan - database lock önle
    conn = sqlite3.connect(DB_PATH)
    c = conn.cursor()
    
    # Tabloları oluştur
    c.execute("""
        CREATE TABLE IF NOT EXISTS minervini_scans (
            id            INTEGER PRIMARY KEY AUTOINCREMENT,
            scan_date     TEXT NOT NULL,
            ticker        TEXT NOT NULL,
            company       TEXT,
            sector        TEXT,
            industry      TEXT,
            price         REAL,
            change_pct    TEXT,
            volume        INTEGER,
            market_cap    REAL,
            pe            REAL,
            eps_qoq       TEXT,
            sales_qoq     TEXT,
            ma200_slope   REAL,
            passed        INTEGER DEFAULT 1,
            grade         TEXT,
            UNIQUE(scan_date, ticker)
        )
    """)
    for col_sql in [
        "ALTER TABLE minervini_scans ADD COLUMN earnings_date TEXT",
        "ALTER TABLE minervini_scans ADD COLUMN eps_last_updated TEXT",
        "ALTER TABLE minervini_scans ADD COLUMN sales_last_updated TEXT",
    ]:
        try:
            c.execute(col_sql)
        except sqlite3.OperationalError:
            pass

    c.execute("""
        CREATE TABLE IF NOT EXISTS minervini_fundamental_scans (
            id          INTEGER PRIMARY KEY AUTOINCREMENT,
            scan_date   TEXT NOT NULL,
            ticker      TEXT NOT NULL,
            company     TEXT,
            sector      TEXT,
            industry    TEXT,
            price       REAL,
            change_pct  TEXT,
            volume      INTEGER,
            market_cap  REAL,
            pe          REAL,
            ma200_slope REAL,
            UNIQUE(scan_date, ticker)
        )
    """)
    
    c.execute("""
        CREATE TABLE IF NOT EXISTS minervini_fundamental_only (
            id          INTEGER PRIMARY KEY AUTOINCREMENT,
            scan_date   TEXT NOT NULL,
            ticker      TEXT NOT NULL,
            company     TEXT,
            sector      TEXT,
            industry    TEXT,
            price       REAL,
            change_pct  TEXT,
            volume      INTEGER,
            market_cap  REAL,
            pe          REAL,
            UNIQUE(scan_date, ticker)
        )
    """)
    for col_sql in [
        "ALTER TABLE minervini_fundamental_only ADD COLUMN ma200_slope REAL",
        "ALTER TABLE minervini_fundamental_only ADD COLUMN eps_qoq TEXT",
        "ALTER TABLE minervini_fundamental_only ADD COLUMN sales_qoq TEXT",
        "ALTER TABLE minervini_fundamental_only ADD COLUMN grade TEXT",
    ]:
        try:
            c.execute(col_sql)
        except sqlite3.OperationalError:
            pass

    c.execute("""
        CREATE TABLE IF NOT EXISTS minervini_52w_high (
            id          INTEGER PRIMARY KEY AUTOINCREMENT,
            scan_date   TEXT NOT NULL,
            ticker      TEXT NOT NULL,
            company     TEXT,
            sector      TEXT,
            industry    TEXT,
            price       REAL,
            change_pct  TEXT,
            volume      INTEGER,
            market_cap  REAL,
            ma200_slope REAL,
            eps_qoq     REAL,
            sales_qoq   REAL,
            grade       TEXT,
            UNIQUE(scan_date, ticker)
        )
    """)

    conn.commit()

    scan_date = str(date.today())
    print(f"Tarih: {scan_date}")

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
    slopes = check_ma200_slope(tickers)

    # 3. Kaydet
    print("\n3. Veritabanına kaydediliyor...")
    saved = 0
    
    for _, row in df.iterrows():
        ticker = row["Ticker"]
        slope  = slopes.get(ticker, None)
        
        # Kural 3: MA200 yükselişte (slope > 0)
        passed = 1 if slope is not None and slope > 0 else 0
        
        try:
            c.execute("""
                INSERT INTO minervini_scans
                (scan_date, ticker, company, sector, industry,
                 price, change_pct, volume, market_cap, pe,
                 ma200_slope, passed)
                VALUES (?,?,?,?,?,?,?,?,?,?,?,?)
                ON CONFLICT(scan_date, ticker) DO UPDATE SET
                    company     = excluded.company,
                    sector      = excluded.sector,
                    industry    = excluded.industry,
                    price       = excluded.price,
                    change_pct  = excluded.change_pct,
                    volume      = excluded.volume,
                    market_cap  = excluded.market_cap,
                    pe          = excluded.pe,
                    ma200_slope = excluded.ma200_slope,
                    passed      = excluded.passed
            """, (
                scan_date,
                ticker,
                row.get("Company", ""),
                row.get("Sector", ""),
                row.get("Industry", ""),
                row.get("Price", 0),
                row.get("Change", ""),
                row.get("Volume", 0),
                row.get("Market Cap", 0),
                row.get("P/E", 0),
                slope,
                passed
            ))
            saved += 1
        except Exception as e:
            print(f"  Kayıt hatası {ticker}: {e}")
            break  # ilk hatada dur
    
    conn.commit()

    passed = sum(1 for s in slopes.values() if s is not None and s > 0)
    
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
                SET eps_qoq = ?, sales_qoq = ?, grade = ?,
                    earnings_date = ?, eps_last_updated = ?, sales_last_updated = ?
                WHERE scan_date = ? AND ticker = ?
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
        slopes_fund = check_ma200_slope(tickers_fund)
        saved_fund = 0
        
        for _, row in df_fund.iterrows():
            ticker = row["Ticker"]
            slope  = slopes_fund.get(ticker, None)
            passed = 1 if slope is not None and slope > 0 else 0

            if passed == 0:
                continue  # MA200 slope geçemeyenleri kaydetme

            try:
                c.execute("""
                    INSERT OR REPLACE INTO minervini_fundamental_scans
                    (scan_date, ticker, company, sector, industry,
                     price, change_pct, volume, market_cap, pe, ma200_slope)
                    VALUES (?,?,?,?,?,?,?,?,?,?,?)
                """, (
                    scan_date,
                    ticker,
                    row.get("Company", ""),
                    row.get("Sector", ""),
                    row.get("Industry", ""),
                    row.get("Price", 0),
                    row.get("Change", ""),
                    row.get("Volume", 0),
                    row.get("Market Cap", 0),
                    row.get("P/E", 0),
                    slope,
                ))
                saved_fund += 1
            except Exception as e:
                print(f"  Kayıt hatası {ticker}: {e}")
        
        conn.commit()
        passed_fund = sum(1 for s in slopes_fund.values() if s is not None and s > 0)
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
        placeholders = ','.join('?' * len(tickers_fo))
        c.execute(f"""
            SELECT ticker, ma200_slope, eps_qoq, sales_qoq, grade
            FROM minervini_scans
            WHERE scan_date = ? AND ticker IN ({placeholders})
        """, [scan_date] + tickers_fo)
        cached = {
            row[0]: {"slope": row[1], "eps_qoq": row[2], "sales_qoq": row[3], "grade": row[4]}
            for row in c.fetchall()
        }

        need_new_data = [t for t in tickers_fo if t not in cached]
        print(f"  minervini_scans'dan alinan : {len(cached)}")
        print(f"  Yeni veri cekilecek        : {len(need_new_data)}")

        # Yeni tickerlar icin MA200 slope (yfinance batch)
        fresh_slopes = check_ma200_slope(need_new_data) if need_new_data else {}

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
                slope    = cached[ticker]["slope"]
                eps_qoq  = cached[ticker]["eps_qoq"]
                sales_qoq = cached[ticker]["sales_qoq"]
                grade    = cached[ticker]["grade"]
            else:
                slope    = fresh_slopes.get(ticker)
                eps_data = fresh_eps.get(ticker, {})
                eps_qoq  = eps_data.get("eps_qoq")
                sales_qoq = eps_data.get("sales_qoq")
                grade    = eps_data.get("grade", "D")

            try:
                c.execute("""
                    INSERT OR REPLACE INTO minervini_fundamental_only
                    (scan_date, ticker, company, sector, industry,
                     price, change_pct, volume, market_cap, pe,
                     ma200_slope, eps_qoq, sales_qoq, grade)
                    VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?)
                """, (
                    scan_date,
                    ticker,
                    row.get("Company", ""),
                    row.get("Sector", ""),
                    row.get("Industry", ""),
                    row.get("Price", 0),
                    row.get("Change", ""),
                    row.get("Volume", 0),
                    row.get("Market Cap", 0),
                    row.get("P/E", 0),
                    slope,
                    eps_qoq,
                    sales_qoq,
                    grade,
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
        placeholders = ','.join('?' * len(tickers_52w))
        c.execute(f"""
            SELECT ticker, ma200_slope, eps_qoq, sales_qoq, grade
            FROM minervini_scans
            WHERE scan_date = ? AND ticker IN ({placeholders})
        """, [scan_date] + tickers_52w)
        cached_52w = {
            row[0]: {"slope": row[1], "eps_qoq": row[2], "sales_qoq": row[3], "grade": row[4]}
            for row in c.fetchall()
        }

        need_new_52w = [t for t in tickers_52w if t not in cached_52w]
        print(f"  minervini_scans'dan alinan : {len(cached_52w)}")
        print(f"  Yeni veri cekilecek        : {len(need_new_52w)}")

        # Yeni tickerlar icin MA200 slope (yfinance batch)
        fresh_slopes_52w = check_ma200_slope(need_new_52w) if need_new_52w else {}

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
                eps_qoq   = cached_52w[ticker]["eps_qoq"]
                sales_qoq = cached_52w[ticker]["sales_qoq"]
                grade     = cached_52w[ticker]["grade"]
            else:
                slope     = fresh_slopes_52w.get(ticker)
                eps_data  = fresh_eps_52w.get(ticker, {})
                eps_qoq   = eps_data.get("eps_qoq")
                sales_qoq = eps_data.get("sales_qoq")
                grade     = eps_data.get("grade", "D")

            try:
                c.execute("""
                    INSERT OR REPLACE INTO minervini_52w_high
                    (scan_date, ticker, company, sector, industry,
                     price, change_pct, volume, market_cap,
                     ma200_slope, eps_qoq, sales_qoq, grade)
                    VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?)
                """, (
                    scan_date,
                    ticker,
                    row.get("Company", ""),
                    row.get("Sector", ""),
                    row.get("Industry", ""),
                    row.get("Price", 0),
                    row.get("Change", ""),
                    row.get("Volume", 0),
                    row.get("Market Cap", 0),
                    slope,
                    eps_qoq,
                    sales_qoq,
                    grade,
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