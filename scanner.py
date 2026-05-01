import os
import sqlite3
import requests
import pandas as pd
import yfinance as yf
from io import StringIO
from dotenv import load_dotenv
from datetime import date

load_dotenv()
FINVIZ_KEY = os.getenv("FINVIZ_API_KEY")
DB_PATH = os.path.join(os.path.dirname(__file__), "quanfina.db")

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
            UNIQUE(scan_date, ticker)
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
    ])
    
    url = (
        f"https://elite.finviz.com/export.ashx?"
        f"v=152&f={filters}&auth={FINVIZ_KEY}&ft=4"
    )
    
    r = requests.get(url, headers={"User-Agent": "Mozilla/5.0"}, timeout=15)
    df = pd.read_csv(StringIO(r.text))
    print(f"Finviz filtresi geçti: {len(df)} hisse")
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
                INSERT OR REPLACE INTO minervini_scans
                (scan_date, ticker, company, sector, industry,
                 price, change_pct, volume, market_cap, pe,
                 ma200_slope, passed)
                VALUES (?,?,?,?,?,?,?,?,?,?,?,?)
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

# --- ANA AKIŞ ---
def run_scan():
    print("=== QUANFINA SCANNER v2 (Hızlı) ===")
    init_db()
    
    scan_date = str(date.today())
    print(f"Tarih: {scan_date}")

    # 1. Finviz filtresi
    print("\n1. Finviz Elite filtresi çalışıyor...")
    df = get_finviz_screener()
    
    if df.empty:
        print("Hiç hisse bulunamadı.")
        return
    
    tickers = df["Ticker"].tolist()

    # 2. Sadece geçenler için MA200 slope
    print("\n2. MA200 slope kontrolü (yfinance toplu indirme)...")
    slopes = check_ma200_slope(tickers)

    # 3. Kaydet
    print("\n3. Veritabanına kaydediliyor...")
    saved = save_results(df, slopes, scan_date)

    passed = sum(1 for s in slopes.values() if s is not None and s > 0)
    
    print(f"\n✅ TARAMA TAMAMLANDI!")
    print(f"   Finviz filtresi geçen : {len(tickers)}")
    print(f"   MA200 slope geçen     : {passed}")
    print(f"   Toplam kayıt          : {saved}")
    print(f"   Tarih                 : {scan_date}")

    # --- FUNDAMENTAL TARAMA ---
    print("\n=== FUNDAMENTAL TARAMA BAŞLIYOR ===")
    init_fundamental_table()
    df_fund = get_finviz_fundamental()

    if not df_fund.empty:
        tickers_fund = df_fund["Ticker"].tolist()
        print(f"MA200 slope kontrolü: {len(tickers_fund)} hisse...")
        slopes_fund = check_ma200_slope(tickers_fund)
        saved_fund = save_fundamental_results(df_fund, slopes_fund, scan_date)
        passed_fund = sum(1 for s in slopes_fund.values() if s is not None and s > 0)
        print(f"\n✅ FUNDAMENTAL TARAMA TAMAMLANDI!")
        print(f"   Finviz filtresi geçen : {len(tickers_fund)}")
        print(f"   MA200 slope geçen     : {passed_fund}")
        print(f"   Toplam kayıt          : {saved_fund}")

    # --- SADECE TEMEL TARAMA ---
    print("\n=== SADECE TEMEL TARAMA BAŞLIYOR ===")
    init_fundamental_only_table()
    df_fund_only = get_finviz_fundamental_only()

    if not df_fund_only.empty:
        saved_fund_only = save_fundamental_only(df_fund_only, scan_date)
        print(f"\n✅ TEMEL TARAMA TAMAMLANDI!")
        print(f"   Temel filtresi geçen : {len(df_fund_only)}")
        print(f"   Toplam kayıt         : {saved_fund_only}")

# --- TEMEL KRİTERLER TABLOSU ---
def init_fundamental_table():
    conn = sqlite3.connect(DB_PATH)
    c = conn.cursor()
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
    conn.commit()
    conn.close()

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
    ])

    url = (
        f"https://elite.finviz.com/export.ashx?"
        f"v=152&f={filters}&auth={FINVIZ_KEY}&ft=4"
    )

    r = requests.get(url, headers={"User-Agent": "Mozilla/5.0"}, timeout=15)
    df = pd.read_csv(StringIO(r.text))
    print(f"Fundamental filtresi geçti: {len(df)} hisse")
    return df

def save_fundamental_results(df_finviz, slopes, scan_date):
    conn = sqlite3.connect(DB_PATH)
    c = conn.cursor()
    saved = 0

    for _, row in df_finviz.iterrows():
        ticker = row["Ticker"]
        slope  = slopes.get(ticker, None)
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
            saved += 1
        except Exception as e:
            print(f"  Kayıt hatası {ticker}: {e}")

    conn.commit()
    conn.close()
    return saved

def init_fundamental_only_table():
    conn = sqlite3.connect(DB_PATH)
    c = conn.cursor()
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
    conn.commit()
    conn.close()

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
    ])

    url = (
        f"https://elite.finviz.com/export.ashx?"
        f"v=152&f={filters}&auth={FINVIZ_KEY}&ft=4"
    )

    r = requests.get(url, headers={"User-Agent": "Mozilla/5.0"}, timeout=15)
    df = pd.read_csv(StringIO(r.text))
    print(f"Temel filtresi geçti: {len(df)} hisse")
    return df

def save_fundamental_only(df_finviz, scan_date):
    conn = sqlite3.connect(DB_PATH)
    c = conn.cursor()
    saved = 0

    for _, row in df_finviz.iterrows():
        try:
            c.execute("""
                INSERT OR REPLACE INTO minervini_fundamental_only
                (scan_date, ticker, company, sector, industry,
                 price, change_pct, volume, market_cap, pe)
                VALUES (?,?,?,?,?,?,?,?,?,?)
            """, (
                scan_date,
                row["Ticker"],
                row.get("Company", ""),
                row.get("Sector", ""),
                row.get("Industry", ""),
                row.get("Price", 0),
                row.get("Change", ""),
                row.get("Volume", 0),
                row.get("Market Cap", 0),
                row.get("P/E", 0),
            ))
            saved += 1
        except Exception as e:
            print(f"  Kayıt hatası {row['Ticker']}: {e}")

    conn.commit()
    conn.close()
    return saved

if __name__ == "__main__":
    run_scan()