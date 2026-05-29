"""
scanner_helpers.py — scanner.py SAF (pure) yardimci fonksiyonlari (Paket 373).

scanner.py (1955 satir gunluk tarama motoru) network/DB-coupled; bu modul ondan
cikarilan SAF (network/DB bagimsiz, deterministik) fonksiyonlari tutar. test-first
guvenlik agi (tests/test_scanner_pure.py 24 test) ile cikarma guvenli yapildi
(Kural #24 saglam gidelim). scanner.py bunlari import eder; cagrilar degismez.

Increment 1 (P373): parse_earnings_date + _parse_pct + _compute_grade (3 basit,
self-contained). RS rank + signal detect ikinci increment'te (pandas-bagimli).
"""
from datetime import date, datetime, timedelta


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


# Increment 2 (P374): pandas-bagimli saf fonksiyonlar. ohlcv_df / closes Series
# argumandan gelir (pd.X cagrisi yok -> pandas import gerekmez, duck-typed).
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
