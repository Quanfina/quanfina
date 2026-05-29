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
