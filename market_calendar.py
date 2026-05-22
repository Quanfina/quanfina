"""
ABD Borsa Takvim Utility — Sprint 4-bis.7 (22 May 2026)

Sn. Ferit talimat: "veri çekme saati ABD borsa saatleri ABD tatiller veri çekme
sistemini geliştirelim Türkiye'de yaşadığımı unutma."

Mimari:
  - NYSE/NASDAQ takvimi (her ikisi aynı gün açık/kapalı — federal tatil eşlikli)
  - Hafta sonu (Cumartesi, Pazar) kapalı
  - 10 federal/borsa tatil günü (2026-2027 hardcoded)
  - Yarı gün (1/2 day): Thanksgiving sonrası Cuma + Christmas Eve → 13:00 ET kapanış
  - Yaz/kış saati: zoneinfo otomatik handles EDT/EST geçişleri

Türkiye saat farkı:
  - Yaz dönemi (EDT, Mart-Kasım): TR = ET + 7 saat
  - Kış dönemi (EST, Kasım-Mart): TR = ET + 8 saat
  - Borsa açılış (9:30 ET) TR'de: yazın 16:30, kışın 17:30
  - Borsa kapanış (16:00 ET) TR'de: yazın 23:00, kışın 00:00 (ertesi gün)

Bağımlılık: stdlib zoneinfo (Python 3.9+). Ek paket gerekmez.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date, datetime, time, timedelta
from typing import Optional
from zoneinfo import ZoneInfo

ET_TZ = ZoneInfo("America/New_York")
TR_TZ = ZoneInfo("Europe/Istanbul")
UTC_TZ = ZoneInfo("UTC")

# Borsa saatleri (ET — yaz/kış otomatik)
MARKET_OPEN_HOUR = 9
MARKET_OPEN_MINUTE = 30
MARKET_CLOSE_HOUR = 16
MARKET_CLOSE_MINUTE = 0
PRE_MARKET_OPEN_HOUR = 4  # 4:00 ET pre-market başlar
POST_MARKET_CLOSE_HOUR = 20  # 20:00 ET after-hours sonu
EARLY_CLOSE_HOUR = 13  # Yarı gün kapanış (Black Friday, Christmas Eve)


# ABD Borsa Tatilleri — manuel hardcoded (NYSE/NASDAQ ortak)
# Cumartesi'ye denk gelen tatiller → Cuma observed
# Pazar'a denk gelen tatiller → Pazartesi observed
US_MARKET_HOLIDAYS: dict[date, str] = {
    # 2026
    date(2026, 1, 1):  "New Year's Day",
    date(2026, 1, 19): "Martin Luther King Jr. Day",
    date(2026, 2, 16): "Presidents' Day",
    date(2026, 4, 3):  "Good Friday",
    date(2026, 5, 25): "Memorial Day",
    date(2026, 6, 19): "Juneteenth",
    date(2026, 7, 3):  "Independence Day (4 Tem Cumartesi, observed Cuma)",
    date(2026, 9, 7):  "Labor Day",
    date(2026, 11, 26): "Thanksgiving Day",
    date(2026, 12, 25): "Christmas Day",
    # 2027
    date(2027, 1, 1):  "New Year's Day",
    date(2027, 1, 18): "Martin Luther King Jr. Day",
    date(2027, 2, 15): "Presidents' Day",
    date(2027, 3, 26): "Good Friday",
    date(2027, 5, 31): "Memorial Day",
    date(2027, 6, 18): "Juneteenth (19 Haziran Cumartesi, observed Cuma)",
    date(2027, 7, 5):  "Independence Day (4 Tem Pazar, observed Pazartesi)",
    date(2027, 9, 6):  "Labor Day",
    date(2027, 11, 25): "Thanksgiving Day",
    date(2027, 12, 24): "Christmas Day (25 Aralık Cumartesi, observed Cuma)",
}

# Yarı gün (Early Close 13:00 ET) — Thanksgiving sonrası Cuma + Christmas Eve
US_MARKET_EARLY_CLOSE: dict[date, str] = {
    # 2026
    date(2026, 11, 27): "Black Friday (yarı gün, 13:00 ET kapanış)",
    date(2026, 12, 24): "Christmas Eve (yarı gün, 13:00 ET kapanış)",
    # 2027
    date(2027, 11, 26): "Black Friday (yarı gün, 13:00 ET kapanış)",
    date(2027, 12, 23): "Christmas Eve (yarı gün, 13:00 ET kapanış — 24 Aralık Cuma değil)",
}


@dataclass(frozen=True)
class MarketStatus:
    """Anlık piyasa durumu — TR + ET saat dilimleri."""
    is_open: bool                # Ana seans (9:30-16:00 ET) açık mı?
    session: str                 # "regular" | "pre_market" | "post_market" | "closed"
    reason: Optional[str]        # Kapalıysa sebep ("Hafta sonu", "Tatil: ...")
    is_early_close: bool         # Yarı gün mü (13:00 ET kapanış)?
    now_et: datetime
    now_tr: datetime
    next_open_et: datetime       # Sonraki açılış (ET)
    next_open_tr: datetime       # Sonraki açılış (TR)
    last_trading_day: date


# =============================================================
# Temel Sorgu Fonksiyonları
# =============================================================

def is_weekend(d: date) -> bool:
    """Cumartesi (5) veya Pazar (6) mı?"""
    return d.weekday() >= 5


def is_us_holiday(d: date) -> bool:
    """Bu tarih ABD borsa tatili mi?"""
    return d in US_MARKET_HOLIDAYS


def get_holiday_name(d: date) -> Optional[str]:
    """Tatil ise ismini döndür, yoksa None."""
    return US_MARKET_HOLIDAYS.get(d)


def is_early_close_day(d: date) -> bool:
    """Yarı gün mü (13:00 ET kapanış)?"""
    return d in US_MARKET_EARLY_CLOSE


def is_us_market_day(d: date) -> bool:
    """
    Bu tarihte ABD borsası işlem yapıyor mu?
    (Hafta sonu DEĞİL + tatil DEĞİL)
    Yarı günler de işlem günü sayılır (sadece kapanış erken).
    """
    return not is_weekend(d) and not is_us_holiday(d)


def last_trading_day_before(d: date) -> date:
    """
    Verilen tarihten önceki (veya günlük) en son işlem günü.
    Eğer verilen tarih kendisi işlem günüyse, onu döndürür.
    """
    cursor = d
    # Tatil + hafta sonu max ardışık 4-5 gün — 14 gün taban yeter
    for _ in range(14):
        if is_us_market_day(cursor):
            return cursor
        cursor -= timedelta(days=1)
    return cursor  # Defensive fallback


def next_trading_day_after(d: date) -> date:
    """Verilen tarihten sonraki en yakın işlem günü (verilen tarih hariç)."""
    cursor = d + timedelta(days=1)
    for _ in range(14):
        if is_us_market_day(cursor):
            return cursor
        cursor += timedelta(days=1)
    return cursor


# =============================================================
# Saat Dilimi Dönüşümleri
# =============================================================

def now_et() -> datetime:
    """Şu an ABD doğu saati (ET, yaz/kış otomatik)."""
    return datetime.now(UTC_TZ).astimezone(ET_TZ)


def now_tr() -> datetime:
    """Şu an Türkiye saati (UTC+3 sabit)."""
    return datetime.now(UTC_TZ).astimezone(TR_TZ)


def to_tr_time(dt: datetime) -> datetime:
    """Verilen datetime'ı Türkiye saatine çevir."""
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=ET_TZ)
    return dt.astimezone(TR_TZ)


def to_et_time(dt: datetime) -> datetime:
    """Verilen datetime'ı ET saatine çevir."""
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=TR_TZ)
    return dt.astimezone(ET_TZ)


# =============================================================
# Piyasa Durumu (Hauptfunktion)
# =============================================================

def is_us_market_open(dt: Optional[datetime] = None) -> bool:
    """
    Verilen ET datetime'da ana seans (9:30-16:00 ET) açık mı?
    Yarı günde 13:00 ET sonrası False döner.
    dt=None ise şu an.
    """
    if dt is None:
        dt = now_et()
    elif dt.tzinfo is None:
        dt = dt.replace(tzinfo=ET_TZ)
    else:
        dt = dt.astimezone(ET_TZ)

    today = dt.date()
    if not is_us_market_day(today):
        return False

    open_time = dt.replace(
        hour=MARKET_OPEN_HOUR, minute=MARKET_OPEN_MINUTE,
        second=0, microsecond=0,
    )
    if is_early_close_day(today):
        close_time = dt.replace(hour=EARLY_CLOSE_HOUR, minute=0, second=0, microsecond=0)
    else:
        close_time = dt.replace(
            hour=MARKET_CLOSE_HOUR, minute=MARKET_CLOSE_MINUTE,
            second=0, microsecond=0,
        )

    return open_time <= dt < close_time


def market_status_now() -> MarketStatus:
    """
    Şu anki piyasa durumu — TR + ET saatleri, sebep, sonraki açılış.

    Returns:
        MarketStatus dataclass
    """
    et = now_et()
    tr = now_tr()
    today_et = et.date()

    # Sebep tespiti
    if is_weekend(today_et):
        reason = "Hafta sonu"
        session = "closed"
        is_open_flag = False
    elif is_us_holiday(today_et):
        reason = f"Tatil: {get_holiday_name(today_et)}"
        session = "closed"
        is_open_flag = False
    else:
        # Hafta içi, açık gün
        pre_open = et.replace(hour=PRE_MARKET_OPEN_HOUR, minute=0, second=0, microsecond=0)
        market_open = et.replace(
            hour=MARKET_OPEN_HOUR, minute=MARKET_OPEN_MINUTE,
            second=0, microsecond=0,
        )
        if is_early_close_day(today_et):
            market_close = et.replace(hour=EARLY_CLOSE_HOUR, minute=0, second=0, microsecond=0)
        else:
            market_close = et.replace(
                hour=MARKET_CLOSE_HOUR, minute=MARKET_CLOSE_MINUTE,
                second=0, microsecond=0,
            )
        post_close = et.replace(hour=POST_MARKET_CLOSE_HOUR, minute=0, second=0, microsecond=0)

        if pre_open <= et < market_open:
            session = "pre_market"
            is_open_flag = False  # Ana seans henüz açık değil
            reason = "Pre-market (henüz ana seans açılmadı)"
        elif market_open <= et < market_close:
            session = "regular"
            is_open_flag = True
            reason = None
        elif market_close <= et < post_close:
            session = "post_market"
            is_open_flag = False  # Ana seans kapandı
            reason = "After-hours (ana seans kapandı)"
        else:
            session = "closed"
            is_open_flag = False
            reason = "Gece (pre-market 4:00 ET'de açılacak)"

    # Sonraki açılış
    if is_open_flag:
        # Şu an açıksa, sonraki açılış = ertesi işlem günü
        next_day = next_trading_day_after(today_et)
    elif session == "pre_market":
        # Pre-market'tayız, ana açılış bugün 9:30 ET
        next_day = today_et
    else:
        # Kapalı (hafta sonu, tatil, gece, after-hours) → sonraki işlem günü
        if is_us_market_day(today_et) and et.time() < time(MARKET_OPEN_HOUR, MARKET_OPEN_MINUTE):
            # Bugün işlem günü ama 9:30 öncesi (gece veya pre-market) → bugün
            next_day = today_et
        else:
            next_day = next_trading_day_after(today_et)

    next_open_et_dt = datetime.combine(
        next_day,
        time(MARKET_OPEN_HOUR, MARKET_OPEN_MINUTE),
        tzinfo=ET_TZ,
    )
    next_open_tr_dt = next_open_et_dt.astimezone(TR_TZ)

    return MarketStatus(
        is_open=is_open_flag,
        session=session,
        reason=reason,
        is_early_close=is_early_close_day(today_et),
        now_et=et,
        now_tr=tr,
        next_open_et=next_open_et_dt,
        next_open_tr=next_open_tr_dt,
        last_trading_day=last_trading_day_before(today_et),
    )


# =============================================================
# Veri Çekme Karar Fonksiyonu (scanner.py için)
# =============================================================

def should_scan_today(today_et: Optional[date] = None) -> tuple[bool, str]:
    """
    Bugün scan çalışmalı mı? (scanner.py kısa devre için)

    Returns:
        (should_run, reason)
        - (True, "Hafta içi işlem günü") → scan tetiklenir
        - (False, "Hafta sonu") veya (False, "Tatil: Memorial Day") → scan atlanır

    Pratik kullanım scanner.py'da:
        from market_calendar import should_scan_today
        ok, reason = should_scan_today()
        if not ok:
            print(f"[SKIP] Bugün scan atlandı: {reason}")
            return
    """
    if today_et is None:
        today_et = now_et().date()

    if is_weekend(today_et):
        return False, "Hafta sonu (ABD borsa kapalı)"

    if is_us_holiday(today_et):
        return False, f"Tatil: {get_holiday_name(today_et)}"

    return True, "İşlem günü (ABD borsa açık)"


# =============================================================
# CLI Test (manuel çalıştırma için)
# =============================================================

if __name__ == "__main__":
    import sys

    if sys.stdout and hasattr(sys.stdout, "reconfigure"):
        try:
            sys.stdout.reconfigure(encoding="utf-8")
        except Exception:
            pass

    print("=== ABD Borsa Takvim — Anlık Durum ===\n")

    status = market_status_now()
    print(f"ET şu an     : {status.now_et.strftime('%Y-%m-%d %H:%M:%S %Z')}")
    print(f"TR şu an     : {status.now_tr.strftime('%Y-%m-%d %H:%M:%S %Z')}")
    print(f"Açık mı?     : {'EVET 🟢' if status.is_open else 'HAYIR 🔴'}")
    print(f"Session      : {status.session}")
    print(f"Sebep        : {status.reason or '—'}")
    print(f"Yarı gün?    : {'EVET' if status.is_early_close else 'HAYIR'}")
    print(f"Son işlem    : {status.last_trading_day.isoformat()}")
    print(f"Sonraki açılış (ET): {status.next_open_et.strftime('%Y-%m-%d %H:%M %Z')}")
    print(f"Sonraki açılış (TR): {status.next_open_tr.strftime('%Y-%m-%d %H:%M %Z')}")

    print("\n=== Scanner Karar Fonksiyonu ===")
    ok, reason = should_scan_today()
    print(f"Scan tetiklensin mi? {'EVET' if ok else 'HAYIR'} — {reason}")
