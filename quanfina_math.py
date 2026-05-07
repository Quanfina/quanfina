"""
quanfina_math.py — Minervini Matematik Motoru
15 pure fonksiyon, Long/Short uyumlu, DB bağımlılığı yok.
"""

import math
import datetime
import pytz

LONG = 1
SHORT = 2


# ---------------------------------------------------------------------------
# F1 — Günlük Değişim ($)
# ---------------------------------------------------------------------------

def change_dollars(current_price: float, previous_close: float) -> float:
    """Anlık fiyat ile önceki kapanış arasındaki fark ($)."""
    return current_price - previous_close


# ---------------------------------------------------------------------------
# F2 — Günlük Değişim (%)
# ---------------------------------------------------------------------------

def change_percentage(current_price: float, previous_close: float) -> float:
    """Günlük yüzde değişim. Önceki kapanış sıfırsa 0 döner."""
    if previous_close == 0:
        return 0.0
    return 100.0 * (current_price - previous_close) / previous_close


# ---------------------------------------------------------------------------
# F3 — Yüzde K/Z — MASTER FUNCTION (Long/Short uyumlu)
# ---------------------------------------------------------------------------

def percent_change(entry: float, current: float, invest_type: int) -> float:
    """
    Giriş fiyatına göre yüzde kâr/zarar.
    invest_type: 1=LONG, 2=SHORT
    """
    if entry == 0:
        return 0.0
    if invest_type == LONG:
        return (current - entry) / entry * 100.0
    else:  # SHORT
        return (entry - current) / entry * 100.0


# ---------------------------------------------------------------------------
# F4 — Dolar K/Z — MASTER FUNCTION (Long/Short uyumlu)
# ---------------------------------------------------------------------------

def dollar_change(
    entry: float, current: float, shares: int, invest_type: int
) -> float:
    """
    Toplam dolar kâr/zarar.
    invest_type: 1=LONG, 2=SHORT
    """
    diff = current - entry
    if invest_type == SHORT:
        diff = entry - current
    return diff * shares


# ---------------------------------------------------------------------------
# F5 — R-Multiple  (Mark Minervini "Risk-First" felsefesi)
# ---------------------------------------------------------------------------

def r_multiple(
    entry: float,
    stop: float,
    current: float,
    invest_type: int,
    shares: int = 1,
) -> float:
    """
    Risk birimi başına kâr/zarar çarpanı.
    Stop yön ihlalinde R = 0 (negatif R döndürülmez).
    """
    # Stop yön ihlali kontrolü
    if invest_type == LONG and entry <= stop:
        return 0.0
    if invest_type == SHORT and entry >= stop:
        return 0.0

    risk_distance = abs(entry - stop)
    if risk_distance == 0:
        return 0.0

    pnl = dollar_change(entry, current, shares, invest_type)
    # Negatif R döndürülmez — sıfırda kesilir
    return max(pnl / risk_distance, 0.0)


# ---------------------------------------------------------------------------
# F6 — Risk Dolarları
# ---------------------------------------------------------------------------

def risk_dollars(
    entry: float, stop: float, shares: int, invest_type: int
) -> float:
    """Pozisyonun toplam dolar riski (stop'a kadar olan kayıp)."""
    sign = 1 if invest_type == LONG else -1
    return max((entry - stop) * sign * shares, 0.0)


# ---------------------------------------------------------------------------
# F7 — Stop Loss Yüzdesi
# ---------------------------------------------------------------------------

def stop_loss_percentage(
    entry: float, stop: float, invest_type: int
) -> float:
    """Stop seviyesinin giriş fiyatına göre yüzde uzaklığı."""
    return percent_change(entry, stop, invest_type)


# ---------------------------------------------------------------------------
# F8 — Stop Loss Break-Even Shares  (Mark "Sell Half on Profit")
# ---------------------------------------------------------------------------

def stop_loss_break_even_shares(
    entry: float,
    stop: float,
    current: float,
    shares: int,
    invest_type: int,
) -> int:
    """
    Mevcut fiyatta kaç hisse satılırsa toplam zarar sıfırlanır?
    Kâr yoksa tüm hisseleri döndürür.
    """
    risk_per_share = abs(entry - stop)
    if invest_type == LONG:
        profit_per_share = current - entry
    else:
        profit_per_share = entry - current

    if profit_per_share <= 0:
        return shares  # Kâr yok, hepsini sat

    total_risk = risk_per_share * shares
    return math.ceil(total_risk / profit_per_share)


# ---------------------------------------------------------------------------
# F9 — 52 Haftalık Yüksek Mesafesi (%)
# ---------------------------------------------------------------------------

def off_52w_high_pct(high_52w: float, current_price: float) -> float:
    """
    Anlık fiyatın 52 haftalık zirveye mesafesi (%).
    Negatif = zirvinin altında, pozitif = zirveyi geçti.
    """
    if high_52w == 0:
        return 0.0
    return (current_price - high_52w) / high_52w * 100.0


# ---------------------------------------------------------------------------
# F10 — Pozisyon Değeri
# ---------------------------------------------------------------------------

def position_value(current_price: float, shares: int) -> float:
    """Mevcut fiyat × hisse adedi."""
    return current_price * shares


# ---------------------------------------------------------------------------
# F11 — V50 (50-Gün Ortalama Hacim Karşılaştırması)
# ---------------------------------------------------------------------------

def v50_pct(current_volume: int, avg_vol_50: int) -> float:
    """Anlık hacmin 50-gün ortalamasına göre yüzde sapması."""
    if avg_vol_50 == 0:
        return 0.0
    return percent_change(float(avg_vol_50), float(current_volume), LONG)


# ---------------------------------------------------------------------------
# F12 — VRR (Volume Run Rate — Zaman Ayarlı)
# ---------------------------------------------------------------------------

def vrr_volume_run_rate(
    volume: int, avg_vol_50d: int, is_market_open: bool
) -> float:
    """
    NY market saatlerine göre zaman ayarlı hacim run rate.
    Market kapalıysa basit V50 oranı döner.
    """
    ny = pytz.timezone("America/New_York")
    now = datetime.datetime.now(ny)
    market_open = now.replace(hour=9, minute=30, second=0, microsecond=0)
    market_close = now.replace(hour=16, minute=0, second=0, microsecond=0)

    if not is_market_open or now < market_open or now > market_close:
        # Market kapalı — basit oran
        return percent_change(float(avg_vol_50d), float(volume), LONG)

    total_seconds = (market_close - market_open).total_seconds()
    elapsed_seconds = (now - market_open).total_seconds()

    if total_seconds == 0 or elapsed_seconds <= 0:
        return 0.0

    elapsed_ratio = elapsed_seconds / total_seconds
    expected_volume = avg_vol_50d * elapsed_ratio

    if expected_volume == 0:
        return 0.0

    return percent_change(expected_volume, float(volume), LONG)


# ---------------------------------------------------------------------------
# F13 — Spread ($)
# ---------------------------------------------------------------------------

def spread_dollars(ask: float, bid: float) -> float:
    """Bid-ask farkı dolar cinsinden."""
    return ask - bid


# ---------------------------------------------------------------------------
# F14 — Spread (%)
# ---------------------------------------------------------------------------

def spread_percentage(ask: float, bid: float) -> float:
    """Bid-ask farkının mid-price'a oranı (%). Mid sıfırsa 0 döner."""
    mid = (ask + bid) / 2.0
    if mid == 0:
        return 0.0
    return (ask - bid) / mid * 100.0


# ---------------------------------------------------------------------------
# F15 — SMA20 Mesafesi (%)
# ---------------------------------------------------------------------------

def sma20_distance_pct(current_price: float, sma_20: float) -> float:
    """
    Anlık fiyatın 20-gün hareketli ortalamasına mesafesi (%).
    Mark: +/-20DMA pozisyon sağlığı göstergesi.
    """
    if sma_20 == 0:
        return 0.0
    return (current_price - sma_20) / sma_20 * 100.0
