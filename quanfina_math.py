"""
quanfina_math.py — Minervini Matematik Motoru
15 pure fonksiyon, Long/Short uyumlu, DB bağımlılığı yok.
"""

import math
import datetime
import pytz
from typing import Literal, Optional
from dataclasses import dataclass

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


# =============================================================================
# Konu 16 — Stop Yönetimi (Mark Minervini Bölüm 12-13)
# =============================================================================

@dataclass
class StopRecommendation:
    """Stop yönetimi önerisi.

    severity: OK / INFO / WARNING / CRITICAL
    message: İnsan-okur açıklama (UI'da rozet/tooltip)
    suggested_value: Önerilen yeni stop fiyatı (varsa)
    """
    severity: Literal["OK", "INFO", "WARNING", "CRITICAL"]
    message: str
    suggested_value: Optional[float] = None


def check_initial_stop(
    entry_price: float,
    stop_loss: float,
    invest_type: str = "LONG"
) -> StopRecommendation:
    """Initial stop %4-5 ideal, %7 uyarı, %10 mutlak sınır.

    Kaynak: Think and Trade Like a Champion - Bölüm 2-3
    """
    if entry_price <= 0 or stop_loss <= 0:
        return StopRecommendation("CRITICAL", "Geçersiz entry_price veya stop_loss")

    stop_pct = abs(entry_price - stop_loss) / entry_price * 100
    ideal_stop = (
        entry_price * 0.95 if invest_type.upper() == "LONG"
        else entry_price * 1.05
    )

    if stop_pct > 10:
        return StopRecommendation(
            severity="CRITICAL",
            message=f"Stop %{stop_pct:.1f} — Minervini MAX %10 siniri asildi!",
            suggested_value=round(ideal_stop, 4)
        )
    if stop_pct > 7:
        return StopRecommendation(
            severity="WARNING",
            message=f"Stop %{stop_pct:.1f} — Minervini ortalamasi %4-5",
            suggested_value=round(ideal_stop, 4)
        )
    if stop_pct > 5:
        return StopRecommendation(
            severity="INFO",
            message=f"Stop %{stop_pct:.1f} — kabul edilebilir, ideal %4-5"
        )
    return StopRecommendation(
        severity="OK",
        message=f"Stop %{stop_pct:.1f} — Minervini ideal araligi"
    )


def should_move_to_breakeven(
    entry_price: float,
    stop_loss: float,
    current_price: float,
    invest_type: str = "LONG"
) -> StopRecommendation:
    """Free Trade kurali: Kar 2-3x stop mesafesine ulasinca breakeven'e tasi.

    Kaynak: Think and Trade Like a Champion - Bolum 2
    """
    if entry_price <= 0 or stop_loss <= 0 or current_price <= 0:
        return StopRecommendation("CRITICAL", "Gecersiz fiyat degerleri")

    is_long = invest_type.upper() == "LONG"

    if (is_long and stop_loss >= entry_price) or (not is_long and stop_loss <= entry_price):
        return StopRecommendation("OK", "Stop zaten breakeven'da veya ustunde")

    initial_stop_pct = abs(entry_price - stop_loss) / entry_price * 100

    if is_long:
        current_gain_pct = (current_price - entry_price) / entry_price * 100
    else:
        current_gain_pct = (entry_price - current_price) / entry_price * 100

    if current_gain_pct <= 0:
        return StopRecommendation(
            "OK",
            f"Henuz kar yok (P&L %{current_gain_pct:.1f}) — Free Trade icin erken"
        )

    multiplier = current_gain_pct / initial_stop_pct

    if multiplier >= 3.0:
        return StopRecommendation(
            severity="WARNING",
            message=f"Kar %{current_gain_pct:.1f} (3x stop mesafesi) — Stop'u breakeven'a TASIYIN!",
            suggested_value=round(entry_price, 4)
        )
    if multiplier >= 2.0:
        return StopRecommendation(
            severity="INFO",
            message=f"Kar %{current_gain_pct:.1f} (2x stop mesafesi) — Breakeven tasima zamani yaklasiyor",
            suggested_value=round(entry_price, 4)
        )
    needed_pct = initial_stop_pct * 2
    return StopRecommendation(
        severity="OK",
        message=f"Kar %{current_gain_pct:.1f} — 2x mesafe icin %{needed_pct:.1f} gerekli"
    )


def should_sell_half(
    pnl_pct: float,
    user_avg_gain_pct: float = 12.0
) -> StopRecommendation:
    """Sell Half: Kar +%20'ye veya kullanici RBA ortalamasinin 2.5x katina ulasinca yari sat.

    Kaynak: Trade Like a Stock Market Wizard - Bolum 13

    user_avg_gain_pct: Kullanicinin RBA ortalama kazanc % (default %12)
    """
    threshold_static = 20.0
    threshold_dynamic = user_avg_gain_pct * 2.5
    threshold = max(threshold_static, threshold_dynamic)

    if pnl_pct >= threshold * 1.5:
        return StopRecommendation(
            severity="WARNING",
            message=f"Kar %{pnl_pct:.1f} — Sell Half BUYUK olcude gecikti! (esik: %{threshold:.0f})"
        )
    if pnl_pct >= threshold:
        return StopRecommendation(
            severity="INFO",
            message=f"Kar %{pnl_pct:.1f} — Sell Half ZAMANI (esik: %{threshold:.0f}). Kazan-kazan moduna gir."
        )
    return StopRecommendation(
        severity="OK",
        message=f"Kar %{pnl_pct:.1f} — Sell Half icin erken (esik: %{threshold:.0f})"
    )


def check_50ma_trail_stop(
    current_price: float,
    ma50: float,
    invest_type: str = "LONG",
    is_climax_run: bool = False
) -> StopRecommendation:
    """50-MA Trail Stop: LONG'da fiyat 50-MA altina kaparsa cikis.
    Climax Run varsa 20-MA'ya sikistir.

    Kaynak: Trade Like a Stock Market Wizard - Bolum 13
    """
    if current_price <= 0 or ma50 <= 0:
        return StopRecommendation("CRITICAL", "Gecersiz fiyat veya MA50")

    if is_climax_run:
        return StopRecommendation(
            severity="WARNING",
            message="Climax Run aktif → Trail stop'u 20-MA'ya SIKISTIR (50-MA cok genis)"
        )

    is_long = invest_type.upper() == "LONG"

    if is_long:
        if current_price < ma50:
            distance = (ma50 - current_price) / ma50 * 100
            return StopRecommendation(
                severity="CRITICAL",
                message=f"Fiyat 50-MA altinda (%{distance:.1f} asagi) — CIKIS sinyali!"
            )
        distance = (current_price - ma50) / ma50 * 100
        return StopRecommendation(
            severity="OK",
            message=f"Fiyat 50-MA ustunde (%{distance:.1f} yukari) — Trend saglikli"
        )

    if current_price > ma50:
        distance = (current_price - ma50) / ma50 * 100
        return StopRecommendation(
            severity="CRITICAL",
            message=f"Fiyat 50-MA ustunde (%{distance:.1f} yukari) — SHORT CIKIS sinyali!"
        )
    distance = (ma50 - current_price) / ma50 * 100
    return StopRecommendation(
        severity="OK",
        message=f"Fiyat 50-MA altinda (%{distance:.1f} asagi) — SHORT trend saglikli"
    )


def check_volatility_position_size(
    atr: float,
    current_price: float,
    proposed_position_pct: float
) -> StopRecommendation:
    """Bucking Bronco kurali: Volatil hisse → POZISYON kucultulur (stop genisletilemez).

    Kaynak: Think and Trade Like a Champion - Bolum 3

    Esikler:
    - ATR/fiyat > %8 → Bucking Bronco (max %5 portfoy)
    - ATR/fiyat > %5 → Yuksek volatilite (max %10 portfoy)
    """
    if current_price <= 0 or atr <= 0:
        return StopRecommendation("CRITICAL", "Gecersiz ATR veya fiyat")

    atr_pct = atr / current_price * 100

    if atr_pct > 8.0:
        return StopRecommendation(
            severity="CRITICAL",
            message=f"ATR %{atr_pct:.1f} — Bucking Bronco! MAX %5 pozisyon (onerilen: %{proposed_position_pct:.0f})",
            suggested_value=5.0
        )
    if atr_pct > 5.0 and proposed_position_pct > 10.0:
        return StopRecommendation(
            severity="WARNING",
            message=f"ATR %{atr_pct:.1f} — Yuksek volatilite. Pozisyon %10 sinirla",
            suggested_value=10.0
        )
    return StopRecommendation(
        severity="OK",
        message=f"ATR %{atr_pct:.1f} — Normal volatilite"
    )


# =============================================================================
# Konu 11 — Distribution Days (Mark Minervini Bölüm 5)
# =============================================================================

def count_distribution_days(
    price_history: list[tuple[str, float, float]],
    lookback_days: int = 20
) -> StopRecommendation:
    """Distribution Day = price down + volume up (vs prev day).

    Esik: 4+ in 4 hafta (~20 trading days) → Under Pressure

    Kaynak: Trade Like a Stock Market Wizard - Bolum 5

    Args:
        price_history: [(date_str, close, volume), ...] kronolojik sirali
        lookback_days: Bakilacak gun sayisi (default 20 = ~4 hafta)

    Returns:
        StopRecommendation with suggested_value = distribution day sayisi
    """
    if len(price_history) < 2:
        return StopRecommendation(
            "OK",
            "Yetersiz veri (en az 2 gun gerekli)",
            suggested_value=0.0
        )

    recent = price_history[-lookback_days:] if len(price_history) > lookback_days else price_history

    if len(recent) < 2:
        return StopRecommendation("OK", "Yetersiz veri", suggested_value=0.0)

    count = 0
    for i in range(1, len(recent)):
        prev_close, prev_volume = recent[i - 1][1], recent[i - 1][2]
        curr_close, curr_volume = recent[i][1], recent[i][2]

        if curr_close < prev_close and curr_volume > prev_volume:
            count += 1

    actual_lookback = len(recent)
    weeks = actual_lookback / 5.0

    if count >= 6:
        severity = "CRITICAL"
        msg = f"{count} Distribution Day in {weeks:.1f} hafta — AGIR baski!"
    elif count >= 4:
        severity = "WARNING"
        msg = f"{count} Distribution Day in {weeks:.1f} hafta — UNDER PRESSURE"
    elif count >= 2:
        severity = "INFO"
        msg = f"{count} Distribution Day in {weeks:.1f} hafta — yakin takip"
    else:
        severity = "OK"
        msg = f"{count} Distribution Day in {weeks:.1f} hafta — saglikli"

    return StopRecommendation(
        severity=severity,
        message=msg,
        suggested_value=float(count)
    )
