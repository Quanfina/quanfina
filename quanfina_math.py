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


@dataclass
class RBAMetrics:
    """
    Mark Minervini RBA — Result Based Analysis metrikleri.

    Kaynak: Notebook B3 Modül 7 + NotebookLM Konu 14 + Bundle adjustedRatio/battingAvg
    Min trade gözlem: 30 (kitap kuralı — istatistiksel anlamlılık)
    """
    num_trades: int
    win_rate: float           # 0.0-1.0
    avg_gain_pct: float
    avg_loss_pct: float       # Negatif (kayıp)
    largest_gain_pct: float
    largest_loss_pct: float
    adjusted_ratio: float     # (Win% × AvgGain) / (Loss% × |AvgLoss|)
    expectancy_pct: float     # (Win% × AvgGain) - (Loss% × |AvgLoss|)
    is_statistically_significant: bool  # >=30 trade


# =============================================================================
# TradeGrader — 17 kategori (EK 10 sentezi)
# Bundle gradeThresholds birebir — NotebookLM + Gemini + Bundle 3 kaynak doğrulamalı
# =============================================================================
# target_pct yorumu:
#   - Pozitif sayı = TABAN (>=X% olmalı, yüksek hedeflenir)
#   - Negatif sayı = TAVAN (<=|X|% olmalı, düşük hedeflenir)
#   - None         = INFO (eşik yok, sadece kayıt)
GRADE_CATEGORIES: dict[str, tuple[str, str, Optional[float]]] = {
    # ENTRY (alım) — 7 kategori
    "BP":  ("Bought perfect",          "ENTRY",  80.0),
    "BE":  ("Bought early",            "ENTRY", -10.0),
    "BL":  ("Bought late",             "ENTRY", -10.0),
    "FS":  ("Faulty setup",            "ENTRY", -10.0),
    "BB":  ("Bad buy",                 "ENTRY", -10.0),
    "EB":  ("Emotional buy",           "ENTRY",  -2.0),
    "CE":  ("Chased extended",         "ENTRY",  -2.0),
    # EXIT (kâr realize) — 5 kategori
    "SP":  ("Sold perfect",            "EXIT",   30.0),
    "SE":  ("Sold early",              "EXIT",  -50.0),
    "SL":  ("Sold late",               "EXIT",  -20.0),
    "RFR": ("Reduced to finance risk", "EXIT",   None),
    "BS":  ("Bad sale",                "EXIT",  -10.0),
    # LOSS (zarar kes) — 5 kategori
    "CLP": ("Cut loss perfect",        "LOSS",   95.0),
    "CLE": ("Cut loss early",          "LOSS",  -30.0),
    "CLL": ("Cut loss late",           "LOSS",   -5.0),
    "COT": ("Choked off trade",        "LOSS",  -30.0),
    "ES":  ("Emotional sale",          "LOSS",   -5.0),
}


@dataclass
class GradeSuggestion:
    """
    TradeGrader önerisi — kategori + güven seviyesi + neden.

    Kullanıcı bu öneriyi kabul/red/değiştir edebilir (manuel karar son).
    Boş code = "öneri yok" (kullanıcı manuel atar).

    Kaynak: notebook/EK10_TradeGrader_Sentezi.md
    """
    code: str
    name: str
    confidence: Literal["HIGH", "MEDIUM", "LOW"]
    reason: str


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
            message=f"Stop %{stop_pct:.1f} — %10 mutlak kritik sinir asildi (Mark canon %4-5 ideal / %7 uyari)!",
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
# Konu 11 — Distribution Days
# =============================================================================
# NOT (Paket 360): Eski count_distribution_days (StopRecommendation donen, ~57
# satir) KALDIRILDI. Python son tanimi kullandigi icin GOLGELENMIS OLU koddu —
# asagidaki canonical surum (Mark KARAR #488, O'Neil mekanik, (closes, volumes,
# lookback) -> dict imzasi) zaten production + tum testler tarafindan cagriliyordu.
# api/main.py:1148 + scanner.py o surumu kullaniyor; eskiye yalniz _archive referans
# verdi. Tek tanim birakildi (DRY Ilke #4 + Kural #18 negatif tescil). Sn. Ferit
# "sen karar ver" onayi (28 May).


# =============================================================================
# Sprint 4-bis.4 — Brandon VCP Olgunluk (Minervini_Video.md sat. 2823-2867, 10:20)
# KARAR #461 (19 May 2026) — Pre-Compute stratejisi (tek motor felsefesi)
# Kaynak: Brandon healthy pullback ratio + is_begrudgingly_pulling_back
# =============================================================================


# Eşikler — Sn. Ferit/Master kalibrasyon noktası (mevcut: Brandon video 10:20 kanonu)
VCP_LOOKBACK_DAYS = 5            # son N gün incelenir
VCP_AVG_DROP_THRESHOLD = 1.5     # % - günlük ortalama düşüş üst sınırı
VCP_VOL_DRY_RATIO = 0.70         # son gün vol < 50-gün MA × bu oran (PASS filtre)
VCP_VOL_DRY_RATIO_EXCELLENT = 0.50  # KARAR #466: Mark canon "%50 alti en siki" — A+ seviye
VCP_TIGHT_RANGE_PCT = 2.0        # % - intraday range_pct üst sınırı
VCP_MIN_HISTORY = 50             # 50-gün MA hesabı için gerekli minimum
VCP_PULLBACK_EXCELLENT = 0.10    # ratio ≤ → score 100
VCP_PULLBACK_GOOD = 0.25         # ratio ≤ → score 80
VCP_PULLBACK_ACCEPTABLE = 0.40   # ratio ≤ → score 60


def compute_pullback_health(rally_pct: float, pullback_pct: float) -> dict:
    """Brandon Healthy Pullback Ratio — Minervini_Video.md sat. 2853-2867.

    Pullback'in yükseliş'e oranı sağlıklı mı kontrol eder. Brandon'ın
    sözü: "%80 ralli yapmış hissede sadece %8'lik geri çekilme görmeyi
    tercih ederim" (video 10:20).

    Args:
        rally_pct: Son ralli yüzdesi (pozitif, örn. 80.0 = %80 ralli)
        pullback_pct: Mevcut geri çekilme yüzdesi (pozitif, örn. 8.0 = %8 düşüş)

    Returns:
        {"health": "EXCELLENT"|"GOOD"|"ACCEPTABLE"|"TOO_DEEP",
         "score": 100|80|60|20, "ratio": pullback_pct/rally_pct}

    Örnekler:
        %80 ralli → %8 düşüş    = ratio 0.10 → EXCELLENT (100)
        %80 ralli → %20 düşüş   = ratio 0.25 → GOOD (80)
        %100 ralli → %30+ düşüş = ratio 0.30+ → TOO_DEEP (20)
    """
    if rally_pct <= 0:
        return {"health": "TOO_DEEP", "score": 20, "ratio": 1.0}

    ratio = pullback_pct / rally_pct

    if ratio <= VCP_PULLBACK_EXCELLENT:
        return {"health": "EXCELLENT", "score": 100, "ratio": ratio}
    if ratio <= VCP_PULLBACK_GOOD:
        return {"health": "GOOD", "score": 80, "ratio": ratio}
    if ratio <= VCP_PULLBACK_ACCEPTABLE:
        return {"health": "ACCEPTABLE", "score": 60, "ratio": ratio}
    return {"health": "TOO_DEEP", "score": 20, "ratio": ratio}


def compute_vcp_pass(price_volume_history: Optional[list[dict]]) -> bool:
    """Brandon "Begrudgingly Pull Back" + V-Dry VCP olgunluk tespiti.
    Minervini_Video.md sat. 2823-2834 (video 10:20) kanon.

    KARAR #464 (19 May 2026, 3 kanal onayli):
      PVH OHLC formatinda zorunlu — `range_pct = (high-low)/close*100`
      gercek hesap. Eski close-only PVH (Migration 003 oncesi) yetersiz
      veri sayilir, False doner (backward compat).

    3 koşul aynı anda (AND):
      1. is_small_drops: Son 5 gün ortalama düşüş (close-to-close) < %1.5
      2. volume_drying: Son gün hacim < 50-gün ortalama × 0.70 (muhafazakar
         filtre, KARAR ADAY #466 EXCELLENT seviye 0.50 ileride)
      3. tight_closes: Son 5 gun GUN-ICI range_pct < %2
         range_pct = (high - low) / close * 100

    Master + Minervini Uzmani + Bonus FMP/ASX 3 kanal cross-check
    sonucu close proxy KESINLIKLE REDDEDILDI ("Sagilam gidelim" + Mark
    canon Trade Like a Stock Market Wizard Bolum 10 + Bonus FMP
    matematik formul (Peak-Trough)/Peak*100).

    Args:
        price_volume_history: list[dict]
          [{"date":..., "open":..., "high":..., "low":..., "close":..., "volume":...}, ...]
          veya None / kisa liste / eski close-only format -> False

    Returns:
        bool: True = VCP setup'i olgun (Brandon kriteri 3 kosul AND)

    Backward compat: Eski {date, close, volume} format -> False (yetersiz veri)
    """
    if not price_volume_history or len(price_volume_history) < VCP_MIN_HISTORY:
        return False

    try:
        recent = price_volume_history[-VCP_LOOKBACK_DAYS:]
        prev_day = price_volume_history[-(VCP_LOOKBACK_DAYS + 1)]
        last_50 = price_volume_history[-VCP_MIN_HISTORY:]

        if len(recent) < VCP_LOOKBACK_DAYS:
            return False

        # KARAR #464 — OHLC SCHEMA KONTROL (backward compat)
        # Eski PVH (close-only) -> False, yeni PVH (OHLC) -> devam
        sample = recent[0]
        if "high" not in sample or "low" not in sample:
            return False

        # 1. is_small_drops: ardisik close-to-close ortalama dusus < %1.5
        all_window = [prev_day] + recent
        pct_changes = [
            (all_window[i]["close"] - all_window[i-1]["close"]) / all_window[i-1]["close"] * 100
            for i in range(1, len(all_window))
            if all_window[i-1]["close"] > 0
        ]
        drops = [abs(c) for c in pct_changes if c < 0]
        avg_drop = sum(drops) / max(len(drops), 1) if drops else 0.0
        is_small_drops = avg_drop < VCP_AVG_DROP_THRESHOLD

        # 2. volume_drying: son gun hacim < 50-gun MA * 0.70 (muhafazakar)
        avg_50d_volume = sum(d["volume"] for d in last_50) / len(last_50)
        if avg_50d_volume <= 0:
            return False
        last_volume = recent[-1]["volume"]
        volume_drying = last_volume < avg_50d_volume * VCP_VOL_DRY_RATIO

        # 3. tight_closes: GUN-ICI range_pct < %2 (KARAR #464 — gercek hesap)
        # range_pct = (high - low) / close * 100 — Bonus FMP matematik formul
        # Mark canon "Trade Like a Stock Market Wizard" Bolum 10 intraday range
        range_pcts = []
        for d in recent:
            close_val = d["close"]
            if close_val <= 0:
                return False
            range_pcts.append((d["high"] - d["low"]) / close_val * 100)
        tight_closes = all(rp < VCP_TIGHT_RANGE_PCT for rp in range_pcts)

        return bool(is_small_drops and volume_drying and tight_closes)
    except (KeyError, TypeError, ValueError, ZeroDivisionError):
        return False


def compute_vcp_quality(price_volume_history: Optional[list[dict]]) -> Optional[str]:
    """VCP Kalite Skoru (KARAR #466, 20 May 2026, 3 kanal sentezi).

    Sn. Ferit'in 3 NotebookLM cross-check sonrasi yetki devri ile (Kural #23
    otonom mod) tescillenen iki seviyeli VCP kalite tespit fonksiyonu.

    3 kanal sentezi (Master + Minervini Uzmani + Bonus FMP):
      - Master: "0.70 muhafazakar yansima, 0.50 ideal pivot ani"
      - Minervini Uzmani: "0.70 guvenli filtre, 0.50 ideal gun hedefi"
      - Bonus FMP: "0.40-0.50 altin standart, 0.70 gevsek (ASX deneyimi)"

    Sentez: 0.70 = PASS filtre (genis ekran), 0.50 = EXCELLENT (Mark canon
    "%50 alti en siki" + Bonus FMP "altin standart").

    Bu fonksiyon compute_vcp_pass'a PARALEL — compute_vcp_pass tek seviyeli
    (True/False), compute_vcp_quality iki seviyeli (EXCELLENT/PASS/None).

    Args:
        price_volume_history: list[dict] OHLC formatinda
          [{"date":..., "open":..., "high":..., "low":..., "close":..., "volume":...}, ...]

    Returns:
        - "EXCELLENT" — Mark canon "en siki gun" + Brandon tum sartlar (VOL_DRY 0.50)
        - "PASS"      — Brandon muhafazakar filtre (VOL_DRY 0.70)
        - None        — yetersiz veri veya sartlar saglanmadi (failsafe)

    UI kullanim onerisi (KARAR ADAY #466 frontend):
        EXCELLENT -> yesil koyu rozet "A+ Kalite"
        PASS      -> yesil acik rozet "Olgun"
        None      -> rozet yok

    Backward compat: Eski close-only PVH -> None (high/low yok)
    """
    if not price_volume_history or len(price_volume_history) < VCP_MIN_HISTORY:
        return None

    try:
        recent = price_volume_history[-VCP_LOOKBACK_DAYS:]
        prev_day = price_volume_history[-(VCP_LOOKBACK_DAYS + 1)]
        last_50 = price_volume_history[-VCP_MIN_HISTORY:]

        if len(recent) < VCP_LOOKBACK_DAYS:
            return None

        # Schema kontrol (backward compat — close-only -> None)
        sample = recent[0]
        if "high" not in sample or "low" not in sample:
            return None

        # 1. is_small_drops (ardisik close-to-close)
        all_window = [prev_day] + recent
        pct_changes = [
            (all_window[i]["close"] - all_window[i-1]["close"]) / all_window[i-1]["close"] * 100
            for i in range(1, len(all_window))
            if all_window[i-1]["close"] > 0
        ]
        drops = [abs(c) for c in pct_changes if c < 0]
        avg_drop = sum(drops) / max(len(drops), 1) if drops else 0.0
        is_small_drops = avg_drop < VCP_AVG_DROP_THRESHOLD

        # 2. tight_closes (intraday range)
        range_pcts = []
        for d in recent:
            close_val = d["close"]
            if close_val <= 0:
                return None
            range_pcts.append((d["high"] - d["low"]) / close_val * 100)
        tight_closes = all(rp < VCP_TIGHT_RANGE_PCT for rp in range_pcts)

        # is_small_drops VEYA tight_closes saglanmadi -> direkt None
        if not (is_small_drops and tight_closes):
            return None

        # 3. Hacim seviyesi (iki seviye karsilastirma)
        avg_50d_volume = sum(d["volume"] for d in last_50) / len(last_50)
        if avg_50d_volume <= 0:
            return None
        last_volume = recent[-1]["volume"]
        vol_ratio = last_volume / avg_50d_volume

        # EXCELLENT esik (0.50) — Mark canon "%50 alti en siki gun"
        if vol_ratio < VCP_VOL_DRY_RATIO_EXCELLENT:
            return "EXCELLENT"
        # PASS esik (0.70) — Brandon muhafazakar filtre
        if vol_ratio < VCP_VOL_DRY_RATIO:
            return "PASS"
        # Hacim hala 50d MA *0.70 ustunde -> failsafe None
        return None
    except (KeyError, TypeError, ValueError, ZeroDivisionError):
        return None


# =============================================================================
# Sprint 4-bis.5 — Inside Day + Outside Day + VCP Ready Score
# KARAR #465 (Minervini Uzmani onerisi, 20 May 2026)
# Kaynak: Trade Like a Stock Market Wizard Bolum 10 (Inside Day "arzin
# tukenmesinin kesin kaniti") + Think and Trade Like a Champion Bolum 1
# (Outside Day Negative Reversal "Violations Soon After a Breakout")
# =============================================================================


# Eşikler (KARAR #465)
OUTSIDE_DAY_VOLUME_RATIO = 1.5    # Outside Day yuksek hacim esigi (vol > prev * 1.5)
VCP_READY_SCORE_INSIDE_WEIGHT = 50  # max puan (Inside Day orani)
VCP_READY_SCORE_VOL_WEIGHT = 30     # max puan (V-Dry hacim)
VCP_READY_SCORE_TIGHT_WEIGHT = 20   # max puan (intraday tight)
VCP_READY_SCORE_HIGH_THRESHOLD = 70  # >= bu deger -> "ready" filtre


def compute_inside_day(prev_day: dict, today: dict) -> bool:
    """Inside Day = bugun gun-ici range tamamen dunkunun icinde.
    Mark canon (Trade Like a Stock Market Wizard Bolum 10):
      "Inside Day = arzin tukendiginin ve firtina oncesi sessizligin kesin kaniti"

    Formul: today.high <= prev.high AND today.low >= prev.low

    Args:
        prev_day: {"high": ..., "low": ...}
        today: {"high": ..., "low": ...}

    Returns:
        bool: True = Inside Day (volatilite sıkışmasi sinyali)
    """
    try:
        return bool(today["high"] <= prev_day["high"]
                    and today["low"] >= prev_day["low"])
    except (KeyError, TypeError):
        return False


def compute_outside_day_negative_reversal(prev_day: dict, today: dict,
                                          vol_ratio: float = OUTSIDE_DAY_VOLUME_RATIO) -> bool:
    """Outside Day Negative Reversal = breakout sonrasi "Violation" sinyali.
    Mark canon (Think and Trade Like a Champion Bolum 1 - Violations):
      Geniş range (outside) + dusuk kapanis (negative) + yuksek hacim
      = breakout basarisizligi, otomatik pozisyon kuculme tetigi

    3 kosul AND:
      1. Outside: today.high > prev.high AND today.low < prev.low
      2. Negative: today.close < prev.close
      3. High volume: today.volume > prev.volume * vol_ratio (default 1.5)

    Args:
        prev_day: {"high","low","close","volume"}
        today: {"high","low","close","volume"}
        vol_ratio: hacim catma esigi (default 1.5 = %50 fazla)

    Returns:
        bool: True = Negative Reversal (Violation, pozisyon kuculme tetigi)
    """
    try:
        outside = today["high"] > prev_day["high"] and today["low"] < prev_day["low"]
        negative = today["close"] < prev_day["close"]
        high_vol = today["volume"] > prev_day["volume"] * vol_ratio
        return bool(outside and negative and high_vol)
    except (KeyError, TypeError):
        return False


def compute_vcp_ready_score(price_volume_history: Optional[list[dict]],
                            lookback: int = 3) -> Optional[int]:
    """VCP Ready Score (0-100) — Minervini Uzmani KARAR #465 onerisi.

    Pivot bolgesinde son `lookback` gun (default 3) icinde:
      - Kac Inside Day var (max 50 puan, "arzin tukenmesi")
      - Hacim 50d MA'ya gore ne kadar dustu (max 30 puan, V-Dry)
      - Son 5 gun intraday range ne kadar tight (max 20 puan)

    Score = inside_score + vol_score + tight_score

    Hedef: >= VCP_READY_SCORE_HIGH_THRESHOLD (70) -> "Ready" filtresi
    (11. Ready screen `vcp_ready_high` SQL: WHERE vcp_ready_score >= 70).

    Args:
        price_volume_history: OHLC formatinda PVH (Migration 003 sonrasi)
        lookback: Inside Day kontrolu icin gun sayisi (default 3)

    Returns:
        int 0-100: Ready Score
        None: yetersiz veri / OHLC yok / hata

    Backward compat: Eski close-only PVH -> None
    """
    if not price_volume_history or len(price_volume_history) < VCP_MIN_HISTORY:
        return None

    try:
        sample = price_volume_history[-1]
        if "high" not in sample or "low" not in sample:
            return None  # backward compat — OHLC yok

        # 1. Inside Day sayisi (son `lookback` gun)
        recent_for_inside = price_volume_history[-(lookback + 1):]
        inside_count = 0
        for i in range(1, len(recent_for_inside)):
            if compute_inside_day(recent_for_inside[i - 1], recent_for_inside[i]):
                inside_count += 1
        inside_score = int((inside_count / lookback) * VCP_READY_SCORE_INSIDE_WEIGHT)

        # 2. V-Dry hacim seviyesi (50d MA karsilastirma)
        last_50 = price_volume_history[-VCP_MIN_HISTORY:]
        avg_50d_volume = sum(d["volume"] for d in last_50) / len(last_50)
        if avg_50d_volume <= 0:
            return None
        last_volume = price_volume_history[-1]["volume"]
        vol_ratio = last_volume / avg_50d_volume

        if vol_ratio < VCP_VOL_DRY_RATIO_EXCELLENT:  # 0.50 -> 30 puan (full)
            vol_score = VCP_READY_SCORE_VOL_WEIGHT
        elif vol_ratio < VCP_VOL_DRY_RATIO:           # 0.70 -> 20 puan
            vol_score = int(VCP_READY_SCORE_VOL_WEIGHT * 2 / 3)
        elif vol_ratio < 1.0:                          # 1.00 alti -> 10 puan
            vol_score = int(VCP_READY_SCORE_VOL_WEIGHT / 3)
        else:
            vol_score = 0

        # 3. Intraday tight (son 5 gun)
        last_5 = price_volume_history[-VCP_LOOKBACK_DAYS:]
        tight_count = 0
        for d in last_5:
            if d["close"] > 0:
                range_pct = (d["high"] - d["low"]) / d["close"] * 100
                if range_pct < VCP_TIGHT_RANGE_PCT:
                    tight_count += 1
        tight_score = int((tight_count / VCP_LOOKBACK_DAYS) * VCP_READY_SCORE_TIGHT_WEIGHT)

        return inside_score + vol_score + tight_score
    except (KeyError, TypeError, ValueError, ZeroDivisionError):
        return None


# =============================================================================
# Sprint 4-bis.5 KARAR #467 — Power Play (High Tight Flag)
# Kaynak: Trade Like a Stock Market Wizard Bolum 10 + FMP_Matematik.md Konu 20
# Mark canon KESIN esikler (lokal birikim, Kural #22 hafiza taramasi sonucu)
# =============================================================================

# POLE (direk) esikleri
POWER_PLAY_POLE_MAX_WEEKS = 8           # KESIN: max sure
POWER_PLAY_POLE_MAX_DAYS = POWER_PLAY_POLE_MAX_WEEKS * 5  # 40 trading gun
POWER_PLAY_POLE_MIN_RISE_PCT = 100      # KESIN: min %100 yukselis

# FLAG (bayrak) esikleri
POWER_PLAY_FLAG_MIN_WEEKS = 2           # KESIN: min sure (10 gun)
POWER_PLAY_FLAG_MAX_WEEKS = 6           # KESIN: max sure (30 gun)
POWER_PLAY_FLAG_MAX_PULLBACK_PCT = 25   # KESIN: max duzeltme
POWER_PLAY_FLAG_MIN_PULLBACK_PCT = 10   # KESIN: alti = zaten sikismis


def compute_power_play_pass(price_volume_history: Optional[list[dict]]) -> bool:
    """Power Play / High Tight Flag tespit (KARAR #467, 20 May 2026).

    Mark canon KESIN esikler (Trade Like a Stock Market Wizard Bolum 10,
    FMP_Matematik.md Konu 20 hazir kod referansi):

    POLE (direk):
      - Sure: 8 hafta veya daha KISA (40 trading gun max)
      - Yukselis: minimum %100 (altinda Power Play DEGIL, siradan VCP)

    FLAG (bayrak):
      - Sure: 2-6 hafta (10-30 trading gun)
      - Duzeltme: %10-25 araliginda (alti zaten sikismis, ustu reddet)

    BREAKOUT (pivot):
      - Pivot = Flag High (KESIN)

    Args:
        price_volume_history: OHLC formatinda PVH (Migration 003 sonrasi)
                              En az 80 gun gerekli (POLE 40 + FLAG 30 + marj)

    Returns:
        bool: True = Power Play setup'i (POLE + FLAG kosullari saglandi)

    Backward compat: Eski close-only PVH veya kisa veri -> False
    """
    required_days = POWER_PLAY_POLE_MAX_DAYS + POWER_PLAY_FLAG_MAX_WEEKS * 5  # 70
    if not price_volume_history or len(price_volume_history) < required_days:
        return False

    try:
        sample = price_volume_history[-1]
        if "high" not in sample or "low" not in sample:
            return False  # backward compat — OHLC yok

        flag_days = POWER_PLAY_FLAG_MAX_WEEKS * 5  # 30

        # POLE bolgesi: en eski (POLE_MAX_DAYS + FLAG_MAX_DAYS) ile flag oncesi
        pole_period = price_volume_history[-(POWER_PLAY_POLE_MAX_DAYS + flag_days):-flag_days]
        if len(pole_period) < POWER_PLAY_POLE_MAX_DAYS // 2:  # en az yari
            return False

        pole_lows = [d["low"] for d in pole_period if d["low"] > 0]
        pole_highs = [d["high"] for d in pole_period if d["high"] > 0]
        if not pole_lows or not pole_highs:
            return False
        pole_low = min(pole_lows)
        pole_high = max(pole_highs)

        if pole_low <= 0:
            return False
        pole_rise_pct = (pole_high - pole_low) / pole_low * 100

        # KESIN: %100 alti Power Play DEGIL
        if pole_rise_pct < POWER_PLAY_POLE_MIN_RISE_PCT:
            return False

        # FLAG bolgesi: son 30 gun
        flag_period = price_volume_history[-flag_days:]
        flag_lows = [d["low"] for d in flag_period if d["low"] > 0]
        if not flag_lows:
            return False
        flag_low = min(flag_lows)

        # FLAG duzeltme: pole_high'dan flag_low'a dusus yuzdesi
        if pole_high <= 0:
            return False
        flag_pullback_pct = (pole_high - flag_low) / pole_high * 100

        # KESIN aralik: %10-25 (alti zaten sikismis = PASS, ustu reddet)
        if flag_pullback_pct > POWER_PLAY_FLAG_MAX_PULLBACK_PCT:
            return False
        # Alti = zaten sikismis = Power Play TRUE (Mark canon: VCP daralmasi
        # aranmaya gerek yok, direkt pivot Flag High'da)

        return True
    except (KeyError, TypeError, ValueError, ZeroDivisionError):
        return False


# =============================================================================
# Konu 14 — RBA Result Based Analysis (Mark Minervini Bölüm 4)
# =============================================================================


def compute_rba_metrics(closed_trades: list[dict]) -> RBAMetrics:
    """
    Mark Minervini RBA — Result Based Analysis hesaplaması.

    Kaynak: Notebook B3 Modül 7.1 + NotebookLM Konu 14

    Args:
        closed_trades: Kapanmış trade listesi. Her dict 'pnl_pct' field'i içermeli.
                       Örnek: [{'pnl_pct': 12.5}, {'pnl_pct': -3.2}, ...]

    Returns:
        RBAMetrics: Hesaplanmış metrikler. Boş listede sıfır state döner.

    Notlar:
        - Tüm trade'ler kazanıyorsa adjusted_ratio = float('inf')
        - Tüm trade'ler kaybediyorsa adjusted_ratio = 0.0
        - is_statistically_significant: True iff num_trades >= 30
    """
    if not closed_trades:
        return RBAMetrics(
            num_trades=0,
            win_rate=0.0,
            avg_gain_pct=0.0,
            avg_loss_pct=0.0,
            largest_gain_pct=0.0,
            largest_loss_pct=0.0,
            adjusted_ratio=0.0,
            expectancy_pct=0.0,
            is_statistically_significant=False,
        )

    pnls = [t['pnl_pct'] for t in closed_trades]
    winners = [p for p in pnls if p > 0]
    losers = [p for p in pnls if p < 0]

    win_rate = len(winners) / len(pnls)
    avg_gain = sum(winners) / len(winners) if winners else 0.0
    avg_loss = sum(losers) / len(losers) if losers else 0.0  # negatif

    # Adjusted Ratio (Bundle adjustedRatio formülü birebir)
    if losers and avg_loss != 0:
        adjusted_ratio = (win_rate * avg_gain) / ((1 - win_rate) * abs(avg_loss))
    elif winners and not losers:
        # Tüm trade'ler kazanç = sonsuz edge. ANCAK float('inf') JSON serialize
        # EDILEMEZ (FastAPI 500 — "Out of range float not JSON compliant").
        # JSON-safe capped değer: 99.0 = pratik sonsuz (UI'da "çok yüksek edge").
        adjusted_ratio = 99.0
    else:
        adjusted_ratio = 0.0

    expectancy = (win_rate * avg_gain) - ((1 - win_rate) * abs(avg_loss))

    return RBAMetrics(
        num_trades=len(pnls),
        win_rate=win_rate,
        avg_gain_pct=avg_gain,
        avg_loss_pct=avg_loss,
        largest_gain_pct=max(pnls),
        largest_loss_pct=min(pnls),
        adjusted_ratio=adjusted_ratio,
        expectancy_pct=expectancy,
        is_statistically_significant=len(pnls) >= 30,
    )


def should_drop_setup(rba: RBAMetrics) -> StopRecommendation:
    """
    Bir setup'ı bırakma kararı — RBA metrikleri üzerinden.

    Kaynak: Notebook B3 Modül 7.2 + NotebookLM Konu 14
    Kitap referansı: Think and Trade Like a Champion — Bölüm 4

    Kriter Hiyerarşisi:
    1. Min 30 trade gözlem yoksa → INFO (yeterli veri yok)
    2. Adjusted Ratio < 1.0 → CRITICAL (negatif edge, BIRAK)
    3. abs(avg_loss) > avg_gain → WARNING (setup zayıflıyor)
    4. win_rate < 0.30 → WARNING (Minervini ortalaması %50+)
    5. Aksi halde → OK
    """
    if not rba.is_statistically_significant:
        return StopRecommendation(
            severity="INFO",
            message=f"📊 Sadece {rba.num_trades} trade. Min 30 trade gözlem gerekli (istatistiksel anlamlılık)."
        )

    if rba.adjusted_ratio < 1.0:
        return StopRecommendation(
            severity="CRITICAL",
            message=f"🛑 Adjusted Ratio {rba.adjusted_ratio:.2f} < 1.0 — Setup NEGATİF EDGE. BIRAK!"
        )

    if abs(rba.avg_loss_pct) > rba.avg_gain_pct:
        return StopRecommendation(
            severity="WARNING",
            message=f"⚠️ Avg Loss %{abs(rba.avg_loss_pct):.1f} > Avg Gain %{rba.avg_gain_pct:.1f} — Setup zayıflıyor"
        )

    if rba.win_rate < 0.30:
        return StopRecommendation(
            severity="WARNING",
            message=f"⚠️ Win rate %{rba.win_rate*100:.0f} çok düşük (Minervini ortalaması %50+)"
        )

    return StopRecommendation(
        severity="OK",
        message=f"✅ Setup sağlıklı — AR {rba.adjusted_ratio:.2f}, Win %{rba.win_rate*100:.0f}"
    )


# =============================================================================
# TradeGrader — Öneri Fonksiyonları (EK 10 sentezi)
# =============================================================================

def suggest_entry_grade(
    entry_price: float,
    pivot_price: float,
) -> GradeSuggestion:
    """
    Alım noktası pivot'a göre entry grade önerir.

    BP: pivot ±2% içinde alım
    BL: pivot +2% ile +5% arasında (geç alım)
    CE: pivot +5% üzerinde (çok geç, extended chase)
    BE: pivot -2% altında (erken, setup tamamlanmamış)
    """
    if pivot_price <= 0 or entry_price <= 0:
        return GradeSuggestion("BE", "Bought early", "LOW", "Pivot fiyatı geçersiz")

    deviation_pct = (entry_price - pivot_price) / pivot_price * 100

    if -2.0 <= deviation_pct <= 2.0:
        return GradeSuggestion(
            code="BP",
            name="Bought perfect",
            confidence="HIGH",
            reason=f"Pivot'a {deviation_pct:+.1f}% — ideal alım bölgesi",
        )
    elif 2.0 < deviation_pct <= 5.0:
        return GradeSuggestion(
            code="BL",
            name="Bought late",
            confidence="MEDIUM",
            reason=f"Pivot'a {deviation_pct:+.1f}% — pivot'tan geç alım",
        )
    # O'Neil HTMMIS Bol.3 s.26-27 + Bol.15 s.164: "extended >5-10% past buy point = late".
    # Quanfina muhafazakar uc: >%5 -> CE (chase). (Kitap araligi %5-10; biz %5'te kesiyoruz.)
    elif deviation_pct > 5.0:
        return GradeSuggestion(
            code="CE",
            name="Chased extended",
            confidence="HIGH",
            reason=f"Pivot'a {deviation_pct:+.1f}% — çok uzak, extended chase",
        )
    else:
        return GradeSuggestion(
            code="BE",
            name="Bought early",
            confidence="MEDIUM",
            reason=f"Pivot'a {deviation_pct:+.1f}% — setup tamamlanmadan erken alım",
        )


def suggest_loss_grade(
    entry_price: float,
    exit_price: float,
    stop_loss: float,
    invest_type: str = "LONG",
) -> GradeSuggestion:
    """
    Zararla kapanan trade için grade önerir.

    Sprint 4.7c.4: %10 The Wall absolute ceiling — plan ne olursa olsun ≥%10 kayıp = CLL

    CLP: gerçek zarar ≈ planlı stop (±1%)
    CLE: stop'tan önce çıkış (acele kes)
    CLL: stop geçildikten sonra çıkış (geç kes)
    """
    _itype = LONG if invest_type.upper() == "LONG" else SHORT

    planned_stop_pct = abs(stop_loss_percentage(entry_price, stop_loss, _itype))
    actual_pct = percent_change(entry_price, exit_price, _itype)
    actual_loss_abs = abs(actual_pct)

    # Sprint 4.7c.4 — The Wall %10 absolute ceiling
    # Trade Like a Wizard Bölüm 12 — Mark'ın mutlak çıkış sınırı %10
    # Plan ne olursa olsun, %10+ kayıp = disiplin ihlali → her zaman CLL
    if actual_loss_abs >= 10.0:
        return GradeSuggestion(
            "CLL", "Cut loss late", "HIGH",
            f"%{actual_loss_abs:.1f} kayıp — The Wall %10 mutlak sınır aşıldı "
            f"(plan stop %{planned_stop_pct:.1f})",
        )

    # The Wall altında — disiplin ölçümü (plan-vs-actual sapma)
    diff = actual_loss_abs - planned_stop_pct

    if abs(diff) <= 1.0:
        return GradeSuggestion(
            code="CLP",
            name="Cut loss perfect",
            confidence="HIGH",
            reason=f"Gerçek zarar %{actual_loss_abs:.1f} ≈ plan %{planned_stop_pct:.1f} (±1%)",
        )
    elif diff < -1.0:
        return GradeSuggestion(
            code="CLE",
            name="Cut loss early",
            confidence="MEDIUM",
            reason=f"Stop'tan %{abs(diff):.1f} önce çıkıldı (plan %{planned_stop_pct:.1f}, gerçek %{actual_loss_abs:.1f})",
        )
    else:
        return GradeSuggestion(
            code="CLL",
            name="Cut loss late",
            confidence="HIGH",
            reason=f"Stop %{diff:.1f} geçildi (plan %{planned_stop_pct:.1f}, gerçek %{actual_loss_abs:.1f})",
        )


def suggest_exit_grade(
    entry_price: float,
    exit_price: float,
    weeks_held: float,
    invest_type: str = "LONG",
) -> GradeSuggestion:
    """
    Kârlı kapanan trade için grade önerir.

    SP: ≥20% kazanç (yeterli süre tutulduysa HIGH, değilse MEDIUM)
    SP: 10-20% kazanç, <16 hafta (MEDIUM)
    SL: 10-20% kazanç, ≥16 hafta (uzun tutma — kâr erimesi riski)
    SE: <10% kazanç (erken satış)
    """
    _itype = LONG if invest_type.upper() == "LONG" else SHORT

    gain_pct = percent_change(entry_price, exit_price, _itype)

    if gain_pct >= 20.0:
        confidence = "HIGH" if weeks_held >= 6 else "MEDIUM"
        return GradeSuggestion(
            code="SP",
            name="Sold perfect",
            confidence=confidence,
            reason=f"%{gain_pct:.1f} kazanç, {weeks_held:.0f} hafta tutuldu",
        )
    elif gain_pct >= 10.0:
        if weeks_held >= 16:
            return GradeSuggestion(
                code="SL",
                name="Sold late",
                confidence="MEDIUM",
                reason=f"%{gain_pct:.1f} kazanç, {weeks_held:.0f} hafta tutuldu — kâr erimesi olabilir",
            )
        return GradeSuggestion(
            code="SP",
            name="Sold perfect",
            confidence="MEDIUM",
            reason=f"%{gain_pct:.1f} kazanç, {weeks_held:.0f} hafta tutuldu",
        )
    else:
        return GradeSuggestion(
            code="SE",
            name="Sold early",
            confidence="MEDIUM",
            reason=f"Sadece %{gain_pct:.1f} kazanç — erken satış",
        )


def compute_grade_distribution(graded_legs: list[dict]) -> dict:
    """
    Grade listesinden dağılım istatistiği üretir.

    Input:  [{'grade_code': 'BP'}, {'grade_code': 'BL'}, ...]
    Output: {
        'by_code':  {'BP': 3, 'BL': 1, ...},
        'by_group': {'ENTRY': 5, 'EXIT': 3, 'LOSS': 2, 'UNKNOWN': 0},
        'total':    10,
    }
    """
    by_code: dict[str, int] = {}
    by_group: dict[str, int] = {"ENTRY": 0, "EXIT": 0, "LOSS": 0, "UNKNOWN": 0}

    for leg in graded_legs:
        code = leg.get("grade_code", "")
        by_code[code] = by_code.get(code, 0) + 1

        group = GRADE_CATEGORIES[code][1] if code in GRADE_CATEGORIES else "UNKNOWN"
        by_group[group] = by_group.get(group, 0) + 1

    return {
        "by_code": by_code,
        "by_group": by_group,
        "total": len(graded_legs),
    }


# =============================================================================
# SPRINT 4-bis.7 — FAZ 1 B PAKET — Mark 4-Kitap Hassas KARAR'ları
# Tescil: Vizyon v22.00 (24 May 2026)
# Detay: notebook/Sprint_4_bis_7_Mark_HASSAS_Tarama.md
# =============================================================================

# Mark KESIN sabitleri (TLSMW Ch 12 + TTLC s.45, 143-144)
MARK_STOP_ABSOLUTE_CAP_PCT: float = 10.0       # TLSMW Ch 12 s.277 — "no more than 10 percent"
MARK_STOP_TARGET_PCT_RANGE: tuple = (5.0, 7.0)  # TLSMW Ch 12 — "average maybe 6 or 7 percent"
MARK_EQUITY_RISK_MIN_PCT: float = 1.25         # TTLC s.143 — conservative
MARK_EQUITY_RISK_MAX_PCT: float = 2.50         # TTLC s.143 — aggressive
MARK_PORTFOLIO_MAX_STOCKS: int = 20            # TTLC s.144 — "no more than 20 positions"
MARK_PORTFOLIO_OPTIMAL_STOCKS: tuple = (4, 12) # TTLC s.144 — "4-12 stocks total"
MARK_POSITION_OPTIMAL_PCT_RANGE: tuple = (20.0, 25.0)  # TTLC s.144 — "20-25 percent in best names"
MARK_POSITION_MAX_PCT: float = 50.0            # TTLC s.144 — "Never take position larger than 50%"

# KARAR ADAY #732 (24 May 2026) — Mark Pyramid 3-Tier sabit (KARAR #487 Mark birebir).
# Kaynak zinciri: TraderLion Lesson 7 (Pilot) + Brandon Video (Standart) +
# Mark Video Kelly 2:1 (Full) + Mark X "Trades not working = no size increase" (Kilit).
MARK_PYRAMID_PILOT_PCT_RANGE: tuple = (1.0, 3.0)        # Zayıf piyasa / nakitten giriş
MARK_PYRAMID_STANDARD_PCT_RANGE: tuple = (6.25, 12.5)   # Normal piyasa + trades working
MARK_PYRAMID_FULL_PCT_RANGE: tuple = (15.0, 25.0)       # Peak market + Kelly 2:1


def compute_dynamic_stop(
    rba: Optional[RBAMetrics] = None,
    fallback_pct: float = 7.0
) -> dict:
    """KARAR ADAY #914 — Mark Dynamic Stop = avg_gain / 2 (TTLC s.299 "Trader's Cardinal Sin").

    Mark birebir (TTLC s.299):
        "Allowing your loss on a trade to exceed your average gain is what I call the
        trader's cardinal sin. ... Stop loss should be no more than HALF the average gain."

    Hesap:
        stop_pct = min(avg_gain / 2, MARK_STOP_ABSOLUTE_CAP_PCT)

    Eğer RBA istatistiksel olarak anlamlı değilse (<30 trade) → fallback_pct kullanılır
    (Mark KESIN min trade gözlem kuralı).

    Args:
        rba: RBAMetrics — kullanıcının gerçek trade istatistikleri (Result-Based)
        fallback_pct: RBA yokken kullanılacak varsayılan stop (Mark default %6-7 range)

    Returns:
        dict — Dynamic stop tavsiyesi
        {
            'recommended_stop_pct': float,
            'method': 'rba_based' | 'fallback',
            'absolute_cap_applied': bool,
            'mark_says': str (TTLC alıntı),
            'rba_anlamli_mi': bool
        }

    Kaynak: TTLC s.299 + Sprint_4_bis_7_Mark_HASSAS_Tarama.md KARAR ADAY #914
    """
    if rba is not None and rba.is_statistically_significant and rba.avg_gain_pct > 0:
        half_gain = rba.avg_gain_pct / 2.0
        absolute_capped = half_gain > MARK_STOP_ABSOLUTE_CAP_PCT
        recommended = min(half_gain, MARK_STOP_ABSOLUTE_CAP_PCT)
        if absolute_capped:
            says = (f"Avg gain %{rba.avg_gain_pct:.1f} → half = %{half_gain:.1f}, "
                    f"Mark %10 absolute cap aktif (TTLC s.299)")
        else:
            says = (f"Avg gain %{rba.avg_gain_pct:.1f} → half = %{half_gain:.1f} "
                    f"(Mark KESIN: trader's cardinal sin avoid)")
        return {
            'recommended_stop_pct': round(recommended, 2),
            'method': 'rba_based',
            'absolute_cap_applied': absolute_capped,
            'mark_says': says,
            'rba_anlamli_mi': True,
        }

    # Fallback: RBA yok veya yetersiz trade
    capped = min(fallback_pct, MARK_STOP_ABSOLUTE_CAP_PCT)
    return {
        'recommended_stop_pct': round(capped, 2),
        'method': 'fallback',
        'absolute_cap_applied': fallback_pct > MARK_STOP_ABSOLUTE_CAP_PCT,
        'mark_says': (f"RBA yok veya <30 trade — fallback %{capped:.1f} "
                      f"(Mark default %5-7 range, TTLC s.45)"),
        'rba_anlamli_mi': False,
    }


def mark_position_sizer(
    portfolio_value: float,
    target_risk_pct: float = 2.0,
    max_stop_pct: float = 7.0
) -> dict:
    """KARAR ADAY #969 — Mark Position Sizing (TTLC s.143-144).

    Mark birebir (TTLC s.143):
        "Your maximum risk should be no more than 1.25 to 2.5 percent of your equity
        on any one trade."

    Mark birebir (TTLC s.143):
        "Either your stop moves or your position size moves. One or the other must
        be adjusted to dial in the correct amount of risk."

    Hesap (Mark KESIN formula):
        risk_dollars   = portfolio_value * (target_risk_pct / 100)
        position_value = risk_dollars / (max_stop_pct / 100)
        position_pct   = position_value / portfolio_value * 100

    Validation:
        - target_risk_pct ∈ [1.25, 2.50] (Mark KESIN range)
        - max_stop_pct ≤ 10 (Mark absolute cap)
        - position_pct ≤ 50 (Mark "never take position larger than 50%")

    Args:
        portfolio_value: Toplam portfolio $ değeri
        target_risk_pct: Equity risk yüzdesi (%1.25-2.50)
        max_stop_pct: Stop loss yüzdesi (≤%10)

    Returns:
        dict — Position size önerisi + warnings

    Kaynak: TTLC s.143-144 + Sprint_4_bis_7_Mark_HASSAS_Tarama.md KARAR ADAY #969
    """
    warnings: list[str] = []

    if portfolio_value <= 0:
        return {
            'error': 'portfolio_value must be > 0',
            'position_dollars': 0.0,
            'position_pct': 0.0,
            'risk_dollars': 0.0,
            'risk_pct': 0.0,
            'warnings': ['Geçersiz portfolio_value'],
            'mark_says': '',
        }

    if target_risk_pct < MARK_EQUITY_RISK_MIN_PCT:
        warnings.append(
            f"Risk %{target_risk_pct:.2f} < Mark min %{MARK_EQUITY_RISK_MIN_PCT} "
            f"(TTLC s.143 — conservative range)"
        )
    elif target_risk_pct > MARK_EQUITY_RISK_MAX_PCT:
        warnings.append(
            f"Risk %{target_risk_pct:.2f} > Mark MAX %{MARK_EQUITY_RISK_MAX_PCT} "
            f"(TTLC s.143 — never exceed)"
        )

    if max_stop_pct > MARK_STOP_ABSOLUTE_CAP_PCT:
        warnings.append(
            f"Stop %{max_stop_pct:.1f} > Mark absolute cap %{MARK_STOP_ABSOLUTE_CAP_PCT} "
            f"(TLSMW Ch 12 — '10 percent maximum allowance')"
        )

    if max_stop_pct <= 0:
        return {
            'error': 'max_stop_pct must be > 0',
            'position_dollars': 0.0,
            'position_pct': 0.0,
            'risk_dollars': 0.0,
            'risk_pct': target_risk_pct,
            'warnings': warnings + ['Geçersiz max_stop_pct'],
            'mark_says': '',
        }

    risk_dollars = portfolio_value * (target_risk_pct / 100.0)
    position_dollars = risk_dollars / (max_stop_pct / 100.0)
    position_pct = (position_dollars / portfolio_value) * 100.0

    if position_pct > MARK_POSITION_MAX_PCT:
        warnings.append(
            f"Position %{position_pct:.1f} > Mark MAX %{MARK_POSITION_MAX_PCT} "
            f"(TTLC s.144 — never take position larger than 50%)"
        )

    # Tier öneri
    if MARK_POSITION_OPTIMAL_PCT_RANGE[0] <= position_pct <= MARK_POSITION_OPTIMAL_PCT_RANGE[1]:
        tier = 'optimal'
        says = (f"Position %{position_pct:.1f} — Mark optimal "
                f"%{MARK_POSITION_OPTIMAL_PCT_RANGE[0]}-{MARK_POSITION_OPTIMAL_PCT_RANGE[1]} "
                f"(TTLC s.144 best names)")
    elif position_pct < MARK_POSITION_OPTIMAL_PCT_RANGE[0]:
        tier = 'pilot_buy'
        says = (f"Position %{position_pct:.1f} — Mark pilot buy tier "
                f"(TTLC s.91 toe-in-the-water)")
    else:
        tier = 'aggressive'
        says = (f"Position %{position_pct:.1f} — agresif tier, dikkat "
                f"(TTLC s.144 optimal {MARK_POSITION_OPTIMAL_PCT_RANGE[0]}-{MARK_POSITION_OPTIMAL_PCT_RANGE[1]} aralığı)")

    return {
        'position_dollars': round(position_dollars, 2),
        'position_pct': round(position_pct, 2),
        'risk_dollars': round(risk_dollars, 2),
        'risk_pct': round(target_risk_pct, 2),
        'tier': tier,
        'warnings': warnings,
        'mark_says': says,
    }


def mark_six_rule_check(
    risk_pct: float,
    stop_pct: float,
    avg_loss_pct: Optional[float],
    position_pct: float,
    is_best_name: bool,
    total_positions: int
) -> dict:
    """KARAR ADAY #970 — Mark 6-Rule Position Enforcement (TTLC s.144).

    Mark birebir 6 madde (TTLC s.144):
        1. %1.25-2.50 risk of total equity
        2. %10 maximum stop
        3. Losses average no more than %5-6
        4. Never take position larger than %50
        5. Shoot for optimal %20-25 positions in best names
        6. No more than 10-12 stocks total (16-20 large portfolios)

    Args:
        risk_pct: Toplam equity risk yüzdesi bu pozisyon için
        stop_pct: Stop loss yüzdesi
        avg_loss_pct: RBA avg loss yüzdesi (None = bilgi yok)
        position_pct: Position size yüzdesi (portfolio'nun)
        is_best_name: Sn. Ferit "best name" işaretlemiş mi?
        total_positions: Mevcut portfolio'daki toplam stok sayısı

    Returns:
        dict — 6 kural check sonucu
        {
            'all_pass': bool,
            'rules': [
                {'rule_no': 1, 'pass': True, 'message': '...', 'mark_says': '...'},
                ...
            ],
            'pass_count': int (0-6),
            'critical_violations': list[int]
        }

    Kaynak: TTLC s.144 + Sprint_4_bis_7_Mark_HASSAS_Tarama.md KARAR ADAY #970
    """
    rules: list[dict] = []

    # Rule 1: %1.25-2.50 risk of total equity
    r1_pass = MARK_EQUITY_RISK_MIN_PCT <= risk_pct <= MARK_EQUITY_RISK_MAX_PCT
    rules.append({
        'rule_no': 1,
        'rule': 'Risk %1.25-2.50 of equity',
        'pass': r1_pass,
        'value': risk_pct,
        'message': f"Risk %{risk_pct:.2f}" + (
            " ✅ Mark range" if r1_pass
            else f" ⚠️ Mark %{MARK_EQUITY_RISK_MIN_PCT}-{MARK_EQUITY_RISK_MAX_PCT} dışında"
        ),
        'mark_says': "TTLC s.143 — no more than 1.25-2.5% equity",
        'critical': not r1_pass and risk_pct > MARK_EQUITY_RISK_MAX_PCT,
    })

    # Rule 2: %10 max stop
    r2_pass = stop_pct <= MARK_STOP_ABSOLUTE_CAP_PCT
    rules.append({
        'rule_no': 2,
        'rule': 'Stop ≤ %10 max',
        'pass': r2_pass,
        'value': stop_pct,
        'message': f"Stop %{stop_pct:.1f}" + (
            " ✅ Mark cap" if r2_pass
            else f" 🔴 Mark MAX %{MARK_STOP_ABSOLUTE_CAP_PCT} aşıldı"
        ),
        'mark_says': "TLSMW Ch 12 — 10 percent maximum allowance",
        'critical': not r2_pass,
    })

    # Rule 3: Losses avg ≤ %5-6
    if avg_loss_pct is not None:
        avg_loss_abs = abs(avg_loss_pct)
        r3_pass = avg_loss_abs <= 6.0
        rules.append({
            'rule_no': 3,
            'rule': 'Avg loss ≤ %5-6',
            'pass': r3_pass,
            'value': avg_loss_abs,
            'message': f"Avg loss %{avg_loss_abs:.1f}" + (
                " ✅ Mark target" if r3_pass
                else " ⚠️ Mark %5-6 hedefin üstünde"
            ),
            'mark_says': "TTLC s.143 — losses should average no more than 5-6%",
            'critical': False,
        })
    else:
        rules.append({
            'rule_no': 3,
            'rule': 'Avg loss ≤ %5-6',
            'pass': True,  # Bilgi yokken pass kabul
            'value': None,
            'message': "Avg loss bilgisi yok (RBA yetersiz)",
            'mark_says': "TTLC s.143 — 30+ trade gerekli istatistik için",
            'critical': False,
        })

    # Rule 4: Position ≤ %50
    r4_pass = position_pct <= MARK_POSITION_MAX_PCT
    rules.append({
        'rule_no': 4,
        'rule': 'Position ≤ %50',
        'pass': r4_pass,
        'value': position_pct,
        'message': f"Position %{position_pct:.1f}" + (
            " ✅" if r4_pass
            else f" 🔴 Mark MAX %{MARK_POSITION_MAX_PCT} aşıldı"
        ),
        'mark_says': "TTLC s.144 — never take position larger than 50%",
        'critical': not r4_pass,
    })

    # Rule 5: Best names %20-25 (advisory)
    if is_best_name:
        r5_pass = (MARK_POSITION_OPTIMAL_PCT_RANGE[0] <= position_pct
                   <= MARK_POSITION_OPTIMAL_PCT_RANGE[1])
        rules.append({
            'rule_no': 5,
            'rule': 'Best names %20-25',
            'pass': r5_pass,
            'value': position_pct,
            'message': f"Best name + position %{position_pct:.1f}" + (
                " ✅ optimal" if r5_pass
                else f" ℹ️ Mark optimal %{MARK_POSITION_OPTIMAL_PCT_RANGE[0]}-{MARK_POSITION_OPTIMAL_PCT_RANGE[1]}"
            ),
            'mark_says': "TTLC s.144 — 20-25% in best names",
            'critical': False,
        })
    else:
        rules.append({
            'rule_no': 5,
            'rule': 'Best names %20-25',
            'pass': True,  # Best name değilse skip
            'value': None,
            'message': "Best name değil — Rule 5 atlandı",
            'mark_says': "TTLC s.144 — best names only",
            'critical': False,
        })

    # Rule 6: Total positions ≤ 10-12 (or 16-20 large)
    r6_pass = total_positions <= MARK_PORTFOLIO_MAX_STOCKS
    optimal_match = MARK_PORTFOLIO_OPTIMAL_STOCKS[0] <= total_positions <= MARK_PORTFOLIO_OPTIMAL_STOCKS[1]
    rules.append({
        'rule_no': 6,
        'rule': 'Toplam ≤ 10-12 (or 16-20 large)',
        'pass': r6_pass,
        'value': total_positions,
        'message': f"Toplam {total_positions} stok" + (
            " ✅ Mark optimal" if optimal_match
            else " ⚠️ Mark large portfolio (16-20)" if r6_pass
            else f" 🔴 Mark MAX {MARK_PORTFOLIO_MAX_STOCKS} aşıldı"
        ),
        'mark_says': "TTLC s.144 — 10-12 typical, 16-20 large, max 20",
        'critical': not r6_pass,
    })

    pass_count = sum(1 for r in rules if r['pass'])
    critical_violations = [r['rule_no'] for r in rules if r['critical']]

    return {
        'all_pass': pass_count == 6,
        'rules': rules,
        'pass_count': pass_count,
        'critical_violations': critical_violations,
    }


# =============================================================================
# SPRINT 4-bis.7 — FAZ 2 BAŞLANGIÇ — Mark EPS Acceleration (KARAR ADAY #834)
# Tescil: Vizyon v22.00 + v22.01
# Detay: notebook/Sprint_4_bis_7_Mark_HASSAS_Tarama.md
# =============================================================================

# Mark KESIN EPS sabitler (TLSMW Ch 7 + Ch 8 hassas)
MARK_EPS_MIN_GROWTH_PCT: float = 20.0          # TLSMW s.127 minimum
MARK_EPS_SUPERPERFORMANCE_PCT: float = 30.0    # TLSMW s.127 superperformance
MARK_EPS_BULL_MARKET_PCT: float = 40.0         # TLSMW s.127 bull market
MARK_EPS_TURNAROUND_PCT: float = 100.0         # TLSMW s.137 turnaround
MARK_EPS_ACCEL_MIN_QUARTERS: int = 3           # TLSMW s.131 3-quarter accel
MARK_EPS_90PCT_RULE_THRESHOLD: float = 25.0    # TLSMW s.131 "%90 winners"


def detect_eps_acceleration(eps_growth_yoy_last_4q: list[float]) -> dict:
    """KARAR ADAY #834 — Mark EPS Acceleration Detector (TLSMW s.131).

    Mark birebir (TLSMW s.131):
        "More than 90 percent of the biggest stock market winners showed some
        form of earnings acceleration before or during their huge price moves."

    Mark KESIN örnek (s.131):
        q-4: -%5, q-3: +%10, q-2: +%28, q-1: +%56 = 3 quarter acceleration

    Hesap:
        - is_accelerating: q[i] < q[i+1] for i in range(len-1) — STRICT artış
        - magnitude_pct_pts: q[-1] - q[0] (toplam acceleration büyüklüğü)
        - mark_90pct_rule: STRICT acceleration + current quarter > %25
        - phase: 'accelerating' | 'decelerating' | 'flat'

    Args:
        eps_growth_yoy_last_4q: Son 4 çeyrek YoY EPS büyüme oranı [q-3, q-2, q-1, current]
            En eski quarter ilk, current quarter son sırada.
            Örnek: [-5.0, 10.0, 28.0, 56.0]

    Returns:
        dict:
        {
            'accelerating': bool,        # 3-quarter strict acceleration
            'magnitude_pct_pts': float,  # current - oldest
            'mark_90pct_rule': bool,     # Mark KESIN 90% winners pattern
            'phase': 'accelerating' | 'decelerating' | 'flat' | 'invalid',
            'tier': 'minimum' | 'superperformance' | 'bull_market' | 'turnaround' | 'below_minimum',
            'mark_says': str,
            'quarters_count': int,
        }

    Kaynak: TLSMW s.131 + Sprint_4_bis_7_Mark_HASSAS_Tarama.md KARAR ADAY #834
    """
    if not eps_growth_yoy_last_4q or len(eps_growth_yoy_last_4q) < 2:
        return {
            'accelerating': False,
            'magnitude_pct_pts': 0.0,
            'mark_90pct_rule': False,
            'phase': 'invalid',
            'tier': 'below_minimum',
            'mark_says': 'En az 2 çeyrek EPS gerekli',
            'quarters_count': 0,
        }

    q = eps_growth_yoy_last_4q
    n = len(q)
    current = q[-1]

    # Strict acceleration check (Mark KESIN — 3+ quarter monotonic)
    is_strict_accel = all(q[i] < q[i + 1] for i in range(n - 1))
    is_strict_decel = all(q[i] > q[i + 1] for i in range(n - 1))

    magnitude = current - q[0]

    # Phase classification
    if is_strict_accel:
        phase = 'accelerating'
    elif is_strict_decel:
        phase = 'decelerating'
    else:
        phase = 'flat'

    # Mark 90% Rule: STRICT acceleration + current quarter > %25 threshold
    mark_90pct = is_strict_accel and current > MARK_EPS_90PCT_RULE_THRESHOLD

    # Tier classification (Mark KESIN tier'lar)
    if current >= MARK_EPS_TURNAROUND_PCT:
        tier = 'turnaround'
    elif current >= MARK_EPS_BULL_MARKET_PCT:
        tier = 'bull_market'
    elif current >= MARK_EPS_SUPERPERFORMANCE_PCT:
        tier = 'superperformance'
    elif current >= MARK_EPS_MIN_GROWTH_PCT:
        tier = 'minimum'
    else:
        tier = 'below_minimum'

    # Mark says
    if mark_90pct:
        says = (f"3-quarter strict acceleration + current %{current:.1f} > %{MARK_EPS_90PCT_RULE_THRESHOLD} "
                f"= Mark 90% Rule MATCH (TLSMW s.131)")
    elif is_strict_accel:
        says = (f"Strict acceleration ama current %{current:.1f} < %{MARK_EPS_90PCT_RULE_THRESHOLD} "
                f"(Mark 90% Rule için threshold gerekli)")
    elif is_strict_decel:
        says = f"Strict DECELERATION — Mark uyarı (Dell paten, TLSMW s.138)"
    else:
        says = "Düzensiz EPS — Mark 90% Rule için pattern yok"

    return {
        'accelerating': is_strict_accel,
        'magnitude_pct_pts': round(magnitude, 2),
        'mark_90pct_rule': mark_90pct,
        'phase': phase,
        'tier': tier,
        'mark_says': says,
        'quarters_count': n,
    }


def detect_code_33(
    eps_growth_yoy_last_4q: list[float],
    sales_growth_yoy_last_4q: list[float],
    net_margin_last_4q: list[float],
) -> dict:
    """KARAR ADAY #855 — Mark Code 33 Detector (TLSMW s.173).

    Mark birebir (TLSMW s.173):
        "Code 33 situation: three quarters of acceleration in earnings, sales,
        AND profit margins. That's a potent recipe."

    Mark Monster Beverage (MNST) 2003-2005:
        "Classic Code 33 annual acceleration ... superperformance condition"

    Hesap (Mark KESIN ÜÇLÜ accel):
        - eps_accel: 3-quarter EPS YoY strict artış
        - sales_accel: 3-quarter satış YoY strict artış
        - margin_expanding: 3-quarter net margin strict artış
        - Tüm 3'ü PASS → CODE_33 elite tier

    Args:
        eps_growth_yoy_last_4q: Son 4 çeyrek EPS YoY büyüme
        sales_growth_yoy_last_4q: Son 4 çeyrek satış YoY büyüme
        net_margin_last_4q: Son 4 çeyrek net margin yüzdesi

    Returns:
        dict:
        {
            'pattern': 'CODE_33' | 'partial' | 'none',
            'eps_accel': bool,
            'sales_accel': bool,
            'margin_expanding': bool,
            'pass_count': int (0-3),
            'mark_says': str,
            'tier': 'elite' | 'partial_2' | 'partial_1' | 'none',
        }

    Kaynak: TLSMW s.173 + Sprint_4_bis_7_Mark_HASSAS_Tarama.md KARAR ADAY #855
    """
    def _strict_accel(arr: list[float]) -> bool:
        if not arr or len(arr) < MARK_EPS_ACCEL_MIN_QUARTERS + 1:
            return False
        return all(arr[i] < arr[i + 1] for i in range(len(arr) - 1))

    eps_accel = _strict_accel(eps_growth_yoy_last_4q)
    sales_accel = _strict_accel(sales_growth_yoy_last_4q)
    margin_expanding = _strict_accel(net_margin_last_4q)

    pass_count = sum([eps_accel, sales_accel, margin_expanding])

    if pass_count == 3:
        pattern = 'CODE_33'
        tier = 'elite'
        says = ("CODE 33 elite — EPS + Sales + Margin 3-quarter triple accel "
                "(Mark KESIN superperformance condition, TLSMW s.173)")
    elif pass_count == 2:
        pattern = 'partial'
        tier = 'partial_2'
        says = ("Partial Code 33 — 2/3 accel (eksik faktör Mark için kritik, "
                "TLSMW s.173 'potent recipe' için 3/3 ZORUNLU)")
    elif pass_count == 1:
        pattern = 'partial'
        tier = 'partial_1'
        says = "Sadece 1/3 accel — Mark Code 33 imzası YOK"
    else:
        pattern = 'none'
        tier = 'none'
        says = "Code 33 pattern yok — Mark superperformance condition eksik"

    return {
        'pattern': pattern,
        'eps_accel': eps_accel,
        'sales_accel': sales_accel,
        'margin_expanding': margin_expanding,
        'pass_count': pass_count,
        'mark_says': says,
        'tier': tier,
    }


# =============================================================================
# SPRINT 4-bis.7 — FAZ 2 GENİŞLETME — Mark Pattern Detectorlar
# Tescil: Vizyon v22.02 + v22.03
# KARAR ADAY #893 (Tennis Ball) + #882 (Volume Asymmetry) + #864 (Leader Behavior)
# Detay: notebook/Sprint_4_bis_7_Mark_HASSAS_Tarama.md
# =============================================================================

# Mark KESIN Tennis Ball sabitleri (TLSMW Ch 10 s.253-254)
TENNIS_BALL_PULLBACK_MAX_DAYS: int = 7         # "brief pullbacks 2-5 days even 1-2 weeks"
TENNIS_BALL_RECOVERY_MAX_DAYS: int = 14        # "recovers within 1-2 weeks"
EGG_PULLBACK_MIN_DAYS: int = 14                # Egg warning (>14 gün)

# Mark KESIN Volume Asymmetry sabitleri (TTLC Sec 1 + TLSMW s.234)
VOLUME_ASYMMETRY_HEALTHY_RATIO: float = 1.5    # Up/down vol ratio min healthy
VOLUME_ASYMMETRY_DISTRIBUTION_RATIO: float = 0.8  # < 0.8 = distribution warning

# Mark KESIN Leader Behavior Fingerprint (TLSMW s.184)
LEADER_ADVANCE_MIN_PCT: float = 15.0           # +15-20% advance
LEADER_ADVANCE_MAX_PCT: float = 25.0
LEADER_PULLBACK_MIN_PCT: float = 5.0           # -5-10% pullback
LEADER_PULLBACK_MAX_PCT: float = 12.0


def detect_tennis_ball(
    breakout_date_idx: int,
    daily_history: list[dict]
) -> dict:
    """KARAR ADAY #893 — Mark Tennis Ball Detector (TLSMW s.253-254).

    Mark birebir (William M. B. Berger / TLSMW s.253):
        "I want to own tennis balls, not eggs."

    Mark sağlıklı paten:
        - Breakout sonrası pullback 2-5 gün (max ~7)
        - Recovery 1-2 hafta (max ~14 gün)
        - Volume contraction during pullback, expand back to new highs

    Egg warning:
        - Pullback > 14 gün veya recovery yok = EGG (SAT)

    Args:
        breakout_date_idx: Breakout günü daily_history içindeki index
        daily_history: Liste, her item dict
            {'date': str, 'close': float, 'high': float, 'low': float, 'volume': int}
            Index 0 = en eski, index -1 = en yeni

    Returns:
        dict:
        {
            'pattern': 'TENNIS_BALL' | 'EGG' | 'STILL_RUNNING' | 'INVALID',
            'pullback_days': int | None,
            'recovery_days': int | None,
            'recovered': bool,
            'pullback_depth_pct': float | None,
            'mark_says': str,
        }

    Kaynak: TLSMW s.253-254 + Sprint_4_bis_7_Mark_HASSAS_Tarama.md KARAR ADAY #893
    """
    if not daily_history or breakout_date_idx < 0 or breakout_date_idx >= len(daily_history) - 1:
        return {
            'pattern': 'INVALID',
            'pullback_days': None,
            'recovery_days': None,
            'recovered': False,
            'pullback_depth_pct': None,
            'mark_says': 'Geçersiz breakout_date_idx veya boş history',
        }

    breakout_high = daily_history[breakout_date_idx].get('high', daily_history[breakout_date_idx].get('close', 0))
    if breakout_high <= 0:
        return {
            'pattern': 'INVALID',
            'pullback_days': None,
            'recovery_days': None,
            'recovered': False,
            'pullback_depth_pct': None,
            'mark_says': 'Geçersiz breakout_high',
        }

    # Pullback phase: closing price drop after breakout
    pullback_low = breakout_high
    pullback_low_idx = breakout_date_idx
    for i in range(breakout_date_idx + 1, len(daily_history)):
        close_i = daily_history[i].get('close', 0)
        if close_i < pullback_low:
            pullback_low = close_i
            pullback_low_idx = i

    pullback_days = pullback_low_idx - breakout_date_idx
    pullback_depth_pct = ((breakout_high - pullback_low) / breakout_high) * 100 if breakout_high > 0 else 0

    # No real pullback yet (still running up)
    if pullback_days == 0 or pullback_depth_pct < 1.0:
        return {
            'pattern': 'STILL_RUNNING',
            'pullback_days': 0,
            'recovery_days': None,
            'recovered': False,
            'pullback_depth_pct': 0.0,
            'mark_says': 'Henüz pullback yok — stock yükselişte',
        }

    # Recovery phase: close back above breakout_high after pullback_low_idx
    recovery_days = None
    recovered = False
    for j in range(pullback_low_idx + 1, len(daily_history)):
        close_j = daily_history[j].get('close', 0)
        if close_j > breakout_high:
            recovery_days = j - pullback_low_idx
            recovered = True
            break

    if not recovered:
        # Still in pullback or recovering
        if pullback_days > EGG_PULLBACK_MIN_DAYS:
            return {
                'pattern': 'EGG',
                'pullback_days': pullback_days,
                'recovery_days': None,
                'recovered': False,
                'pullback_depth_pct': round(pullback_depth_pct, 2),
                'mark_says': (f"Pullback {pullback_days} gün > {EGG_PULLBACK_MIN_DAYS} — "
                              f"EGG warning (Mark Berger TLSMW s.253: SAT)"),
            }
        return {
            'pattern': 'STILL_RUNNING',
            'pullback_days': pullback_days,
            'recovery_days': None,
            'recovered': False,
            'pullback_depth_pct': round(pullback_depth_pct, 2),
            'mark_says': f"Pullback {pullback_days} gün — recovery bekleniyor",
        }

    # Recovered — check tennis ball criteria
    total_cycle_days = pullback_days + recovery_days
    if (pullback_days <= TENNIS_BALL_PULLBACK_MAX_DAYS
            and recovery_days <= TENNIS_BALL_RECOVERY_MAX_DAYS):
        pattern = 'TENNIS_BALL'
        says = (f"Pullback {pullback_days}g + Recovery {recovery_days}g = TENNIS BALL "
                f"(Mark KESIN TLSMW s.253 healthy bounce)")
    else:
        pattern = 'EGG'
        says = (f"Pullback {pullback_days}g + Recovery {recovery_days}g — "
                f"Mark eşikleri ({TENNIS_BALL_PULLBACK_MAX_DAYS}/{TENNIS_BALL_RECOVERY_MAX_DAYS}) "
                f"aşıldı = EGG warning")

    return {
        'pattern': pattern,
        'pullback_days': pullback_days,
        'recovery_days': recovery_days,
        'recovered': True,
        'pullback_depth_pct': round(pullback_depth_pct, 2),
        'mark_says': says,
    }


def compute_volume_asymmetry(daily_history: list[dict], lookback_days: int = 20) -> dict:
    """KARAR ADAY #882 — Mark Volume Asymmetry Tracker (TLSMW s.234 + TTLC Sec 1).

    Mark birebir (TLSMW s.234):
        "Look for big up days that are larger and occur more frequently than
        big down days."

    Mark birebir (TTLC Sec 1):
        "Stocks under institutional accumulation almost always display this type
        of price action."

    Hesap:
        up_volume_avg = up gün hacim ortalaması (lookback içinde)
        down_volume_avg = down gün hacim ortalaması
        asymmetry_ratio = up_volume_avg / down_volume_avg

    Tier:
        >= 1.5: healthy accumulation (Mark KESIN)
        0.8-1.5: neutral
        < 0.8: distribution warning

    Args:
        daily_history: Liste, her item {'close': float, 'volume': int}
        lookback_days: Geriye bakış (varsayılan 20)

    Returns:
        dict:
        {
            'asymmetry_ratio': float,
            'up_days_count': int,
            'down_days_count': int,
            'up_volume_avg': float,
            'down_volume_avg': float,
            'tier': 'healthy' | 'neutral' | 'distribution',
            'mark_says': str,
        }

    Kaynak: TLSMW s.234 + TTLC Sec 1 + Sprint_4_bis_7_Mark_HASSAS_Tarama.md
    """
    if not daily_history or len(daily_history) < 3:
        return {
            'asymmetry_ratio': 0.0,
            'up_days_count': 0,
            'down_days_count': 0,
            'up_volume_avg': 0.0,
            'down_volume_avg': 0.0,
            'tier': 'invalid',
            'mark_says': 'Yetersiz veri (min 3 gün)',
        }

    # Lookback son N gün
    history = daily_history[-lookback_days:] if len(daily_history) > lookback_days else daily_history
    if len(history) < 3:
        return {
            'asymmetry_ratio': 0.0,
            'up_days_count': 0,
            'down_days_count': 0,
            'up_volume_avg': 0.0,
            'down_volume_avg': 0.0,
            'tier': 'invalid',
            'mark_says': 'Yetersiz veri',
        }

    up_volumes = []
    down_volumes = []
    prev_close = history[0].get('close', 0)
    for i in range(1, len(history)):
        close_i = history[i].get('close', 0)
        vol_i = history[i].get('volume', 0)
        if close_i > prev_close:
            up_volumes.append(vol_i)
        elif close_i < prev_close:
            down_volumes.append(vol_i)
        prev_close = close_i

    up_count = len(up_volumes)
    down_count = len(down_volumes)
    up_avg = sum(up_volumes) / up_count if up_count > 0 else 0.0
    down_avg = sum(down_volumes) / down_count if down_count > 0 else 1.0  # avoid div/0

    if down_avg == 0:
        # Tum down-gunler 0 hacim = sonsuz asimetri. float('inf') JSON serialize
        # EDILEMEZ (FastAPI 500 — /api/risk/volume-asymmetry). 99.0 = pratik sonsuz
        # (RBA adjusted_ratio ile ayni JSON-safe cap pateni).
        ratio = 99.0 if up_avg > 0 else 0.0
    else:
        ratio = up_avg / down_avg

    if ratio >= VOLUME_ASYMMETRY_HEALTHY_RATIO:
        tier = 'healthy'
        says = (f"Up/Down vol ratio {ratio:.2f} >= {VOLUME_ASYMMETRY_HEALTHY_RATIO} = "
                f"healthy institutional accumulation (Mark KESIN TLSMW s.234)")
    elif ratio < VOLUME_ASYMMETRY_DISTRIBUTION_RATIO:
        tier = 'distribution'
        says = (f"Up/Down vol ratio {ratio:.2f} < {VOLUME_ASYMMETRY_DISTRIBUTION_RATIO} = "
                f"DISTRIBUTION warning (institutional selling)")
    else:
        tier = 'neutral'
        says = f"Up/Down vol ratio {ratio:.2f} — neutral, henüz pattern yok"

    return {
        'asymmetry_ratio': round(ratio, 3),
        'up_days_count': up_count,
        'down_days_count': down_count,
        'up_volume_avg': round(up_avg, 0),
        'down_volume_avg': round(down_avg, 0),
        'tier': tier,
        'mark_says': says,
    }


def detect_leader_fingerprint(
    advance_segments: list[float],
    pullback_segments: list[float],
) -> dict:
    """KARAR ADAY #864 — Mark Leader Behavior Fingerprint (TLSMW s.184).

    Mark birebir (TLSMW s.184):
        "Leading stocks will generally advance 15 to 20 percent and then rest,
        during which time they may pull back 5 to 10 percent."

    Mark Humana 1977-1978 case study referans paten.

    Hesap:
        - advance_segments: Her rally segment'in yüzde kazancı (örn. [18.5, 22.1, 16.8])
        - pullback_segments: Her pullback segment'in yüzde kaybı (örn. [7.2, 9.5, 5.8])
        - Mark eşikleri ile karşılaştırma

    Tier:
        - 'leader_classic': Tüm advance %15-25, tüm pullback %5-12 (Mark KESIN ideal)
        - 'leader_partial': Çoğu segment Mark eşiğinde
        - 'not_leader': Mark eşiklerinin dışında çoğu

    Args:
        advance_segments: Rally yüzdeleri (pozitif)
        pullback_segments: Pullback yüzdeleri (pozitif, abs değer)

    Returns:
        dict

    Kaynak: TLSMW s.184 + Sprint_4_bis_7_Mark_HASSAS_Tarama.md KARAR ADAY #864
    """
    if not advance_segments or not pullback_segments:
        # P376 fix: INVALID dali normal dal ile AYNI anahtarlari dondurmeli; aksi
        # halde /api/risk/leader-fingerprint endpoint result['total_advances'] ->
        # KeyError -> 500 (bos segment girdisinde). total_advances/total_pullbacks/
        # advance_pct_in_range/pullback_pct_in_range eklendi (tutarli return kontrati).
        return {
            'pattern': 'INVALID',
            'advances_in_range': 0,
            'pullbacks_in_range': 0,
            'total_advances': 0,
            'total_pullbacks': 0,
            'advance_pct_in_range': 0.0,
            'pullback_pct_in_range': 0.0,
            'total_segments': 0,
            'tier': 'invalid',
            'mark_says': 'Yetersiz veri (advance + pullback gerekli)',
        }

    advances_in_range = sum(
        1 for a in advance_segments
        if LEADER_ADVANCE_MIN_PCT <= a <= LEADER_ADVANCE_MAX_PCT
    )
    pullbacks_in_range = sum(
        1 for p in pullback_segments
        if LEADER_PULLBACK_MIN_PCT <= p <= LEADER_PULLBACK_MAX_PCT
    )

    total_adv = len(advance_segments)
    total_pb = len(pullback_segments)
    total_segments = total_adv + total_pb

    adv_pct_in_range = (advances_in_range / total_adv * 100) if total_adv > 0 else 0
    pb_pct_in_range = (pullbacks_in_range / total_pb * 100) if total_pb > 0 else 0

    # Classic: Tümü Mark eşiğinde
    if (advances_in_range == total_adv and pullbacks_in_range == total_pb
            and total_adv >= 2 and total_pb >= 2):
        tier = 'leader_classic'
        pattern = 'LEADER_FINGERPRINT'
        says = (f"Tüm {total_adv} advance + {total_pb} pullback Mark eşiğinde "
                f"(advance %{LEADER_ADVANCE_MIN_PCT}-{LEADER_ADVANCE_MAX_PCT}, "
                f"pullback %{LEADER_PULLBACK_MIN_PCT}-{LEADER_PULLBACK_MAX_PCT}) "
                f"= LEADER (Mark KESIN TLSMW s.184)")
    elif adv_pct_in_range >= 60 and pb_pct_in_range >= 60:
        tier = 'leader_partial'
        pattern = 'LEADER_PARTIAL'
        says = (f"Advance %{adv_pct_in_range:.0f} + Pullback %{pb_pct_in_range:.0f} "
                f"Mark eşiğinde — partial leader pattern")
    else:
        tier = 'not_leader'
        pattern = 'NOT_LEADER'
        says = (f"Advance %{adv_pct_in_range:.0f} + Pullback %{pb_pct_in_range:.0f} "
                f"Mark eşiklerinin dışında — leader pattern yok")

    return {
        'pattern': pattern,
        'advances_in_range': advances_in_range,
        'pullbacks_in_range': pullbacks_in_range,
        'total_advances': total_adv,
        'total_pullbacks': total_pb,
        'advance_pct_in_range': round(adv_pct_in_range, 1),
        'pullback_pct_in_range': round(pb_pct_in_range, 1),
        'tier': tier,
        'mark_says': says,
    }


# =============================================================================
# SPRINT 4-bis.7 — FAZ 2 GENİŞLETME 2 — Mark New High + Darvas Box
# Tescil: Vizyon v22.04 + v22.05
# KARAR ADAY #876 (New High Pivot Mandate) + #874 (Darvas Box)
# Detay: notebook/Sprint_4_bis_7_Mark_HASSAS_Tarama.md
# =============================================================================

# Mark KESIN 52-week High sabitler (TLSMW Ch 10 s.221-224)
NEW_HIGH_PIVOT_THRESHOLD_PCT: float = 5.0       # Mark KESIN: 52w high %5 içinde
NEW_HIGH_BUY_READY_THRESHOLD_PCT: float = 2.0   # Yakın pivot trigger (elit)
NEW_HIGH_BREAKOUT_THRESHOLD_PCT: float = -1.0   # 52w high üstüne %1 = breakout

# Mark KESIN Darvas Box sabitler (TLSMW Ch 10 s.215)
DARVAS_MIN_WEEKS: int = 4                       # Mark "4-7 hafta flat base"
DARVAS_MAX_WEEKS: int = 7
DARVAS_MAX_RANGE_PCT: float = 15.0              # Mark "%10-15 max correction"
DARVAS_IDEAL_RANGE_PCT: float = 10.0            # Mark ideal


def check_new_high_pivot(
    current_price: float,
    high_52w: float
) -> dict:
    """KARAR ADAY #876 — Mark New High Pivot Mandate (TLSMW s.221-224).

    Mark birebir (TLSMW s.221-222):
        "A stock making a new 52-week high during the early stages of a fresh
        bull market could be a stellar performer in its infancy."

        "Why Buy Near a New High? ... A stock hitting a new high has NO
        OVERHEAD SUPPLY to contend with."

    Mark KESIN Watchlist'ten Focus'a geçiş kriterleri:
        - Stock 52-week high %5 içinde (Mark KESIN yakınlık)
        - %2 içinde = "Buy Ready" tier (elit pozisyon)
        - %1 üstünde = "Breakout" (Mark KESIN tetik)

    Mark KESIN case studies:
        - TASR +%540 → all-time high → +%1,800
        - MNST Ağu 2003 ATH → +%8,000
        - YHOO +%170 → ATH → +%4,300
        - MSFT 1989 ATH → 54-fold (5,400%)

    Args:
        current_price: Bugünkü kapanış fiyatı
        high_52w: 52-week high

    Returns:
        dict:
        {
            'tier': 'breakout' | 'buy_ready' | 'near_pivot' | 'too_far' | 'invalid',
            'distance_from_high_pct': float,  # Negatif = üstünde, pozitif = altında
            'mark_says': str,
            'is_actionable': bool,  # Mark Watchlist'ten Focus'a geçiş onayı
        }

    Kaynak: TLSMW s.221-224 + Sprint_4_bis_7_Mark_HASSAS_Tarama.md KARAR ADAY #876
    """
    if current_price <= 0 or high_52w <= 0:
        return {
            'tier': 'invalid',
            'distance_from_high_pct': 0.0,
            'mark_says': 'Geçersiz fiyat verisi',
            'is_actionable': False,
        }

    # Pozitif = high altında, negatif = high üstünde
    distance_pct = ((high_52w - current_price) / high_52w) * 100

    if distance_pct <= NEW_HIGH_BREAKOUT_THRESHOLD_PCT:
        # Stock 52w high üstüne çıktı (breakout)
        tier = 'breakout'
        says = (f"Stock 52w high %{abs(distance_pct):.1f} üstünde — "
                f"BREAKOUT (Mark KESIN no overhead supply, TLSMW s.221)")
        actionable = True
    elif distance_pct <= NEW_HIGH_BUY_READY_THRESHOLD_PCT:
        # %2 içinde — Buy Ready
        tier = 'buy_ready'
        says = (f"Stock 52w high %{distance_pct:.1f} altında — "
                f"BUY READY (Mark KESIN elit pozisyon)")
        actionable = True
    elif distance_pct <= NEW_HIGH_PIVOT_THRESHOLD_PCT:
        # %5 içinde — pivot mandate uyuyor
        tier = 'near_pivot'
        says = (f"Stock 52w high %{distance_pct:.1f} altında — "
                f"NEAR PIVOT (Mark KESIN Watchlist→Focus eşiği)")
        actionable = True
    else:
        # %5'ten uzakta — Mark KESIN pivot mandate ihlal
        tier = 'too_far'
        says = (f"Stock 52w high %{distance_pct:.1f} altında — "
                f"Mark KESIN %{NEW_HIGH_PIVOT_THRESHOLD_PCT} eşiği aşıldı, "
                f"Watchlist'te bekle (TLSMW s.222)")
        actionable = False

    return {
        'tier': tier,
        'distance_from_high_pct': round(distance_pct, 2),
        'mark_says': says,
        'is_actionable': actionable,
    }


def detect_darvas_box(price_volume_history: Optional[list[dict]]) -> dict:
    """KARAR ADAY #874 — Mark Darvas Box Detector (TLSMW s.215).

    Mark birebir (TLSMW s.215):
        "Flat base ... square-shaped Darvas box pattern, 4 to 7 weeks
        in duration ... 10 to 15 percent correction from high point to low point."

    Mark Darvas Box vs VCP:
        - Darvas Box: TIGHT BOX, no real contraction (range sabit)
        - VCP: Contraction Ts (halving)
        - Her ikisi de Mark KESIN — bazı stoklar Darvas Box, bazıları VCP

    Hesap:
        - Lookback: 4-7 hafta = 20-35 trading günü
        - Range: max(high) - min(low) / max(high)
        - %10-15 max = Darvas Box (Mark KESIN)

    Args:
        price_volume_history: PVH dict listesi (her item: close, high, low, volume)
            Index 0 = en eski, index -1 = en yeni

    Returns:
        dict:
        {
            'pattern': 'DARVAS_BOX' | 'DARVAS_LOOSE' | 'NOT_DARVAS' | 'INVALID',
            'duration_days': int,
            'range_pct': float,
            'tier': 'ideal' | 'acceptable' | 'too_loose' | 'too_short' | 'invalid',
            'mark_says': str,
        }

    Kaynak: TLSMW s.215 + Sprint_4_bis_7_Mark_HASSAS_Tarama.md KARAR ADAY #874
    """
    if not price_volume_history or len(price_volume_history) < DARVAS_MIN_WEEKS * 5:
        return {
            'pattern': 'INVALID',
            'duration_days': 0,
            'range_pct': 0.0,
            'tier': 'invalid',
            'mark_says': f'Yetersiz veri (min {DARVAS_MIN_WEEKS * 5} gün gerekli)',
        }

    # Son 4-7 hafta = 20-35 trading günü
    pvh = price_volume_history
    duration_days = min(len(pvh), DARVAS_MAX_WEEKS * 5)

    # Range: en yüksek high ve en düşük low
    window = pvh[-duration_days:]
    highs = [d.get('high', d.get('close', 0)) for d in window]
    lows = [d.get('low', d.get('close', 0)) for d in window]

    max_high = max(highs) if highs else 0
    min_low = min(lows) if lows else 0

    if max_high <= 0:
        return {
            'pattern': 'INVALID',
            'duration_days': duration_days,
            'range_pct': 0.0,
            'tier': 'invalid',
            'mark_says': 'Geçersiz fiyat (max_high <= 0)',
        }

    range_pct = ((max_high - min_low) / max_high) * 100

    # Süre kontrolü (hafta cinsi)
    weeks = duration_days / 5
    if weeks < DARVAS_MIN_WEEKS:
        return {
            'pattern': 'NOT_DARVAS',
            'duration_days': duration_days,
            'range_pct': round(range_pct, 2),
            'tier': 'too_short',
            'mark_says': (f"Süre {weeks:.1f} hafta < Mark min {DARVAS_MIN_WEEKS} "
                          f"(TLSMW s.215 — flat base 4-7 hafta)"),
        }

    # Range kontrolü
    if range_pct <= DARVAS_IDEAL_RANGE_PCT:
        pattern = 'DARVAS_BOX'
        tier = 'ideal'
        says = (f"Range %{range_pct:.1f} ≤ %{DARVAS_IDEAL_RANGE_PCT} — "
                f"DARVAS BOX ideal (Mark KESIN TLSMW s.215 tight box)")
    elif range_pct <= DARVAS_MAX_RANGE_PCT:
        pattern = 'DARVAS_BOX'
        tier = 'acceptable'
        says = (f"Range %{range_pct:.1f} — Mark eşiği içinde "
                f"(%{DARVAS_IDEAL_RANGE_PCT}-{DARVAS_MAX_RANGE_PCT})")
    elif range_pct <= 20.0:
        pattern = 'DARVAS_LOOSE'
        tier = 'too_loose'
        says = (f"Range %{range_pct:.1f} — Mark Darvas eşiği (%{DARVAS_MAX_RANGE_PCT}) "
                f"aşıldı, VCP kontrol et")
    else:
        pattern = 'NOT_DARVAS'
        tier = 'too_loose'
        says = (f"Range %{range_pct:.1f} >%20 — Darvas Box değil, "
                f"loose base (Mark TLSMW s.225 deep correction)")

    return {
        'pattern': pattern,
        'duration_days': duration_days,
        'range_pct': round(range_pct, 2),
        'tier': tier,
        'mark_says': says,
    }


# =============================================================================
# SPRINT 4-bis.7 — FAZ 2 GENİŞLETME 3 — Mark Pivot Disiplin + 20-DMA Hold
# Tescil: Vizyon v22.05
# KARAR ADAY #885 (Wait for Pivot) + #888 (20-DMA Hold)
# Detay: notebook/Sprint_4_bis_7_Mark_HASSAS_Tarama.md
# =============================================================================

# Mark KESIN Pivot disiplini sabitler (TTLC s.243)
PIVOT_TRIGGER_VOLUME_MULTIPLIER: float = 1.5    # Mark KESIN expanded volume
PIVOT_TRIGGER_PRICE_BUFFER_PCT: float = 0.2     # Pivot fiyatın %0.2 üstünde (gerçek kırılma)

# Mark KESIN 20-DMA Hold sabitler (TTLC s.247)
TWENTY_DMA_TOLERANCE_PCT: float = 0.5            # %0.5 altı = tolerans (intraday wiggle)
TWENTY_DMA_BREACH_DAYS_WARNING: int = 1          # 1 gün altı = uyarı
TWENTY_DMA_BREACH_DAYS_EXIT: int = 2             # 2 ardışık = exit signal


def check_pivot_trigger(
    current_price: float,
    pivot_price: float,
    today_volume: int,
    avg_volume_50d: int,
    today_high: float,
    today_low: float,
    today_close: float,
) -> dict:
    """KARAR ADAY #885 — Mark "Wait for Pivot" Discipline (TTLC s.243-244).

    Mark birebir (TTLC s.243):
        "Assuming that a stock will break out is dangerous. ... Let the stock
        break above the pivot and prove itself."

    Mark KESIN pivot tetik kriterleri:
        - Stock pivot_price üstüne çıkmalı (gerçek kırılma)
        - Volume expanding (50-DMA ortalamasının %150+ üstü)
        - Close upper half of day range (alım baskısı)

    Args:
        current_price: Mevcut fiyat (genelde close)
        pivot_price: Mark pivot point (base high veya cup-handle high)
        today_volume: Bugünkü hacim
        avg_volume_50d: 50-gün ortalama hacim
        today_high: Bugün en yüksek
        today_low: Bugün en düşük
        today_close: Bugün kapanış

    Returns:
        dict:
        {
            'should_buy': bool,
            'tier': 'triggered' | 'almost' | 'wait' | 'invalid',
            'above_pivot': bool,
            'volume_expanded': bool,
            'upper_half_close': bool,
            'mark_says': str,
        }

    Kaynak: TTLC s.243 + Sprint_4_bis_7_Mark_HASSAS_Tarama.md KARAR ADAY #885
    """
    if pivot_price <= 0 or current_price <= 0 or avg_volume_50d <= 0:
        return {
            'should_buy': False,
            'tier': 'invalid',
            'above_pivot': False,
            'volume_expanded': False,
            'upper_half_close': False,
            'mark_says': 'Geçersiz veri',
        }

    # 1) Above pivot (gerçek kırılma — Mark KESIN buffer)
    buffer_threshold = pivot_price * (1 + PIVOT_TRIGGER_PRICE_BUFFER_PCT / 100)
    above_pivot = current_price > buffer_threshold

    # 2) Volume expanded (Mark KESIN expanding volume)
    volume_expanded = today_volume > avg_volume_50d * PIVOT_TRIGGER_VOLUME_MULTIPLIER

    # 3) Upper half close (alım baskısı)
    if today_high > today_low:
        midpoint = (today_high + today_low) / 2
        upper_half_close = today_close > midpoint
    else:
        upper_half_close = False

    pass_count = sum([above_pivot, volume_expanded, upper_half_close])

    if pass_count == 3:
        should_buy = True
        tier = 'triggered'
        says = (f"3/3 Mark KESIN tetik — pivot ${pivot_price:.2f} üstünde + "
                f"hacim {today_volume/avg_volume_50d:.1f}x + upper half close = BUY (TTLC s.243)")
    elif pass_count == 2 and above_pivot:
        should_buy = False
        tier = 'almost'
        says = (f"2/3 — pivot tetiklendi ama hacim/upper half eksik. "
                f"Mark 'Wait for Pivot' disipline: 1 gün daha gözle")
    else:
        should_buy = False
        tier = 'wait'
        says = (f"{pass_count}/3 Mark eşiği eksik — 'Assume breakout YASAK' "
                f"(TTLC s.243), bekle")

    return {
        'should_buy': should_buy,
        'tier': tier,
        'above_pivot': above_pivot,
        'volume_expanded': volume_expanded,
        'upper_half_close': upper_half_close,
        'mark_says': says,
    }


def check_20dma_hold(
    closes_last_5d: list[float],
    sma_20_last_5d: list[float],
) -> dict:
    """KARAR ADAY #888 — Mark 20-DMA Hold Detector (TTLC s.247).

    Mark birebir (TTLC s.247):
        "Once the stock successfully breaks out, the stock price should hold
        its 20-day moving average and IN MOST CASES SHOULD NOT CLOSE BELOW IT."

    Mark KESIN warning system:
        - 1 gün 20-DMA altı kapanış = WARNING
        - 2 ardışık gün altı = EXIT SIGNAL
        - Heavy volume + altı = "even worse"

    Args:
        closes_last_5d: Son 5 günün kapanışları [day-4, day-3, day-2, day-1, today]
        sma_20_last_5d: Son 5 günün 20-DMA değerleri (aynı index)

    Returns:
        dict:
        {
            'status': 'safe' | 'warning' | 'exit_signal' | 'invalid',
            'days_below_20dma': int,
            'consecutive_days_below': int,
            'last_close_below': bool,
            'mark_says': str,
        }

    Kaynak: TTLC s.247 + Sprint_4_bis_7_Mark_HASSAS_Tarama.md KARAR ADAY #888
    """
    if (not closes_last_5d or not sma_20_last_5d
            or len(closes_last_5d) != len(sma_20_last_5d)
            or len(closes_last_5d) < 2):
        return {
            'status': 'invalid',
            'days_below_20dma': 0,
            'consecutive_days_below': 0,
            'last_close_below': False,
            'mark_says': 'Yetersiz veri (en az 2 gün + eşit uzunluk)',
        }

    # Her gün için 20-DMA altında mı kontrol
    below_flags = []
    for close, sma in zip(closes_last_5d, sma_20_last_5d):
        if sma <= 0:
            below_flags.append(False)
            continue
        # Tolerance buffer (intraday wiggle)
        threshold = sma * (1 - TWENTY_DMA_TOLERANCE_PCT / 100)
        below_flags.append(close < threshold)

    days_below = sum(below_flags)
    last_close_below = below_flags[-1]

    # Ardışık gün hesabı (en yakın geçmişten geriye)
    consecutive = 0
    for flag in reversed(below_flags):
        if flag:
            consecutive += 1
        else:
            break

    if consecutive >= TWENTY_DMA_BREACH_DAYS_EXIT:
        status = 'exit_signal'
        says = (f"{consecutive} ardışık gün 20-DMA altı = EXIT SIGNAL "
                f"(Mark KESIN TTLC s.247 — should NOT close below)")
    elif last_close_below:
        status = 'warning'
        says = (f"Bugün 20-DMA altında kapanış — WARNING "
                f"(Mark TTLC s.247, 2 ardışık = exit)")
    else:
        status = 'safe'
        says = f"20-DMA üzerinde kapanış — Mark KESIN ideal post-breakout davranış"

    return {
        'status': status,
        'days_below_20dma': days_below,
        'consecutive_days_below': consecutive,
        'last_close_below': last_close_below,
        'mark_says': says,
    }


# =============================================================================
# SPRINT 4-bis.7 — FAZ 2 SON — Earnings Gap Breakout (KARAR ADAY #890)
# Tescil: Vizyon v22.05
# Detay: notebook/Sprint_4_bis_7_Mark_HASSAS_Tarama.md
# =============================================================================

# Mark KESIN Earnings Gap sabitler (TLSMW s.250 — Foster Wheeler örneği)
EARNINGS_GAP_MIN_PCT: float = 3.0                # %3+ gap (Mark KESIN anlamlı)
EARNINGS_GAP_BIG_PCT: float = 7.0                # %7+ büyük gap
EARNINGS_VOLUME_MIN_MULTIPLIER: float = 2.0      # 2x avg vol (Mark KESIN 2-10x range)
EARNINGS_CLOSE_HIGH_THRESHOLD_PCT: float = 80.0  # Close gün yüksekinin %80+ (alım baskısı)


def detect_earnings_gap_breakout(
    pre_earnings_close: float,
    earnings_day_open: float,
    earnings_day_high: float,
    earnings_day_low: float,
    earnings_day_close: float,
    earnings_day_volume: int,
    avg_volume_50d: int,
) -> dict:
    """KARAR ADAY #890 — Mark Earnings Gap Breakout Detector (TLSMW s.250).

    Mark birebir (TLSMW s.250):
        "In April 2007, I purchased Foster Wheeler on a gap fueled by a great
        earnings report announced in the morning. Note the **huge breakout
        volume** and the **close right at the high of the day**. It then rose
        180 percent in nine months from this point."

    Mark KESIN earnings gap kriterleri:
        1. Gap up: open > previous close (%3+ minimum, %7+ büyük)
        2. Huge breakout volume (2-10x avg 50-DMA)
        3. Close at upper portion of day range (alım baskısı, %80+ ideal)

    Mark + Dick's Sporting Goods 2003 patterns aynı (s.222):
        "Gapped up big after positive forward guidance. Overwhelming volume."

    Args:
        pre_earnings_close: Earnings'ten önceki kapanış
        earnings_day_open: Earnings günü açılış
        earnings_day_high: Earnings günü en yüksek
        earnings_day_low: Earnings günü en düşük
        earnings_day_close: Earnings günü kapanış
        earnings_day_volume: Earnings günü hacim
        avg_volume_50d: 50-gün ortalama hacim

    Returns:
        dict:
        {
            'pattern': 'EARNINGS_GAP_BREAKOUT' | 'EARNINGS_GAP_MILD' | 'NO_GAP' | 'INVALID',
            'gap_pct': float,         # Open vs previous close
            'volume_multiplier': float,
            'close_position_pct': float,  # Close gün range içinde nerede (0-100)
            'tier': 'institutional' | 'mild' | 'weak' | 'invalid',
            'mark_says': str,
        }

    Kaynak: TLSMW s.250 + Sprint_4_bis_7_Mark_HASSAS_Tarama.md KARAR ADAY #890
    """
    if (pre_earnings_close <= 0 or earnings_day_open <= 0
            or earnings_day_high <= 0 or earnings_day_close <= 0
            or earnings_day_volume <= 0 or avg_volume_50d <= 0):
        return {
            'pattern': 'INVALID',
            'gap_pct': 0.0,
            'volume_multiplier': 0.0,
            'close_position_pct': 0.0,
            'tier': 'invalid',
            'mark_says': 'Geçersiz veri (sıfır veya negatif)',
        }

    # 1) Gap percentage
    gap_pct = ((earnings_day_open - pre_earnings_close) / pre_earnings_close) * 100

    # 2) Volume multiplier
    volume_multiplier = earnings_day_volume / avg_volume_50d

    # 3) Close position within day range (Mark "close at high")
    if earnings_day_high > earnings_day_low:
        close_position_pct = ((earnings_day_close - earnings_day_low) /
                              (earnings_day_high - earnings_day_low)) * 100
    else:
        close_position_pct = 50.0  # neutral

    # Tier classification
    if gap_pct < EARNINGS_GAP_MIN_PCT:
        pattern = 'NO_GAP'
        tier = 'weak'
        says = (f"Gap %{gap_pct:.1f} < Mark min %{EARNINGS_GAP_MIN_PCT} — "
                f"earnings reaction zayıf, paten yok")
    elif (gap_pct >= EARNINGS_GAP_BIG_PCT
            and volume_multiplier >= EARNINGS_VOLUME_MIN_MULTIPLIER
            and close_position_pct >= EARNINGS_CLOSE_HIGH_THRESHOLD_PCT):
        # Full Mark KESIN (Foster Wheeler paten)
        pattern = 'EARNINGS_GAP_BREAKOUT'
        tier = 'institutional'
        says = (f"Gap %{gap_pct:.1f} + Volume {volume_multiplier:.1f}x + "
                f"Close %{close_position_pct:.0f} day high = INSTITUTIONAL EARNINGS GAP "
                f"(Mark KESIN Foster Wheeler paten, TLSMW s.250)")
    elif (gap_pct >= EARNINGS_GAP_MIN_PCT
            and volume_multiplier >= EARNINGS_VOLUME_MIN_MULTIPLIER):
        pattern = 'EARNINGS_GAP_MILD'
        tier = 'mild'
        says = (f"Gap %{gap_pct:.1f} + Volume {volume_multiplier:.1f}x — "
                f"earnings gap var ama close pozisyonu zayıf (Mark KESIN için %80+ gerekli)")
    else:
        pattern = 'EARNINGS_GAP_MILD'
        tier = 'weak'
        says = (f"Gap %{gap_pct:.1f} ama volume {volume_multiplier:.1f}x < "
                f"Mark min {EARNINGS_VOLUME_MIN_MULTIPLIER}x — institutional değil")

    return {
        'pattern': pattern,
        'gap_pct': round(gap_pct, 2),
        'volume_multiplier': round(volume_multiplier, 2),
        'close_position_pct': round(close_position_pct, 1),
        'tier': tier,
        'mark_says': says,
    }


# =============================================================================
# KARAR #732 — Mark Pyramid Tier Calculator (KARAR #487 v20.98 Mark birebir)
# =============================================================================

def compute_pyramid_tier(
    position_value: float,
    portfolio_value: float,
    prev_tier_profitable: bool = False,
) -> dict:
    """Mark Pyramid 3-Tier tier tespit + Mark X "Trades Working" güvenlik kilidi.

    KARAR #487 (Vizyon v20.98) Mark birebir kaynak zinciri:
    - PILOT     %1-3 (TraderLion Lesson 7) - zayıf piyasa / nakitten giriş
    - STANDARD  %6.25-12.5 (Brandon Video) - normal piyasa + trades working
    - FULL      %15-25 (Mark Video Kelly 2:1) - peak market + 60% win rate
    - Mark X kilit: önceki tier kâra geçmeden bir üst tier'a YASAK

    Args:
        position_value: Pozisyon $ (entry_price * shares)
        portfolio_value: Toplam portföy $
        prev_tier_profitable: Pilot/Standart kâra geçti mi (Mark X kilit)

    Returns:
        dict:
        {
            'tier': 'BELOW_PILOT' | 'PILOT' | 'STANDARD' | 'FULL' | 'OVER_MAX',
            'position_pct': float (0-100),
            'severity': 'ok' | 'info' | 'warn' | 'violation',
            'next_tier': 'PILOT' | 'STANDARD' | 'FULL' | None,
            'mark_says': str (felsefe yorumu),
        }

    Kaynak: notebook Vizyon v20.98 KARAR #487 + scripts/Sprint_4_bis_7
    """
    if portfolio_value <= 0 or position_value <= 0:
        return {
            'tier': 'BELOW_PILOT',
            'position_pct': 0.0,
            'severity': 'info',
            'next_tier': 'PILOT',
            'mark_says': 'Pozisyon değeri sıfır — Pilot tier (%1-3) ile başla.',
        }

    pct = (position_value / portfolio_value) * 100.0
    pilot_min, pilot_max = MARK_PYRAMID_PILOT_PCT_RANGE
    std_min, std_max = MARK_PYRAMID_STANDARD_PCT_RANGE
    full_min, full_max = MARK_PYRAMID_FULL_PCT_RANGE

    if pct < pilot_min:
        return {
            'tier': 'BELOW_PILOT', 'position_pct': round(pct, 2),
            'severity': 'info', 'next_tier': 'PILOT',
            'mark_says': 'Pilot tier altinda. Mark felsefesi: %1-3 pilot ile basla.',
        }
    if pct <= pilot_max:
        return {
            'tier': 'PILOT', 'position_pct': round(pct, 2),
            'severity': 'ok', 'next_tier': 'STANDARD',
            'mark_says': 'Pilot tier (%1-3). Mark TraderLion: nakitten ilk giris icin ideal.',
        }
    if pct < std_min:
        # Pilot/Standart arasi - Mark X kilit denetle
        return {
            'tier': 'PILOT', 'position_pct': round(pct, 2),
            'severity': 'info' if prev_tier_profitable else 'warn',
            'next_tier': 'STANDARD',
            'mark_says': (
                'Pilot ustu, Standart alti - pilot kara gecti, Standart hazir.'
                if prev_tier_profitable
                else "Pilot ustu, Standart alti - Mark X: pilot kara gecmeden Standart YASAK."
            ),
        }
    if pct <= std_max:
        return {
            'tier': 'STANDARD', 'position_pct': round(pct, 2),
            'severity': 'ok' if prev_tier_profitable else 'warn',
            'next_tier': 'FULL',
            'mark_says': (
                'Standart tier (%6.25-12.5). Mark Brandon: normal piyasa + trades working.'
                if prev_tier_profitable
                else "Standart tier - Mark X kilit: pilot kara gecmedi, risk arttirma riskli."
            ),
        }
    if pct < full_min:
        return {
            'tier': 'STANDARD', 'position_pct': round(pct, 2),
            'severity': 'info' if prev_tier_profitable else 'warn',
            'next_tier': 'FULL',
            'mark_says': (
                'Standart ustu, Full alti - Standart kara gecti, Full hazir.'
                if prev_tier_profitable
                else "Standart ustu, Full alti - Mark X: standart kara gecmemis."
            ),
        }
    if pct <= full_max:
        return {
            'tier': 'FULL', 'position_pct': round(pct, 2),
            'severity': 'ok' if prev_tier_profitable else 'warn',
            'next_tier': None,
            'mark_says': (
                'Full tier (%15-25). Mark Kelly 2:1: peak market + 60% win rate.'
                if prev_tier_profitable
                else "Full tier - Mark X: onceki tier kara gecmedi, asiri risk."
            ),
        }
    return {
        'tier': 'OVER_MAX', 'position_pct': round(pct, 2),
        'severity': 'violation', 'next_tier': None,
        'mark_says': (
            f'%{pct:.1f} - Mark MAX. MARK_POSITION_MAX_PCT={MARK_POSITION_MAX_PCT}% sert tavan, '
            'tier ustu asiri risk.'
        ),
    }


# =============================================================================
# KARAR #488 — Distribution Day Detector (O'Neil/Mark mekanik)
# =============================================================================

# O'Neil orijinal eşik (KALICI İLKE #4 birebir):
DISTRIBUTION_DAY_CLOSE_THRESHOLD_PCT: float = -0.2  # close <= -0.2% (O'Neil)
DISTRIBUTION_DAY_LOOKBACK_DAYS: int = 20             # 4-5 hafta penceresi


def count_distribution_days(
    closes: list[float],
    volumes: list[int],
    lookback_days: int = DISTRIBUTION_DAY_LOOKBACK_DAYS,
) -> dict:
    """Mark KARAR #488 — Distribution Day sayimi (O'Neil mekanik).

    O'Neil birebir kriter (TLSMW + Mark Regime referansi):
    - Close <= -0.2% (gun bazli yuzde dusus)
    - Volume bir onceki gunden YUKSEK (institutional satis kaniti)
    - Lookback 4-5 hafta (~20 trade gunu)

    Mark KARAR #488 4-Katman threshold:
    - 0-2 DD -> HEALTHY
    - 3 DD   -> CAUTION
    - 4 DD   -> UNDER_PRESSURE (Hard Filter)
    - 5+ DD  -> BEAR_PRESSURE

    Args:
        closes:  Tarihsel close fiyatlari (eskiden yeniye, son eleman bugun)
        volumes: Tarihsel hacim (closes ile esit uzunluk)
        lookback_days: Pencere (default 20)

    Returns:
        dict:
        {
            'count': int (DD sayimi son pencere),
            'lookback': int,
            'regime_hint': 'HEALTHY' | 'CAUTION' | 'UNDER_PRESSURE' | 'BEAR_PRESSURE',
            'mark_says': str,
        }

    Kaynak: Vizyon v20.99 KARAR #488 O'Neil + Mark
    """
    if not closes or len(closes) != len(volumes):
        return {
            'count': 0, 'lookback': lookback_days,
            'regime_hint': 'HEALTHY',
            'mark_says': 'Yetersiz veri - DD sayimi yapilmadi (closes/volumes esit uzunluk gerek).',
        }

    n = len(closes)
    if n < 2:
        return {
            'count': 0, 'lookback': lookback_days,
            'regime_hint': 'HEALTHY',
            'mark_says': 'En az 2 gun veri gerek (DD karsilastirma icin).',
        }

    # Son lookback_days pencere (1+ gunluk veri gerekli)
    start_idx = max(1, n - lookback_days)
    dd_count = 0
    for i in range(start_idx, n):
        prev_close = closes[i - 1]
        curr_close = closes[i]
        prev_vol = volumes[i - 1]
        curr_vol = volumes[i]
        if prev_close <= 0:
            continue
        pct_change = ((curr_close - prev_close) / prev_close) * 100.0
        # O'Neil DD: close <= -0.2% + volume > onceki gun
        if pct_change <= DISTRIBUTION_DAY_CLOSE_THRESHOLD_PCT and curr_vol > prev_vol:
            dd_count += 1

    # Mark KARAR #488 4-katman mapping
    if dd_count <= 2:
        regime = 'HEALTHY'
        says = f'{dd_count} DD - Mark Regime HEALTHY (0-2 DD). Yeni alim izinli.'
    elif dd_count == 3:
        regime = 'CAUTION'
        says = f'{dd_count} DD - Mark Regime CAUTION. Yeni alim siki kriter.'
    elif dd_count == 4:
        regime = 'UNDER_PRESSURE'
        says = f'{dd_count} DD - Mark Regime UNDER_PRESSURE (O\'Neil Hard Filter). Yeni alim YASAK.'
    else:
        regime = 'BEAR_PRESSURE'
        says = f'{dd_count} DD - Mark Regime BEAR_PRESSURE (5+ DD). %25 max veya nakit.'

    return {
        'count': dd_count,
        'lookback': lookback_days,
        'regime_hint': regime,
        'mark_says': says,
    }


# =============================================================================
# Carr Stage Analysis Detector (Stan Weinstein 4-Stage, Notebook kitaplar/Carr.md)
# =============================================================================

# Stan Weinstein 4-stage (Carr Stage Analizi):
# Stage 1: Basing (30W MA flat, price near MA)         - hazırlık
# Stage 2: Advancing (price > 30W MA + slope positive) - alım fazı
# Stage 3: Topping (slope flattening + volume div)     - çıkış hazırlık
# Stage 4: Declining (price < 30W MA + slope negative) - uzak dur

CARR_STAGE_MA_WINDOW: int = 150  # 30 hafta × 5 gün
CARR_STAGE_SLOPE_FLAT_PCT: float = 1.0  # 30W MA yıllık eğim mutlak < %1 = flat
CARR_STAGE_PROXIMITY_PCT: float = 5.0   # ±%5 = "near MA" (Stage 1/3 testi)


def compute_carr_stage(
    closes: list[float],
    volumes: list[int],
    ma_window: int = CARR_STAGE_MA_WINDOW,
) -> dict:
    """Carr Stage Analizi — Stan Weinstein 4-Stage Detector.

    Notebook kaynak: notebook/kitaplar/Carr.md (Carr stratejisi tescili).
    Quanfina Carr Stage Analizi NotebookLM (Aşama 4.1).

    Stage tanımları:
    - 1 (Basing):    30W MA flat (slope ≈ 0), price ±%5 MA yakını
    - 2 (Advancing): price > 30W MA + slope > 0 (Mark + Carr kabul fazı)
    - 3 (Topping):   slope flattening (positive ama düşüyor), hacim diverjans
    - 4 (Declining): price < 30W MA + slope < 0 (Mark "Stage 4 = uzak dur")

    Args:
        closes: Tarihsel close fiyatları (eskiden yeniye, son eleman bugün)
        volumes: Tarihsel hacim (closes ile eşit uzunluk)
        ma_window: 30W MA pencere (default 150 gün)

    Returns:
        dict:
        {
            'stage': 1 | 2 | 3 | 4 | None,
            'stage_label': str,
            'ma_value': float | None,
            'price_vs_ma_pct': float | None,
            'slope_pct_per_year': float | None,
            'mark_says': str,
        }

    Kaynak: notebook/kitaplar/Carr.md + Mark TTLC Stage 2 felsefesi
    """
    n = len(closes)
    if n < ma_window or len(volumes) != n:
        return {
            'stage': None,
            'stage_label': 'Belirsiz',
            'ma_value': None,
            'price_vs_ma_pct': None,
            'slope_pct_per_year': None,
            'mark_says': f'Yetersiz veri — min {ma_window} gun close gerek.',
        }

    # 30W SMA hesap (son ma_window kapanış ortalama)
    ma_value = sum(closes[-ma_window:]) / ma_window
    current_price = closes[-1]
    price_vs_ma_pct = ((current_price - ma_value) / ma_value) * 100.0

    # Slope: son 50 gunluk MA degisim orani (yıllık % normalize)
    # Slope = (MA_today - MA_50_days_ago) / MA_50_days_ago * 100 * (252/50)
    slope_window = min(50, n - ma_window)
    if slope_window > 5:
        ma_past = sum(closes[-(ma_window + slope_window):-slope_window]) / ma_window
        slope_raw_pct = ((ma_value - ma_past) / ma_past) * 100.0
        slope_pct_per_year = slope_raw_pct * (252.0 / slope_window)
    else:
        slope_pct_per_year = 0.0

    # Stage tespit (Carr/Weinstein mekanik)
    is_above_ma = price_vs_ma_pct > 0
    is_below_ma = price_vs_ma_pct < 0
    is_near_ma = abs(price_vs_ma_pct) <= CARR_STAGE_PROXIMITY_PCT
    is_slope_positive = slope_pct_per_year > CARR_STAGE_SLOPE_FLAT_PCT
    is_slope_negative = slope_pct_per_year < -CARR_STAGE_SLOPE_FLAT_PCT
    is_slope_flat = not is_slope_positive and not is_slope_negative

    # Stage 4: price < MA + slope negative (en kritik, Mark "uzak dur")
    if is_below_ma and is_slope_negative:
        stage = 4
        label = 'Stage 4 (Declining)'
        says = ('Stage 4 Declining — price 30W MA alti + slope negatif. '
                'Mark felsefesi: bu hisseden UZAK DUR. Carr stratejisi de aynı.')
    # Stage 2: price > MA + slope positive (Mark/Carr alim fazi)
    elif is_above_ma and is_slope_positive:
        stage = 2
        label = 'Stage 2 (Advancing)'
        says = ('Stage 2 Advancing — price 30W MA ustu + slope pozitif. '
                'Mark + Carr kabul fazi: superperformance hisseleri burada.')
    # Stage 3: above MA ama slope flattening (volume diverjansi kontrolu basitleştirilmiş)
    elif is_above_ma and is_slope_flat:
        stage = 3
        label = 'Stage 3 (Topping)'
        says = ('Stage 3 Topping — price MA ustu ama slope dusuyor. '
                'Carr: cikis hazirlik, kar kilitleme. Mark: trail stop sıkı.')
    # Stage 1: near MA + slope flat (basing/consolidation)
    elif is_near_ma and is_slope_flat:
        stage = 1
        label = 'Stage 1 (Basing)'
        says = ('Stage 1 Basing — 30W MA flat + price MA yakini. '
                'Carr: hazirlik fazi, breakout izle. Mark: pivot tetiklemesi bekle.')
    # Slope negatif ama near/above MA (bozulan trend)
    elif is_slope_negative:
        stage = 3 if is_above_ma else 4
        if stage == 3:
            label = 'Stage 3 (Erken Topping)'
            says = ('Stage 3 erken topping — slope negative, price hala MA ustu. '
                    'Yakinda Stage 4 riskli, Carr: cikis simdi.')
        else:
            label = 'Stage 4 (Erken Declining)'
            says = ('Stage 4 yaklasti — slope negatif, price MA yaklasiyor. Mark: uzak dur.')
    # Belirsiz - slope pozitif ama MA altı (toparlanma başlangıcı?)
    elif is_below_ma and is_slope_positive:
        stage = 1
        label = 'Stage 1 (Toparlanma)'
        says = ('Stage 1 toparlanma — price MA alti ama slope pozitif. '
                'Carr: hala basing, henuz Stage 2 onayi yok.')
    else:
        stage = None
        label = 'Belirsiz'
        says = f'Carr stage net degil — price MA {price_vs_ma_pct:+.1f}%, slope {slope_pct_per_year:+.1f}%/yr.'

    return {
        'stage': stage,
        'stage_label': label,
        'ma_value': round(ma_value, 2),
        'price_vs_ma_pct': round(price_vs_ma_pct, 2),
        'slope_pct_per_year': round(slope_pct_per_year, 2),
        'mark_says': says,
    }


# ======================================================================
# KARAR #733 alt-paket (Paket 51, 25 May 2026) — Market Breadth
#
# Mark+O'Neil canon: market breadth = listedeki hisselerin yukselen/dusen
# orani. A/D Line (Advance/Decline) ile birikimli sayim. KARAR #488 4-Katman
# Mark Regime'i destekleyen ek metrik — Distribution Days + Market Breadth
# birlikte daha guvenilir regime tespiti.
#
# Mark birebir kaynak (TLSMW Ch 5 + O'Neil HowToMakeMoney):
# - "When the A/D line breaks down before the index, that's a warning"
# - "Healthy markets show advances exceeding declines on volume"
#
# 3 metrik:
# - ad_ratio: bugun advance / decline orani (>1 saglikli)
# - ad_line_cumulative: 20-gun ardisik birikimli A/D
# - breadth_health: 'STRONG' (>1.5) / 'NEUTRAL' (0.8-1.5) / 'WEAK' (<0.8)
# ======================================================================

# A/D ratio esikleri (KARAR #488 paralel)
BREADTH_STRONG_THRESHOLD: float = 1.5   # advance/decline > 1.5 = saglikli
BREADTH_WEAK_THRESHOLD: float = 0.8     # advance/decline < 0.8 = zayif
BREADTH_LOOKBACK_DAYS: int = 20         # A/D Line pencere


def compute_market_breadth(
    advances_daily: list[int],
    declines_daily: list[int],
    lookback_days: int = BREADTH_LOOKBACK_DAYS,
) -> dict:
    """Market Breadth A/D Line + ratio + Mark felsefe yorum.

    Args:
        advances_daily: Gunluk yukselen hisse sayisi (kronolojik, eski->yeni)
        declines_daily: Gunluk dusen hisse sayisi (ayni siralama)
        lookback_days: Birikimli A/D Line penceresi (default 20)

    Returns:
        dict {
            'ad_ratio': float | None,           # Bugun adv/dec
            'ad_line_cumulative': int | None,   # 20-gun birikimli (adv-dec)
            'breadth_health': str | None,       # STRONG/NEUTRAL/WEAK
            'mark_says': str,                   # Mark felsefe yorum
        }

    Mark birebir kaynak:
    - TLSMW Ch 5: "Healthy markets show advances exceeding declines on volume"
    - O'Neil HowToMakeMoney: "A/D line breaks down before index = warning"

    Production'da Cloud SQL minervini_scans + sector_rotation tablosundan
    gunluk advance/decline sayim alinir (AÇIK KONU #75 yfinance pipeline).
    """
    if not advances_daily or not declines_daily:
        return {
            'ad_ratio': None,
            'ad_line_cumulative': None,
            'breadth_health': None,
            'mark_says': 'Market breadth verisi yok — A/D Line hesaplanamiyor.',
        }
    if len(advances_daily) != len(declines_daily):
        return {
            'ad_ratio': None,
            'ad_line_cumulative': None,
            'breadth_health': None,
            'mark_says': 'Market breadth: advances ve declines listesi uzunlugu farkli.',
        }

    # Bugunku A/D ratio (son gun)
    today_adv = advances_daily[-1]
    today_dec = declines_daily[-1]
    # today_dec == 0 (sifir declines, cok guclu gun) = sonsuz A/D ratio. float('inf')
    # JSON serialize EDILEMEZ (FastAPI 500 — /api/market/status). 99.0 = pratik sonsuz
    # (RBA adjusted_ratio + volume_asymmetry ile ayni JSON-safe cap pateni).
    ad_ratio = round(today_adv / today_dec, 2) if today_dec > 0 else 99.0

    # 20-gun birikimli A/D Line (advance - decline toplamı)
    lookback = min(lookback_days, len(advances_daily))
    recent_adv = advances_daily[-lookback:]
    recent_dec = declines_daily[-lookback:]
    ad_line_cumulative = sum(recent_adv) - sum(recent_dec)

    # Saglik kategorisi
    if ad_ratio >= BREADTH_STRONG_THRESHOLD:
        breadth_health = 'STRONG'
        says = (f'Market Breadth STRONG (A/D {ad_ratio:.2f}) — Mark: advances '
                f'exceed declines, saglikli rally. 20-gun A/D cumulative '
                f'+{ad_line_cumulative}. Yeni alim onaylar.')
    elif ad_ratio >= BREADTH_WEAK_THRESHOLD:
        breadth_health = 'NEUTRAL'
        says = (f'Market Breadth NEUTRAL (A/D {ad_ratio:.2f}) — karisik sinyal. '
                f'20-gun A/D {ad_line_cumulative:+d}. Mark: index ralliyse '
                f'ama A/D zayifsa, ayrismaya dikkat.')
    else:
        breadth_health = 'WEAK'
        says = (f'Market Breadth WEAK (A/D {ad_ratio:.2f}) — declines exceed '
                f'advances. 20-gun A/D {ad_line_cumulative:+d}. O\'Neil: A/D '
                f'index oncesi zayifliyorsa erken uyari.')

    return {
        'ad_ratio': ad_ratio,
        'ad_line_cumulative': ad_line_cumulative,
        'breadth_health': breadth_health,
        'mark_says': says,
    }


# ======================================================================
# KARAR #733 alt-paket (Paket 56, 25 May 2026) — Breadth Divergence
#
# Mark+O'Neil canon "erken uyari" mekanigi: Index (SPY/QQQ) yukseliyor
# ama A/D Line zayifliyorsa BEARISH DIVERGENCE — gercek piyasa lideri
# darling birkac mega-cap, geniş tabanli rally olmuyor. Tarihsel:
# 1999 dot-com, 2007 housing — index pozitif iken A/D Line aylar
# oncesi tepe yapip dustu.
#
# 4 kategori:
# - CONFIRMED_UP: Index up + A/D up → saglikli rally (Mark Stage 2 onay)
# - BEARISH_DIVERGENCE: Index up + A/D down → KRITIK uyari (O'Neil)
# - BULLISH_DIVERGENCE: Index down + A/D up → toparlanma adayi
# - CONFIRMED_DOWN: Index down + A/D down → bear pressure dogrulandi
# - NEUTRAL: Index/AD trend belirsiz (esik altinda) → bekle
#
# Mark birebir kaynak:
# - O'Neil HowToMakeMoney: "Best stocks make lows first" + A/D erken uyari
# - TLSMW Ch 5: "Healthy markets show broad participation"
# ======================================================================

# Divergence trend esik (% degisim)
DIVERGENCE_INDEX_TREND_THRESHOLD_PCT: float = 1.0   # Index >+1% up, <-1% down
DIVERGENCE_AD_TREND_THRESHOLD: int = 500            # A/D cumulative >+500 up, <-500 down


def compute_breadth_divergence(
    index_closes: list[float],
    advances_daily: list[int],
    declines_daily: list[int],
    lookback_days: int = 10,
) -> dict:
    """Index trend vs A/D Line trend ayrismasi tespiti — Mark+O'Neil erken uyari.

    Args:
        index_closes: SPY/QQQ gunluk kapanis fiyatlari (kronolojik)
        advances_daily: Gunluk yukselen hisse sayisi
        declines_daily: Gunluk dusen hisse sayisi
        lookback_days: Trend hesap penceresi (default 10 gun)

    Returns:
        dict {
            'divergence': str | None,        # CONFIRMED_UP/BEARISH_DIVERGENCE/...
            'index_change_pct': float | None, # lookback boyunca % degisim
            'ad_trend_delta': int | None,     # lookback boyunca A/D cumulative delta
            'severity': str,                  # ok / info / warn / critical
            'mark_says': str,
        }

    Mark birebir kaynak:
    - O'Neil HowToMakeMoney: "Best stocks make lows first" (A/D index oncesi)
    - TLSMW Ch 5: "Healthy markets show broad participation"
    """
    # Edge: bos veya yetersiz veri
    if not index_closes or not advances_daily or not declines_daily:
        return {
            'divergence': None,
            'index_change_pct': None,
            'ad_trend_delta': None,
            'severity': 'info',
            'mark_says': 'Yetersiz veri — index/A/D trend hesaplanamiyor.',
        }
    if len(advances_daily) != len(declines_daily):
        return {
            'divergence': None,
            'index_change_pct': None,
            'ad_trend_delta': None,
            'severity': 'info',
            'mark_says': 'Divergence: advances/declines listesi uzunlugu farkli.',
        }

    # Lookback window
    n_index = min(lookback_days, len(index_closes) - 1)
    n_breadth = min(lookback_days, len(advances_daily))
    if n_index < 2 or n_breadth < 2:
        return {
            'divergence': None,
            'index_change_pct': None,
            'ad_trend_delta': None,
            'severity': 'info',
            'mark_says': 'Yetersiz veri penceresi (en az 2 gun gerekli).',
        }

    # Index % degisim (lookback once -> bugun)
    index_start = index_closes[-n_index - 1] if len(index_closes) > n_index else index_closes[0]
    index_now = index_closes[-1]
    index_change_pct = round((index_now - index_start) / index_start * 100, 2) if index_start > 0 else 0.0

    # A/D Line trend delta: ilk yari ortalama vs son yari ortalama
    # Trendin yonunu ve buyuklugunu ayrim eder (sadece cumulative degil)
    recent_adv = advances_daily[-n_breadth:]
    recent_dec = declines_daily[-n_breadth:]
    half = n_breadth // 2
    if half == 0:
        first_half_net = recent_adv[0] - recent_dec[0]
        second_half_net = recent_adv[-1] - recent_dec[-1]
    else:
        first_half_net = sum(a - d for a, d in zip(recent_adv[:half], recent_dec[:half]))
        second_half_net = sum(a - d for a, d in zip(recent_adv[half:], recent_dec[half:]))
    ad_trend_delta = second_half_net - first_half_net

    # Trend yonu
    is_index_up = index_change_pct > DIVERGENCE_INDEX_TREND_THRESHOLD_PCT
    is_index_down = index_change_pct < -DIVERGENCE_INDEX_TREND_THRESHOLD_PCT
    is_ad_up = ad_trend_delta > DIVERGENCE_AD_TREND_THRESHOLD
    is_ad_down = ad_trend_delta < -DIVERGENCE_AD_TREND_THRESHOLD

    # 4 kategori + neutral
    if is_index_up and is_ad_up:
        divergence = 'CONFIRMED_UP'
        severity = 'ok'
        says = (f'Index {index_change_pct:+.2f}% + A/D trend {ad_trend_delta:+d} — '
                f'Mark Stage 2 onayli rally. Geniş katilim, lider hisseler yukseliyor.')
    elif is_index_up and is_ad_down:
        divergence = 'BEARISH_DIVERGENCE'
        severity = 'critical'
        says = (f'⚠️ BEARISH DIVERGENCE — Index {index_change_pct:+.2f}% ama '
                f'A/D trend {ad_trend_delta:+d} (zayif). O\'Neil: A/D index '
                f'oncesi zayifliyorsa erken uyari. Tarihsel: 1999 / 2007 paten.')
    elif is_index_down and is_ad_up:
        divergence = 'BULLISH_DIVERGENCE'
        severity = 'info'
        says = (f'Bullish divergence — Index {index_change_pct:+.2f}% dustu ama '
                f'A/D trend {ad_trend_delta:+d} (saglikli). Toparlanma adayi, '
                f'Mark Stage 1 -> 2 gecisi izle.')
    elif is_index_down and is_ad_down:
        divergence = 'CONFIRMED_DOWN'
        severity = 'warn'
        says = (f'Confirmed weakness — Index {index_change_pct:+.2f}% + A/D '
                f'{ad_trend_delta:+d}. Bear pressure dogrulandi, Mark Stage 4 '
                f'uzak dur.')
    else:
        divergence = 'NEUTRAL'
        severity = 'info'
        says = (f'Trend belirsiz — Index {index_change_pct:+.2f}%, A/D '
                f'{ad_trend_delta:+d}. Esik altinda, sinyalsiz bekle.')

    return {
        'divergence': divergence,
        'index_change_pct': index_change_pct,
        'ad_trend_delta': ad_trend_delta,
        'severity': severity,
        'mark_says': says,
    }


# ======================================================================
# KARAR #733 alt-paket (Paket 64, 25 May 2026) — Follow-Through Day
#
# Mark/O'Neil canon "FTD" mekanigi: Bear correction/pressure içinde
# major index (SPY/QQQ) dip yapip 4-10 gun sonra +1.5-2% sıçrama
# (yuksek hacimle teyit). Bu "Follow-Through Day" — Stage 1 -> Stage 2
# gecişin onayı. Mark birebir: "FTD before FTD is no signal."
#
# Tarihsel paten:
# - 2003 Mart 17: SPY +3.5% FTD -> bull market basladi
# - 2009 Mart 12: SPY +4.1% FTD -> finansal kriz dip onayı
# - 2020 Nisan 6: SPY +7% FTD -> COVID dip recovery
#
# Algoritma:
# 1. Lookback 4-10 gun arası bir gun +THRESHOLD %  + hacim onceki >1.0x
# 2. Önceki dip (lowest close) tespit
# 3. FTD gun sayisi (dip'ten sonraki gun sayisi)
# 4. Hacim teyidi (volume_today > volume_yesterday)
#
# Birebir kaynak (3-kaynak triangule, 11 Haz 2026):
# - O'Neil "How to Make Money in Stocks" Bol.7 s.59-60: rally'nin 3.-10. gunu
#   (genelde 4-7. gun), endeks gun icinde "+1% or more", hacim onceki gunden
#   ARTIS. KITAP ESIGI = %1 (%1.7 DEGIL).
# - FTD_MIN_GAIN_PCT=1.7 ise IBD'nin KITAP-SONRASI modern revizesidir (%1.0
#   cok fazla yanlis FTD uretince ~%1.5-1.7'e cekildi) — kitap degil guncel IBD.
# - TLSMW Ch 4: 4. gunden onceki FTD daha az guvenilir (Mark teyit).
# - 4-7 gun ideal pencere (O'Neil CANSLIM "Best stocks make lows first").
# ======================================================================

# FTD esikleri (Mark/O'Neil canon)
FTD_MIN_GAIN_PCT: float = 1.7        # IBD-modern esik; KITAP (O'Neil s.59) = %1
FTD_VOLUME_MULTIPLIER: float = 1.0   # Hacim onceki >=1.0x (Mark: yuksek hacim)
FTD_WINDOW_MIN_DAYS: int = 4         # Dip'ten sonra 4. gun veya sonrasi
FTD_WINDOW_MAX_DAYS: int = 10        # 10. gunu gecince FTD gecerligi azalir


def compute_follow_through_day(
    closes: list[float],
    volumes: list[int],
    lookback_days: int = 15,
) -> dict:
    """Mark/O'Neil Follow-Through Day tespiti — bear dip'ten sonraki rally onayı.

    Args:
        closes: Major index (SPY/QQQ) günlük kapanış fiyatları (kronolojik)
        volumes: Aynı gün hacim
        lookback_days: Önceki dip arama penceresi (default 15)

    Returns:
        dict {
            'ftd_detected': bool,
            'ftd_day_index': int | None,    # closes listesinde FTD günü
            'ftd_gain_pct': float | None,    # FTD günü % değişim
            'days_after_low': int | None,    # Dip'ten kaç gün sonra
            'volume_confirmed': bool,         # FTD günü hacim > önceki
            'previous_low': float | None,     # Bulunan dip fiyatı
            'mark_says': str,
        }

    Mark birebir kaynak:
    - O'Neil: "FTD %1.7-2% gun + hacim onceki >1x"
    - TLSMW Ch 4: "FTD 4-7 gun ideal, <4 yanıltıcı, >10 zayıf"
    """
    if not closes or not volumes:
        return {
            'ftd_detected': False,
            'ftd_day_index': None,
            'ftd_gain_pct': None,
            'days_after_low': None,
            'volume_confirmed': False,
            'previous_low': None,
            'mark_says': 'Yetersiz veri — FTD tespiti yapılamıyor.',
        }
    if len(closes) != len(volumes):
        return {
            'ftd_detected': False,
            'ftd_day_index': None,
            'ftd_gain_pct': None,
            'days_after_low': None,
            'volume_confirmed': False,
            'previous_low': None,
            'mark_says': 'FTD: closes/volumes listesi uzunluğu farklı.',
        }
    if len(closes) < FTD_WINDOW_MIN_DAYS + 2:
        return {
            'ftd_detected': False,
            'ftd_day_index': None,
            'ftd_gain_pct': None,
            'days_after_low': None,
            'volume_confirmed': False,
            'previous_low': None,
            'mark_says': f'Yetersiz veri — en az {FTD_WINDOW_MIN_DAYS + 2} gün gerek.',
        }

    # Son lookback gün içinde dip arama
    window_size = min(lookback_days, len(closes))
    recent_closes = closes[-window_size:]
    recent_volumes = volumes[-window_size:]
    low_index_in_window = recent_closes.index(min(recent_closes))
    previous_low = recent_closes[low_index_in_window]
    # Pencere içindeki dip günü → tam listede index
    low_index = len(closes) - window_size + low_index_in_window

    # Dip günü pencerenin sonuna çok yakınsa (henuz dip yapıldı), FTD aramaya
    # uygun değil — en az FTD_WINDOW_MIN_DAYS sonrası gelecek günler lazım
    days_since_low = (len(closes) - 1) - low_index
    if days_since_low < FTD_WINDOW_MIN_DAYS:
        return {
            'ftd_detected': False,
            'ftd_day_index': None,
            'ftd_gain_pct': None,
            'days_after_low': days_since_low,
            'volume_confirmed': False,
            'previous_low': round(previous_low, 2),
            'mark_says': (f'Dip {days_since_low} gün önce, FTD penceresi '
                          f'({FTD_WINDOW_MIN_DAYS}-{FTD_WINDOW_MAX_DAYS} gün) henüz başlamadı.'),
        }

    # FTD penceresi (dip + 4 ile dip + 10 gün arası)
    ftd_window_start = low_index + FTD_WINDOW_MIN_DAYS
    ftd_window_end = min(low_index + FTD_WINDOW_MAX_DAYS, len(closes) - 1)

    # En son güçlü gün ara (en yüksek % değişim FTD penceresinde)
    best_ftd_index = None
    best_ftd_gain = 0.0
    for i in range(ftd_window_start, ftd_window_end + 1):
        if i == 0:
            continue
        prev_close = closes[i - 1]
        if prev_close <= 0:
            continue
        gain_pct = (closes[i] - prev_close) / prev_close * 100
        if gain_pct >= FTD_MIN_GAIN_PCT and gain_pct > best_ftd_gain:
            best_ftd_gain = gain_pct
            best_ftd_index = i

    if best_ftd_index is None:
        return {
            'ftd_detected': False,
            'ftd_day_index': None,
            'ftd_gain_pct': None,
            'days_after_low': days_since_low,
            'volume_confirmed': False,
            'previous_low': round(previous_low, 2),
            'mark_says': (f'Dip\'ten {days_since_low} gün geçti, pencere '
                          f'içinde +{FTD_MIN_GAIN_PCT}% sıçrama YOK. Bekle.'),
        }

    # FTD detected — hacim teyit
    prev_vol = volumes[best_ftd_index - 1]
    today_vol = volumes[best_ftd_index]
    volume_confirmed = (
        prev_vol > 0 and today_vol >= prev_vol * FTD_VOLUME_MULTIPLIER
    )
    days_after = best_ftd_index - low_index

    if volume_confirmed:
        says = (f'✓ FTD ONAYI — Day {days_after}: +{best_ftd_gain:.2f}% '
                f'sıçrama + hacim teyidi. Mark/O\'Neil: Stage 1 -> Stage 2 '
                f'geçiş, lider hisseler izle. Tarihsel: 2003/2009/2020.')
    else:
        says = (f'⚠️ FTD ZAYIF — Day {days_after}: +{best_ftd_gain:.2f}% '
                f'sıçrama AMA hacim teyit yok ({today_vol} < {prev_vol}). '
                f'Mark: gerçek FTD hacim onceki >1x ister.')

    return {
        'ftd_detected': True,
        'ftd_day_index': best_ftd_index,
        'ftd_gain_pct': round(best_ftd_gain, 2),
        'days_after_low': days_after,
        'volume_confirmed': volume_confirmed,
        'previous_low': round(previous_low, 2),
        'mark_says': says,
    }


# ======================================================================
# KARAR #733 alt-paket (Paket 70, 25 May 2026) — Pivot Breakout
#
# Mark TLSMW Ch 10 canon: VCP, Cup&Handle, Flat Base sonrasi pivot
# noktasi (son N-gün tepe) üstüne kırılım = AL sinyali. Hacim teyit
# zorunlu — kırılım hacim 50-gün ort >1.5x değilse "false breakout"
# (Mark: "Volume confirms breakout").
#
# Algoritma:
# 1. Lookback (default 20 gün) içinde en yüksek kapanış = pivot
# 2. Son gün kapanış > pivot * (1 + buffer_pct) → BREAKOUT
# 3. Kırılım günü hacim / 50-gün ortalama hacim >= 1.5x → confirmed
# 4. Buffer (default %0.5) gun-içi gürültüye karşı küçük tampon
#
# 4 ana çıktı:
# - CONFIRMED: pivot üzerinde + hacim teyit (Mark AL canon)
# - WEAK: pivot üzerinde + hacim düşük (false breakout riski)
# - NEAR_PIVOT: pivot yakını ama kırılmadı (izlemede)
# - BELOW_PIVOT: pivot altı (henüz kırılım yok)
#
# Mark birebir kaynak:
# - TLSMW Ch 10: "VCP pivot break + hacim teyit zorunlu"
# - O'Neil CANSLIM: "breakout volume 40%+ üzeri ort"
# ======================================================================

PIVOT_BUFFER_PCT: float = 0.5            # %0.5 gun-ici gurultu tamponu
PIVOT_VOLUME_CONFIRM_RATIO: float = 1.5  # Kırılım hacim 50g ort >=1.5x
PIVOT_VOLUME_LOOKBACK_DAYS: int = 50     # Hacim ortalama penceresi
PIVOT_PRICE_LOOKBACK_DAYS: int = 20      # Pivot noktasi (son N gun tepe)


def compute_pivot_breakout(
    closes: list[float],
    volumes: list[int],
    price_lookback: int = PIVOT_PRICE_LOOKBACK_DAYS,
    volume_lookback: int = PIVOT_VOLUME_LOOKBACK_DAYS,
) -> dict:
    """Mark TLSMW Ch 10 pivot kırılım + hacim teyit canon.

    Args:
        closes: Hisse günlük kapanış fiyatları (kronolojik)
        volumes: Aynı gün hacim
        price_lookback: Pivot noktası lookback (default 20 gün)
        volume_lookback: Hacim ortalama lookback (default 50 gün)

    Returns:
        dict {
            'status': str,                # CONFIRMED/WEAK/NEAR_PIVOT/BELOW_PIVOT
            'pivot_price': float | None,  # Tespit edilen pivot fiyat
            'current_price': float,        # Bugün kapanış
            'breakout_pct': float | None,  # Pivot üstü/altı % (+/-)
            'volume_multiplier': float | None,  # Hacim / 50g ort
            'volume_confirmed': bool,
            'mark_says': str,
        }

    Mark birebir kaynak:
    - TLSMW Ch 10: "VCP pivot break + hacim teyit zorunlu"
    - O'Neil CANSLIM: "Volume confirms breakout"
    """
    if not closes or not volumes:
        return {
            'status': None,
            'pivot_price': None,
            'current_price': 0.0,
            'breakout_pct': None,
            'volume_multiplier': None,
            'volume_confirmed': False,
            'mark_says': 'Yetersiz veri — pivot breakout hesaplanamıyor.',
        }
    if len(closes) != len(volumes):
        return {
            'status': None,
            'pivot_price': None,
            'current_price': 0.0,
            'breakout_pct': None,
            'volume_multiplier': None,
            'volume_confirmed': False,
            'mark_says': 'Pivot: closes/volumes listesi uzunluğu farklı.',
        }
    if len(closes) < price_lookback + 1:
        return {
            'status': None,
            'pivot_price': None,
            'current_price': closes[-1] if closes else 0.0,
            'breakout_pct': None,
            'volume_multiplier': None,
            'volume_confirmed': False,
            'mark_says': f'Yetersiz veri — en az {price_lookback + 1} gün gerek.',
        }

    current_price = closes[-1]
    current_volume = volumes[-1]

    # Pivot = son lookback gün içinde en yüksek kapanış (BUGÜN HARİÇ)
    # Bugün kırılıyor mu test edilecek, pivot geçmiş tepe.
    pivot_window = closes[-(price_lookback + 1):-1]  # son lookback gün, bugün hariç
    pivot_price = max(pivot_window)

    # Hacim ortalama (son volume_lookback gün, bugün dahil değil)
    vol_window_size = min(volume_lookback, len(volumes) - 1)
    if vol_window_size <= 0:
        avg_volume = current_volume  # fallback
    else:
        recent_volumes = volumes[-(vol_window_size + 1):-1]
        avg_volume = sum(recent_volumes) / len(recent_volumes) if recent_volumes else 0
    volume_multiplier = (
        round(current_volume / avg_volume, 2) if avg_volume > 0 else None
    )

    # Breakout test
    buffer_threshold = pivot_price * (1 + PIVOT_BUFFER_PCT / 100.0)
    near_threshold = pivot_price * 0.97  # %3 alt yakın izleme bandı
    breakout_pct = round((current_price - pivot_price) / pivot_price * 100, 2) if pivot_price > 0 else 0.0

    # Hacim teyit
    volume_confirmed = (
        volume_multiplier is not None
        and volume_multiplier >= PIVOT_VOLUME_CONFIRM_RATIO
    )

    # Status karar
    if current_price > buffer_threshold:
        if volume_confirmed:
            status = 'CONFIRMED'
            says = (f'✓ AL Sinyali — Pivot ${pivot_price:.2f} üstü '
                    f'{breakout_pct:+.2f}%, hacim {volume_multiplier:.1f}x. '
                    f'Mark TLSMW Ch 10: Volume confirms breakout. O\'Neil '
                    f'CANSLIM canon — entry pozisyon zamanı.')
        else:
            status = 'WEAK'
            says = (f'⚠️ ZAYIF Kırılım — Pivot ${pivot_price:.2f} üstü '
                    f'{breakout_pct:+.2f}% AMA hacim {volume_multiplier or 0:.1f}x '
                    f'(<{PIVOT_VOLUME_CONFIRM_RATIO}x). Mark: "false breakout" '
                    f'riski, hacim onayi yok — beklemede kal.')
    elif current_price >= near_threshold:
        status = 'NEAR_PIVOT'
        says = (f'⏳ Pivot Yakını — Mevcut ${current_price:.2f} / Pivot '
                f'${pivot_price:.2f} ({breakout_pct:+.2f}%). Mark: izlemede '
                f'kal, gerçek kırılım hacim onaylı olsun.')
    else:
        status = 'BELOW_PIVOT'
        says = (f'Pivot Altı — Mevcut ${current_price:.2f} pivot '
                f'${pivot_price:.2f} altında {breakout_pct:+.2f}%. Mark: '
                f'henüz AL noktası değil, base oluşumunu bekle.')

    return {
        'status': status,
        'pivot_price': round(pivot_price, 2),
        'current_price': round(current_price, 2),
        'breakout_pct': breakout_pct,
        'volume_multiplier': volume_multiplier,
        'volume_confirmed': volume_confirmed,
        'mark_says': says,
    }


# ======================================================================
# KARAR #733 alt-paket (Paket 73, 25 May 2026) — Brandon Avg Gain Expectancy
#
# Mark Brandon Video canon: "Average gain %20, average loss %4, win
# rate %50 minimum". Trader expectancy hesabı:
# E = (P_win × Avg_gain) - (P_loss × Avg_loss)
#
# Mark birebir (Brandon interview, TLSMW Ch 12-13):
# - Avg gain >= 20% (winning trades)
# - Avg loss <= 4% (stop loss disciplined)
# - Win rate >= 50% (consistency)
# - Expectancy >= 10% per trade (asimetri)
#
# 4 kategori:
# - EXCELLENT (E >= 15%): Mark canon zirvesi
# - GOOD (E >= 10%): Brandon eşiği
# - ACCEPTABLE (E >= 5%): asgari karlılık
# - POOR (E < 5%): disiplinsizlik (RBA Drop Setup)
# ======================================================================

BRANDON_AVG_GAIN_TARGET_PCT: float = 20.0
BRANDON_AVG_LOSS_TARGET_PCT: float = 4.0
BRANDON_WIN_RATE_TARGET: float = 0.50
BRANDON_EXPECTANCY_EXCELLENT_PCT: float = 15.0
BRANDON_EXPECTANCY_GOOD_PCT: float = 10.0
BRANDON_EXPECTANCY_ACCEPTABLE_PCT: float = 5.0


def compute_brandon_expectancy(
    avg_gain_pct: float,
    avg_loss_pct: float,
    win_rate: float,
) -> dict:
    """Mark Brandon Video expectancy hesabı.

    Args:
        avg_gain_pct: Kazanan trade'lerin ortalama getirisi (%)
        avg_loss_pct: Kaybeden trade'lerin ortalama kaybı (% — pozitif değer!)
        win_rate: Kazanma oranı (0.0-1.0)

    Returns:
        dict {
            'expectancy_pct': float,    # Trade başına ortalama expectancy
            'category': str,             # EXCELLENT/GOOD/ACCEPTABLE/POOR
            'meets_brandon_targets': bool,
            'mark_says': str,
        }
    """
    if not (0.0 <= win_rate <= 1.0):
        return {
            'expectancy_pct': 0.0,
            'category': None,
            'meets_brandon_targets': False,
            'mark_says': 'Win rate 0.0-1.0 arasında olmalı.',
        }
    if avg_gain_pct < 0 or avg_loss_pct < 0:
        return {
            'expectancy_pct': 0.0,
            'category': None,
            'meets_brandon_targets': False,
            'mark_says': 'avg_gain_pct ve avg_loss_pct pozitif olmalı.',
        }

    expectancy = (win_rate * avg_gain_pct) - ((1 - win_rate) * avg_loss_pct)
    expectancy = round(expectancy, 2)

    meets_targets = (
        avg_gain_pct >= BRANDON_AVG_GAIN_TARGET_PCT
        and avg_loss_pct <= BRANDON_AVG_LOSS_TARGET_PCT
        and win_rate >= BRANDON_WIN_RATE_TARGET
    )

    if expectancy >= BRANDON_EXPECTANCY_EXCELLENT_PCT:
        category = 'EXCELLENT'
        says = (f'✓ EXCELLENT Expectancy {expectancy:+.2f}% — Mark Brandon '
                f'canon zirvesi. Win {win_rate*100:.0f}% × +{avg_gain_pct:.1f}% '
                f'vs Lose × -{avg_loss_pct:.1f}%. Mark: disiplin pekiştir.')
    elif expectancy >= BRANDON_EXPECTANCY_GOOD_PCT:
        category = 'GOOD'
        says = (f'Brandon eşiği OK — Expectancy {expectancy:+.2f}%. '
                f'Mark video: %20 avg gain / %4 avg loss / %50 win rate '
                f'hedeflerine yakın. Devam.')
    elif expectancy >= BRANDON_EXPECTANCY_ACCEPTABLE_PCT:
        category = 'ACCEPTABLE'
        says = (f'Asgari karlılık — Expectancy {expectancy:+.2f}%. '
                f'Mark: setup kalitesini artır veya stop disiplini sıkılaştır.')
    else:
        category = 'POOR'
        says = (f'⚠️ POOR Expectancy {expectancy:+.2f}% — Mark TTLC Sec 4 '
                f'RBA: setup terk et veya kayıt tut. Disiplin kontrol gerek.')

    return {
        'expectancy_pct': expectancy,
        'category': category,
        'meets_brandon_targets': meets_targets,
        'mark_says': says,
    }


# ======================================================================
# Paket 455 (11 Haz 2026) — Cup-with-Handle Base Detection
#
# William O'Neil "How to Make Money in Stocks" (CAN SLIM) Bol.15 birebir.
# Uc-kaynak danisma (NotebookLM O'Neil CAN SLIM + Minervini x2) ile TUM esikler
# kitap-sayfa atifli (Kural #26 — uydurma yok):
#   - Onceki trend : min +%30 advance + RS iyilesme        (s.165)
#   - Kupa derinligi: %12-33                                 (s.162-163)
#   - Kupa suresi  : 7-65 hafta = 35-325 islem gunu          (s.162)
#   - Kupa sekli   : U (yuvarlak), dar V REDDEDILIR           (s.163)
#   - Kulp derinligi: <=%15; >%15 (bogada) gevsek/kusurlu     (s.164,178)
#   - Kulp konumu  : tabanin UST YARISI + 200-gun MA ustu     (s.163-164)
#   - Kulp suresi  : >1-2 hafta (>~5 islem gunu)              (s.163)
#   - Shakeout     : kulp sonunda onceki dusuk altina sarkma  (s.163-164)
#   - Pivot        : kulp zirvesi/direnc; +%50 hacim kirilim  (s.164-165)
#                    NOT: "kulp zirvesi +10 cent" KITAPTA YOK (IBD konvansiyonu).
#   - Kulp hacmi   : "extreme/dramatic dry-up" NITEL          (s.163-164)
#                    KITAPTA % YOK -> Quanfina heuristic (kulp ort < taban ort *0.8).
#   - Kusurlu      : alt yari / 200MA alti / yukari-kama / >%15 (s.164,178)
#
# v1 not: tek pre-breakout taban tarar (rim = pencere max high; ilk olusum = sol
# kenar). Kirilim sonrasi (rim asilmis) taban tespit edilmez — forming base hedefi.
# ======================================================================

CUP_PRIOR_UPTREND_MIN_PCT: float = 30.0    # O'Neil s.165 — base oncesi min advance
CUP_DEPTH_MIN_PCT: float = 12.0            # O'Neil s.162-163 — kupa min derinlik
CUP_DEPTH_MAX_PCT: float = 33.0            # O'Neil s.162-163 — kupa max (saglikli)
CUP_DURATION_MIN_DAYS: int = 35            # 7 hafta (s.162)
CUP_DURATION_MAX_DAYS: int = 325           # 65 hafta (s.162)
HANDLE_DEPTH_MAX_PCT: float = 15.0         # O'Neil s.164,178 — >%15 gevsek/kusurlu
HANDLE_MIN_DAYS: int = 5                   # >1 hafta (s.163)
HANDLE_VOLUME_DRYUP_RATIO: float = 0.8     # Quanfina heuristic (kitapta % yok, s.163-164 nitel)
CUP_HANDLE_MIN_HISTORY: int = 60           # min veri (gercek kullanim 250+ ideal)


def compute_cup_with_handle(
    highs: list[float],
    lows: list[float],
    closes: list[float],
    volumes: list[float] | None = None,
    ma200: float | None = None,
) -> dict:
    """O'Neil CAN SLIM cup-with-handle base tespiti (HTMMIS Bol.15, kitap-birebir esikler).

    Args:
        highs/lows/closes: gunluk OHLC (kronolojik, esit uzunluk)
        volumes: gunluk hacim (opsiyonel — kulp dry-up degerlendirmesi)
        ma200: 200-gun SMA (opsiyonel; yoksa mevcut close ort)

    Returns:
        dict {detected, quality, pivot_price, prior_uptrend_pct, cup_depth_pct,
              cup_duration_days, handle_depth_pct, handle_in_upper_half, shakeout,
              faults, mark_says}

    Kaynak: O'Neil HTMMIS Bol.15 s.162-165,178 (her esik modul-ust yorumunda atifli).
    """
    def _none(msg: str) -> dict:
        return {
            'detected': False, 'quality': 'NONE', 'pivot_price': None,
            'prior_uptrend_pct': None, 'cup_depth_pct': None, 'cup_duration_days': None,
            'handle_depth_pct': None, 'handle_in_upper_half': False, 'shakeout': False,
            'faults': [], 'mark_says': msg,
        }

    if not highs or not lows or not closes:
        return _none('Yetersiz veri — cup-with-handle hesaplanamiyor.')
    n = len(closes)
    if not (len(highs) == len(lows) == n):
        return _none('Cup-handle: highs/lows/closes uzunluk farkli.')
    if n < CUP_HANDLE_MIN_HISTORY:
        return _none(f'Yetersiz veri — en az {CUP_HANDLE_MIN_HISTORY} gun gerek.')

    # 1) Taban penceresi: son <=325 gun. Rim = bu pencerede ilk en yuksek high (sol kenar).
    window = min(n, CUP_DURATION_MAX_DAYS)
    seg_h = highs[-window:]
    seg_l = lows[-window:]
    rim_rel = seg_h.index(max(seg_h))
    rim_price = seg_h[rim_rel]
    rim_global = n - window + rim_rel
    if rim_price <= 0:
        return _none('Rim fiyati gecersiz.')
    if window - rim_rel < CUP_DURATION_MIN_DAYS:
        return _none('Rim cok yakin (kirilim sonrasi?) — forming taban yok.')

    # 2) Kupa dibi = rim'den SONRA en dusuk low
    after_rim_l = seg_l[rim_rel:]
    cup_low_rel = after_rim_l.index(min(after_rim_l))
    cup_low_idx = rim_rel + cup_low_rel
    cup_low_price = after_rim_l[cup_low_rel]
    cup_depth_pct = round((rim_price - cup_low_price) / rim_price * 100, 2)

    # 3) Kulp = kupa dibinden sonra toparlanma tepesi (handle_high) + sonraki geri cekilme
    after_low_h = seg_h[cup_low_idx:]
    if len(after_low_h) < HANDLE_MIN_DAYS + 1:
        return _none('Kupa sag tarafi/kulp icin yetersiz veri.')
    handle_high_rel = after_low_h.index(max(after_low_h))
    handle_high_idx = cup_low_idx + handle_high_rel
    handle_high_price = after_low_h[handle_high_rel]
    if handle_high_price <= 0:
        return _none('Kulp zirvesi gecersiz.')

    after_hh_l = seg_l[handle_high_idx:]
    handle_len = len(after_hh_l) - 1
    if handle_len < HANDLE_MIN_DAYS:
        return _none(f'Kulp cok kisa (<{HANDLE_MIN_DAYS} gun) — O\'Neil s.163 >1 hafta.')
    handle_low_price = min(after_hh_l)
    handle_depth_pct = round((handle_high_price - handle_low_price) / handle_high_price * 100, 2)
    cup_duration_days = handle_high_idx - rim_rel  # rim -> kulp zirvesi = taban uzunlugu

    # 4) Onceki trend: rim oncesi advance (rim'den geriye 252 gun dip)
    prior_window = lows[max(0, rim_global - 252):rim_global + 1]
    prior_low = min(prior_window) if prior_window else cup_low_price
    prior_uptrend_pct = round((rim_price - prior_low) / prior_low * 100, 2) if prior_low > 0 else 0.0

    # 5) MA200 (yoksa mevcut close ort)
    if ma200 is None:
        ma200 = sum(closes[-200:]) / min(200, n)

    # 6) Kulp konum: tabanin UST YARISI (s.163-164)
    cup_mid = cup_low_price + 0.5 * (rim_price - cup_low_price)
    handle_in_upper_half = handle_low_price > cup_mid

    # 7) Shakeout (s.163-164): kulp dibi, kulp oncesi 10-bar dusugun altina sarkti mi
    pre_handle_lows = seg_l[max(0, handle_high_idx - 10):handle_high_idx]
    prior_minor_low = min(pre_handle_lows) if pre_handle_lows else handle_low_price
    shakeout = handle_low_price < prior_minor_low

    # 7b) Upward-wedging kulp = MAJOR flaw (s.164,178): saglikli kulp dipleri ASAGI
    # suruklenir (son shakeout). Dipler yukari kama yaparsa "yeterli talep/shakeout
    # yok" demek. 11 Haz cift danisma teyit: NotebookLM O'Neil ("handles that ...
    # drift upwards along their price lows have a higher probability of failing") +
    # IBD ("avoid bases that carry an upward-wedging handle; this is a flaw").
    # handle_high bar'inin low'unu (after_hh_l[0]) haric tut — kulp pullback dipleri trendini olc
    _handle_lows = after_hh_l[1:] if len(after_hh_l) > 1 else after_hh_l
    wedging_up = False
    if len(_handle_lows) >= 4:
        _mid = len(_handle_lows) // 2
        wedging_up = min(_handle_lows[_mid:]) > min(_handle_lows[:_mid])

    # 8) U vs V (s.163): dip etrafinda zaman (kupa dibinin +%5 icinde >=5 bar = genis U)
    near_bottom = [lo for lo in after_rim_l if lo <= cup_low_price * 1.05]
    is_u_shaped = len(near_bottom) >= 5

    # 9) Kulp hacim dry-up (s.163-164 NITEL — Quanfina heuristic, kitapta % yok)
    handle_volume_dryup = None
    if volumes and len(volumes) == n:
        seg_v = volumes[-window:]
        base_vol = seg_v[rim_rel:handle_high_idx]
        handle_vol = seg_v[handle_high_idx:]
        if base_vol and handle_vol:
            base_avg = sum(base_vol) / len(base_vol)
            handle_avg = sum(handle_vol) / len(handle_vol)
            handle_volume_dryup = base_avg > 0 and handle_avg <= base_avg * HANDLE_VOLUME_DRYUP_RATIO

    # --- Cekirdek yapi var mi? ---
    if not (cup_depth_pct >= CUP_DEPTH_MIN_PCT and cup_duration_days >= CUP_DURATION_MIN_DAYS
            and handle_len >= HANDLE_MIN_DAYS and handle_depth_pct > 0):
        return _none('Cup-with-handle yapisi yok (kupa/kulp olcumleri tutmuyor).')

    # --- Kusur (fault) tespiti (s.164,178) ---
    faults: list[str] = []
    if cup_depth_pct > CUP_DEPTH_MAX_PCT:
        faults.append(f'kupa %{cup_depth_pct} derin (>%{CUP_DEPTH_MAX_PCT:.0f}, s.162-163)')
    if cup_duration_days > CUP_DURATION_MAX_DAYS:
        faults.append(f'kupa {cup_duration_days}g (>65 hafta, s.162)')
    if prior_uptrend_pct < CUP_PRIOR_UPTREND_MIN_PCT:
        faults.append(f'onceki trend %{prior_uptrend_pct} (<%30, s.165)')
    if handle_depth_pct > HANDLE_DEPTH_MAX_PCT:
        faults.append(f'kulp %{handle_depth_pct} (>%15 gevsek/kusurlu, s.164,178)')
    if not handle_in_upper_half:
        faults.append('kulp tabanin ALT yarisinda (zayif, s.164)')
    if handle_high_price < ma200:
        faults.append('kulp 200-gun MA altinda (zayif, s.164)')
    if not is_u_shaped:
        faults.append('kupa dibi dar V — U sekli degil (s.163)')
    if not shakeout:
        faults.append('kulp sonunda shakeout yok (s.163-164)')
    if wedging_up:
        faults.append('kulp yukari kama — dipler yukseliyor, shakeout yok (s.164,178)')

    detected = len(faults) == 0
    if detected:
        quality = 'EXCELLENT' if handle_volume_dryup else 'GOOD'
    elif len(faults) <= 2:
        quality = 'MARGINAL'
    else:
        quality = 'NONE'
        detected = False

    pivot_price = round(handle_high_price, 2)
    fault_str = '; '.join(faults)
    if quality == 'EXCELLENT':
        says = (f'✓ EXCELLENT Cup-with-Handle — pivot ${pivot_price}. Kupa %{cup_depth_pct} '
                f'({cup_duration_days}g, U) + kulp %{handle_depth_pct} ust yari + shakeout + '
                f'hacim dry-up. O\'Neil canon tam; +%50 hacim kirilimda AL adayi.')
    elif quality == 'GOOD':
        says = (f'GOOD Cup-with-Handle — pivot ${pivot_price}. Kupa %{cup_depth_pct}, kulp '
                f'%{handle_depth_pct}. O\'Neil yapi tutuyor; hacim kirilim teyidi bekle.')
    elif quality == 'MARGINAL':
        says = (f'⚠️ MARGINAL Cup-with-Handle — pivot ${pivot_price}. Kusur: {fault_str}. '
                f'O\'Neil: zayif yapi, dikkatli.')
    else:
        says = f'Cup-with-Handle kusurlu — {fault_str}.'

    return {
        'detected': detected,
        'quality': quality,
        'pivot_price': pivot_price,
        'prior_uptrend_pct': prior_uptrend_pct,
        'cup_depth_pct': cup_depth_pct,
        'cup_duration_days': cup_duration_days,
        'handle_depth_pct': handle_depth_pct,
        'handle_in_upper_half': handle_in_upper_half,
        'shakeout': shakeout,
        'faults': faults,
        'mark_says': says,
    }


# ======================================================================
# Paket 458 (11 Haz 2026) — Flat Base Detection
#
# William O'Neil "How to Make Money in Stocks" (CAN SLIM) + IBD Flat Base.
# Derin internet arastirma (workflow, IBD/MarketSmith/TraderLion cok-kaynak HIGH
# guven) ile esikler kaynak-atifli (Kural #26 — uydurma yok):
#   - Min sure    : >=5 hafta = 25 islem gunu (IBD "at least five weeks")
#   - Maks sure   : MarketSmith "5 to 65 week" -> 325 gun (kesin ust yok)
#   - Maks derinlik: <=%15 intraday tepe-dip (IBD + MarketSmith); >%25 = flat base DEGIL
#   - Onceki advance: >=%20 later-stage 2./3. baz (IBD/Invezz). NOT: TraderLion %30
#     der ama o genel ilk-baz kurali — flat-base-ozgu sayi %20 (MEDIUM guven).
#   - Yatay/dar   : "fairly tight sideways" NITEL — kaynakta SAYISAL esik YOK
#     -> Quanfina heuristic (baz 1.yari-2.yari ort farki <=%8 = yatay).
#   - Pivot       : baz icindeki en yuksek (+10 cent IBD); +%40 hacim kirilim teyit.
#   - Hacim       : baz ici dry-up (ozellikle down haftalar); kirilim +%40-50 (canon %40).
#   - Kusurlu     : >%15 genis/gevsek, downtrend, <5 hafta, gec-asama (3./4. baz).
#
# v1 not: en yeni hem SIG hem YATAY pencereyi tarar (monotonik trend sideways
# kontrolunde elenir). Base-stage sayimi (kacinci baz) YOK -> gec-asama kusuru
# tespit edilmez (GELISTIRILMESI LAZIM, gelecek is).
# ======================================================================

FLAT_BASE_MIN_DAYS: int = 25                   # IBD >=5 hafta
FLAT_BASE_MAX_DAYS: int = 325                  # MarketSmith 5-65 hafta
FLAT_BASE_DEPTH_MAX_PCT: float = 15.0          # IBD/MarketSmith maks derinlik
FLAT_BASE_DEPTH_REJECT_PCT: float = 25.0       # Investing.com ">25% flat base degil"
FLAT_BASE_PRIOR_ADVANCE_MIN_PCT: float = 20.0  # IBD/Invezz prior uptrend (MEDIUM: TraderLion 30)
FLAT_BASE_BREAKOUT_VOL_RATIO: float = 1.40     # IBD "+40% above 50-day" (50% looser pratisyen)
FLAT_BASE_SIDEWAYS_TOL_PCT: float = 8.0        # Quanfina heuristic (kitapta sayisal tightness YOK)


def compute_flat_base(
    highs: list[float],
    lows: list[float],
    closes: list[float],
    volumes: list[float] | None = None,
) -> dict:
    """O'Neil/IBD flat base tespiti (IBD-canon esikler, derin arastirma + Kural #26).

    Args:
        highs/lows/closes: gunluk OHLC (kronolojik, esit uzunluk)
        volumes: gunluk hacim (opsiyonel — baz dry-up degerlendirmesi)

    Returns:
        dict {detected, quality, pivot_price, prior_advance_pct, base_depth_pct,
              base_duration_days, is_sideways, volume_dryup, faults, mark_says}
    """
    def _none(msg: str) -> dict:
        return {
            'detected': False, 'quality': 'NONE', 'pivot_price': None,
            'prior_advance_pct': None, 'base_depth_pct': None, 'base_duration_days': None,
            'is_sideways': False, 'volume_dryup': None, 'faults': [], 'mark_says': msg,
        }

    if not highs or not lows or not closes:
        return _none('Yetersiz veri — flat base hesaplanamiyor.')
    n = len(closes)
    if not (len(highs) == len(lows) == n):
        return _none('Flat base: highs/lows/closes uzunluk farkli.')
    if n < FLAT_BASE_MIN_DAYS + 25:
        return _none(f'Yetersiz veri — en az {FLAT_BASE_MIN_DAYS + 25} gun gerek.')

    # 1) Tight band tahmini (son MIN_DAYS) + bar-bazli geri yurume.
    #    Overshoot (high > ceiling) veya deep-dip (low < floor) baz sinirini belirler
    #    -> onceki advance/kirilim absorbe edilmez (aggregate ort. creep'i onlenir).
    max_extend = min(n - 25, FLAT_BASE_MAX_DAYS)  # onceki advance icin >=25 bar birak
    ref_high = max(highs[-FLAT_BASE_MIN_DAYS:])
    ref_low = min(lows[-FLAT_BASE_MIN_DAYS:])
    rng = ref_high - ref_low
    floor = ref_low - rng * 0.5          # asagi modest genisleme toleransi
    ceiling = ref_high * 1.03            # yukari kirilim/overshoot siniri
    k = 0
    while k < max_extend:
        idx = n - 1 - k
        if lows[idx] >= floor and highs[idx] <= ceiling:
            k += 1
        else:
            break
    base_len = k
    if base_len < FLAT_BASE_MIN_DAYS:
        return _none(f'Yatay konsolidasyon <{FLAT_BASE_MIN_DAYS}g (5 hafta) — kisa veya trend icinde.')

    base_high = max(highs[-base_len:])
    base_low = min(lows[-base_len:])
    if base_high <= 0:
        return _none('Baz fiyati gecersiz.')
    base_depth_pct = round((base_high - base_low) / base_high * 100, 2)
    if base_depth_pct > FLAT_BASE_DEPTH_REJECT_PCT:
        return _none(f'Derinlik %{base_depth_pct} (>%{FLAT_BASE_DEPTH_REJECT_PCT:.0f}) — flat base degil.')

    # 2) Yatay (sideways) FILTRE — trend ise flat base degil (1.yari vs 2.yari ort)
    base_closes = closes[-base_len:]
    half = base_len // 2
    first_avg = sum(base_closes[:half]) / half
    second_avg = sum(base_closes[half:]) / (base_len - half)
    half_diff_pct = round(abs(second_avg - first_avg) / first_avg * 100, 2) if first_avg > 0 else 999.0
    is_sideways = half_diff_pct <= FLAT_BASE_SIDEWAYS_TOL_PCT
    if not is_sideways:
        return _none(f'Baz yatay degil (1.yari-2.yari %{half_diff_pct} fark) — trend, flat base degil.')

    # 3) Onceki advance: baz oncesi ~252 gun dip -> baz zirvesi (later-stage sarti)
    pre = lows[max(0, n - base_len - 252):n - base_len]
    prior_low = min(pre) if pre else base_low
    prior_advance_pct = round((base_high - prior_low) / prior_low * 100, 2) if prior_low > 0 else 0.0

    # 4) Hacim dry-up (baz ort < onceki advance ort) — Quanfina heuristic (kitapta % yok)
    volume_dryup = None
    if volumes and len(volumes) == n:
        base_vol = volumes[-base_len:]
        pre_vol = volumes[max(0, n - base_len - 50):n - base_len]
        if base_vol and pre_vol:
            base_avg = sum(base_vol) / len(base_vol)
            pre_avg = sum(pre_vol) / len(pre_vol)
            volume_dryup = pre_avg > 0 and base_avg < pre_avg

    # --- Kusur tespiti (IBD) ---
    faults: list[str] = []
    if base_depth_pct > FLAT_BASE_DEPTH_MAX_PCT:
        faults.append(f'derinlik %{base_depth_pct} (>%15 genis/gevsek, IBD)')
    if prior_advance_pct < FLAT_BASE_PRIOR_ADVANCE_MIN_PCT:
        faults.append(f'onceki advance %{prior_advance_pct} (<%20 — later-stage degil)')

    detected = len(faults) == 0
    if detected:
        quality = 'EXCELLENT' if (volume_dryup and base_depth_pct <= 10.0) else 'GOOD'
    elif len(faults) == 1:
        quality = 'MARGINAL'
    else:
        quality = 'NONE'
        detected = False

    pivot_price = round(base_high, 2)
    weeks = round(base_len / 5, 1)
    fault_str = '; '.join(faults)
    if quality == 'EXCELLENT':
        says = (f'✓ EXCELLENT Flat Base — pivot ${pivot_price}. {weeks} hafta yatay, derinlik '
                f'%{base_depth_pct} (sig), onceki advance %{prior_advance_pct}, hacim dry-up. '
                f'O\'Neil later-stage canon; +%40 hacim kirilimda AL adayi.')
    elif quality == 'GOOD':
        says = (f'GOOD Flat Base — pivot ${pivot_price}. {weeks} hafta yatay, derinlik %{base_depth_pct}, '
                f'onceki advance %{prior_advance_pct}. O\'Neil yapi tutuyor; +%40 hacim kirilim bekle.')
    elif quality == 'MARGINAL':
        says = (f'⚠️ MARGINAL Flat Base — pivot ${pivot_price}. Kusur: {fault_str}. O\'Neil: dikkatli.')
    else:
        says = f'Flat Base kusurlu — {fault_str}.'

    return {
        'detected': detected,
        'quality': quality,
        'pivot_price': pivot_price,
        'prior_advance_pct': prior_advance_pct,
        'base_depth_pct': base_depth_pct,
        'base_duration_days': base_len,
        'is_sideways': is_sideways,
        'volume_dryup': volume_dryup,
        'faults': faults,
        'mark_says': says,
    }


# ======================================================================
# Paket 460 (11 Haz 2026) — Double Bottom (W) Detection
#
# William O'Neil "How to Make Money in Stocks" (CAN SLIM) + IBD/MarketSmith Double
# Bottom. Derin internet arastirma (workflow, MarketSmith resmi + IBD + TraderLion
# HIGH guven) ile esikler kaynak-atifli (Kural #26 — uydurma yok):
#   - Yapi      : "W" = iki dip + arada orta tepe (middle peak)
#   - UNDERCUT  : 2. dip 1. dibin ALTINA iner (shakeout) — TANIMLAYICI/ZORUNLU.
#                 Inmezse "double bottom DEGIL, basarisizliga acik" (TraderLion).
#                 Undercut DERINLIGI icin kaynakta sayisal esik YOK (sadece "slightly lower").
#   - Pivot     : orta tepenin (W ortasi) yuksekligi (+10 cent IBD). NOT: sag-ust tepe DEGIL.
#   - Min sure  : >=7 hafta = 35 islem gunu (IBD/MarketSmith); 7-65 hafta (MarketSmith)
#   - Onceki advance: >=%30 (O'Neil GENEL baz kurali — cup/flat/double hepsi). Kaynak:
#                 IBD/MarketSmith metodolojisi (kitap sayfa atfi turda dogrulanamadi).
#   - Derinlik  : IBD "no more than 30% or 35%" (normal ~30, zayif piyasa ~35);
#                 tipik %20-30; sert ayida %40-50; MarketSmith aralik %10-50. >%50 red.
#   - Hacim     : 2. dipte dry-up; kirilim ort. +%40-50 (IBD).
#   - Kusurlu   : 2. dip undercut yok (=double bottom degil), orta tepe alt yarida,
#                 cok derin/genis, gec-asama.
#
# v1 not: W-finder rim=pencere max high; orta tepe=post-rim max (son 5 bar haric);
# 1. dip=orta tepe oncesi min, 2. dip=orta tepe sonrasi min. Base-stage sayimi YOK
# (gec-asama kusuru tespit edilmez, GELISTIRILMESI LAZIM).
# ======================================================================

DB_MIN_DAYS: int = 35                      # IBD/MarketSmith >=7 hafta
DB_MAX_DAYS: int = 325                     # MarketSmith 7-65 hafta
DB_DEPTH_MAX_PCT: float = 35.0             # IBD "no more than 30% or 35%"
DB_DEPTH_REJECT_PCT: float = 50.0          # MarketSmith max %50 (sert ayi)
DB_PRIOR_ADVANCE_MIN_PCT: float = 30.0     # O'Neil genel baz kurali (IBD/MarketSmith)
DB_BREAKOUT_VOL_RATIO: float = 1.40        # IBD kirilim "+40% above average"


def compute_double_bottom(
    highs: list[float],
    lows: list[float],
    closes: list[float],
    volumes: list[float] | None = None,
) -> dict:
    """O'Neil/IBD double bottom (W) tespiti (kaynak-atifli esikler + Kural #26).

    Args:
        highs/lows/closes: gunluk OHLC (kronolojik, esit uzunluk)
        volumes: gunluk hacim (opsiyonel — 2. dip dry-up degerlendirmesi)

    Returns:
        dict {detected, quality, pivot_price, prior_advance_pct, base_depth_pct,
              base_duration_days, undercut, first_low, second_low, middle_peak,
              faults, mark_says}
    """
    def _none(msg: str) -> dict:
        return {
            'detected': False, 'quality': 'NONE', 'pivot_price': None,
            'prior_advance_pct': None, 'base_depth_pct': None, 'base_duration_days': None,
            'undercut': False, 'first_low': None, 'second_low': None, 'middle_peak': None,
            'faults': [], 'mark_says': msg,
        }

    if not highs or not lows or not closes:
        return _none('Yetersiz veri — double bottom hesaplanamiyor.')
    n = len(closes)
    if not (len(highs) == len(lows) == n):
        return _none('Double bottom: highs/lows/closes uzunluk farkli.')
    if n < DB_MIN_DAYS + 20:
        return _none(f'Yetersiz veri — en az {DB_MIN_DAYS + 20} gun gerek.')

    # 1) Rim (sol kenar = baz oncesi tepe) = pencere max high
    window = min(n, DB_MAX_DAYS)
    seg_h = highs[-window:]
    seg_l = lows[-window:]
    rim_rel = seg_h.index(max(seg_h))
    rim_high = seg_h[rim_rel]
    if rim_high <= 0:
        return _none('Rim fiyati gecersiz.')
    rim_global = n - window + rim_rel
    base_len = window - rim_rel
    if base_len < DB_MIN_DAYS:
        return _none(f'Rim cok yakin / baz <{DB_MIN_DAYS}g (7 hafta) — kirilim sonrasi?')

    # 2) W yapisi: post-rim bolge -> iki dip (yari-yari) + ARALARINDAKI orta tepe.
    #    Orta tepe iki dip ARASINDA aranir (rim'e bitisik post[0] yuksekligi degil) ->
    #    gercek W ortasi yakalanir, sol kenar (rim) disarida kalir.
    post_h = seg_h[rim_rel + 1:]
    post_l = seg_l[rim_rel + 1:]
    if len(post_h) < 15:
        return _none('W yapisi icin yetersiz post-rim veri.')
    half = len(post_l) // 2
    if half < 3:
        return _none('W yapisi icin yetersiz post-rim veri.')
    l1_idx = post_l[:half].index(min(post_l[:half]))            # 1. dip (ilk yari)
    l2_idx = half + post_l[half:].index(min(post_l[half:]))     # 2. dip (ikinci yari)
    if l2_idx <= l1_idx + 1:
        return _none('Iki dip ayrismiyor — W yapisi yok.')
    first_low = post_l[l1_idx]
    second_low = post_l[l2_idx]
    mp_region = post_h[l1_idx:l2_idx + 1]
    mp_idx = l1_idx + mp_region.index(max(mp_region))           # orta tepe (iki dip arasi en yuksek)
    mp_high = post_h[mp_idx]
    base_low = min(first_low, second_low)
    base_depth_pct = round((rim_high - base_low) / rim_high * 100, 2)
    if base_depth_pct > DB_DEPTH_REJECT_PCT:
        return _none(f'Derinlik %{base_depth_pct} (>%{DB_DEPTH_REJECT_PCT:.0f}) — double bottom degil.')

    # 3) UNDERCUT (tanimlayici): 2. dip 1. dibi gecmeli — yoksa double bottom DEGIL
    undercut = second_low <= first_low
    if not undercut:
        return _none('2. dip 1. dibi undercut etmiyor — O\'Neil double bottom degil (TraderLion).')

    # 4) Orta tepe bazin ust yarisinda mi (zayif W kontrolu)
    base_mid = (rim_high + base_low) / 2
    mp_upper_half = mp_high > base_mid

    # 5) Onceki advance (>=%30 O'Neil baz kurali)
    pre = lows[max(0, rim_global - 252):rim_global]
    prior_low = min(pre) if pre else base_low
    prior_advance_pct = round((rim_high - prior_low) / prior_low * 100, 2) if prior_low > 0 else 0.0

    # 6) 2. dip hacim dry-up (heuristic — kitapta sayisal esik yok)
    volume_dryup = None
    if volumes and len(volumes) == n:
        seg_v = volumes[-window:]
        post_v = seg_v[rim_rel + 1:]
        v_l1 = post_v[l1_idx:mp_idx]
        v_l2 = post_v[mp_idx:l2_idx + 1]
        if v_l1 and v_l2:
            a1 = sum(v_l1) / len(v_l1)
            a2 = sum(v_l2) / len(v_l2)
            volume_dryup = a1 > 0 and a2 <= a1   # 2. dip ort hacmi 1. dipten dusuk/esit

    # --- Kusur tespiti ---
    faults: list[str] = []
    if base_depth_pct > DB_DEPTH_MAX_PCT:
        faults.append(f'derinlik %{base_depth_pct} (>%35, sadece sert ayida — IBD)')
    if prior_advance_pct < DB_PRIOR_ADVANCE_MIN_PCT:
        faults.append(f'onceki advance %{prior_advance_pct} (<%30, O\'Neil baz kurali)')
    if not mp_upper_half:
        faults.append('orta tepe bazin alt yarisinda (zayif W)')

    detected = len(faults) == 0
    if detected:
        quality = 'EXCELLENT' if (volume_dryup and base_depth_pct <= 30.0) else 'GOOD'
    elif len(faults) == 1:
        quality = 'MARGINAL'
    else:
        quality = 'NONE'
        detected = False

    pivot_price = round(mp_high, 2)
    weeks = round(base_len / 5, 1)
    l1r = round(first_low, 2)
    l2r = round(second_low, 2)
    fault_str = '; '.join(faults)
    if quality == 'EXCELLENT':
        says = (f'✓ EXCELLENT Double Bottom (W) — pivot ${pivot_price} (orta tepe). {weeks} hafta, '
                f'derinlik %{base_depth_pct}, 2. dip ${l2r} < 1. dip ${l1r} (undercut/shakeout), '
                f'onceki advance %{prior_advance_pct}. O\'Neil canon tam; +%40 hacim kirilimda AL.')
    elif quality == 'GOOD':
        says = (f'GOOD Double Bottom (W) — pivot ${pivot_price} (orta tepe). 2. dip ${l2r} undercut ✓, '
                f'derinlik %{base_depth_pct}. O\'Neil yapi tutuyor; +%40 hacim kirilim bekle.')
    elif quality == 'MARGINAL':
        says = (f'⚠️ MARGINAL Double Bottom — pivot ${pivot_price}. Kusur: {fault_str}. O\'Neil: dikkatli.')
    else:
        says = f'Double Bottom kusurlu — {fault_str}.'

    return {
        'detected': detected,
        'quality': quality,
        'pivot_price': pivot_price,
        'prior_advance_pct': prior_advance_pct,
        'base_depth_pct': base_depth_pct,
        'base_duration_days': base_len,
        'undercut': undercut,
        'first_low': l1r,
        'second_low': l2r,
        'middle_peak': pivot_price,
        'faults': faults,
        'mark_says': says,
    }


# ======================================================================
# KARAR #733 alt-paket (Paket 76, 25 May 2026) — Overhead Supply Detection
#
# Mark TLSMW Ch 10 canon: "Overhead supply" = bir hissenin geçmişte
# bir fiyat seviyesinden düştüğü ve şimdi o seviyeye geri yaklaştığı
# durumda eski sahipler "kayıp kapansın" diye satar — direnç oluşur.
#
# Algoritma:
# - Mevcut fiyatın üstünde son N gün içinde max kapanış (overhead_price)
# - O zirveden ne kadar düşüş yaşandı (drop_pct)
# - O zirveye ne kadar yakınız (proximity_pct)
# - Yüksek hacim günü o zirvede oldu mu (heavy_volume_at_top)
#
# Mark birebir kaynak:
# - TLSMW Ch 10: "Detecting Overhead Supply"
# - O'Neil: "Stocks have memory at old highs"
#
# 3 kategori:
# - HEAVY: Overhead %20+ üstte + yakın yaklaşım (direnç kalın)
# - MODERATE: Overhead %10-20 + orta yakınlık
# - NONE: Overhead %3 altı veya çok uzak (temiz, breakout uygun)
# ======================================================================

OVERHEAD_PROXIMITY_HEAVY_PCT: float = 5.0    # %5 yakını = direnç kritik
OVERHEAD_DROP_HEAVY_PCT: float = 20.0        # %20+ düşüş = ağır overhead
OVERHEAD_DROP_MODERATE_PCT: float = 10.0     # %10-20 = orta
OVERHEAD_LOOKBACK_DAYS: int = 60             # 60 gün geriye bak


def compute_overhead_supply(
    closes: list[float],
    lookback_days: int = OVERHEAD_LOOKBACK_DAYS,
) -> dict:
    """Mark TLSMW Ch 10 Overhead Supply tespiti.

    Args:
        closes: Günlük kapanış fiyatları (kronolojik)
        lookback_days: Geçmiş tepe arama penceresi (default 60)

    Returns:
        dict {
            'category': str,           # HEAVY / MODERATE / NONE
            'overhead_price': float | None,    # Tespit edilen geçmiş tepe
            'drop_pct': float | None,           # Tepeden mevcut düşüş %
            'proximity_pct': float | None,      # Tepeye yakınlık %
            'mark_says': str,
        }
    """
    if not closes:
        return {
            'category': None,
            'overhead_price': None,
            'drop_pct': None,
            'proximity_pct': None,
            'mark_says': 'Yetersiz veri — overhead supply hesaplanamıyor.',
        }
    if len(closes) < 10:
        return {
            'category': None,
            'overhead_price': None,
            'drop_pct': None,
            'proximity_pct': None,
            'mark_says': 'Yetersiz veri — en az 10 gün gerek.',
        }

    current_price = closes[-1]
    window_size = min(lookback_days, len(closes))
    window = closes[-window_size:]

    # Mevcut fiyatın üstündeki en yüksek nokta (overhead)
    overhead_candidates = [p for p in window if p > current_price]
    if not overhead_candidates:
        # Hiç overhead yok — temiz
        return {
            'category': 'NONE',
            'overhead_price': None,
            'drop_pct': 0.0,
            'proximity_pct': None,
            'mark_says': (f'Overhead supply yok — mevcut ${current_price:.2f} '
                          f'lookback içinde en yüksek. Mark: temiz tarla, '
                          f'breakout için uygun.'),
        }

    overhead_price = max(overhead_candidates)
    drop_pct = round((overhead_price - current_price) / overhead_price * 100, 2)
    proximity_pct = round((overhead_price - current_price) / current_price * 100, 2)

    if drop_pct >= OVERHEAD_DROP_HEAVY_PCT:
        category = 'HEAVY'
        says = (f'⚠️ HEAVY Overhead — ${overhead_price:.2f} üstte (%{drop_pct:.1f} '
                f'üzeri tepe), mevcut %{proximity_pct:.1f} uzakta. Mark TLSMW '
                f'Ch 10: eski sahipler "kayıp kapansın" satışı — direnç kalın.')
    elif drop_pct >= OVERHEAD_DROP_MODERATE_PCT:
        category = 'MODERATE'
        says = (f'MODERATE Overhead — ${overhead_price:.2f} (%{drop_pct:.1f} '
                f'üzeri). Mark: orta direnç, kırılım hacim teyitli olmalı.')
    else:
        category = 'NONE'
        says = (f'Hafif/Yok Overhead — ${overhead_price:.2f} sadece '
                f'%{drop_pct:.1f} üzeri. Mark: temiz, breakout uygun.')

    return {
        'category': category,
        'overhead_price': round(overhead_price, 2),
        'drop_pct': drop_pct,
        'proximity_pct': proximity_pct,
        'mark_says': says,
    }


# ======================================================================
# CLIMAX RUN — Mark TLSMW Ch 9 (KARAR #733 alt-paket, Paket 86)
# ======================================================================
# Climax run = parabolic / exhaustion / "last gasp" zirve.
# Mark: "When everyone is screaming buy at the top, that's when you sell".
#
# Tanım:
# - Hisse uzun süredir trend halinde (advance)
# - Son 1-3 haftada keskin hızlanma (parabolic curve)
# - %25+ yükseliş kısa sürede
# - Yüksek hacim + gap-up'lar (exhaustion gap)
# - Slope hızlanıyor (acceleration > normal)
#
# Mark birebir kaynak:
# - TLSMW Ch 9: "Selling Short and Climax Runs"
# - O'Neil: "Exhaustion gap" terimi
# - Minervini Trade Like a Stock Market Wizard: "Climax top" karakteristikleri
#
# 4 kategori:
# - CLIMAX_TOP: Güçlü kanıt — parabolic + büyük kazanç + exhaustion gap (SAT)
# - POTENTIAL_CLIMAX: Bazı işaretler — büyük kazanç ama tam parabolic değil (DİKKAT)
# - HEALTHY_ADVANCE: Normal yükseliş trendi (TUT)
# - NONE: Veri yetersiz veya trend yok
# ======================================================================

CLIMAX_RUN_LOOKBACK_DAYS: int = 15           # Son 15 gün climax penceresi
CLIMAX_RUN_GAIN_TOP_PCT: float = 25.0        # %25+ kısa sürede = climax
CLIMAX_RUN_GAIN_POTENTIAL_PCT: float = 15.0  # %15-25 = potansiyel
CLIMAX_GAP_UP_PCT: float = 3.0               # Quanfina heuristic — kitapta (O'Neil/Mark) gap % esigi YOK
CLIMAX_MIN_GAP_DAYS: int = 2                 # En az 2 gap-up gerek = climax kanıt


def compute_climax_run(
    closes: list[float],
    opens: list[float] | None = None,
    volumes: list[float] | None = None,
    lookback_days: int = CLIMAX_RUN_LOOKBACK_DAYS,
) -> dict:
    """Mark TLSMW Ch 9 Climax Run tespiti.

    Args:
        closes: Günlük kapanışlar (kronolojik)
        opens: Günlük açılışlar (opsiyonel — gap analizi için)
        volumes: Günlük hacimler (opsiyonel — exhaustion teyidi)
        lookback_days: Climax penceresi (default 15 gün)

    Returns:
        dict {
            'category': str,        # CLIMAX_TOP/POTENTIAL_CLIMAX/HEALTHY_ADVANCE/NONE
            'gain_pct': float | None,        # Pencere içi kazanç %
            'gap_up_days': int | None,       # Gap-up gün sayısı
            'avg_volume_ratio': float | None, # Son 5 gün / önceki 10 gün hacim
            'mark_says': str,
        }
    """
    if not closes or len(closes) < lookback_days + 5:
        return {
            'category': None,
            'gain_pct': None,
            'gap_up_days': None,
            'avg_volume_ratio': None,
            'mark_says': f'Yetersiz veri — en az {lookback_days + 5} gün gerek.',
        }

    window = closes[-lookback_days:]
    start_price = window[0]
    current_price = window[-1]
    if start_price <= 0:
        return {
            'category': None,
            'gain_pct': None,
            'gap_up_days': None,
            'avg_volume_ratio': None,
            'mark_says': 'Geçersiz fiyat verisi.',
        }

    gain_pct = round((current_price - start_price) / start_price * 100, 2)

    # Gap-up analizi (opens vs prev close)
    gap_up_days = 0
    if opens is not None and len(opens) >= lookback_days:
        opens_window = opens[-lookback_days:]
        for i in range(1, len(window)):
            prev_close = window[i - 1]
            today_open = opens_window[i]
            if prev_close > 0:
                gap_pct = (today_open - prev_close) / prev_close * 100
                if gap_pct >= CLIMAX_GAP_UP_PCT:
                    gap_up_days += 1

    # Hacim oranı — son 5 gün vs önceki 10 gün
    avg_volume_ratio = None
    if volumes is not None and len(volumes) >= lookback_days:
        vol_window = volumes[-lookback_days:]
        recent_5 = vol_window[-5:]
        prior_10 = vol_window[-15:-5] if len(vol_window) >= 15 else vol_window[:-5]
        if prior_10:
            avg_recent = sum(recent_5) / len(recent_5)
            avg_prior = sum(prior_10) / len(prior_10)
            if avg_prior > 0:
                avg_volume_ratio = round(avg_recent / avg_prior, 2)

    # Kategori karar matrisi
    if gain_pct >= CLIMAX_RUN_GAIN_TOP_PCT and gap_up_days >= CLIMAX_MIN_GAP_DAYS:
        category = 'CLIMAX_TOP'
        says = (f'🔴 CLIMAX TOP — %{gain_pct} kazanç {lookback_days} günde + '
                f'{gap_up_days} exhaustion gap. Mark TLSMW Ch 9: "Last gasp — '
                f'satın al çığlığı zirvede. SAT veya stop sıkılaştır."')
    elif gain_pct >= CLIMAX_RUN_GAIN_TOP_PCT:
        category = 'POTENTIAL_CLIMAX'
        says = (f'⚠️ POTENTIAL CLIMAX — %{gain_pct} kazanç {lookback_days} günde, '
                f'gap yetersiz ({gap_up_days}). Mark: "parabolic ama kanıt eksik, '
                f'stop sıkılaştır, çıkışa hazırlan."')
    elif gain_pct >= CLIMAX_RUN_GAIN_POTENTIAL_PCT:
        category = 'POTENTIAL_CLIMAX'
        says = (f'⚠️ POTENTIAL CLIMAX — %{gain_pct} kazanç orta, dikkat. '
                f'Mark: "ivme yüksek, breakout sonrası normal mi yoksa climax mı?".')
    elif gain_pct > 0:
        category = 'HEALTHY_ADVANCE'
        says = (f'✅ HEALTHY ADVANCE — %{gain_pct} normal trend, climax sinyali yok. '
                f'Mark: "let winners run".')
    else:
        category = 'NONE'
        says = (f'Trend yok / negatif (%{gain_pct}) — climax mantıken geçersiz.')

    return {
        'category': category,
        'gain_pct': gain_pct,
        'gap_up_days': gap_up_days if opens else None,
        'avg_volume_ratio': avg_volume_ratio,
        'mark_says': says,
    }


# ======================================================================
# RELATIVE STRENGTH RATING — Mark TLSMW Ch 3-5 / IBD canon (Paket 91)
# ======================================================================
# Mark Minervini ve O'Neil IBD RS Rating: Hisseyi son 12 ay getirisinde
# tüm evrene göre yüzdelik dilime (1-99) yerleştirir.
#
# Birebir kaynak (3-kaynak triangule, 11 Haz 2026):
# - O'Neil "How to Make Money in Stocks" Bol.5 s.35-37: "RS rank 80 or higher";
#   "super leaders ... 90 or higher just before breaking out"; <70 = laggard.
# - Minervini Trend Template Kural 8 (TLSMW s.94): "RS no less than 70,
#   preferably in the 80s or 90s". -> LEADER>=80 / STRONG>=70 birebir kitap.
# - CANSLIM 'L' = Leader (RS 80+).
#
# Quanfina basitleştirilmiş hesap (canon yorumu):
# - 12 aylık getiri AĞIRLIKLI: son çeyrek %40, önceki 3 çeyrek %20 her biri
# - Benchmark (SPY) getirisi ile oran -> percentile-mimic 1-99
# - 4 kategori:
#   LEADER (>=80): IBD canon — alım adayı (Mark "Top 20%")
#   STRONG (70-79): Üst orta — izleme
#   AVERAGE (50-69): Vasat
#   LAGGARD (<50): Zayıf — Mark "uzak dur"
# ======================================================================

RS_LOOKBACK_DAYS: int = 252                    # 12 ay yaklaşık
RS_QUARTER_DAYS: int = 63                       # 3 ay yaklaşık
RS_WEIGHT_RECENT_Q: float = 0.40                # Son çeyrek %40
RS_WEIGHT_PRIOR_Q: float = 0.20                 # Diğer 3 çeyrek %20 her
RS_THRESHOLD_LEADER: int = 80                   # O'Neil s.35-37 + Minervini R8 (super lider 90+)
RS_THRESHOLD_STRONG: int = 70
RS_THRESHOLD_AVERAGE: int = 50


def rs_rating_category(rs: int) -> str:
    """RS Rating (1-99) -> IBD canon kategori. Tek kaynak eşik (LEADER/STRONG/
    AVERAGE/LAGGARD). DB-first scan rs_ibd ve compute path birebir aynı eşiği
    kullansın diye paylaşımlı helper (P441 — /hisse RS tutarsızlığı fix)."""
    if rs >= RS_THRESHOLD_LEADER:
        return "LEADER"
    if rs >= RS_THRESHOLD_STRONG:
        return "STRONG"
    if rs >= RS_THRESHOLD_AVERAGE:
        return "AVERAGE"
    return "LAGGARD"


def compute_relative_strength_rating(
    stock_closes: list[float],
    benchmark_closes: list[float],
    lookback_days: int = RS_LOOKBACK_DAYS,
) -> dict:
    """Mark/IBD canon RS Rating hesabı.

    Args:
        stock_closes: Hisse günlük kapanışları (kronolojik, >= lookback_days)
        benchmark_closes: SPY veya QQQ benchmark kapanışları (aynı uzunluk)
        lookback_days: Geriye bakış (default 252 = ~12 ay)

    Returns:
        dict {
            'rs_rating': int | None,        # 1-99 IBD-mimic skoru
            'category': str,                 # LEADER/STRONG/AVERAGE/LAGGARD
            'stock_return_pct': float | None,  # Ağırlıklı stok getirisi
            'benchmark_return_pct': float | None,  # Benchmark getirisi
            'outperform_pct': float | None,  # Stock - Benchmark (mutlak fark)
            'mark_says': str,
        }
    """
    if not stock_closes or not benchmark_closes:
        return {
            'rs_rating': None,
            'category': None,
            'stock_return_pct': None,
            'benchmark_return_pct': None,
            'outperform_pct': None,
            'mark_says': 'Yetersiz veri — RS Rating hesaplanamıyor.',
        }
    if len(stock_closes) < lookback_days or len(benchmark_closes) < lookback_days:
        return {
            'rs_rating': None,
            'category': None,
            'stock_return_pct': None,
            'benchmark_return_pct': None,
            'outperform_pct': None,
            'mark_says': f'Yetersiz veri — en az {lookback_days} gün gerek '
                         f'(stok: {len(stock_closes)}, benchmark: {len(benchmark_closes)}).',
        }

    # Son lookback_days dilimi al
    s = stock_closes[-lookback_days:]
    b = benchmark_closes[-lookback_days:]

    # Ağırlıklı çeyrek getirileri
    def weighted_return(closes: list[float]) -> float:
        # 4 çeyrek: Q1 (son), Q2, Q3, Q4 (en eski)
        q1_start = -RS_QUARTER_DAYS
        q2_start = -2 * RS_QUARTER_DAYS
        q3_start = -3 * RS_QUARTER_DAYS
        q4_start = -4 * RS_QUARTER_DAYS

        # Çeyrek başlangıçları geçerli mi?
        if abs(q4_start) > len(closes):
            # Sadece toplam getiri (fallback)
            if closes[0] <= 0:
                return 0.0
            return (closes[-1] - closes[0]) / closes[0] * 100

        # Her çeyreğin başı ve sonu
        q1_ret = (closes[-1] - closes[q1_start]) / closes[q1_start] if closes[q1_start] > 0 else 0.0
        q2_ret = (closes[q1_start] - closes[q2_start]) / closes[q2_start] if closes[q2_start] > 0 else 0.0
        q3_ret = (closes[q2_start] - closes[q3_start]) / closes[q3_start] if closes[q3_start] > 0 else 0.0
        q4_ret = (closes[q3_start] - closes[q4_start]) / closes[q4_start] if closes[q4_start] > 0 else 0.0

        return (
            RS_WEIGHT_RECENT_Q * q1_ret
            + RS_WEIGHT_PRIOR_Q * q2_ret
            + RS_WEIGHT_PRIOR_Q * q3_ret
            + RS_WEIGHT_PRIOR_Q * q4_ret
        ) * 100

    stock_ret = weighted_return(s)
    bench_ret = weighted_return(b)
    outperform = stock_ret - bench_ret

    # IBD-mimic 1-99 skor — outperform aralık bazlı
    # +50% outperform -> 99, +20% -> 80, 0% -> 50, -20% -> 30, -50% -> 1
    if outperform >= 50:
        rs = 99
    elif outperform >= 20:
        # 20-50 arası 80-99 lineer
        rs = int(80 + (outperform - 20) / 30 * 19)
    elif outperform >= 0:
        # 0-20 arası 50-79 lineer
        rs = int(50 + outperform / 20 * 29)
    elif outperform >= -20:
        # -20 to 0 arası 30-49 lineer
        rs = int(30 + (outperform + 20) / 20 * 19)
    elif outperform >= -50:
        # -50 to -20 arası 1-29 lineer
        rs = int(1 + (outperform + 50) / 30 * 28)
    else:
        rs = 1

    rs = max(1, min(99, rs))  # Clamp [1, 99]

    if rs >= RS_THRESHOLD_LEADER:
        category = 'LEADER'
        says = (f'✓ LEADER RS {rs} — IBD canon "Top 20%". Mark: '
                f'"strength leading strength" — alım adayı. '
                f'Outperform: %{outperform:+.1f} vs benchmark.')
    elif rs >= RS_THRESHOLD_STRONG:
        category = 'STRONG'
        says = (f'STRONG RS {rs} — üst orta dilim. Mark: izleme listesi, '
                f'breakout için bekle. Outperform: %{outperform:+.1f}.')
    elif rs >= RS_THRESHOLD_AVERAGE:
        category = 'AVERAGE'
        says = (f'AVERAGE RS {rs} — vasat performans. Mark: leadership '
                f'eksik, başka aday bul. Outperform: %{outperform:+.1f}.')
    else:
        category = 'LAGGARD'
        says = (f'⚠️ LAGGARD RS {rs} — alt dilim. Mark TLSMW Ch 4: '
                f'"uzak dur, leader olmadan kazanç zor". '
                f'Underperform: %{outperform:+.1f}.')

    return {
        'rs_rating': rs,
        'category': category,
        'stock_return_pct': round(stock_ret, 2),
        'benchmark_return_pct': round(bench_ret, 2),
        'outperform_pct': round(outperform, 2),
        'mark_says': says,
    }


# ======================================================================
# ATR VOLATILITY — Mark TLSMW Ch 11 / Wilder canon (Paket 100)
# ======================================================================
# Average True Range — günlük volatilite ölçüsü, Mark stop yerleştirme.
#
# Mark birebir:
# - "ATR-based stops give the stock room to breathe"
# - TLSMW Ch 11: "Don't get stopped out by noise"
# - Stop = Entry - (ATR × multiplier), tipik multiplier 2-3 (sıkı/normal)
#
# Wilder formül (J. Welles Wilder 1978):
# - TR_today = max(high-low, |high-prev_close|, |low-prev_close|)
# - ATR = 14-day rolling average of TR
#
# Volatilite kategorisi (kapanış fiyatına göre ATR % normalize):
# - LOW (<2%): Sessiz, sıkı (Mark VCP final daralma adayı)
# - NORMAL (2-4%): Sağlıklı trend
# - HIGH (4-7%): Yüksek volatilite — stop genişletilmeli
# - EXTREME (>7%): Aşırı oynak — pozisyon küçült veya kaçın
# ======================================================================

ATR_PERIOD: int = 14                          # Wilder canon 14 gün
ATR_VOL_LOW_PCT: float = 2.0
ATR_VOL_NORMAL_PCT: float = 4.0
ATR_VOL_HIGH_PCT: float = 7.0
ATR_STOP_MULTIPLIER_TIGHT: float = 2.0       # Pivot bazlı
ATR_STOP_MULTIPLIER_NORMAL: float = 2.5
ATR_STOP_MULTIPLIER_LOOSE: float = 3.0       # Trail için


def compute_atr_volatility(
    highs: list[float],
    lows: list[float],
    closes: list[float],
    period: int = ATR_PERIOD,
) -> dict:
    """Wilder canon ATR + Mark TLSMW Ch 11 stop placement.

    Args:
        highs: Günlük yüksek fiyatlar (kronolojik)
        lows: Günlük düşük fiyatlar
        closes: Günlük kapanışlar (önceki kapanış için)
        period: ATR rolling penceresi (default 14)

    Returns:
        dict {
            'atr': float | None,                    # Mutlak ATR ($)
            'atr_pct': float | None,                 # ATR / current close %
            'category': str,                         # LOW/NORMAL/HIGH/EXTREME
            'suggested_stop_tight': float | None,    # Entry - 2×ATR
            'suggested_stop_normal': float | None,   # Entry - 2.5×ATR
            'suggested_stop_loose': float | None,    # Entry - 3×ATR
            'mark_says': str,
        }
    """
    if not highs or not lows or not closes:
        return {
            'atr': None,
            'atr_pct': None,
            'category': None,
            'suggested_stop_tight': None,
            'suggested_stop_normal': None,
            'suggested_stop_loose': None,
            'mark_says': 'Yetersiz veri — ATR hesaplanamıyor.',
        }

    if len(highs) != len(lows) or len(lows) != len(closes):
        return {
            'atr': None,
            'atr_pct': None,
            'category': None,
            'suggested_stop_tight': None,
            'suggested_stop_normal': None,
            'suggested_stop_loose': None,
            'mark_says': 'highs/lows/closes uzunlukları eşit olmalı.',
        }

    if len(closes) < period + 1:
        return {
            'atr': None,
            'atr_pct': None,
            'category': None,
            'suggested_stop_tight': None,
            'suggested_stop_normal': None,
            'suggested_stop_loose': None,
            'mark_says': f'Yetersiz veri — en az {period + 1} gün gerek.',
        }

    # True Range listesi (ilk gün yok — prev_close yok)
    trs = []
    for i in range(1, len(closes)):
        h = highs[i]
        l = lows[i]
        prev_c = closes[i - 1]
        tr = max(h - l, abs(h - prev_c), abs(l - prev_c))
        trs.append(tr)

    # ATR = son `period` gün TR ortalaması (basit ortalama Wilder yaklaşımı)
    recent_trs = trs[-period:]
    atr = sum(recent_trs) / len(recent_trs)
    current_close = closes[-1]
    if current_close <= 0:
        return {
            'atr': None,
            'atr_pct': None,
            'category': None,
            'suggested_stop_tight': None,
            'suggested_stop_normal': None,
            'suggested_stop_loose': None,
            'mark_says': 'Geçersiz kapanış fiyatı.',
        }

    atr_pct = round(atr / current_close * 100, 2)
    atr = round(atr, 2)

    if atr_pct < ATR_VOL_LOW_PCT:
        category = 'LOW'
        says = (f'LOW Volatilite — ATR ${atr} (%{atr_pct} kapanış). '
                f'Mark VCP: sessiz daralma, final kontraksiyon adayı.')
    elif atr_pct < ATR_VOL_NORMAL_PCT:
        category = 'NORMAL'
        says = (f'NORMAL Volatilite — ATR ${atr} (%{atr_pct}). Mark TLSMW '
                f'Ch 11: sağlıklı trend, standart stop yeterli.')
    elif atr_pct < ATR_VOL_HIGH_PCT:
        category = 'HIGH'
        says = (f'⚠️ HIGH Volatilite — ATR ${atr} (%{atr_pct}). Mark: '
                f'stop genişletilmeli, pozisyon boyutu küçültülmeli.')
    else:
        category = 'EXTREME'
        says = (f'🔴 EXTREME Volatilite — ATR ${atr} (%{atr_pct}). Mark: '
                f'"Don\'t fight the noise" — pozisyon küçült veya kaçın.')

    suggested_tight = round(current_close - ATR_STOP_MULTIPLIER_TIGHT * atr, 2)
    suggested_normal = round(current_close - ATR_STOP_MULTIPLIER_NORMAL * atr, 2)
    suggested_loose = round(current_close - ATR_STOP_MULTIPLIER_LOOSE * atr, 2)

    return {
        'atr': atr,
        'atr_pct': atr_pct,
        'category': category,
        'suggested_stop_tight': suggested_tight,
        'suggested_stop_normal': suggested_normal,
        'suggested_stop_loose': suggested_loose,
        'mark_says': says,
    }


# ======================================================================
# DISTRIBUTION DAY SEVERITY — O'Neil canon refine (Paket 105)
# ======================================================================
# DD count var (count_distribution_days), severity kategorisi eksikti.
# O'Neil "Three to four distribution days within four weeks is a warning."
# Mark TLSMW Ch 8: piyasa zayıflık göstergesi DD birikimi.
#
# 4 kategori (son 25 gün penceresi):
# - CLEAN (0-2 DD): Piyasa sağlıklı, alıma açık
# - CAUTION (3-4 DD): Mark "stop sıkılaştır, yeni alımları yavaşlat"
# - HEAVY (5-6 DD): Mark "yeni alım YASAK, açık trade'ler taranıyor"
# - EXTREME (7+ DD): Mark "piyasa under pressure, BEAR PRESSURE yaklaşıyor"
# ======================================================================

DD_SEVERITY_CAUTION_MIN: int = 3
DD_SEVERITY_HEAVY_MIN: int = 5
DD_SEVERITY_EXTREME_MIN: int = 7


def compute_distribution_day_severity(dd_count: int) -> dict:
    """O'Neil DD severity kategorisi.

    Args:
        dd_count: Son 25 gün içindeki DD sayısı (count_distribution_days
                  çıktısı)

    Returns:
        dict {
            'severity': str,           # CLEAN/CAUTION/HEAVY/EXTREME
            'allocation_factor': float, # 1.0=normal, 0.5=yarı, 0.0=yasak
            'mark_says': str,
        }
    """
    if dd_count < 0:
        return {
            'severity': None,
            'allocation_factor': 0.0,
            'mark_says': 'Geçersiz DD sayısı.',
        }

    if dd_count >= DD_SEVERITY_EXTREME_MIN:
        severity = 'EXTREME'
        factor = 0.0
        says = (f'🔴 EXTREME — {dd_count} DD son 25 günde. Mark: piyasa '
                f'"under pressure", BEAR PRESSURE yaklaşıyor. Yeni alım '
                f'YASAK, açık trade defansif moda.')
    elif dd_count >= DD_SEVERITY_HEAVY_MIN:
        severity = 'HEAVY'
        factor = 0.25
        says = (f'⚠️ HEAVY — {dd_count} DD. Mark: yeni alım YASAK, açık '
                f'trade\'ler stop sıkılaştır. O\'Neil "5+ DD = piyasa '
                f'kuruyor". Pilot pozisyon (%1-2) sadece lider hisse.')
    elif dd_count >= DD_SEVERITY_CAUTION_MIN:
        severity = 'CAUTION'
        factor = 0.50
        says = (f'CAUTION — {dd_count} DD. O\'Neil "3-4 DD 4 hafta = uyarı". '
                f'Mark: yeni alımları yavaşlat, allocation %50, stop '
                f'sıkılaştır.')
    else:
        severity = 'CLEAN'
        factor = 1.0
        says = (f'CLEAN — {dd_count} DD son 25 günde. Mark: piyasa sağlıklı, '
                f'tam allocation, leadership güçlü.')

    return {
        'severity': severity,
        'allocation_factor': factor,
        'mark_says': says,
    }


# ======================================================================
# RELATIVE VOLUME — Mark hacim asimetri canon (Paket 106)
# ======================================================================
# Mark TLSMW Ch 6: "Volume tells the story" — bugünkü hacim normal mi,
# pivot kırılım/distribution sinyali mi?
#
# Formül: rel_vol = today_volume / 50-day average volume
#
# 5 kategori:
# - DRYING (<0.7x): Mark VCP final contraction — hacim kuruyor
# - QUIET (0.7-1.0): Sıradan, gürültüsüz
# - NORMAL (1.0-1.5): Sağlıklı katılım
# - HIGH (1.5-2.5): Mark pivot kırılım eşiği (>=1.5x)
# - SURGE (>2.5x): Mark "smart money" — climax veya breakout zirvesi
# ======================================================================

REL_VOL_DRYING_MAX: float = 0.7
REL_VOL_QUIET_MAX: float = 1.0
REL_VOL_NORMAL_MAX: float = 1.5
REL_VOL_HIGH_MAX: float = 2.5
REL_VOL_50D_PERIOD: int = 50


def compute_relative_volume(
    volumes: list[float],
    period: int = REL_VOL_50D_PERIOD,
) -> dict:
    """Mark TLSMW Ch 6 / O'Neil canon Relative Volume.

    Args:
        volumes: Günlük hacim listesi (kronolojik, en az period+1 gün)
        period: 50 gün ortalama baz

    Returns:
        dict {
            'rel_vol': float | None,  # Bugün / 50g ortalama
            'today_volume': int | None,
            'avg_volume': int | None,
            'category': str,           # DRYING/QUIET/NORMAL/HIGH/SURGE
            'mark_says': str,
        }
    """
    if not volumes or len(volumes) < period + 1:
        return {
            'rel_vol': None,
            'today_volume': None,
            'avg_volume': None,
            'category': None,
            'mark_says': f'Yetersiz veri — en az {period + 1} gün gerek.',
        }

    today_vol = volumes[-1]
    historical = volumes[-(period + 1):-1]  # son period gün, bugün hariç
    avg = sum(historical) / len(historical)
    if avg <= 0:
        return {
            'rel_vol': None,
            'today_volume': int(today_vol),
            'avg_volume': 0,
            'category': None,
            'mark_says': 'Geçersiz ortalama hacim (sıfır).',
        }

    rel_vol = round(today_vol / avg, 2)

    if rel_vol < REL_VOL_DRYING_MAX:
        category = 'DRYING'
        says = (f'DRYING — rel_vol {rel_vol}x. Mark VCP final contraction: '
                f'"hacim kuruyor, kırılım yakın". %50 altı = ideal sıkışma.')
    elif rel_vol < REL_VOL_QUIET_MAX:
        category = 'QUIET'
        says = (f'QUIET — rel_vol {rel_vol}x. Sıradan, gürültüsüz seyir. '
                f'Mark: bekleme modunda.')
    elif rel_vol < REL_VOL_NORMAL_MAX:
        category = 'NORMAL'
        says = (f'NORMAL — rel_vol {rel_vol}x. Sağlıklı katılım, trend '
                f'devamı.')
    elif rel_vol < REL_VOL_HIGH_MAX:
        category = 'HIGH'
        says = (f'HIGH — rel_vol {rel_vol}x. Mark pivot kırılım eşiği '
                f'(>=1.5x). "Smart money giriyor" — breakout teyit adayı.')
    else:
        category = 'SURGE'
        says = (f'🔴 SURGE — rel_vol {rel_vol}x. Mark TLSMW Ch 9: '
                f'"abnormal volume" — climax veya distribution. Stop '
                f'sıkılaştır, kâr koru.')

    return {
        'rel_vol': rel_vol,
        'today_volume': int(today_vol),
        'avg_volume': int(avg),
        'category': category,
        'mark_says': says,
    }


# ======================================================================
# BREAKOUT QUALITY — Mark TLSMW Ch 10 puan kompoziti (Paket 107)
# ======================================================================
# Pivot kırılım kalitesi 0-100 puan. Mark canon birleşik:
# - Hacim teyit (50g >=1.5x) -> 30 puan
# - Gap-up open (kırılım günü) -> 20 puan
# - breakout_pct ideal +%1.7..%5 -> 25; %5-10 uzamis -> 15; >%10 chase -> 5 (O'Neil s.26-27)
# - Önceki kontraksiyon (VCP daralma) -> 15 puan
# - Overhead temiz -> 10 puan
#
# 4 kategori:
# - EXCELLENT (>=85): A++ kırılım, full size pozisyon
# - GOOD (70-84): Standart Mark canon, normal size
# - MARGINAL (50-69): Zayıf teyit, pilot pozisyon
# - POOR (<50): Mark "alma" - başka aday ara
# ======================================================================

BREAKOUT_QUALITY_EXCELLENT_MIN: int = 85
BREAKOUT_QUALITY_GOOD_MIN: int = 70
BREAKOUT_QUALITY_MARGINAL_MIN: int = 50


def compute_breakout_quality(
    volume_confirmed: bool,
    gap_up: bool,
    breakout_pct: float,
    prior_contraction: bool = False,
    overhead_clean: bool = False,
) -> dict:
    """Mark TLSMW Ch 10 pivot kırılım kalite kompoziti.

    Args:
        volume_confirmed: Hacim >=1.5x 50g ort (O'Neil HTMMIS s.33,164: breakout
            "at least 50% above normal" = 1.5x — KITAP BIREBIR)
        gap_up: Kırılım günü open > prev close +%3 (Quanfina heuristic — kitapta
            gap-up % esigi YOK; O'Neil s.45 & Mark s.219 sadece niteliksel/devasa hacim)
        breakout_pct: Pivot üstü %. O'Neil s.26-27,164: ideal +%0.5..%5; %5-10
            uzamis; >%10 chase/gec — yuksek breakout_pct artik tam puan ALMAZ
        prior_contraction: Önceki VCP daralma (Minervini TLSMW s.199, volatilite düşmüş)
        overhead_clean: Overhead temiz (Minervini — temiz tarla)

    Returns:
        dict {
            'score': int 0-100,
            'category': str EXCELLENT/GOOD/MARGINAL/POOR
            'breakdown': dict,
            'mark_says': str,
        }
    """
    # O'Neil HTMMIS s.26-27,164: pivot'tan %5-10 uzasa "gec kaldin", >%10 chase.
    # Taze kirilim ideal +%1.7..%5 -> tam puan; %5-10 yari; >%10 dusuk; <%0.5 -> 0.
    # (Eski mantik: breakout_pct ne kadar buyukse o kadar puan -> chase'i de 25
    #  puanla odullendiriyordu. 11 Haz 2026 uc-kaynak triangule ile duzeltildi.)
    bo = breakout_pct
    if bo < 0.5:
        bo_score = 0          # pivot henuz kirilmadi
    elif bo < 1.7:
        bo_score = 15         # zayif/marjinal kirilim
    elif bo <= 5.0:
        bo_score = 25         # ideal taze kirilim (O'Neil alim bolgesi)
    elif bo <= 10.0:
        bo_score = 15         # uzamaya basladi (O'Neil %5-10 = gec)
    else:
        bo_score = 5          # chase >%10 — cok gec
    breakdown = {
        'volume': 30 if volume_confirmed else 0,
        'gap_up': 20 if gap_up else 0,
        'breakout_strength': bo_score,
        'prior_contraction': 15 if prior_contraction else 0,
        'overhead_clean': 10 if overhead_clean else 0,
    }
    score = sum(breakdown.values())

    if score >= BREAKOUT_QUALITY_EXCELLENT_MIN:
        category = 'EXCELLENT'
        says = (f'✓ EXCELLENT Breakout {score}/100 — Mark A++ canon. '
                f'Full size pozisyon aday, hacim+gap+momentum birleşik.')
    elif score >= BREAKOUT_QUALITY_GOOD_MIN:
        category = 'GOOD'
        says = (f'GOOD Breakout {score}/100 — Mark standart canon. '
                f'Normal size pozisyon, ana kriterler tutuyor.')
    elif score >= BREAKOUT_QUALITY_MARGINAL_MIN:
        category = 'MARGINAL'
        says = (f'⚠️ MARGINAL Breakout {score}/100 — Mark zayıf teyit. '
                f'Sadece pilot %1-2 pozisyon, kanıt eksik.')
    else:
        category = 'POOR'
        says = (f'POOR Breakout {score}/100 — Mark "fake breakout" riski. '
                f'Alma, başka aday ara veya bekle.')

    return {
        'score': score,
        'category': category,
        'breakdown': breakdown,
        'mark_says': says,
    }


# ======================================================================
# STAGE TRANSITION — Mark TLSMW Ch 4 (Paket 120)
# ======================================================================
# Stan Weinstein 4-Stage + Mark Minervini: Stage 1 (birikim/dip) ->
# Stage 2 (advancing) geçişi erken uyarı.
#
# Mark birebir (TLSMW Ch 4):
# - Stage 1: Quiet accumulation under resistance (30W MA düz veya hafif aşağı)
# - Stage 2 BEGIN: Break above 30W MA on volume (volume confirmation şart)
# - Stage 2 CONFIRMED: Üst trend kanıtlı (price > 30W MA, slope yukarı)
#
# 30W MA = 150 günlük SMA (Mark canon "30-week moving average").
#
# 4 kategori:
# - NO_TRANSITION: Stage 1 sürüyor veya Stage 3/4 (geçiş yok)
# - EARLY_STAGE_2: Yeni kırılım (1-10 gün, fragile, henüz teyit yok)
# - CONFIRMED_STAGE_2: Sağlam Stage 2 (>10 gün üstte + slope yukarı + hacim)
# - STAGE_2_MATURE: Olgun trend (>60 gün, climax riski artıyor)
# ======================================================================

STAGE_MA_PERIOD: int = 150              # 30W MA
STAGE_EARLY_DAYS_MAX: int = 10          # 1-10 gün = early
STAGE_MATURE_DAYS_MIN: int = 60         # 60+ gün = mature
STAGE_SLOPE_LOOKBACK: int = 30          # 30 gün slope hesap


def compute_stage_transition(
    closes: list[float],
    volumes: Optional[list[float]] = None,
    ma_period: int = STAGE_MA_PERIOD,
) -> dict:
    """Mark TLSMW Ch 4 Stage 1->2 geçiş tespiti.

    Args:
        closes: Günlük kapanışlar (kronolojik, en az ma_period gün)
        volumes: Opsiyonel hacim listesi (teyit için)
        ma_period: SMA penceresi (default 150 = 30 hafta)

    Returns:
        dict {
            'category': str,                 # NO_TRANSITION / EARLY / CONFIRMED / MATURE
            'ma_value': float | None,         # Mevcut 30W MA
            'price_above_ma_pct': float | None,  # Mevcut % üst
            'slope_pct': float | None,        # 30 gün MA slope %
            'days_above_ma': int | None,      # Üstte kalınan gün
            'volume_trend': str | None,       # RISING/STABLE/FALLING (opsiyonel)
            'mark_says': str,
        }
    """
    if not closes or len(closes) < ma_period + STAGE_SLOPE_LOOKBACK:
        return {
            'category': None,
            'ma_value': None,
            'price_above_ma_pct': None,
            'slope_pct': None,
            'days_above_ma': None,
            'volume_trend': None,
            'mark_says': f'Yetersiz veri — en az {ma_period + STAGE_SLOPE_LOOKBACK} gün gerek.',
        }

    # MA hesabı (son ma_period gün)
    ma_window = closes[-ma_period:]
    ma_value = sum(ma_window) / ma_period

    current = closes[-1]
    if ma_value <= 0:
        return {
            'category': None,
            'ma_value': None,
            'price_above_ma_pct': None,
            'slope_pct': None,
            'days_above_ma': None,
            'volume_trend': None,
            'mark_says': 'Geçersiz MA değeri.',
        }

    price_above_ma_pct = round((current - ma_value) / ma_value * 100, 2)

    # MA slope (30 gün önceki MA ile karşılaştır)
    earlier_ma_window = closes[-(ma_period + STAGE_SLOPE_LOOKBACK):-STAGE_SLOPE_LOOKBACK]
    earlier_ma = sum(earlier_ma_window) / ma_period
    slope_pct = round((ma_value - earlier_ma) / earlier_ma * 100, 2) if earlier_ma > 0 else 0.0

    # Üstte kalınan gün sayısı (geriye saymak)
    days_above = 0
    for i in range(len(closes) - 1, -1, -1):
        # MA'nın o günkü değeri için pencere kaydır
        if i < ma_period - 1:
            break
        window = closes[i - ma_period + 1:i + 1]
        ma_at = sum(window) / ma_period
        if closes[i] > ma_at:
            days_above += 1
        else:
            break

    # Hacim trendi (opsiyonel)
    volume_trend = None
    if volumes is not None and len(volumes) >= 30:
        recent_10 = sum(volumes[-10:]) / 10
        prior_20 = sum(volumes[-30:-10]) / 20
        if prior_20 > 0:
            ratio = recent_10 / prior_20
            if ratio >= 1.20:
                volume_trend = 'RISING'
            elif ratio <= 0.80:
                volume_trend = 'FALLING'
            else:
                volume_trend = 'STABLE'

    # Kategori belirleme
    if current <= ma_value:
        # MA altı = Stage 1 veya 3/4 (geçiş yok)
        category = 'NO_TRANSITION'
        says = (f'NO_TRANSITION — Fiyat ${current:.2f} <= 30W MA ${ma_value:.2f}. '
                f'Mark TLSMW Ch 4: "Stage 1 quiet accumulation" sürüyor veya Stage '
                f'3/4 zayıflık. Henüz kırılım yok.')
    elif days_above <= STAGE_EARLY_DAYS_MAX:
        category = 'EARLY_STAGE_2'
        vol_extra = f' Hacim trendi: {volume_trend}.' if volume_trend else ''
        says = (f'⚡ EARLY STAGE 2 — Fiyat %{price_above_ma_pct} 30W MA üstünde '
                f'({days_above} gün). Mark: "yeni kırılım, fragile". Stop {ma_value:.2f} '
                f'altına. Hacim teyit gerek.{vol_extra}')
    elif days_above >= STAGE_MATURE_DAYS_MIN:
        category = 'STAGE_2_MATURE'
        says = (f'STAGE 2 MATURE — {days_above} gün 30W MA üstünde. Mark: trend '
                f'olgun, climax riski yaklaşıyor. Stop sıkılaştır, kâr koru. '
                f'Slope %{slope_pct}.')
    else:
        category = 'CONFIRMED_STAGE_2'
        vol_extra = f' Hacim: {volume_trend}.' if volume_trend else ''
        slope_note = 'pozitif slope' if slope_pct > 0 else 'düz slope (dikkat)'
        says = (f'✓ CONFIRMED STAGE 2 — {days_above} gün üstte, {slope_note} '
                f'%{slope_pct}, fiyat %{price_above_ma_pct} üst. Mark: trend takip, '
                f'pyramid uygun.{vol_extra}')

    return {
        'category': category,
        'ma_value': round(ma_value, 2),
        'price_above_ma_pct': price_above_ma_pct,
        'slope_pct': slope_pct,
        'days_above_ma': days_above,
        'volume_trend': volume_trend,
        'mark_says': says,
    }


# =============================================================================
# P425 (31 May 2026) — Ek Piyasa Göstergeleri (derin tarama F4/F5/F7 sentezi)
# Kaynak: notebook/analizler/Market_Health_Metodoloji_Arastirma.md
# Kural #26: her formül kanon kaynaklı + sayısal eşik atıflı. Kural #28: yetersiz
# veri durumunda MOCK sayı YOK — 'data_sufficient': False + gerekli/mevcut gün.
# =============================================================================

# Faber GTAA (SSRN 2007) — 10 aylık SMA timing kuralı. Aylık ~21 işlem günü.
FABER_SMA_MONTHS = 10
FABER_TRADING_DAYS_PER_MONTH = 21

# McClellan Oscillator — 19/39 günlük EMA (Trend %10/%5 sabitlerine eşdeğer).
MCCLELLAN_FAST_EMA = 19
MCCLELLAN_SLOW_EMA = 39

# Zweig Breadth Thrust — 10 günlük EMA, 0.40 -> 0.615 sıçraması (10 gün içinde).
ZWEIG_EMA_DAYS = 10
ZWEIG_LOW_THRESHOLD = 0.40
ZWEIG_HIGH_THRESHOLD = 0.615


def _ema(values: list[float], period: int) -> Optional[float]:
    """Standart EMA (son değer). period+ veri gerek, yoksa None."""
    if not values or len(values) < period:
        return None
    k = 2.0 / (period + 1)
    ema = sum(values[:period]) / period  # ilk SMA seed
    for v in values[period:]:
        ema = v * k + ema * (1 - k)
    return ema


def compute_faber_timing(closes: list[float]) -> dict:
    """Faber GTAA 10-aylık SMA timing (SSRN 2007, F4 kanon).

    Kural birebir: "Buy when monthly price > 10-month SMA. Sell and move to
    cash when monthly price < 10-month SMA." Tek girdi: fiyat.

    Args:
        closes: SPY günlük kapanış (kronolojik, en az 10 ay * 21 ~ 210 gün).

    Returns:
        dict {
            'data_sufficient': bool,
            'signal': 'INVESTED' | 'CASH' | None,   # fiyat > 10mo SMA -> INVESTED
            'price': float | None,                    # son aylık kapanış
            'sma_10mo': float | None,
            'pct_vs_sma': float | None,               # (fiyat-SMA)/SMA * 100
            'days_needed': int, 'days_have': int,
            'says': str,
        }
    """
    need = FABER_SMA_MONTHS * FABER_TRADING_DAYS_PER_MONTH  # ~210
    have = len(closes) if closes else 0
    if have < need:
        return {
            'data_sufficient': False, 'signal': None, 'price': None,
            'sma_10mo': None, 'pct_vs_sma': None,
            'days_needed': need, 'days_have': have,
            'says': f'Yetersiz veri — Faber 10-ay SMA icin ~{need} gun gerek ({have} var).',
        }
    # Aylık kapanışlar: her ~21 günde bir (son 10 ay). closes[-1] = en güncel.
    monthly = closes[-(FABER_SMA_MONTHS * FABER_TRADING_DAYS_PER_MONTH)::FABER_TRADING_DAYS_PER_MONTH]
    if len(monthly) < FABER_SMA_MONTHS:
        # ornekleme yetersiz -> son 10 ayi garanti et
        monthly = [closes[-1 - i * FABER_TRADING_DAYS_PER_MONTH]
                   for i in range(FABER_SMA_MONTHS)][::-1]
    sma_10mo = sum(monthly) / len(monthly)
    price = closes[-1]
    pct_vs_sma = ((price - sma_10mo) / sma_10mo * 100) if sma_10mo else 0.0
    invested = price > sma_10mo
    return {
        'data_sufficient': True,
        'signal': 'INVESTED' if invested else 'CASH',
        'price': round(price, 2),
        'sma_10mo': round(sma_10mo, 2),
        'pct_vs_sma': round(pct_vs_sma, 2),
        'days_needed': need, 'days_have': have,
        'says': ('Fiyat 10-ay SMA ustunde -> yatirimli (Faber GTAA AL).'
                 if invested else
                 'Fiyat 10-ay SMA altinda -> nakit (Faber GTAA SAT).'),
    }


def compute_mcclellan_oscillator(advances: list[int], declines: list[int]) -> dict:
    """McClellan Oscillator (F7 kanon): (19g EMA - 39g EMA) of (adv - dec).

    Pozitif = breadth momentum yukari, negatif = asagi. Sifir cizgisi gecisleri
    momentum donusu. Kaynak: mcoscillator.com + StockCharts (19/39 EMA, %10/%5
    Trend sabitlerine matematiksel esdegerdir).

    Args:
        advances/declines: günlük yükselen/düşen sayısı (kronolojik).

    Returns:
        dict {'data_sufficient': bool, 'value': float|None, 'signal': str|None,
              'days_needed': int, 'days_have': int, 'says': str}
    """
    need = MCCLELLAN_SLOW_EMA  # 39
    have = min(len(advances), len(declines)) if advances and declines else 0
    if have < need:
        return {
            'data_sufficient': False, 'value': None, 'signal': None,
            'days_needed': need, 'days_have': have,
            'says': f'Yetersiz veri — McClellan icin {need} gun gerek ({have} var, birikiyor).',
        }
    net = [a - d for a, d in zip(advances, declines)]
    fast = _ema(net, MCCLELLAN_FAST_EMA)
    slow = _ema(net, MCCLELLAN_SLOW_EMA)
    if fast is None or slow is None:
        return {
            'data_sufficient': False, 'value': None, 'signal': None,
            'days_needed': need, 'days_have': have,
            'says': 'EMA hesaplanamadi.',
        }
    value = fast - slow
    signal = 'BULLISH' if value > 0 else 'BEARISH'
    return {
        'data_sufficient': True,
        'value': round(value, 1),
        'signal': signal,
        'days_needed': need, 'days_have': have,
        'says': (f'McClellan {round(value,1)} > 0 -> breadth momentum yukari.'
                 if value > 0 else
                 f'McClellan {round(value,1)} < 0 -> breadth momentum asagi.'),
    }


def compute_zweig_breadth_thrust(advances: list[int], declines: list[int]) -> dict:
    """Zweig Breadth Thrust (F-research): 10 günlük EMA of adv/(adv+dec).

    Thrust = EMA 10 gün içinde 0.40 altından 0.615 üstüne çıkarsa (nadir,
    güçlü boğa başlangıcı sinyali). Kaynak: Martin Zweig "Winning on Wall
    Street". Burada mevcut EMA + thrust durumu döner.

    Args:
        advances/declines: günlük yükselen/düşen (kronolojik, >=10 gün).

    Returns:
        dict {'data_sufficient': bool, 'ema_ratio': float|None,
              'thrust_active': bool, 'zone': str|None, ...}
    """
    need = ZWEIG_EMA_DAYS  # 10
    have = min(len(advances), len(declines)) if advances and declines else 0
    if have < need:
        return {
            'data_sufficient': False, 'ema_ratio': None, 'thrust_active': False,
            'zone': None, 'days_needed': need, 'days_have': have,
            'says': f'Yetersiz veri — Zweig icin {need} gun gerek ({have} var).',
        }
    # adv/(adv+dec) oranlari -> 10 gunluk EMA
    ratios = [a / (a + d) if (a + d) > 0 else 0.5 for a, d in zip(advances, declines)]
    ema_ratio = _ema(ratios, ZWEIG_EMA_DAYS)
    if ema_ratio is None:
        return {
            'data_sufficient': False, 'ema_ratio': None, 'thrust_active': False,
            'zone': None, 'days_needed': need, 'days_have': have,
            'says': 'EMA hesaplanamadi.',
        }
    # Thrust event tespiti icin EMA serisinin 10 gun once <0.40, simdi >0.615
    # olmasi gerek. Tek-nokta EMA ile event tespiti sinirli -> zone bildir.
    if ema_ratio >= ZWEIG_HIGH_THRESHOLD:
        zone = 'THRUST_ZONE'
    elif ema_ratio <= ZWEIG_LOW_THRESHOLD:
        zone = 'OVERSOLD'
    else:
        zone = 'NEUTRAL'
    return {
        'data_sufficient': True,
        'ema_ratio': round(ema_ratio, 3),
        'thrust_active': ema_ratio >= ZWEIG_HIGH_THRESHOLD,
        'zone': zone,
        'low_threshold': ZWEIG_LOW_THRESHOLD,
        'high_threshold': ZWEIG_HIGH_THRESHOLD,
        'days_needed': need, 'days_have': have,
        'says': {
            'THRUST_ZONE': f'10g A/D EMA {round(ema_ratio,3)} >= 0.615 -> Breadth Thrust bolgesi (guclu boga).',
            'OVERSOLD': f'10g A/D EMA {round(ema_ratio,3)} <= 0.40 -> asiri satim (thrust adayi).',
            'NEUTRAL': f'10g A/D EMA {round(ema_ratio,3)} -> notr bolge (0.40-0.615 arasi).',
        }[zone],
    }
