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
        adjusted_ratio = float('inf')
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
