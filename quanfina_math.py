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
VCP_VOL_DRY_RATIO = 0.70         # son gün vol < 50-gün MA × bu oran
VCP_TIGHT_RANGE_PCT = 2.0        # % - ardışık close değişim üst sınırı (range_pct proxy)
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

    3 koşul aynı anda:
      1. is_small_drops: Son 5 gün ortalama düşüş < %1.5
      2. volume_drying: Son gün hacim < 50-gün ortalama × 0.70
      3. tight_closes: Ardışık close değişim < %2 (PVH range_pct proxy)

    NOT: Brandon orijinal formülünde "range_pct" gün-içi high-low farkı.
    Quanfina PVH formatı {date, close, volume} - high/low YOK.
    Proxy: ardışık close değişim < %2 (yaklaşık eşdeğer, Sprint 4-bis.5 adayı
    PVH genişletme high/low ekleyerek tam doğru ölçüm).

    Args:
        price_volume_history: list[dict] - [{"date":..., "close":..., "volume":...}, ...]
                              veya None/kısa liste

    Returns:
        bool: True = VCP setup'i olgun (Brandon kriteri 3 koşul AND)
    """
    if not price_volume_history or len(price_volume_history) < VCP_MIN_HISTORY:
        return False

    try:
        recent = price_volume_history[-VCP_LOOKBACK_DAYS:]
        # Ardışık close değişim için bir önceki gün de gerekli
        prev_day = price_volume_history[-(VCP_LOOKBACK_DAYS + 1)]
        last_50 = price_volume_history[-VCP_MIN_HISTORY:]

        if len(recent) < VCP_LOOKBACK_DAYS:
            return False

        # 1. is_small_drops: ardışık close değişim ortalama düşüş < %1.5
        all_window = [prev_day] + recent
        pct_changes = [
            (all_window[i]["close"] - all_window[i-1]["close"]) / all_window[i-1]["close"] * 100
            for i in range(1, len(all_window))
            if all_window[i-1]["close"] > 0
        ]
        drops = [abs(c) for c in pct_changes if c < 0]
        avg_drop = sum(drops) / max(len(drops), 1) if drops else 0.0
        is_small_drops = avg_drop < VCP_AVG_DROP_THRESHOLD

        # 2. volume_drying: son gün hacim < 50-gün MA × 0.70
        avg_50d_volume = sum(d["volume"] for d in last_50) / len(last_50)
        if avg_50d_volume <= 0:
            return False
        last_volume = recent[-1]["volume"]
        volume_drying = last_volume < avg_50d_volume * VCP_VOL_DRY_RATIO

        # 3. tight_closes: ardışık close değişim < %2 (range_pct proxy)
        close_changes_abs = [abs(c) for c in pct_changes]
        tight_closes = all(c < VCP_TIGHT_RANGE_PCT for c in close_changes_abs)

        return bool(is_small_drops and volume_drying and tight_closes)
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
