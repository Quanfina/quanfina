"use client";

import { useMemo } from "react";
import { useTrades } from "@/hooks/use-trades";
import { useMarketStatus } from "@/hooks/use-market-status";

/**
 * Paket 193 (26 May 2026): Trading Mode otomatik tetik (Vizyon KALICI İLKE #10).
 *
 * Sn. Ferit'in trade davranış mod sistemi — disiplin = içsel, sistem = dışsal.
 * Sn. Ferit kuralla TARTIŞMAZ; mod otomatik tetik, sistem öneri verir.
 *
 * 4 Mod:
 * | Mod       | Tetik                                                       | Sizing  | Davranış                            |
 * |-----------|-------------------------------------------------------------|---------|-------------------------------------|
 * | Normal    | Default (hiçbiri yok)                                       | %1 R    | Tüm sinyaller AL/GEÇ                |
 * | Rehab     | Ardışık 3+ kayıp VEYA drawdown >%10                         | %0.5 R  | Yeni AL önce ⚠️ rehab uyarısı       |
 * | Defansif  | Market Health <30 VEYA Stage 3-4 piyasa                     | Kapanış | Yeni AL'lar BLOK                    |
 * | Agresif   | Market Health >70 + ardışık 5+ kazanç                       | %1.5-2 R| Conviction High sinyaller öncelikli |
 *
 * Disiplin: AI MANUEL OVERRIDE YASAK. Sn. Ferit "Normal mode hep" demek
 * için manuel ayar yapar (gelecek paket). Şu an pure deterministic hesap.
 */

export type TradingMode = "normal" | "rehab" | "defansif" | "agresif";

export interface TradingModeInfo {
  mode: TradingMode;
  reason: string;            // İnsan okunabilir tetik açıklaması
  emoji: string;
  color: string;
  recommendedSizingPct: number;   // 0.5 / 1.0 / 1.5
  uiBehavior: string;
  // İstatistikler (UI için)
  consecutiveWins: number;
  consecutiveLosses: number;
  totalClosedTrades: number;
}

/**
 * Açık + kapalı trade'lerden + piyasa sağlığından mod hesap.
 * Trade verisi: useTrades zaten cache'li (60s).
 * Market: useMarketStatus zaten cache'li.
 */
// Realized drawdown tetiği için notional portföy tabanı. OpenPositionsRiskPanel
// ile aynı konvansiyon ($100k) — DRY. İleride gerçek portföy değerine bağlanabilir.
const REALIZED_DD_BASE = 100_000;
// Rehab tetik eşiği — Mark TTLC s.187 disiplini (CLAUDE.md Mod Geçişleri tablosu).
const REHAB_DRAWDOWN_PCT = 10;

export function useTradingMode(): TradingModeInfo {
  const trades = useTrades();
  const market = useMarketStatus();

  return useMemo(() => {
    const allTrades = trades.data ?? [];
    const closedTrades = allTrades
      .filter((t) => t.status === "closed" && t.pl_dollar != null)
      .sort((a, b) => (b.exit_date ?? "").localeCompare(a.exit_date ?? ""));  // En yeni ÖNCE

    // Ardışık kazanç/kayıp sayım (en yeni'den geriye)
    let consecutiveWins = 0;
    let consecutiveLosses = 0;
    for (const t of closedTrades) {
      const pl = t.pl_dollar ?? 0;
      if (pl > 0) {
        if (consecutiveLosses > 0) break;  // Streak kırıldı
        consecutiveWins += 1;
      } else if (pl < 0) {
        if (consecutiveWins > 0) break;
        consecutiveLosses += 1;
      } else {
        break;
      }
    }

    const totalClosedTrades = closedTrades.length;

    // Realized drawdown — kapalı trade kümülatif P&L equity eğrisinden (eskiden
    // yeniye), $100k notional tabandan tepe-noktaya (high-water mark) göre.
    // Açık pozisyon hariç (realized-only). Tepe-noktadan >%10 düşüş = Rehab tetiği.
    let equity = REALIZED_DD_BASE;
    let peakEquity = REALIZED_DD_BASE;
    for (let i = closedTrades.length - 1; i >= 0; i--) {
      // closedTrades en-yeni-önce sıralı → geriye it (i--) = kronolojik (eskiden yeniye)
      equity += closedTrades[i].pl_dollar ?? 0;
      if (equity > peakEquity) peakEquity = equity;
    }
    const currentDrawdownPct =
      peakEquity > 0 ? ((peakEquity - equity) / peakEquity) * 100 : 0;

    const marketHealthScore = market.data?.market_health_score ?? null;
    // Piyasa Stage Mark canon — health_label / mode kullan
    const marketMode = market.data?.suggested_mode ?? null;  // LONG / CAUTION / DEFENSIVE
    const isMarketWeak = marketHealthScore != null && marketHealthScore < 30;
    const isMarketStrong = marketHealthScore != null && marketHealthScore > 70;
    // P451 (DD araştırma follow-up): İLKE #10 "Defansif = Piyasa Stage 3-4 VEYA
    // MH<30". Eski kod SADECE MH<30 bakıyordu → Stage 3 (topping) market MH>=30
    // ise defansif kaçırıyordu; getDefansifBlockMessage "Stage 3-4" iddia ediyordu
    // (kod-mesaj tutarsızlığı). 2+ endeks Stage 3-4 = piyasa bozulması
    // (_compute_market_health "2+ Stage 4" pateniyle tutarlı; conservative — 1 değil 2).
    const stage34Count = [
      market.data?.spy_stage, market.data?.qqq_stage, market.data?.iwm_stage,
    ].filter((s) => s != null && s >= 3).length;
    const isMarketStage34 = stage34Count >= 2;

    // Tetik sırası (önce kötü, sonra iyi):
    // 1. Defansif (piyasa zayıf VEYA Stage 3-4 — İLKE #10)
    if (isMarketWeak || isMarketStage34) {
      return {
        mode: "defansif",
        reason: isMarketWeak
          ? `Piyasa sağlığı ${marketHealthScore ?? "—"}/100 (<30). Yeni AL'lar bloklu, kapanış öncelikli.`
          : `${stage34Count} endeks Stage 3-4 (topping/düşüş) — İLKE #10 defansif. Yeni AL'lar bloklu.`,
        emoji: "🛡️",
        color: "var(--mtp-danger)",
        recommendedSizingPct: 0,
        uiBehavior: "Yeni AL'lar BLOK — sadece SAT/STOP işlemleri",
        consecutiveWins,
        consecutiveLosses,
        totalClosedTrades,
      };
    }
    // 2. Rehab (ardışık kayıp VEYA realized drawdown >%10 — CLAUDE.md Mod Geçişleri)
    const drawdownTriggered = currentDrawdownPct > REHAB_DRAWDOWN_PCT;
    if (consecutiveLosses >= 3 || drawdownTriggered) {
      const reason = drawdownTriggered
        ? `Realized drawdown %${currentDrawdownPct.toFixed(1)} (>%${REHAB_DRAWDOWN_PCT} eşik). Mark TTLC s.187: yeni trade'lerde pozisyon yarıya.`
        : `${consecutiveLosses} ardışık kayıp. Mark TTLC s.187: yeni trade'lerde pozisyon yarıya.`;
      return {
        mode: "rehab",
        reason,
        emoji: "⚠️",
        color: "#F59E0B",
        recommendedSizingPct: 0.5,
        uiBehavior: "Yeni AL önce ⚠️ uyarısı — %0.5 R sizing (yarım pozisyon)",
        consecutiveWins,
        consecutiveLosses,
        totalClosedTrades,
      };
    }
    // 3. Agresif (piyasa güçlü + ardışık kazanç)
    if (isMarketStrong && consecutiveWins >= 5) {
      return {
        mode: "agresif",
        reason: `Piyasa sağlığı ${marketHealthScore}/100 (>70) + ${consecutiveWins} ardışık kazanç. Mark "Hot hand" — Conviction High odaklı.`,
        emoji: "🚀",
        color: "var(--mtp-excellent)",
        recommendedSizingPct: 1.5,
        uiBehavior: "Conviction High sinyaller öncelikli — %1.5-2 R sizing",
        consecutiveWins,
        consecutiveLosses,
        totalClosedTrades,
      };
    }
    // 4. Default Normal
    return {
      mode: "normal",
      reason: `Normal mod — piyasa stabil${
        marketHealthScore != null ? ` (sağlık ${marketHealthScore}/100)` : ""
      }${marketMode ? `, ${marketMode}` : ""}.`,
      emoji: "●",
      color: "var(--mtp-neutral)",
      recommendedSizingPct: 1.0,
      uiBehavior: "Standart R (%1) sizing, tüm sinyaller değerlendir.",
      consecutiveWins,
      consecutiveLosses,
      totalClosedTrades,
    };
  }, [trades.data, market.data]);
}

/**
 * Paket 234 (27 May 2026): DRY helper — Defansif modda yeni AL bloklayan
 * tutarlı kullanıcı-yüzü mesajı. P227 AddTradeDialog + P235 Sinyaller AL
 * butonu paralel kullanımı için tek kaynak.
 *
 * @example
 * // AddTradeDialog handleSubmit error mesajı (P264)
 * const baseMsg = getDefansifBlockMessage(tradingMode.mode);
 * if (baseMsg) setError(`${baseMsg} 'Riski biliyorum' kutusunu işaretleyin.`);
 *
 * @example
 * // null check (Normal/Rehab/Agresif)
 * getDefansifBlockMessage("normal")    // → null
 * getDefansifBlockMessage("defansif")  // → "DEFANSİF mod aktif..."
 */
export function getDefansifBlockMessage(mode: TradingMode): string | null {
  if (mode === "defansif") {
    return "DEFANSİF mod aktif (piyasa Stage 3-4 veya MH<30) — Mark TTLC s.187: yeni AL BLOK";
  }
  return null;
}

/**
 * Paket 234 (27 May 2026): isNewAlBlocked = mod yeni AL'ı blokluyor mu?
 * Sn. Ferit override seçeneği UI tarafında ayrı state. Helper sadece
 * "default davranış" cevabı.
 *
 * @example
 * // Sinyaller AL butonu disabled (P235)
 * const alBlocked = isNewAlBlocked(tradingMode.mode);
 * <button disabled={alBlocked} title={alBlocked ? "Defansif mod aktif" : ""}>
 *
 * @example
 * // AddTradeDialog submit guard (P227)
 * if (!isClosed && isNewAlBlocked(tradingMode.mode) && !defansifOverride) {
 *   setError(getDefansifBlockMessage(tradingMode.mode));
 *   return;
 * }
 */
export function isNewAlBlocked(mode: TradingMode): boolean {
  return mode === "defansif";
}

/**
 * Paket 259 (27 May 2026): DRY helper — mode rengine göre UI theme.
 * Dashboard (P255), Tarama (P253), AddTradeDialog banner (P196) gibi
 * 5+ yerde tekrarlayan banner stilini tek kaynaktan al.
 *
 * Returns: { background, borderColor, color, emoji, message }
 * - background/borderColor/color: inline style için CSS değerleri
 * - emoji: aria-hidden ikon
 * - message: kısa kullanıcı mesajı (default — sayfaya özel override mümkün)
 *
 * @example
 * // Dashboard Defansif banner (P260)
 * const theme = getModUiTheme(tradingMode.mode);
 * if (!theme) return null;  // Normal: banner gizli
 * <div style={{ background: theme.background, color: theme.color }}>
 *   <span>{theme.emoji}</span>
 *   <b>{theme.shortMessage}</b> ek metin...
 * </div>
 *
 * @example
 * // 4 mod tema dönüş değerleri
 * getModUiTheme("normal")    // → null (banner gizlenir)
 * getModUiTheme("defansif")  // → { emoji: "🛡️", color: "--mtp-danger", ... }
 * getModUiTheme("rehab")     // → { emoji: "🩹", color: "#F59E0B", ... }
 * getModUiTheme("agresif")   // → { emoji: "🚀", color: "--mtp-excellent", ... }
 */
export interface ModUiTheme {
  background: string;
  borderColor: string;
  color: string;
  emoji: string;
  shortMessage: string;
}

export function getModUiTheme(mode: TradingMode): ModUiTheme | null {
  if (mode === "defansif") {
    return {
      background: "rgba(220, 53, 69, 0.08)",
      borderColor: "rgba(220, 53, 69, 0.30)",
      color: "var(--mtp-danger)",
      emoji: "🛡️",
      shortMessage: "DEFANSİF mod aktif — Mark TTLC s.187: yeni AL pozisyonları BLOK.",
    };
  }
  if (mode === "rehab") {
    return {
      background: "rgba(245, 158, 11, 0.08)",
      borderColor: "rgba(245, 158, 11, 0.30)",
      color: "#F59E0B",
      emoji: "🩹",
      shortMessage: "REHAB mod aktif — %0.5 R sizing (yarım pozisyon), yeni AL önce uyarı.",
    };
  }
  if (mode === "agresif") {
    return {
      background: "color-mix(in srgb, var(--mtp-excellent) 8%, transparent)",
      borderColor: "color-mix(in srgb, var(--mtp-excellent) 30%, transparent)",
      color: "var(--mtp-excellent)",
      emoji: "🚀",
      shortMessage: "AGRESİF mod aktif — Hot hand: Conviction High odaklı %1.5-2 R sizing.",
    };
  }
  return null; // normal: banner gösterme
}
