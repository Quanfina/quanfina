/**
 * KARAR ADAY #729 (24 May 2026) — Mark Market Regime helper.
 *
 * KARAR #488 (Vizyon v20.99) — Market Regime 4-Katman × 2-Eksen Matris.
 * Mark birebir + O'Neil mekanik kuralları + Spot SPY %25 dilim allocation.
 *
 * Eksen 1 (Mekanik IBD/O'Neil):
 *   - HEALTHY        (0-2 DD): tam pozisyon, Mark yeşil ışık
 *   - CAUTION        (3 DD): yeni alım sıkı, mevcut pozisyon koru
 *   - UNDER_PRESSURE (4 DD Hard, O'Neil çoklu kanıt): %50 pozisyon, yeni alım yok
 *   - BEAR_PRESSURE  (5+ DD Hard): %25 max pozisyon veya nakit
 *
 * Eksen 2 (Felsefe Mark): Lider Hisse Override —
 *   DD 4+ olsa bile lider hisse Ultra Low Risk %1-2 pilot alım delebilir
 *
 * Reset: FTD (Follow-Through Day) — dipten 4-7. gün hacimli %1.0+ → DD sayacı sıfırlanır.
 */

export type MarketRegime = "HEALTHY" | "CAUTION" | "UNDER_PRESSURE" | "BEAR_PRESSURE";

export interface MarketRegimeInfo {
  regime: MarketRegime;
  label: string;          // Türkçe etiket
  emoji: string;
  color: string;          // CSS color reference
  bgColor: string;
  allocation: string;     // Mark felsefesi pozisyon öneri
  markSays: string;       // Mark birebir / yorum
  newBuyAllowed: boolean; // Yeni alım izinli mi
  pilotOverride: boolean; // Lider hisse pilot %1-2 override
}

const REGIME_MAP: Record<MarketRegime, MarkRegimeStaticData> = {
  HEALTHY: {
    label: "Sağlıklı",
    emoji: "🟢",
    color: "var(--mtp-excellent)",
    bgColor: "rgba(40,167,69,0.10)",
    allocation: "Tam pozisyon (100%)",
    markSays: "Mark felsefesi yeşil ışık — superperformance hisseleri için bu pencere açık.",
    newBuyAllowed: true,
    pilotOverride: true,
  },
  CAUTION: {
    label: "Dikkat",
    emoji: "🟡",
    color: "var(--mtp-neutral)",
    bgColor: "rgba(245,158,11,0.10)",
    allocation: "Mevcut korunur, yeni alım sıkı kriter",
    markSays: "3 Distribution Day — kurumsal satış sinyali. Yeni alım sadece A+ setup'lar.",
    newBuyAllowed: true,
    pilotOverride: true,
  },
  UNDER_PRESSURE: {
    label: "Baskı Altında",
    emoji: "🟠",
    color: "#F59E0B",
    bgColor: "rgba(245,158,11,0.18)",
    allocation: "%50 pozisyon, yeni alım YASAK",
    markSays: "O'Neil Hard Filter: 4 DD / 4-5 hafta → yeni alım dur. Mevcut pozisyonları koru veya azalt.",
    newBuyAllowed: false,
    pilotOverride: true,  // Lider hisse %1-2 delebilir
  },
  BEAR_PRESSURE: {
    label: "Ayı Baskısı",
    emoji: "🔴",
    color: "var(--mtp-danger)",
    bgColor: "rgba(220,53,69,0.12)",
    allocation: "%25 max veya nakit",
    markSays: "5+ DD Hard — bear pressure. Spot SPY %75 nakit, sadece elit lider %1-2 pilot.",
    newBuyAllowed: false,
    pilotOverride: true,  // Sadece elit lider %1-2
  },
};

interface MarkRegimeStaticData {
  label: string;
  emoji: string;
  color: string;
  bgColor: string;
  allocation: string;
  markSays: string;
  newBuyAllowed: boolean;
  pilotOverride: boolean;
}

/**
 * Distribution Days sayısından Market Regime hesabı.
 * Mark KARAR #488 4-Katman + O'Neil mekanik.
 *
 * @param distributionDays Son 4-5 hafta DD sayısı (close ≤ -0.2%)
 * @returns MarketRegimeInfo: tam regime + metadata
 */
export function computeMarketRegime(distributionDays: number): MarketRegimeInfo {
  const dd = Math.max(0, Math.floor(distributionDays));
  let regime: MarketRegime;
  if (dd <= 2) {
    regime = "HEALTHY";
  } else if (dd === 3) {
    regime = "CAUTION";
  } else if (dd === 4) {
    regime = "UNDER_PRESSURE";
  } else {
    regime = "BEAR_PRESSURE";
  }
  return {
    regime,
    ...REGIME_MAP[regime],
  };
}

/**
 * FTD (Follow-Through Day) detection — basitleştirilmiş.
 * Gerçek implementation backend tarafında historical data ile yapılır.
 * Şu an UI placeholder: distribution_days sıfırlanırsa "rally confirmed"
 * notu gösterilir.
 */
export function isFollowThroughDay(
  prevDistributionDays: number,
  currentDistributionDays: number,
): boolean {
  // Reset mekanizması: önce 4+ DD vardı, şimdi 0-1 → FTD muhtemel
  return prevDistributionDays >= 4 && currentDistributionDays <= 1;
}
