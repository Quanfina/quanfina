/**
 * KARAR ADAY #732 (24 May 2026) — Mark Pyramid Tier Calculator helper.
 *
 * KARAR #487 (Vizyon v20.98) — Pyramiding Scale 3-Katman + "Trades Working" Kilidi.
 * Mark birebir kaynak: TraderLion Lesson 7 (Pilot) + Brandon Video (Standart) +
 * Mark Video Kelly 2:1 (Full) + Mark X "Trades not working = no size increase" (Kilit).
 *
 * 3-Tier:
 *   - PILOT     (%1-3 portfolio): zayıf piyasa / nakitten giriş / yüksek volatilite
 *   - STANDARD  (%6.25-12.5): normal piyasa + trades working
 *   - FULL      (%15-25): peak market + Kelly 2:1 R:R + tüm tier kâra geçti
 *
 * Güvenlik kilidi (Mark X):
 *   - Pilot kâra geçmeden Standart'a YASAK
 *   - Standart kâra geçmeden Full'a YASAK
 *
 * Progressive Exposure "vites kutusu" felsefesi.
 */

export type PyramidTier = "PILOT" | "STANDARD" | "FULL";

export interface TierLimits {
  minPct: number;     // Tier alt sınır (% portfolio)
  maxPct: number;     // Tier üst sınır
  label: string;      // TR etiket
  emoji: string;
  description: string;
  markSource: string;
}

export const TIER_LIMITS: Record<PyramidTier, TierLimits> = {
  PILOT: {
    minPct: 1.0,
    maxPct: 3.0,
    label: "Pilot",
    emoji: "🛬",
    description: "Zayıf piyasa / nakitten giriş / yüksek volatilite. Düşük risk başlangıç.",
    markSource: "TraderLion Lesson 7 (Pilot tier)",
  },
  STANDARD: {
    minPct: 6.25,
    maxPct: 12.5,
    label: "Standart",
    emoji: "📊",
    description: "Normal piyasa + trades working. Mark Brandon ortalama pozisyon.",
    markSource: "Brandon Video (Standart tier)",
  },
  FULL: {
    minPct: 15.0,
    maxPct: 25.0,
    label: "Full",
    emoji: "🚀",
    description: "Peak market + Kelly 2:1 R:R + tüm tier kâra geçti. Maksimum güvene dayalı.",
    markSource: "Mark Video Kelly 2:1 + 60% win rate",
  },
};

export const TIER_ORDER: PyramidTier[] = ["PILOT", "STANDARD", "FULL"];

export interface TierEvaluation {
  positionValue: number;
  positionPct: number;
  currentTier: PyramidTier | "BELOW_PILOT" | "OVER_MAX";
  tierLabel: string;
  tierColor: string;
  /** Sonraki tier (varsa) */
  nextTier: PyramidTier | null;
  nextTierGap: { from: number; to: number } | null;
  /** Pyramid disiplin durumu */
  severity: "ok" | "warn" | "violation" | "info";
  markSays: string;
}

const SEVERITY_COLORS: Record<TierEvaluation["severity"], string> = {
  ok: "var(--mtp-excellent)",
  info: "var(--mtp-neutral)",
  warn: "#F59E0B",
  violation: "var(--mtp-danger)",
};

/**
 * Pozisyon yüzdesinden tier tespit + Mark felsefe yorumu.
 *
 * @param positionValue Pozisyon $ (entry_price * shares)
 * @param portfolioValue Toplam portföy $
 * @param prevTierProfitable Pilot/Standart kâra geçti mi (Mark X kilit)
 */
export function evaluatePyramidTier(
  positionValue: number,
  portfolioValue: number,
  prevTierProfitable: boolean = false,
): TierEvaluation {
  if (portfolioValue <= 0 || positionValue <= 0) {
    return {
      positionValue,
      positionPct: 0,
      currentTier: "BELOW_PILOT",
      tierLabel: "—",
      tierColor: "var(--muted-foreground)",
      nextTier: "PILOT",
      nextTierGap: { from: 1.0, to: 3.0 },
      severity: "info",
      markSays: "Pozisyon değeri sıfır — Pilot tier (%1-3) ile başla.",
    };
  }

  const pct = (positionValue / portfolioValue) * 100;
  let currentTier: TierEvaluation["currentTier"];
  let nextTier: PyramidTier | null;
  let severity: TierEvaluation["severity"];
  let markSays: string;

  if (pct < TIER_LIMITS.PILOT.minPct) {
    currentTier = "BELOW_PILOT";
    nextTier = "PILOT";
    severity = "info";
    markSays = "Pilot tier altında. Mark felsefesi: %1-3 pilot ile başla.";
  } else if (pct <= TIER_LIMITS.PILOT.maxPct) {
    currentTier = "PILOT";
    nextTier = "STANDARD";
    severity = "ok";
    markSays = "Pilot tier (%1-3). Mark TraderLion: nakitten ilk giriş için ideal.";
  } else if (pct < TIER_LIMITS.STANDARD.minPct) {
    // Pilot ile Standart arası — Mark X "trades working" kilidini denetle
    currentTier = "PILOT";
    nextTier = "STANDARD";
    severity = prevTierProfitable ? "info" : "warn";
    markSays = prevTierProfitable
      ? "Pilot ve Standart arası — pilot kâra geçti, Standart'a (%6.25) güvenli."
      : "Pilot ve Standart arası — Mark X: pilot kâra geçmeden Standart'a YASAK.";
  } else if (pct <= TIER_LIMITS.STANDARD.maxPct) {
    currentTier = "STANDARD";
    nextTier = "FULL";
    severity = prevTierProfitable ? "ok" : "warn";
    markSays = prevTierProfitable
      ? "Standart tier (%6.25-12.5). Mark Brandon: normal piyasa + trades working."
      : "Standart tier — Mark X kilit: pilot kâra geçti mi? Aksi takdirde risk arttırma YASAK.";
  } else if (pct < TIER_LIMITS.FULL.minPct) {
    currentTier = "STANDARD";
    nextTier = "FULL";
    severity = prevTierProfitable ? "info" : "warn";
    markSays = prevTierProfitable
      ? "Standart üstü, Full altı — Standart kâra geçti, Full'a (%15-25) hazır."
      : "Standart üstü Full altı — Mark X: standart kâra geçmeden Full'a YASAK.";
  } else if (pct <= TIER_LIMITS.FULL.maxPct) {
    currentTier = "FULL";
    nextTier = null;
    severity = prevTierProfitable ? "ok" : "warn";
    markSays = prevTierProfitable
      ? "Full tier (%15-25). Mark Kelly 2:1: peak market + 60% win rate gerek."
      : "Full tier — Mark X: önceki tier kâra geçti mi? Aksi takdirde aşırı risk.";
  } else {
    currentTier = "OVER_MAX";
    nextTier = null;
    severity = "violation";
    markSays = "%25 üstü — Mark MAX. Position MAX_POSITION_MAX_PCT = 50% sert tavan, %25+ ZATEN aşırı.";
  }

  const nextLimits = nextTier ? TIER_LIMITS[nextTier] : null;
  return {
    positionValue,
    positionPct: Math.round(pct * 100) / 100,
    currentTier,
    tierLabel:
      currentTier === "BELOW_PILOT" ? "Pilot Altı" :
      currentTier === "OVER_MAX" ? "MAX AŞILDI" :
      TIER_LIMITS[currentTier].label,
    tierColor: SEVERITY_COLORS[severity],
    nextTier,
    nextTierGap: nextLimits ? { from: nextLimits.minPct, to: nextLimits.maxPct } : null,
    severity,
    markSays,
  };
}

/**
 * Önerilen pozisyon $ (tier minPct × portfolio).
 */
export function suggestPositionDollars(
  portfolioValue: number,
  tier: PyramidTier,
): { min: number; max: number } {
  const limits = TIER_LIMITS[tier];
  return {
    min: portfolioValue * (limits.minPct / 100),
    max: portfolioValue * (limits.maxPct / 100),
  };
}
