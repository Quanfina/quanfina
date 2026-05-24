/**
 * KARAR ADAY #734 (24 May 2026) — Trade R-Multiple helper.
 *
 * Mark TTLC + Notebook B3 Modül 7 — Risk birimi başına kazanç ölçümü.
 * R = (exit - entry) / (entry - stop), Long için.
 *
 * R-Multiple disiplin:
 *   +3R+   : Mark superperformance (Kelly 2:1 üstü)
 *   +2R+   : Mark "iyi trade" eşiği
 *   +1R+   : Risk başına 1× kâr (kabul edilebilir)
 *   0-1R   : Düşük kalite (slippage var)
 *   negatif: Stop ihlali veya plan dışı kayıp
 */

export interface RMultipleResult {
  /** R-Multiple değeri (pozitif/negatif/sıfır) */
  r: number;
  /** Risk başına dolar (entry - stop) × shares */
  riskDollars: number;
  /** Realize kâr/zarar dolar */
  pnlDollars: number;
  /** Kalite tier */
  tier: "excellent" | "good" | "acceptable" | "weak" | "loss";
  /** Renk koduna karşılık gelen CSS değişkeni */
  color: string;
  /** TR etiket */
  label: string;
  /** Mark felsefe yorumu */
  markSays: string;
}

const TIERS: Record<RMultipleResult["tier"], { color: string; label: string; markSays: string }> = {
  excellent: {
    color: "var(--mtp-excellent)",
    label: "Mükemmel",
    markSays: "Mark superperformance — Kelly 2:1 üstü, ideal R disiplin.",
  },
  good: {
    color: "var(--mtp-good, #4B9CD3)",
    label: "İyi",
    markSays: "Mark iyi trade eşiği — risk başına 2× kâr.",
  },
  acceptable: {
    color: "var(--mtp-neutral)",
    label: "Kabul",
    markSays: "Risk başına 1×+ kâr — kabul edilebilir, ama Mark hedefi 2R+.",
  },
  weak: {
    color: "#F59E0B",
    label: "Zayıf",
    markSays: "Düşük R — slippage veya erken çıkış var. RBA defterine kayıt et.",
  },
  loss: {
    color: "var(--mtp-danger)",
    label: "Zarar",
    markSays: "Negatif R — stop ihlali veya plan dışı kayıp. Mark Wall %10 kontrolü yap.",
  },
};

/**
 * Trade'den R-Multiple hesabı (Long varsayım).
 *
 * @param entry Giriş fiyatı
 * @param stop Planlanan stop (entry'den önce)
 * @param exit Çıkış fiyatı (gerçek)
 * @param shares Adet
 * @returns RMultipleResult — r + risk/pnl + tier + Mark felsefe yorumu
 */
export function computeRMultiple(
  entry: number,
  stop: number,
  exit: number,
  shares: number,
): RMultipleResult | null {
  if (!entry || !stop || !exit || !shares) return null;
  if (entry <= 0 || stop <= 0 || exit <= 0 || shares <= 0) return null;
  if (entry <= stop) return null; // Long için entry stop'tan yüksek olmalı

  const riskPerShare = entry - stop;
  const riskDollars = riskPerShare * shares;
  const pnlPerShare = exit - entry;
  const pnlDollars = pnlPerShare * shares;
  const r = pnlPerShare / riskPerShare;

  let tier: RMultipleResult["tier"];
  if (r >= 3.0) tier = "excellent";
  else if (r >= 2.0) tier = "good";
  else if (r >= 1.0) tier = "acceptable";
  else if (r >= 0) tier = "weak";
  else tier = "loss";

  const meta = TIERS[tier];
  return {
    r: Math.round(r * 100) / 100,
    riskDollars: Math.round(riskDollars * 100) / 100,
    pnlDollars: Math.round(pnlDollars * 100) / 100,
    tier,
    color: meta.color,
    label: meta.label,
    markSays: meta.markSays,
  };
}

/** Format helper: "+2.5R" / "−0.7R" */
export function formatR(r: number): string {
  const sign = r > 0 ? "+" : r < 0 ? "−" : "";
  return `${sign}${Math.abs(r).toFixed(2)}R`;
}
