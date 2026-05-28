import type { Trade } from "@/types/trade";

/**
 * Trade istatistik hesabı — İstatistikler sayfası (saf fonksiyon, test edilebilir).
 *
 * Kapalı trade'lerden win rate, P&L, grade dağılımı. Mark RBA disiplini:
 * "Know the truth about your trading" (TTLC Sec 4). Sayılar konuşur (İLKE #11
 * Objektif Ayna Dil).
 */
export interface TradeStats {
  totalClosed: number;
  totalOpen: number;
  winners: number;
  losers: number;
  winRate: number;          // 0-100 (%)
  totalPlDollar: number;
  avgPlPct: number;
  bestPlPct: number | null;
  worstPlPct: number | null;
  gradeDistribution: Record<string, number>;  // A+ / A / B / C / D / F sayım
}

const GRADE_ORDER = ["A+", "A", "B", "C", "D", "F"];

export function computeTradeStats(trades: Trade[]): TradeStats {
  const open = trades.filter((t) => t.status === "open");
  const closed = trades.filter(
    (t) => t.status === "closed" && t.pl_dollar != null
  );

  let winners = 0;
  let losers = 0;
  let totalPlDollar = 0;
  let plPctSum = 0;
  let bestPlPct: number | null = null;
  let worstPlPct: number | null = null;

  const gradeDistribution: Record<string, number> = {};
  for (const g of GRADE_ORDER) gradeDistribution[g] = 0;

  for (const t of closed) {
    const pl = t.pl_dollar ?? 0;
    if (pl > 0) winners += 1;
    else if (pl < 0) losers += 1;
    totalPlDollar += pl;

    const plPct = t.pl_pct ?? 0;
    plPctSum += plPct;
    if (bestPlPct == null || plPct > bestPlPct) bestPlPct = plPct;
    if (worstPlPct == null || plPct < worstPlPct) worstPlPct = plPct;

    if (t.grade && gradeDistribution[t.grade] != null) {
      gradeDistribution[t.grade] += 1;
    }
  }

  const totalClosed = closed.length;
  const winRate = totalClosed > 0 ? (winners / totalClosed) * 100 : 0;
  const avgPlPct = totalClosed > 0 ? plPctSum / totalClosed : 0;

  return {
    totalClosed,
    totalOpen: open.length,
    winners,
    losers,
    winRate,
    totalPlDollar,
    avgPlPct,
    bestPlPct,
    worstPlPct,
    gradeDistribution,
  };
}

export { GRADE_ORDER };
