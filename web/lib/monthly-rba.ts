/**
 * Paket 401: Aylık RBA aggregate helper.
 *
 * Mark TTLC Sec 4 "Know the truth about your trading" canon — kapanan trade'lerden
 * aylık performans aggregate (Markets360 "Tracker" pattern uyarlaması; isim/yapı
 * Quanfina kendi terimleri, clean-room).
 *
 * Mark canon disiplini: Win Rate, Avg Gain%, Avg Loss%, Net%, R-Multiple ratio.
 * İLKE #11 Objektif Ayna Dil: sayısal değer, motivasyon dili yok.
 */
import type { Trade } from "@/types/trade";

export interface MonthlyRbaRow {
  /** "2026-05" formatı — YYYY-MM */
  month: string;
  /** TR ay görsel ("May 2026") */
  monthLabel: string;
  tradeCount: number;
  winners: number;
  losers: number;
  /** Win Rate yüzde (0-100) */
  winRate: number;
  avgGainPct: number;     // sadece kazanan trade'lerin ortalaması
  avgLossPct: number;     // sadece kaybeden trade'lerin ortalaması (negatif)
  netPct: number;         // tüm trade'lerin ortalaması (Mark RBA net)
  totalPlDollar: number;  // dolar net
  /** Avg Gain / |Avg Loss| oranı — Mark canon (>1 = pozitif beklenti) */
  gainLossRatio: number | null;
}

const TR_MONTHS = [
  "Oca", "Şub", "Mar", "Nis", "May", "Haz",
  "Tem", "Ağu", "Eyl", "Eki", "Kas", "Ara",
];

function monthKey(dateStr: string): string {
  // "2026-05-15" -> "2026-05"
  return dateStr.slice(0, 7);
}

function monthLabel(key: string): string {
  // "2026-05" -> "May 2026"
  const [y, m] = key.split("-");
  const idx = parseInt(m, 10) - 1;
  return `${TR_MONTHS[idx] ?? m} ${y}`;
}

/**
 * Kapalı trade'lerden aylık RBA aggregate üretir.
 * Sıralama: en yeni ay önce (DESC).
 */
export function computeMonthlyRba(trades: Trade[]): MonthlyRbaRow[] {
  const closed = trades.filter(
    (t) => t.status === "closed" && t.exit_date != null && t.pl_pct != null,
  );
  if (closed.length === 0) return [];

  // Group by exit_date month
  const groups = new Map<string, Trade[]>();
  for (const t of closed) {
    const key = monthKey(t.exit_date as string);
    const arr = groups.get(key);
    if (arr) arr.push(t);
    else groups.set(key, [t]);
  }

  const rows: MonthlyRbaRow[] = [];
  for (const [key, ts] of groups) {
    const winners = ts.filter((t) => (t.pl_pct ?? 0) > 0);
    const losers = ts.filter((t) => (t.pl_pct ?? 0) < 0);
    const winRate = ts.length > 0 ? (winners.length / ts.length) * 100 : 0;
    const avgGainPct =
      winners.length > 0
        ? winners.reduce((s, t) => s + (t.pl_pct ?? 0), 0) / winners.length
        : 0;
    const avgLossPct =
      losers.length > 0
        ? losers.reduce((s, t) => s + (t.pl_pct ?? 0), 0) / losers.length
        : 0;
    const netPct = ts.reduce((s, t) => s + (t.pl_pct ?? 0), 0) / ts.length;
    const totalPlDollar = ts.reduce((s, t) => s + (t.pl_dollar ?? 0), 0);
    const gainLossRatio =
      Math.abs(avgLossPct) > 0.01 ? avgGainPct / Math.abs(avgLossPct) : null;
    rows.push({
      month: key,
      monthLabel: monthLabel(key),
      tradeCount: ts.length,
      winners: winners.length,
      losers: losers.length,
      winRate,
      avgGainPct,
      avgLossPct,
      netPct,
      totalPlDollar,
      gainLossRatio,
    });
  }

  // En yeni ay önce
  rows.sort((a, b) => b.month.localeCompare(a.month));
  return rows;
}

/**
 * Aylık satır listesinden agregat "Summary" satır üretir (toplam tüm aylar).
 * Mark TTLC Sec 4 RBA: setup-bazlı veya tüm-trade-bazlı aggregate.
 */
export function summarizeMonthlyRba(rows: MonthlyRbaRow[]): MonthlyRbaRow | null {
  if (rows.length === 0) return null;
  const totalTrades = rows.reduce((s, r) => s + r.tradeCount, 0);
  const totalWinners = rows.reduce((s, r) => s + r.winners, 0);
  const totalLosers = rows.reduce((s, r) => s + r.losers, 0);
  const totalPlDollar = rows.reduce((s, r) => s + r.totalPlDollar, 0);
  // Weighted ortalama (her ay trade sayısı ile ağırlık)
  const winRate = totalTrades > 0 ? (totalWinners / totalTrades) * 100 : 0;
  // avgGain/avgLoss weighted
  let gainSum = 0;
  let gainN = 0;
  let lossSum = 0;
  let lossN = 0;
  for (const r of rows) {
    gainSum += r.avgGainPct * r.winners;
    gainN += r.winners;
    lossSum += r.avgLossPct * r.losers;
    lossN += r.losers;
  }
  const avgGainPct = gainN > 0 ? gainSum / gainN : 0;
  const avgLossPct = lossN > 0 ? lossSum / lossN : 0;
  const netPct = totalTrades > 0
    ? rows.reduce((s, r) => s + r.netPct * r.tradeCount, 0) / totalTrades
    : 0;
  const gainLossRatio =
    Math.abs(avgLossPct) > 0.01 ? avgGainPct / Math.abs(avgLossPct) : null;
  return {
    month: "summary",
    monthLabel: "Toplam",
    tradeCount: totalTrades,
    winners: totalWinners,
    losers: totalLosers,
    winRate,
    avgGainPct,
    avgLossPct,
    netPct,
    totalPlDollar,
    gainLossRatio,
  };
}
