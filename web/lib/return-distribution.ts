/**
 * Paket 406: Return aralıkları dağılım helper'ı + Van Tharp Beklenti (E).
 *
 * Mark TTLC Sec 4 RBA + Van Tharp Trade Your Way to Financial Freedom canon
 * hibrit — Sn. Ferit'in trade'lerinin pl_pct mutlak değer aralıklarına dağılımı
 * + her aralığın win/loss + Beklenti (Probabilistic Expectancy) katkısı.
 *
 * Markets360 "DRMA Distribution" pattern uyarlaması (clean-room — DRMA YASAK
 * tescilli indikatör, "Beklenti (E)" Van Tharp jenerik canon kullanıldı).
 *
 * İLKE #11 Objektif Ayna Dil: sayı + birim, motivasyon yok.
 */
import type { Trade } from "@/types/trade";
import { classifyByReturnPct } from "./trade-classification";

export interface ReturnBracket {
  /** Aralık alt sınır (% mutlak) */
  min: number;
  /** Aralık üst sınır (% mutlak) — Infinity son aralıkta */
  max: number;
  /** Görsel etiket (örn. "5-10%") */
  label: string;
}

export interface DistributionRow {
  bracket: ReturnBracket;
  totalTrades: number;
  wins: number;
  losses: number;
  winRate: number;        // %0-100
  netPctSum: number;      // bu aralıktaki tüm trade'lerin pl_pct toplamı (signed)
  netDollarSum: number;   // pl_dollar toplamı (signed)
  /** Bu bracket'ın overall E'ye katkı paya (trade ağırlığı) */
  weight: number;
}

export interface DistributionSummary {
  rows: DistributionRow[];
  totalClosedTrades: number;
  overallWinRate: number;
  avgGainPct: number;
  avgLossPct: number;
  /** Van Tharp Beklenti (E) = WinRate × AvgGain - LossRate × |AvgLoss|
   *  >0 = pozitif beklenti, <0 = sistemli zarar */
  expectancy: number;
}

/**
 * Paper trading için pragmatik bracket'lar — 0% ile 30%+ arası 7 aralık.
 * Mark canon 7% stop + %20+ kazanç hedefleri ile uyumlu (TTLC s.131 + TLSMW Ch 12).
 */
export const RETURN_BRACKETS: ReturnBracket[] = [
  { min: 0, max: 2, label: "0-2%" },
  { min: 2, max: 5, label: "2-5%" },
  { min: 5, max: 10, label: "5-10%" },
  { min: 10, max: 15, label: "10-15%" },
  { min: 15, max: 20, label: "15-20%" },
  { min: 20, max: 30, label: "20-30%" },
  { min: 30, max: Infinity, label: "30%+" },
];

/**
 * Kapalı trade'lerden bracket dağılımı + Van Tharp Beklenti (E) hesapla.
 */
export function computeReturnDistribution(trades: Trade[]): DistributionSummary {
  const closed = trades.filter(
    (t) => t.status === "closed" && t.pl_pct != null,
  );

  const rows: DistributionRow[] = RETURN_BRACKETS.map((bracket) => {
    const inBracket = closed.filter((t) => {
      const absPct = Math.abs(t.pl_pct as number);
      return absPct >= bracket.min && absPct < bracket.max;
    });
    // #24 (DRY): ortak pl_pct sınıflama (bracket içi win/loss/winRate)
    const { winners: wins, losers: losses, winRate } = classifyByReturnPct(inBracket);
    const netPctSum = inBracket.reduce((s, t) => s + (t.pl_pct ?? 0), 0);
    const netDollarSum = inBracket.reduce((s, t) => s + (t.pl_dollar ?? 0), 0);
    const weight = closed.length > 0 ? inBracket.length / closed.length : 0;
    return {
      bracket,
      totalTrades: inBracket.length,
      wins: wins.length,
      losses: losses.length,
      winRate,
      netPctSum,
      netDollarSum,
      weight,
    };
  });

  // Overall Van Tharp Expectancy — #24 (DRY): ortak pl_pct sınıflama
  const { winRate: overallWinRate, avgGainPct, avgLossPct } = classifyByReturnPct(closed);
  const winRateDec = overallWinRate / 100;
  const lossRateDec = 1 - winRateDec;
  // E = winRate × avgGain − lossRate × |avgLoss|  (Van Tharp, % cinsinden)
  const expectancy = winRateDec * avgGainPct - lossRateDec * Math.abs(avgLossPct);

  return {
    rows,
    totalClosedTrades: closed.length,
    overallWinRate,
    avgGainPct,
    avgLossPct,
    expectancy,
  };
}
