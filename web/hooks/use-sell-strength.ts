"use client";

import { useQuery } from "@tanstack/react-query";

/**
 * Paket 475 (16 Haz 2026): /api/stock/{symbol}/sell-strength hook (KARAR ADAY #976).
 * Mark satış sinyalleri skorlu agregat (açık pozisyonda "ne zaman SAT/azalt").
 * Canon: NotebookLM Quanfina Minervini (Mark TLSMW/TTLC/Mindset, Kural #26).
 */

export type SellCategory = "HOLD" | "WATCH" | "REDUCE" | "SELL";

export interface SellStrengthInfo {
  detected: boolean;
  category: SellCategory | null;
  sell_strength: number;
  signals: string[];
  defensive: string[];
  offensive: string[];
  pct_above_200ma: number | null;
  mark_says: string;
}

async function fetchSellStrength(symbol: string): Promise<SellStrengthInfo> {
  const res = await fetch(`/api/stock/${encodeURIComponent(symbol)}/sell-strength`);
  if (!res.ok) {
    throw new Error(`Sell Strength alınamadı (${symbol}): ${res.status}`);
  }
  return res.json();
}

export function useSellStrength(symbol: string | undefined) {
  return useQuery({
    queryKey: ["sell-strength", symbol],
    queryFn: () => fetchSellStrength(symbol!),
    enabled: !!symbol,
    staleTime: 10 * 60_000,
  });
}
