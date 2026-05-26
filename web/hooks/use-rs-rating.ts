"use client";

import { useQuery } from "@tanstack/react-query";

/**
 * KARAR #733 alt-paket (Paket 94, 26 May 2026): /api/stock/{symbol}/rs hook.
 * Mark TLSMW Ch 3-5 / IBD canon Relative Strength Rating (1-99).
 */

export type RsRatingCategory = "LEADER" | "STRONG" | "AVERAGE" | "LAGGARD";

export interface RsRatingInfo {
  rs_rating: number | null;
  category: RsRatingCategory | null;
  stock_return_pct: number | null;
  benchmark_return_pct: number | null;
  outperform_pct: number | null;
  mark_says: string;
}

async function fetchRsRating(symbol: string): Promise<RsRatingInfo> {
  const res = await fetch(`/api/stock/${encodeURIComponent(symbol)}/rs`);
  if (!res.ok) {
    throw new Error(`RS Rating alınamadı (${symbol}): ${res.status}`);
  }
  return res.json();
}

export function useRsRating(symbol: string | undefined) {
  return useQuery({
    queryKey: ["rs-rating", symbol],
    queryFn: () => fetchRsRating(symbol!),
    enabled: !!symbol,
    staleTime: 30 * 60_000,
  });
}
