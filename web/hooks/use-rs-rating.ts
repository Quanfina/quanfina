"use client";

import { useQuery } from "@tanstack/react-query";
import { fetchStockDetail } from "@/lib/stock-detail-fetch";

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
  // P441: "scan" = cross-sectional IBD RS (info.rs_rating ile tutarlı). "computed"
  // = taranmamış sembol yerel yaklaşığı (cross-sectional değil — Kural #28 işaret).
  source?: "scan" | "computed";
}

async function fetchRsRating(symbol: string): Promise<RsRatingInfo> {
  // #25 (DRY): ortak fetchStockDetail (format-koruyan)
  return fetchStockDetail<RsRatingInfo>(symbol, "rs", "RS Rating");
}

export function useRsRating(symbol: string | undefined) {
  return useQuery({
    queryKey: ["rs-rating", symbol],
    queryFn: () => fetchRsRating(symbol!),
    enabled: !!symbol,
    staleTime: 30 * 60_000,
  });
}
