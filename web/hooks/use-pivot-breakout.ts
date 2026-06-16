"use client";

import { useQuery } from "@tanstack/react-query";
import { fetchStockDetail } from "@/lib/stock-detail-fetch";

/**
 * KARAR #733 alt-paket (Paket 71): /api/stock/{symbol}/pivot hook.
 * Mark TLSMW Ch 10 + O'Neil CANSLIM pivot kırılım canon.
 */

export type PivotBreakoutStatus =
  | "CONFIRMED"
  | "WEAK"
  | "NEAR_PIVOT"
  | "BELOW_PIVOT";

export interface PivotBreakoutInfo {
  status: PivotBreakoutStatus | null;
  pivot_price: number | null;
  current_price: number;
  breakout_pct: number | null;
  volume_multiplier: number | null;
  volume_confirmed: boolean;
  mark_says: string;
}

async function fetchPivotBreakout(symbol: string): Promise<PivotBreakoutInfo> {
  // #25 (DRY): ortak fetchStockDetail (format-koruyan)
  return fetchStockDetail<PivotBreakoutInfo>(symbol, "pivot", "Pivot Breakout");
}

export function usePivotBreakout(symbol: string | undefined) {
  return useQuery({
    queryKey: ["pivot-breakout", symbol],
    queryFn: () => fetchPivotBreakout(symbol!),
    enabled: !!symbol,
    staleTime: 5 * 60_000,
  });
}
