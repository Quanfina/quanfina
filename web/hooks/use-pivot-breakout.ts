"use client";

import { useQuery } from "@tanstack/react-query";

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
  const res = await fetch(`/api/stock/${encodeURIComponent(symbol)}/pivot`);
  if (!res.ok) {
    throw new Error(`Pivot Breakout alınamadı (${symbol}): ${res.status}`);
  }
  return res.json();
}

export function usePivotBreakout(symbol: string | undefined) {
  return useQuery({
    queryKey: ["pivot-breakout", symbol],
    queryFn: () => fetchPivotBreakout(symbol!),
    enabled: !!symbol,
    staleTime: 5 * 60_000,
  });
}
