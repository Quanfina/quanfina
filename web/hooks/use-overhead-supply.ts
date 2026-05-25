"use client";

import { useQuery } from "@tanstack/react-query";

/**
 * KARAR #733 alt-paket (Paket 78): /api/stock/{symbol}/overhead hook.
 * Mark TLSMW Ch 10 Overhead Supply Detection.
 */

export type OverheadSupplyCategory = "HEAVY" | "MODERATE" | "NONE";

export interface OverheadSupplyInfo {
  category: OverheadSupplyCategory | null;
  overhead_price: number | null;
  drop_pct: number | null;
  proximity_pct: number | null;
  mark_says: string;
}

async function fetchOverhead(symbol: string): Promise<OverheadSupplyInfo> {
  const res = await fetch(`/api/stock/${encodeURIComponent(symbol)}/overhead`);
  if (!res.ok) throw new Error(`Overhead Supply alınamadı (${symbol}): ${res.status}`);
  return res.json();
}

export function useOverheadSupply(symbol: string | undefined) {
  return useQuery({
    queryKey: ["overhead-supply", symbol],
    queryFn: () => fetchOverhead(symbol!),
    enabled: !!symbol,
    staleTime: 5 * 60_000,
  });
}
