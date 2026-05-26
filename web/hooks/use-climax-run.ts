"use client";

import { useQuery } from "@tanstack/react-query";

/**
 * KARAR #733 alt-paket (Paket 88, 26 May 2026): /api/stock/{symbol}/climax hook.
 * Mark TLSMW Ch 9 "Selling Short and Climax Runs" canon.
 * Parabolic / exhaustion / "last gasp" tepe tespiti.
 */

export type ClimaxRunCategory =
  | "CLIMAX_TOP"
  | "POTENTIAL_CLIMAX"
  | "HEALTHY_ADVANCE"
  | "NONE";

export interface ClimaxRunInfo {
  category: ClimaxRunCategory | null;
  gain_pct: number | null;
  gap_up_days: number | null;
  avg_volume_ratio: number | null;
  mark_says: string;
}

async function fetchClimaxRun(symbol: string): Promise<ClimaxRunInfo> {
  const res = await fetch(`/api/stock/${encodeURIComponent(symbol)}/climax`);
  if (!res.ok) {
    throw new Error(`Climax Run alınamadı (${symbol}): ${res.status}`);
  }
  return res.json();
}

export function useClimaxRun(symbol: string | undefined) {
  return useQuery({
    queryKey: ["climax-run", symbol],
    queryFn: () => fetchClimaxRun(symbol!),
    enabled: !!symbol,
    staleTime: 5 * 60_000,
  });
}
