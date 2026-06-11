"use client";

import { useQuery } from "@tanstack/react-query";

/**
 * Paket 462 (11 Haz 2026): /api/stock/{symbol}/high-tight-flag hook.
 * O'Neil High Tight Flag (= Minervini Power Play) — flagpole >=%100 + tight flag.
 * En guclu + EN NADIR + EN RISKLI momentum bazi. Esikler kaynak-atifli (Kural #26).
 */

export type HighTightFlagQuality = "EXCELLENT" | "GOOD" | "MARGINAL" | "NONE";

export interface HighTightFlagInfo {
  detected: boolean;
  quality: HighTightFlagQuality | null;
  pivot_price: number | null;
  flagpole_pct: number | null;
  flagpole_weeks: number | null;
  flag_depth_pct: number | null;
  flag_duration_days: number | null;
  volume_dryup: boolean | null;
  faults: string[];
  mark_says: string;
}

async function fetchHighTightFlag(symbol: string): Promise<HighTightFlagInfo> {
  const res = await fetch(`/api/stock/${encodeURIComponent(symbol)}/high-tight-flag`);
  if (!res.ok) {
    throw new Error(`High Tight Flag alınamadı (${symbol}): ${res.status}`);
  }
  return res.json();
}

export function useHighTightFlag(symbol: string | undefined) {
  return useQuery({
    queryKey: ["high-tight-flag", symbol],
    queryFn: () => fetchHighTightFlag(symbol!),
    enabled: !!symbol,
    staleTime: 10 * 60_000,
  });
}
