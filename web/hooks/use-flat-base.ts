"use client";

import { useQuery } from "@tanstack/react-query";

/**
 * Paket 458 (11 Haz 2026): /api/stock/{symbol}/flat-base hook.
 * O'Neil/IBD Flat Base — later-stage yatay konsolidasyon base.
 * Derin arastirma ile esikler IBD-canon (Kural #26).
 */

export type FlatBaseQuality = "EXCELLENT" | "GOOD" | "MARGINAL" | "NONE";

export interface FlatBaseInfo {
  detected: boolean;
  quality: FlatBaseQuality | null;
  pivot_price: number | null;
  prior_advance_pct: number | null;
  base_depth_pct: number | null;
  base_duration_days: number | null;
  is_sideways: boolean;
  volume_dryup: boolean | null;
  faults: string[];
  mark_says: string;
}

async function fetchFlatBase(symbol: string): Promise<FlatBaseInfo> {
  const res = await fetch(`/api/stock/${encodeURIComponent(symbol)}/flat-base`);
  if (!res.ok) {
    throw new Error(`Flat Base alınamadı (${symbol}): ${res.status}`);
  }
  return res.json();
}

export function useFlatBase(symbol: string | undefined) {
  return useQuery({
    queryKey: ["flat-base", symbol],
    queryFn: () => fetchFlatBase(symbol!),
    enabled: !!symbol,
    staleTime: 10 * 60_000,
  });
}
