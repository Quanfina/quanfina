"use client";

import { useQuery } from "@tanstack/react-query";

/**
 * Paket 460 (11 Haz 2026): /api/stock/{symbol}/double-bottom hook.
 * O'Neil/IBD Double Bottom (W) — iki dip + orta tepe, 2. dip undercut (shakeout).
 * Derin arastirma ile esikler kaynak-atifli (Kural #26).
 */

export type DoubleBottomQuality = "EXCELLENT" | "GOOD" | "MARGINAL" | "NONE";

export interface DoubleBottomInfo {
  detected: boolean;
  quality: DoubleBottomQuality | null;
  pivot_price: number | null;
  prior_advance_pct: number | null;
  base_depth_pct: number | null;
  base_duration_days: number | null;
  undercut: boolean;
  first_low: number | null;
  second_low: number | null;
  middle_peak: number | null;
  faults: string[];
  mark_says: string;
}

async function fetchDoubleBottom(symbol: string): Promise<DoubleBottomInfo> {
  const res = await fetch(`/api/stock/${encodeURIComponent(symbol)}/double-bottom`);
  if (!res.ok) {
    throw new Error(`Double Bottom alınamadı (${symbol}): ${res.status}`);
  }
  return res.json();
}

export function useDoubleBottom(symbol: string | undefined) {
  return useQuery({
    queryKey: ["double-bottom", symbol],
    queryFn: () => fetchDoubleBottom(symbol!),
    enabled: !!symbol,
    staleTime: 10 * 60_000,
  });
}
