"use client";

import { useQuery } from "@tanstack/react-query";

/**
 * KARAR #733 alt-paket (Paket 134, 26 May 2026): /api/stock/{symbol}/breakout-quality hook.
 * Mark TLSMW Ch 10 pivot kırılım kalite kompoziti (0-100 puan).
 */

export type BreakoutQualityCategory = "EXCELLENT" | "GOOD" | "MARGINAL" | "POOR";

export interface BreakoutQualityInfo {
  score: number;
  category: BreakoutQualityCategory | null;
  breakdown: {
    volume?: number;
    gap_up?: number;
    breakout_strength?: number;
    prior_contraction?: number;
    overhead_clean?: number;
  };
  mark_says: string;
  // P443 (Kural #28): GERÇEK hesaplanan breakdown bileşenleri (şu an ["volume"]).
  // Diğerleri hardcoded MOCK (AÇIK KONU #75) → kart "hesaplanmadı" işaretler.
  real_components?: string[];
}

async function fetchBQ(symbol: string): Promise<BreakoutQualityInfo> {
  const res = await fetch(`/api/stock/${encodeURIComponent(symbol)}/breakout-quality`);
  if (!res.ok) {
    throw new Error(`Breakout Quality alınamadı (${symbol}): ${res.status}`);
  }
  return res.json();
}

export function useBreakoutQuality(symbol: string | undefined) {
  return useQuery({
    queryKey: ["breakout-quality", symbol],
    queryFn: () => fetchBQ(symbol!),
    enabled: !!symbol,
    staleTime: 10 * 60_000,
  });
}
