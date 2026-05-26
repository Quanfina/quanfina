"use client";

import { useQuery } from "@tanstack/react-query";

/**
 * KARAR #733 alt-paket (Paket 122, 26 May 2026): /api/stock/{symbol}/stage hook.
 * Mark TLSMW Ch 4 + Weinstein Stage 1→2 geçiş tespiti.
 */

export type StageCategory =
  | "NO_TRANSITION"
  | "EARLY_STAGE_2"
  | "CONFIRMED_STAGE_2"
  | "STAGE_2_MATURE";

export type StageVolumeTrend = "RISING" | "STABLE" | "FALLING";

export interface StageTransitionInfo {
  category: StageCategory | null;
  ma_value: number | null;
  price_above_ma_pct: number | null;
  slope_pct: number | null;
  days_above_ma: number | null;
  volume_trend: StageVolumeTrend | null;
  mark_says: string;
}

async function fetchStage(symbol: string): Promise<StageTransitionInfo> {
  const res = await fetch(`/api/stock/${encodeURIComponent(symbol)}/stage`);
  if (!res.ok) {
    throw new Error(`Stage Transition alınamadı (${symbol}): ${res.status}`);
  }
  return res.json();
}

export function useStageTransition(symbol: string | undefined) {
  return useQuery({
    queryKey: ["stage-transition", symbol],
    queryFn: () => fetchStage(symbol!),
    enabled: !!symbol,
    staleTime: 15 * 60_000,
  });
}
