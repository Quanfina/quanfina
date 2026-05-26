"use client";

import { useQuery } from "@tanstack/react-query";

/**
 * KARAR #733 alt-paket (Paket 90, 26 May 2026): /api/trades/expectancy hook.
 * Mark Brandon Video canon — kapalı trade'ler agregasyon expectancy hesabı.
 */

export type BrandonExpectancyCategory =
  | "EXCELLENT"
  | "GOOD"
  | "ACCEPTABLE"
  | "POOR";

export interface BrandonExpectancyInfo {
  expectancy_pct: number;
  category: BrandonExpectancyCategory | null;
  meets_brandon_targets: boolean;
  mark_says: string;
  total_closed: number;
  winners: number;
  losers: number;
  avg_gain_pct: number;
  avg_loss_pct: number;
  win_rate: number;
}

async function fetchExpectancy(): Promise<BrandonExpectancyInfo> {
  const res = await fetch(`/api/trades/expectancy`);
  if (!res.ok) {
    throw new Error(`Expectancy alınamadı: ${res.status}`);
  }
  return res.json();
}

export function useTradesExpectancy() {
  return useQuery({
    queryKey: ["trades-expectancy"],
    queryFn: fetchExpectancy,
    staleTime: 5 * 60_000,
  });
}
