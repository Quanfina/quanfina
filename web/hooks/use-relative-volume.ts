"use client";

import { useQuery } from "@tanstack/react-query";

/**
 * KARAR #733 alt-paket (Paket 133, 26 May 2026): /api/stock/{symbol}/relative-volume hook.
 * Mark TLSMW Ch 6 "Volume tells the story" canon.
 */

export type RelativeVolumeCategory =
  | "DRYING"
  | "QUIET"
  | "NORMAL"
  | "HIGH"
  | "SURGE";

export interface RelativeVolumeInfo {
  rel_vol: number | null;
  today_volume: number | null;
  avg_volume: number | null;
  category: RelativeVolumeCategory | null;
  mark_says: string;
}

async function fetchRelVol(symbol: string): Promise<RelativeVolumeInfo> {
  const res = await fetch(`/api/stock/${encodeURIComponent(symbol)}/relative-volume`);
  if (!res.ok) {
    throw new Error(`Relative Volume alınamadı (${symbol}): ${res.status}`);
  }
  return res.json();
}

export function useRelativeVolume(symbol: string | undefined) {
  return useQuery({
    queryKey: ["relative-volume", symbol],
    queryFn: () => fetchRelVol(symbol!),
    enabled: !!symbol,
    staleTime: 5 * 60_000,
  });
}
