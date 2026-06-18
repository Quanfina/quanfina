"use client";

import { useQuery } from "@tanstack/react-query";

/**
 * Paket 518 (18 Haz 2026): /api/stock/{symbol}/blue-sea hook.
 * Carr 2.baskı "Blue Sea Breakdown" (s.289) — strong-bear trend-takip SHORT (Blue Sky aynası).
 * Asimetri: close > 0.8×52h zirve (s.286). Carr SHORT katalog 1. setup.
 */

export interface BlueSeaResponse {
  detected: boolean;
  direction: "SHORT" | null;
  quality: string | null;
  signal_close: number | null;
  entry: number | null; // ertesi gün signal low altı sell-stop (s.317)
  stop: number | null; // %6 ÜSTTE / %8 cap (s.306)
  target: number | null; // 2R AŞAĞI (s.324)
  risk_pct: number | null;
  rr: number | null;
  low_40d: number | null;
  low_260d: number | null;
  high_260d: number | null;
  obv: number | null;
  macd: number | null;
  mark_says: string;
  is_mock?: boolean;
}

async function fetchBlueSea(symbol: string): Promise<BlueSeaResponse> {
  const res = await fetch(`/api/stock/${encodeURIComponent(symbol)}/blue-sea`);
  if (!res.ok) {
    throw new Error(`Blue Sea alınamadı (${symbol}): ${res.status}`);
  }
  return res.json();
}

export function useBlueSea(symbol: string | undefined) {
  return useQuery({
    queryKey: ["blue-sea", symbol],
    queryFn: () => fetchBlueSea(symbol!),
    enabled: !!symbol,
    staleTime: 5 * 60_000,
  });
}
