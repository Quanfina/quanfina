"use client";

import { useQuery } from "@tanstack/react-query";

/**
 * Paket 500 (18 Haz 2026): /api/stock/{symbol}/mean-reversion hook.
 * Carr 2.baskı (s.356) countertrend Mean Reversion — Quanfina çekirdek frekans setup.
 */

export type MeanRevDirection = "LONG" | "SHORT" | null;

export interface MeanReversionResponse {
  detected: boolean;
  direction: MeanRevDirection;
  quality: string | null;
  entry: number | null;
  stop: number | null;
  target: number | null;
  hard_cap_pct: number;
  time_stop_days: number;
  sma20: number | null;
  lower_bb: number | null;
  upper_bb: number | null;
  mark_says: string;
  // P500: backend sentetik OHLCV fallback işareti (yfinance erişilemezse true).
  is_mock?: boolean;
}

async function fetchMeanReversion(symbol: string): Promise<MeanReversionResponse> {
  const res = await fetch(`/api/stock/${encodeURIComponent(symbol)}/mean-reversion`);
  if (!res.ok) {
    throw new Error(`Mean Reversion alınamadı (${symbol}): ${res.status}`);
  }
  return res.json();
}

export function useMeanReversion(symbol: string | undefined) {
  return useQuery({
    queryKey: ["mean-reversion", symbol],
    queryFn: () => fetchMeanReversion(symbol!),
    enabled: !!symbol,
    staleTime: 5 * 60_000, // 5 dk — günlük countertrend sinyal çabuk değişmez
  });
}
