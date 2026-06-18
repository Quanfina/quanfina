"use client";

import { useQuery } from "@tanstack/react-query";

/**
 * Paket 522 (18 Haz 2026): /api/stock/{symbol}/rising-wedge hook.
 * Carr 2.baskı "Rising Wedge Breakdown" (Böl.19) — uptrend-sonu kama kırılımı SHORT.
 * MACD/OBV bearish divergence. Carr catalog SON setup (%100 tamamlama).
 */

export interface RisingWedgeResponse {
  detected: boolean;
  direction: "SHORT" | null;
  quality: string | null; // "CANDIDATE" | "NONE"
  signal_close: number | null;
  entry: number | null; // OBV kırılımı sonrası kırmızı mum close (eyeball)
  stop: number | null; // %6 ÜSTTE / %8 cap
  target: number | null; // 2R AŞAĞI
  risk_pct: number | null;
  rr: number | null;
  sma50: number | null;
  obv: number | null;
  macd: number | null;
  eyeball_checks: string[];
  mark_says: string;
  is_mock?: boolean;
}

async function fetchRisingWedge(symbol: string): Promise<RisingWedgeResponse> {
  const res = await fetch(`/api/stock/${encodeURIComponent(symbol)}/rising-wedge`);
  if (!res.ok) {
    throw new Error(`Rising Wedge alınamadı (${symbol}): ${res.status}`);
  }
  return res.json();
}

export function useRisingWedge(symbol: string | undefined) {
  return useQuery({
    queryKey: ["rising-wedge", symbol],
    queryFn: () => fetchRisingWedge(symbol!),
    enabled: !!symbol,
    staleTime: 5 * 60_000,
  });
}
