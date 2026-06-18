"use client";

import { useQuery } from "@tanstack/react-query";

/**
 * Paket 506 (18 Haz 2026): /api/stock/{symbol}/pullback hook.
 * Carr 2.baskı "The Pullback" (s.249 entry + s.321-324 exit) — WEAK_BULL trend-takip LONG.
 * Çift danışma (Carr NotebookLM) ile kanon doğrulandı.
 */

export interface PullbackResponse {
  detected: boolean;
  direction: "LONG" | null;
  quality: string | null;
  signal_close: number | null;
  entry: number | null; // ertesi gün signal high üstü buy-stop (s.248)
  stop: number | null; // 50MA -%2 / %8 cap (s.321-322)
  target: number | null; // 2R (s.324)
  risk_pct: number | null;
  rr: number | null;
  sma20: number | null;
  sma50: number | null;
  sma200: number | null;
  stoch_k: number | null;
  mark_says: string;
  // P506: backend sentetik OHLCV fallback / <200 bar işareti.
  is_mock?: boolean;
}

async function fetchPullback(symbol: string): Promise<PullbackResponse> {
  const res = await fetch(`/api/stock/${encodeURIComponent(symbol)}/pullback`);
  if (!res.ok) {
    throw new Error(`Pullback alınamadı (${symbol}): ${res.status}`);
  }
  return res.json();
}

export function usePullback(symbol: string | undefined) {
  return useQuery({
    queryKey: ["pullback", symbol],
    queryFn: () => fetchPullback(symbol!),
    enabled: !!symbol,
    staleTime: 5 * 60_000, // 5 dk — günlük trend-takip sinyali çabuk değişmez
  });
}
