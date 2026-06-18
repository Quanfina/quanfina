"use client";

import { useQuery } from "@tanstack/react-query";

/**
 * Paket 515 (18 Haz 2026): /api/stock/{symbol}/bullish-divergence hook.
 * Carr 2.baskı "Bullish Divergence" (s.258) — uptrend-dip LONG ADAYI: fiyat lower low +
 * 6 göstergeden 2+'si higher low. TIER-2 eyeball. Carr LONG suite'in 5. (son) setup'ı.
 */

export interface BullishDivergenceResponse {
  detected: boolean;
  direction: "LONG" | null;
  quality: string | null; // "CANDIDATE" | "NONE"
  signal_close: number | null;
  entry: number | null; // CLOSE (ertesi gün open proxy, s.262)
  stop: number | null; // sell-off son dibi altı / %8 cap (s.262)
  target: number | null; // 2R (s.324)
  risk_pct: number | null;
  rr: number | null;
  sma50: number | null;
  sma200: number | null;
  divergence_count: number;
  divergence_indicators: string[];
  eyeball_checks: string[];
  mark_says: string;
  is_mock?: boolean;
}

async function fetchBullishDivergence(symbol: string): Promise<BullishDivergenceResponse> {
  const res = await fetch(`/api/stock/${encodeURIComponent(symbol)}/bullish-divergence`);
  if (!res.ok) {
    throw new Error(`Bullish Divergence alınamadı (${symbol}): ${res.status}`);
  }
  return res.json();
}

export function useBullishDivergence(symbol: string | undefined) {
  return useQuery({
    queryKey: ["bullish-divergence", symbol],
    queryFn: () => fetchBullishDivergence(symbol!),
    enabled: !!symbol,
    staleTime: 5 * 60_000,
  });
}
