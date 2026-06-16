"use client";

import { useQuery } from "@tanstack/react-query";
import { fetchStockDetail } from "@/lib/stock-detail-fetch";

/**
 * KARAR #733 alt-paket (Paket 103, 26 May 2026): /api/stock/{symbol}/atr hook.
 * Mark TLSMW Ch 11 / Wilder canon ATR — stop placement disiplini.
 */

export type AtrCategory = "LOW" | "NORMAL" | "HIGH" | "EXTREME";

export interface AtrVolatilityInfo {
  atr: number | null;
  atr_pct: number | null;
  category: AtrCategory | null;
  suggested_stop_tight: number | null;
  suggested_stop_normal: number | null;
  suggested_stop_loose: number | null;
  mark_says: string;
}

async function fetchAtr(symbol: string): Promise<AtrVolatilityInfo> {
  // #25 (DRY): ortak fetchStockDetail (format-koruyan)
  return fetchStockDetail<AtrVolatilityInfo>(symbol, "atr", "ATR");
}

export function useAtrVolatility(symbol: string | undefined) {
  return useQuery({
    queryKey: ["atr-volatility", symbol],
    queryFn: () => fetchAtr(symbol!),
    enabled: !!symbol,
    staleTime: 15 * 60_000,
  });
}
