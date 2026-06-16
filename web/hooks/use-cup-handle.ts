"use client";

import { useQuery } from "@tanstack/react-query";
import { fetchStockDetail } from "@/lib/stock-detail-fetch";

/**
 * Paket 456 (11 Haz 2026): /api/stock/{symbol}/cup-handle hook.
 * O'Neil CAN SLIM "How to Make Money in Stocks" Bol.15 cup-with-handle base.
 * Uc-kaynak danisma ile esikler kitap-birebir (Kural #26).
 */

export type CupHandleQuality = "EXCELLENT" | "GOOD" | "MARGINAL" | "NONE";

export interface CupHandleInfo {
  detected: boolean;
  quality: CupHandleQuality | null;
  pivot_price: number | null;
  prior_uptrend_pct: number | null;
  cup_depth_pct: number | null;
  cup_duration_days: number | null;
  handle_depth_pct: number | null;
  handle_in_upper_half: boolean;
  shakeout: boolean;
  faults: string[];
  mark_says: string;
}

async function fetchCupHandle(symbol: string): Promise<CupHandleInfo> {
  // #25 (DRY): ortak fetchStockDetail (format-koruyan)
  return fetchStockDetail<CupHandleInfo>(symbol, "cup-handle", "Cup-with-Handle");
}

export function useCupHandle(symbol: string | undefined) {
  return useQuery({
    queryKey: ["cup-handle", symbol],
    queryFn: () => fetchCupHandle(symbol!),
    enabled: !!symbol,
    staleTime: 10 * 60_000,
  });
}
