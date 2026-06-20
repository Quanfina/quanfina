"use client";

import { useQuery } from "@tanstack/react-query";

/**
 * Paket 542 (20 Haz 2026): /api/stock/{symbol}/fundamental hook (#834/#855 EPS wire).
 * Minervini CANSLIM "C/A" — çeyreklik EPS + satış büyümesi (Minervini.md s.26 %25-30 Q/Q).
 * SOFT score (s.1152: teknik trend asıl gate, fundamental teyit). Veri: scan + MOCK.
 */

export interface FundamentalResponse {
  symbol: string;
  has_data: boolean;
  eps_qoq: number | null; // çeyreklik EPS büyüme % (Q/Q)
  sales_qoq: number | null; // çeyreklik satış büyüme % (Q/Q)
  eps_pass: boolean | null; // ≥%25 (Minervini s.26)
  sales_pass: boolean | null;
  threshold_pct: number;
  mark_says: string;
}

async function fetchFundamental(symbol: string): Promise<FundamentalResponse> {
  const res = await fetch(`/api/stock/${encodeURIComponent(symbol)}/fundamental`);
  if (!res.ok) {
    throw new Error(`Fundamental alınamadı (${symbol}): ${res.status}`);
  }
  return res.json();
}

export function useFundamental(symbol: string | undefined) {
  return useQuery({
    queryKey: ["fundamental", symbol],
    queryFn: () => fetchFundamental(symbol!),
    enabled: !!symbol,
    staleTime: 30 * 60_000, // 30 dk — çeyreklik veri gün-içi değişmez
  });
}
