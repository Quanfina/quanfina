"use client";

import { useQuery } from "@tanstack/react-query";

/**
 * Paket 569 (20 Haz 2026): /api/stock/{symbol}/earnings-growth hook — CANSLIM 'C' (#75).
 *
 * Minervini CANSLIM-C: çeyreklik EPS+satış YoY büyümesi ≥%25 + HIZLANIYOR (son YoY > önceki).
 * yfinance quarterly income statement (yapısal, .info'dan güvenilir). Veri yoksa available=false
 * (Kural #28 — uydurma yok). Scan eps_qoq tek-çeyrek; bu çok-çeyrek YoY + hızlanma.
 */
export interface EarningsGrowthResponse {
  symbol: string;
  available: boolean;
  revenue_yoy_pct: number | null;
  earnings_yoy_pct: number | null;
  revenue_accelerating: boolean;
  earnings_accelerating: boolean;
  both_pass: boolean;
  quarters_used: number;
  mark_says: string;
}

async function fetchEarningsGrowth(symbol: string): Promise<EarningsGrowthResponse> {
  const res = await fetch(`/api/stock/${encodeURIComponent(symbol)}/earnings-growth`);
  if (!res.ok) {
    throw new Error(`Earnings growth alınamadı (${symbol}): ${res.status}`);
  }
  return res.json();
}

export function useEarningsGrowth(symbol: string | undefined) {
  return useQuery({
    queryKey: ["earnings-growth", symbol],
    queryFn: () => fetchEarningsGrowth(symbol!),
    enabled: !!symbol,
    staleTime: 6 * 60 * 60_000, // 6 saat — çeyreklik veri gün-içi değişmez
  });
}
