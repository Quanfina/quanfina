"use client";

import { useQuery } from "@tanstack/react-query";

/**
 * KARAR #733 alt-paket (Paket 32): /api/carr/stage/{symbol} hook.
 * Stan Weinstein 4-Stage detay tek-hisse.
 */

export type CarrStage = 1 | 2 | 3 | 4 | null;

export interface CarrStageResponse {
  symbol: string;
  stage: CarrStage;
  stage_label: string;
  ma_value: number | null;
  price_vs_ma_pct: number | null;
  slope_pct_per_year: number | null;
  mark_says: string;
  ma_window: number;
  // P413 (31 May 2026 — Kural #28 audit): backend MOCK fallback işareti.
  // _mock_index_history deterministik veri kullanıyorsa true; AÇIK KONU #75
  // yfinance pipeline çözülünce false. UI Stage 4 alert + rozet için sinyal.
  is_mock?: boolean;
}

async function fetchCarrStage(symbol: string): Promise<CarrStageResponse> {
  const res = await fetch(`/api/carr/stage/${encodeURIComponent(symbol)}`);
  if (!res.ok) {
    throw new Error(`Carr Stage alınamadı (${symbol}): ${res.status}`);
  }
  return res.json();
}

export function useCarrStage(symbol: string | undefined) {
  return useQuery({
    queryKey: ["carr-stage", symbol],
    queryFn: () => fetchCarrStage(symbol!),
    enabled: !!symbol,
    staleTime: 5 * 60_000, // 5 dk — günlük 4-stage çabuk değişmez
  });
}
