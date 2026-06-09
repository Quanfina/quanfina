"use client";

import { useQuery } from "@tanstack/react-query";

/**
 * P425 (31 May 2026): Ek piyasa göstergeleri (Faber + McClellan + Zweig).
 * Derin tarama F4/F5/F7 sentezi. Backend /api/market/extra-indicators.
 * Kural #28: data_sufficient=false ise UI "veri birikiyor" gösterir, MOCK yok.
 */

export interface FaberIndicator {
  data_sufficient: boolean;
  signal: "INVESTED" | "CASH" | null;
  price: number | null;
  sma_10mo: number | null;
  pct_vs_sma: number | null;
  days_needed: number;
  days_have: number;
  says: string;
}

export interface McClellanIndicator {
  data_sufficient: boolean;
  value: number | null;
  signal: "BULLISH" | "BEARISH" | null;
  days_needed: number;
  days_have: number;
  says: string;
}

export interface ZweigIndicator {
  data_sufficient: boolean;
  ema_ratio: number | null;
  thrust_active: boolean;
  zone: "THRUST_ZONE" | "OVERSOLD" | "NEUTRAL" | null;
  low_threshold?: number;
  high_threshold?: number;
  days_needed: number;
  days_have: number;
  says: string;
}

export interface ExtraIndicators {
  faber: FaberIndicator;
  mcclellan: McClellanIndicator;
  zweig: ZweigIndicator;
}

async function fetchExtraIndicators(): Promise<ExtraIndicators> {
  // 30sn timeout: SPY 252-bar yfinance fetch + breadth SQL cold-start olabilir
  const res = await fetch("/api/market/extra-indicators", {
    signal: AbortSignal.timeout(30000),
  });
  if (!res.ok) {
    throw new Error(`Ek göstergeler alınamadı: ${res.status}`);
  }
  return res.json();
}

export function useExtraIndicators() {
  return useQuery<ExtraIndicators>({
    queryKey: ["market", "extra-indicators"],
    queryFn: fetchExtraIndicators,
    staleTime: 5 * 60_000, // 5 dk — günlük göstergeler çabuk değişmez
    retry: 1,
  });
}
