"use client";

import { useQuery } from "@tanstack/react-query";

/**
 * Paket 543 (20 Haz 2026): /api/stock/{symbol}/earnings hook (#843).
 * Minervini "earnings crapshoot" (s.249): 5 gün veya daha az kala YENİ breakout alımı YAPMA.
 * yfinance calendar (6h cache). Veri yoksa has_data=false (manuel teyit önerisi).
 */

export interface EarningsResponse {
  symbol: string;
  has_data: boolean;
  earnings_date: string | null; // ISO YYYY-MM-DD
  days_to_earnings: number | null; // negatif = geçmiş
  within_blackout: boolean; // 0 ≤ gün ≤ 5 (Minervini s.249)
  blackout_days: number;
  mark_says: string;
}

async function fetchEarnings(symbol: string): Promise<EarningsResponse> {
  const res = await fetch(`/api/stock/${encodeURIComponent(symbol)}/earnings`);
  if (!res.ok) {
    throw new Error(`Earnings alınamadı (${symbol}): ${res.status}`);
  }
  return res.json();
}

export function useEarnings(symbol: string | undefined) {
  return useQuery({
    queryKey: ["earnings", symbol],
    queryFn: () => fetchEarnings(symbol!),
    enabled: !!symbol,
    staleTime: 6 * 60 * 60_000, // 6 saat — earnings tarihi sık değişmez (backend de 6h cache)
  });
}
