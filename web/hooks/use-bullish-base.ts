"use client";

import { useQuery } from "@tanstack/react-query";

/**
 * Paket 513 (18 Haz 2026): /api/stock/{symbol}/bullish-base hook.
 * Carr 2.baskı "Bullish Base Breakout" (s.291) — CONTRARIAN downtrend-sonu LONG ADAYI.
 * KRİTİK: kırılım beklenmez → entry=close (s.284). OBV+MACD yükseliyor. TIER-2 eyeball.
 */

export interface BullishBaseResponse {
  detected: boolean;
  direction: "LONG" | null;
  quality: string | null; // "CANDIDATE" | "NONE"
  signal_close: number | null;
  entry: number | null; // CLOSE (kırılım beklenmez — ilk yeşil mum, s.284,289)
  stop: number | null; // Ch22 %6 / %8 cap (s.325)
  target: number | null; // 2R (s.324)
  risk_pct: number | null;
  rr: number | null;
  sma20: number | null;
  sma50: number | null;
  sma200: number | null;
  obv: number | null;
  macd: number | null;
  eyeball_checks: string[];
  mark_says: string;
  is_mock?: boolean;
}

async function fetchBullishBase(symbol: string): Promise<BullishBaseResponse> {
  const res = await fetch(`/api/stock/${encodeURIComponent(symbol)}/bullish-base`);
  if (!res.ok) {
    throw new Error(`Bullish Base alınamadı (${symbol}): ${res.status}`);
  }
  return res.json();
}

export function useBullishBase(symbol: string | undefined) {
  return useQuery({
    queryKey: ["bullish-base", symbol],
    queryFn: () => fetchBullishBase(symbol!),
    enabled: !!symbol,
    staleTime: 5 * 60_000,
  });
}
