"use client";

import { useQuery } from "@tanstack/react-query";

/**
 * Paket 508 (18 Haz 2026): /api/stock/{symbol}/blue-sky hook.
 * Carr 2.baskı "Blue Sky Breakout" (s.264-265 entry + s.324-325 exit) — STRONG+WEAK_BULL
 * trend-takip LONG breakout. SMA YOK (s.261), OBV+MACD bazlı. Çift danışma doğrulandı.
 */

export interface BlueSkyResponse {
  detected: boolean;
  direction: "LONG" | null;
  quality: string | null;
  signal_close: number | null;
  entry: number | null; // ertesi gün signal high üstü buy-stop (s.335)
  stop: number | null; // Ch22 evrensel %6 / %8 cap (s.325)
  target: number | null; // 2R (s.324)
  risk_pct: number | null;
  rr: number | null;
  high_40d: number | null;
  high_260d: number | null;
  low_260d: number | null;
  obv: number | null;
  macd: number | null;
  mark_says: string;
  // P508: gerçek 261+ bar yoksa true (52-hafta lookback gerektirir).
  is_mock?: boolean;
}

async function fetchBlueSky(symbol: string): Promise<BlueSkyResponse> {
  const res = await fetch(`/api/stock/${encodeURIComponent(symbol)}/blue-sky`);
  if (!res.ok) {
    throw new Error(`Blue Sky alınamadı (${symbol}): ${res.status}`);
  }
  return res.json();
}

export function useBlueSky(symbol: string | undefined) {
  return useQuery({
    queryKey: ["blue-sky", symbol],
    queryFn: () => fetchBlueSky(symbol!),
    enabled: !!symbol,
    staleTime: 5 * 60_000, // 5 dk — günlük breakout sinyali çabuk değişmez
  });
}
