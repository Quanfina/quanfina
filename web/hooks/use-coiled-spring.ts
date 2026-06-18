"use client";

import { useQuery } from "@tanstack/react-query";

/**
 * Paket 510 (18 Haz 2026): /api/stock/{symbol}/coiled-spring hook.
 * Carr 2.baskı "Coiled Spring" (s.250 entry + s.252-324 exit) — STRONG+WEAK_BULL+RANGE
 * trend-takip LONG ADAYI. GÖSTERGE YOK (s.248 pure pattern). TIER-2 eyeball (CANDIDATE).
 */

export interface CoiledSpringResponse {
  detected: boolean;
  direction: "LONG" | null;
  quality: string | null; // "CANDIDATE" | "NONE"
  signal_close: number | null;
  entry: number | null; // ertesi gün signal high üstü (trendline kırılımı sonrası, s.248)
  stop: number | null; // 50MA -%2 / %8 cap (s.252,303)
  target: number | null; // 2R (s.324)
  risk_pct: number | null;
  rr: number | null;
  sma20: number | null;
  sma50: number | null;
  eyeball_checks: string[]; // TIER-2 göz kararı kontrolleri (s.248-249)
  mark_says: string;
  is_mock?: boolean;
}

async function fetchCoiledSpring(symbol: string): Promise<CoiledSpringResponse> {
  const res = await fetch(`/api/stock/${encodeURIComponent(symbol)}/coiled-spring`);
  if (!res.ok) {
    throw new Error(`Coiled Spring alınamadı (${symbol}): ${res.status}`);
  }
  return res.json();
}

export function useCoiledSpring(symbol: string | undefined) {
  return useQuery({
    queryKey: ["coiled-spring", symbol],
    queryFn: () => fetchCoiledSpring(symbol!),
    enabled: !!symbol,
    staleTime: 5 * 60_000,
  });
}
