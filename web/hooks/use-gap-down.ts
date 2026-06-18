"use client";

import { useQuery } from "@tanstack/react-query";

/**
 * Paket 520 (18 Haz 2026): /api/stock/{symbol}/gap-down hook.
 * Carr 2.baskı "Gap Down" (s.273-274) — uzun-ralli-sonu reversal SHORT. GÖSTERGE YOK (s.275).
 * Carr TIER-1 catalog son (8.) setup. entry=close (s.270), haber teyidi şart (s.272).
 */

export interface GapDownResponse {
  detected: boolean;
  direction: "SHORT" | null;
  quality: string | null; // "CANDIDATE" | "NONE"
  signal_close: number | null;
  entry: number | null; // gap günü close (market emri, s.270)
  stop: number | null; // %6 ÜSTTE / %8 cap (s.305)
  target: number | null; // 2R AŞAĞI (s.324)
  risk_pct: number | null;
  rr: number | null;
  sma50: number | null;
  gap_pct: number | null;
  eyeball_checks: string[];
  mark_says: string;
  is_mock?: boolean;
}

async function fetchGapDown(symbol: string): Promise<GapDownResponse> {
  const res = await fetch(`/api/stock/${encodeURIComponent(symbol)}/gap-down`);
  if (!res.ok) {
    throw new Error(`Gap Down alınamadı (${symbol}): ${res.status}`);
  }
  return res.json();
}

export function useGapDown(symbol: string | undefined) {
  return useQuery({
    queryKey: ["gap-down", symbol],
    queryFn: () => fetchGapDown(symbol!),
    enabled: !!symbol,
    staleTime: 5 * 60_000,
  });
}
