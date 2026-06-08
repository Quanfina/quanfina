"use client";

import { useQuery } from "@tanstack/react-query";

/**
 * P417 (31 May 2026 — Kural #28 /journal seffaflik):
 * /api/trades/info meta endpoint — Sn. Ferit MOCK_TRADES fallback aktif
 * mi gorur (P408 patenli UI banner). DB saglam => is_mock=false.
 */
export interface TradesInfo {
  source: "db" | "mock_fallback";
  count: number;
  is_mock: boolean;
}

async function fetchTradesInfo(): Promise<TradesInfo> {
  const res = await fetch("/api/trades/info", { signal: AbortSignal.timeout(5000) });
  if (!res.ok) {
    throw new Error(`HTTP ${res.status}`);
  }
  return res.json();
}

export function useTradesInfo() {
  return useQuery<TradesInfo>({
    queryKey: ["trades", "info"],
    queryFn: fetchTradesInfo,
    staleTime: 60_000, // 1 dakika — DB durumu cok degismez
    retry: 1,
  });
}
