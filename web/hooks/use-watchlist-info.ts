"use client";

import { useQuery } from "@tanstack/react-query";

/**
 * P565 (20 Haz 2026 — Kural #28 İzleme Listesi şeffaflık):
 * /api/watchlist/info meta endpoint — DB sağlam mı yoksa MOCK_WATCHLIST fallback mi?
 * DB down → is_mock=true → sayfa başında MOCK banner (use-trades-info pattern, P417).
 */
export interface WatchlistInfo {
  source: "db" | "mock_fallback";
  count: number;
  is_mock: boolean;
}

async function fetchWatchlistInfo(): Promise<WatchlistInfo> {
  const res = await fetch("/api/watchlist/info", { signal: AbortSignal.timeout(5000) });
  if (!res.ok) {
    throw new Error(`HTTP ${res.status}`);
  }
  return res.json();
}

export function useWatchlistInfo() {
  return useQuery<WatchlistInfo>({
    queryKey: ["watchlist", "info"],
    queryFn: fetchWatchlistInfo,
    staleTime: 60_000, // 1 dakika — DB durumu çok değişmez
    retry: 1,
  });
}
