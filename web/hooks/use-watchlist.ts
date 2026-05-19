"use client";

import { useQuery } from "@tanstack/react-query";
import type { WatchlistRow } from "@/types/watchlist";

async function fetchWatchlist(): Promise<WatchlistRow[]> {
  // 8sn timeout: DB unreachable iken UI 8sn icinde hata gosterir (eski 75-130sn)
  const res = await fetch("/api/watchlist", { signal: AbortSignal.timeout(8000) });
  if (!res.ok) {
    let body = "";
    try { body = await res.text(); } catch { /* ignore */ }
    throw new Error(`HTTP ${res.status}${body ? ` — ${body.slice(0, 160)}` : ""}`);
  }
  return res.json();
}

export function useWatchlist() {
  return useQuery({
    queryKey: ["watchlist"],
    queryFn: fetchWatchlist,
    staleTime: 60_000,
    retry: 1,
  });
}
