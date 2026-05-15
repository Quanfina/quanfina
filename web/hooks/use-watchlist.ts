"use client";

import { useQuery } from "@tanstack/react-query";
import type { WatchlistRow } from "@/types/watchlist";

async function fetchWatchlist(): Promise<WatchlistRow[]> {
  const res = await fetch("/api/watchlist");
  if (!res.ok) throw new Error(`HTTP ${res.status}`);
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
