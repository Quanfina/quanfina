"use client";

import { useQuery } from "@tanstack/react-query";
import type { MarketStatus } from "@/types/market";

async function fetchMarketStatus(): Promise<MarketStatus> {
  const res = await fetch("/api/market/status");
  if (!res.ok) throw new Error(`HTTP ${res.status}`);
  return res.json();
}

export function useMarketStatus() {
  return useQuery({
    queryKey: ["market", "status"],
    queryFn: fetchMarketStatus,
    staleTime: 60_000,
    retry: 1,
  });
}
