"use client";

import { useQuery } from "@tanstack/react-query";
import type { StockInfo, OhlcvBar } from "@/types/stock";

async function fetchStockInfo(symbol: string): Promise<StockInfo> {
  const res = await fetch(`/api/stock/${symbol}/info`);
  if (!res.ok) throw new Error(`HTTP ${res.status}`);
  return res.json();
}

async function fetchOhlcv(symbol: string): Promise<OhlcvBar[]> {
  const res = await fetch(`/api/stock/${symbol}/ohlcv`);
  if (!res.ok) throw new Error(`HTTP ${res.status}`);
  return res.json();
}

export function useStockInfo(symbol: string) {
  return useQuery({
    queryKey: ["stock", symbol, "info"],
    queryFn: () => fetchStockInfo(symbol),
    staleTime: 60_000,
    retry: 1,
  });
}

export function useOhlcv(symbol: string) {
  return useQuery({
    queryKey: ["stock", symbol, "ohlcv"],
    queryFn: () => fetchOhlcv(symbol),
    staleTime: 60_000,
    retry: 1,
  });
}
