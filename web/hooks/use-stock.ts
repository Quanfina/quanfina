"use client";

import { useQuery } from "@tanstack/react-query";
import type { StockInfo, OhlcvBar } from "@/types/stock";

async function fetchStockInfo(symbol: string): Promise<StockInfo> {
  const res = await fetch(`/api/stock/${symbol}/info`);
  if (!res.ok) throw new Error(`HTTP ${res.status}`);
  return res.json();
}

async function fetchOhlcv(symbol: string): Promise<OhlcvBar[]> {
  // P435: bars=460 (200 MA warmup + ~252 görünür). MA200 grafiğin tüm görünür
  // aralığında dolu olsun (PriceChart son ~252 bara zoom eder).
  const res = await fetch(`/api/stock/${symbol}/ohlcv?bars=460`);
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
