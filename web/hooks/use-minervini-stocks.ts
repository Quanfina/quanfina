"use client";

import { useQuery } from "@tanstack/react-query";
import type { MinerviniStock } from "@/types/minervini";

async function fetchStocks(): Promise<MinerviniStock[]> {
  const res = await fetch("/api/minervini/stocks");
  if (!res.ok) throw new Error(`HTTP ${res.status}`);
  return res.json();
}

export function useMinerviniStocks() {
  return useQuery({
    queryKey: ["minervini", "stocks"],
    queryFn: fetchStocks,
    staleTime: 60_000,
    retry: 1,
  });
}
