"use client";

import { useQuery } from "@tanstack/react-query";

/**
 * Paket 523 (18 Haz 2026): /api/carr/summary hook.
 * Carr 9 setup özet — bugün hangi hisse hangi setup'ı tetikliyor (tek pass aggregate).
 */

export interface CarrSummaryCandidate {
  symbol: string;
  rs_ibd: number | null;
}

export interface CarrSummarySetup {
  slug: string;
  label: string;
  direction: "LONG" | "SHORT";
  count: number;
  candidates: CarrSummaryCandidate[];
}

async function fetchCarrSummary(): Promise<CarrSummarySetup[]> {
  const res = await fetch("/api/carr/summary", { signal: AbortSignal.timeout(60000) });
  if (!res.ok) {
    throw new Error(`Carr özeti alınamadı: ${res.status}`);
  }
  return res.json();
}

export function useCarrSummary() {
  return useQuery({
    queryKey: ["carr", "summary"],
    queryFn: fetchCarrSummary,
    staleTime: 5 * 60_000, // 5 dk — scanner günde 1 kez; backend 30 dk cache
    retry: 1,
  });
}
