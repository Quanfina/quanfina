"use client";

import { useQuery } from "@tanstack/react-query";

/**
 * Sektör Rotasyonu hook — /api/sector-rotation (11 SPDR ETF RS rank).
 *
 * Mark/Minervini lider sektör disiplini: güçlü sektördeki güçlü hisse.
 * scanner.py günlük doldurur (sector_rotation tablosu).
 */
export interface SectorRotationEntry {
  ticker: string;
  sector_name: string;
  perf_1w: number | null;
  perf_1m: number | null;
  perf_3m: number | null;
  perf_6m: number | null;
  perf_1y: number | null;
  rs_score: number | null;
  rs_rank: number | null;
  scan_date: string | null;
}

async function fetchSectorRotation(): Promise<SectorRotationEntry[]> {
  const res = await fetch("/api/sector-rotation");
  if (!res.ok) throw new Error(`Sektör rotasyonu alınamadı: ${res.status}`);
  return res.json();
}

export function useSectorRotation() {
  return useQuery({
    queryKey: ["sector-rotation"],
    queryFn: fetchSectorRotation,
    staleTime: 30 * 60_000, // 30 dk — günlük güncellenir
  });
}
