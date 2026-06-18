"use client";

import { useQuery } from "@tanstack/react-query";

/**
 * Paket 533 (18 Haz 2026): /api/stock/{symbol}/pocket-pivot hook.
 * Pocket Pivot (Minervini.md s.167 + Kacher/Morales) — bazın içinde kurumsal güç dönüşü.
 * Minervini birincil AL DEĞİL, on-deck/ekleme sinyali olarak izler (s.165, 218).
 * Kanon: bugün yukarı gün + hacmi son 10g en yüksek aşağı hacimden büyük (s.167) + Stage 2.
 */

export interface PocketPivotResponse {
  detected: boolean;
  quality: "GOOD" | "CANDIDATE" | "NONE" | null;
  up_volume: number | null;
  max_down_volume_10d: number | null;
  volume_ratio: number | null; // up_vol / max_down_vol (s.167)
  sma10: number | null;
  sma50: number | null;
  off_ten_dma: boolean; // günün düşüğü 10-DMA'ya temas etti mi (Kacher/Morales)
  stage2: boolean; // close > 50-DMA + 50-DMA yükselen (s.186 ön-şart)
  mark_says: string;
  is_mock?: boolean; // gerçek 70+ bar yoksa true (Kural #28)
}

async function fetchPocketPivot(symbol: string): Promise<PocketPivotResponse> {
  const res = await fetch(`/api/stock/${encodeURIComponent(symbol)}/pocket-pivot`);
  if (!res.ok) {
    throw new Error(`Pocket Pivot alınamadı (${symbol}): ${res.status}`);
  }
  return res.json();
}

export function usePocketPivot(symbol: string | undefined) {
  return useQuery({
    queryKey: ["pocket-pivot", symbol],
    queryFn: () => fetchPocketPivot(symbol!),
    enabled: !!symbol,
    staleTime: 5 * 60_000, // 5 dk — günlük hacim sinyali çabuk değişmez
  });
}
