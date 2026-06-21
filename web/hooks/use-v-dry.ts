"use client";

import { useQuery } from "@tanstack/react-query";

/**
 * P576 (21 Haz 2026): /api/stock/{symbol}/v-dry hook.
 * Minervini "Trade Like a Stock Market Wizard" s.150 "Quiet Action" — base olgunlaşırken
 * hacmin SÜREKLİ kuruması (5g pencere ort. / 50g ort.). relative-volume'dan farklı
 * (o tek-gün spot; bu sürdürülen pencere + VCP kanon eşikleri 0.50/0.70).
 */

export type VDryClass = "STRONG" | "IDEAL" | "WEAK";

export interface VDryInfo {
  v_dry_class: VDryClass | null;
  label: string | null;
  vol_ratio: number | null;
  says: string;
}

async function fetchVDry(symbol: string): Promise<VDryInfo> {
  const res = await fetch(`/api/stock/${encodeURIComponent(symbol)}/v-dry`);
  if (!res.ok) {
    throw new Error(`V-Dry alınamadı (${symbol}): ${res.status}`);
  }
  return res.json();
}

export function useVDry(symbol: string | undefined) {
  return useQuery({
    queryKey: ["v-dry", symbol],
    queryFn: () => fetchVDry(symbol!),
    enabled: !!symbol,
    staleTime: 5 * 60_000,
  });
}
