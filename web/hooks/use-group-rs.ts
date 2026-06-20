"use client";

import { useQuery } from "@tanstack/react-query";

/**
 * Paket 553 (20 Haz 2026): /api/stock/{symbol}/group-rs hook — Grup/Endüstri RS onayı.
 *
 * 2 uzman convergence (Minervini TLSMW s.95 "leading stocks in leading groups" + O'Neil
 * CAN SLIM 'L': fiyatın ~yarısı gruptan gelir). sector_rotation (11 SPDR, gerçek scanner
 * verisi) RS rank ile hissenin sektörü lider/nötr/zayıf teyidi. SEKTÖR proxy (gerçek IBD
 * 197 endüstri grubu DEĞİL). MOCK YOK (Kural #28): veri yoksa available=false.
 */

export interface GroupRSResponse {
  symbol: string;
  available: boolean;
  sector: string | null; // SPDR sektör adı (normalize edilmiş)
  rs_rank: number | null; // 1 = en güçlü sektör
  total: number;
  tier: "LEADING" | "NEUTRAL" | "LAGGING" | null;
  is_confirmed: boolean; // LEADING -> grup teyidi tam
  is_lone_wolf: boolean; // LAGGING -> "yalnız kurt" riski
  mark_says: string;
}

async function fetchGroupRS(symbol: string): Promise<GroupRSResponse> {
  const res = await fetch(`/api/stock/${encodeURIComponent(symbol)}/group-rs`);
  if (!res.ok) {
    throw new Error(`Grup RS alınamadı (${symbol}): ${res.status}`);
  }
  return res.json();
}

export function useGroupRS(symbol: string | undefined) {
  return useQuery({
    queryKey: ["group-rs", symbol],
    queryFn: () => fetchGroupRS(symbol!),
    enabled: !!symbol,
    staleTime: 30 * 60_000, // 30 dk — sektör rotasyonu günlük tarama, gün-içi değişmez
  });
}
