"use client";

import { useQuery } from "@tanstack/react-query";
import type { ScreenMeta } from "@/types/screens";

async function fetchScreenMeta(): Promise<ScreenMeta[]> {
  // Meta endpoint statik dict (bellek), DB yok — 5sn fazlasiyla yeter
  const res = await fetch("/api/screens", { signal: AbortSignal.timeout(5000) });
  if (!res.ok) throw new Error(`HTTP ${res.status}`);
  return res.json();
}

/**
 * 8 ready screen meta listesi (dropdown icin).
 * Sabit liste, staleTime uzun.
 */
export function useScreenMeta() {
  return useQuery({
    queryKey: ["screens", "meta"],
    queryFn: fetchScreenMeta,
    staleTime: 60 * 60 * 1000, // 1 saat
    retry: 1,
  });
}
