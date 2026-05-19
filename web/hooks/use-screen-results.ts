"use client";

import { useQuery } from "@tanstack/react-query";
import type { ScreenResultRow, ScreenSlug } from "@/types/screens";

async function fetchScreenResults(
  slug: ScreenSlug,
  limit: number = 500
): Promise<ScreenResultRow[]> {
  const res = await fetch(`/api/screens/${slug}?limit=${limit}`);
  if (!res.ok) throw new Error(`HTTP ${res.status}`);
  return res.json();
}

/**
 * Secili slug icin tarama sonuclari.
 * slug null/undefined ise sorgu kapali (enabled: false).
 */
export function useScreenResults(slug: ScreenSlug | null, limit: number = 500) {
  return useQuery({
    queryKey: ["screens", "results", slug, limit],
    queryFn: () => fetchScreenResults(slug as ScreenSlug, limit),
    staleTime: 60_000, // 1 dakika
    retry: 1,
    enabled: !!slug,
  });
}
