"use client";

import { useQuery } from "@tanstack/react-query";
import type { ScreenResultRow, ScreenSlug } from "@/types/screens";

async function fetchScreenResults(
  slug: ScreenSlug,
  limit: number = 500
): Promise<ScreenResultRow[]> {
  // 30sn timeout (P414, 31 May 2026): Next.js Turbopack dev mode ilk proxy istegi
  // cold-start ile 12sn yetmiyordu (route compile + scan_diff CTE 6+ sn).
  // Production'da bu sorun yok (build edilmis route). Cold-start sonrasi cache'li
  // istekler 200ms-2s.
  const res = await fetch(`/api/screens/${slug}?limit=${limit}`, {
    signal: AbortSignal.timeout(30000),
  });
  if (!res.ok) {
    let body = "";
    try { body = await res.text(); } catch { /* ignore */ }
    throw new Error(`HTTP ${res.status}${body ? ` — ${body.slice(0, 160)}` : ""}`);
  }
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
