"use client";

import { MarkBadgeStrip } from "@/components/mark/MarkBadgeStrip";
import type { WatchlistRow } from "@/types/watchlist";

/**
 * KARAR ADAY #724 (24 May 2026) — Watchlist AG Grid Mark Profili kolonu.
 * DRY: ortak MarkBadgeStrip component, /screens + /hisse detay paten.
 */
export function MarkBadgeCell(p: { data?: WatchlistRow }) {
  if (!p.data) return null;
  if (!p.data.mark_signals) {
    return <span className="text-xs text-muted-foreground">—</span>;
  }
  return (
    <MarkBadgeStrip
      signals={p.data.mark_signals}
      density="compact"
      showEmpty={false}
    />
  );
}
