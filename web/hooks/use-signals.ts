"use client";

import { useQuery } from "@tanstack/react-query";
import type { Signal } from "@/types/signal";

async function fetchSignals(): Promise<Signal[]> {
  // 8sn timeout: DB unreachable iken UI 8sn icinde hata gosterir (watchlist pateni)
  const res = await fetch("/api/signals", { signal: AbortSignal.timeout(8000) });
  if (!res.ok) {
    // 503 informative body parse (api/main.py KARAR #469 + M paket pateni)
    let detail = "";
    try {
      const body = await res.text();
      // FastAPI JSON {"detail": "..."} formatı
      try {
        const json = JSON.parse(body) as { detail?: string };
        detail = json.detail ?? body.slice(0, 160);
      } catch {
        detail = body.slice(0, 160);
      }
    } catch {
      /* ignore */
    }
    throw new Error(`HTTP ${res.status}${detail ? ` — ${detail}` : ""}`);
  }
  return res.json();
}

export function useSignals() {
  return useQuery<Signal[]>({
    queryKey: ["signals"],
    queryFn: fetchSignals,
    staleTime: 60_000,
    retry: 1,
  });
}
