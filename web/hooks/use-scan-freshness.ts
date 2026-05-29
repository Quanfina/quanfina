"use client";

import { useQuery } from "@tanstack/react-query";

/**
 * Tarama veri tazeliği hook'u — /api/scan/freshness'ten çeker (P375).
 *
 * Sn. Ferit "14 gün eski veri" acısı: scanner Cloud Run'da durursa minervini_scans
 * bayat kalır, kullanıcı farkında olmadan eski veriyle trade eder. Bu hook son
 * tarama tarihini + is_stale (calendar_days > 4) bilgisini getirir.
 *
 * Tarama günlük → 5dk cache + 10dk poll yeterli (canlı fiyat gibi sık gerek yok).
 */

export interface ScanFreshness {
  latest_scan_date: string | null;
  is_stale: boolean;
  calendar_days_old: number | null;
  threshold_days: number;
  message: string;
}

async function fetchScanFreshness(): Promise<ScanFreshness> {
  const res = await fetch("/api/scan/freshness");
  if (!res.ok) throw new Error(`HTTP ${res.status}`);
  return res.json();
}

export function useScanFreshness() {
  return useQuery({
    queryKey: ["scan", "freshness"],
    queryFn: fetchScanFreshness,
    staleTime: 5 * 60_000,
    refetchInterval: 10 * 60_000,
    refetchIntervalInBackground: false,
    retry: 1,
  });
}
