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

// B1-03 (05 Tem 2026): tek bağımsız scanner yazım yolunun tazeliği.
export interface SourceFreshness {
  table: string;        // "minervini_scans" | "sector_rotation"
  label: string;        // "Hisse taraması" | "Sektör rotasyonu"
  latest_scan_date: string | null;
  calendar_days_old: number | null;
  is_stale: boolean;
}

export interface ScanFreshness {
  latest_scan_date: string | null;   // minervini_scans (geriye-uyum)
  is_stale: boolean;                  // minervini_scans (DEĞİŞMEZ)
  calendar_days_old: number | null;
  threshold_days: number;
  message: string;                    // B1-03 sonrası kaynak-adlı aggregate
  // B1-03: çok-tablo. api deploy sonrası dolu; eski API'de undefined (geriye-uyum).
  sources?: SourceFreshness[];
  any_stale?: boolean;                // sources'un OR'u — banner sürücüsü
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
