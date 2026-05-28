"use client";

import { useQuery } from "@tanstack/react-query";

/**
 * KARAR ADAY #714 — Pattern Library hook (Migration 010 + /api/patterns P333).
 *
 * 7 Mark/O'Neil canon pattern (TLSMW Ch 10). Detector parametre kaynagi —
 * hardcoded sayi yok (KALICI ILKE #4). Her pattern kitap referansli.
 *
 * Backend: GET /api/patterns
 */
export interface PatternLibraryEntry {
  id: number;
  pattern_name: string;
  mark_book_ref: string | null;
  contraction_count_min: number | null;
  contraction_count_max: number | null;
  base_weeks_min: number | null;
  base_weeks_max: number | null;
  notes: string | null;
}

async function fetchPatterns(): Promise<PatternLibraryEntry[]> {
  const res = await fetch("/api/patterns");
  if (!res.ok) throw new Error(`Pattern Library alınamadı: ${res.status}`);
  return res.json();
}

export function usePatterns() {
  return useQuery({
    queryKey: ["patterns"],
    queryFn: fetchPatterns,
    staleTime: Infinity, // Pattern library nadiren değişir (canon parametre)
  });
}
