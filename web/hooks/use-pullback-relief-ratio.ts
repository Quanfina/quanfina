"use client";

import { useQuery } from "@tanstack/react-query";

/**
 * Paket 570 (20 Haz 2026): /api/market/pullback-relief-ratio hook — Carr piyasa rejim (s.280).
 *
 * Carr "cool pro tip": günlük Pullback aday / Relief Rally aday. >1 ve yükseliyor → boğa rejimi
 * aşırı satıma kayıyor (buyable dip); <1 ve düşüyor → ayı rejimi aşırı alıma kayıyor (shortable
 * bounce). Kanon: Trend Trading for a Living 2.baskı Böl.15 s.280. Veri yoksa available=false.
 */
export interface PullbackReliefPoint {
  scan_date: string;
  ratio: number | null;
  pullback: number;
  relief: number;
}

export interface PullbackReliefRatioResponse {
  available: boolean;
  ratio: number | null;
  pullback_count: number;
  relief_count: number;
  direction: "rising" | "falling" | "flat" | null;
  regime: "BULLISH_OVERSOLD" | "BEARISH_OVERBOUGHT" | "NEUTRAL" | null;
  scan_date: string | null;
  series: PullbackReliefPoint[];
  mark_says: string;
}

async function fetchPRRatio(): Promise<PullbackReliefRatioResponse> {
  const res = await fetch("/api/market/pullback-relief-ratio");
  if (!res.ok) throw new Error(`Pullback/Relief oranı alınamadı: ${res.status}`);
  return res.json();
}

export function usePullbackReliefRatio() {
  return useQuery({
    queryKey: ["pullback-relief-ratio"],
    queryFn: fetchPRRatio,
    staleTime: 30 * 60_000, // 30 dk — günlük tarama, gün-içi değişmez
  });
}
