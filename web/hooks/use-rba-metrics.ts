"use client";

import { useQuery } from "@tanstack/react-query";

/**
 * KARAR ADAY #722 — Mark RBA (Result-Based Analysis) hook.
 *
 * Mark TTLC Sec 4: "Know the truth about your trading."
 * Backend: GET /api/rba/metrics?strategy=X&setup_type=Y
 */

export interface RbaMetrics {
  num_trades: number;
  win_rate: number;          // 0.0 - 1.0
  avg_gain_pct: number;
  avg_loss_pct: number;      // negatif
  largest_gain_pct: number;
  largest_loss_pct: number;
  adjusted_ratio: number;    // (Win% × AvgGain) / (Loss% × |AvgLoss|)
  expectancy_pct: number;    // (Win% × AvgGain) - (Loss% × |AvgLoss|)
  is_statistically_significant: boolean;  // >= 30 trade
}

export type RbaSeverity = "OK" | "INFO" | "WARNING" | "CRITICAL";

export interface RbaRecommendation {
  severity: RbaSeverity;
  message: string;
}

export interface RbaResponse {
  metrics: RbaMetrics;
  recommendation: RbaRecommendation;
  filter_strategy: string | null;
  filter_setup_type: string | null;
}

interface UseRbaMetricsOptions {
  strategy?: string;
  setup_type?: string;
  enabled?: boolean;
}

async function fetchRbaMetrics(
  strategy?: string,
  setup_type?: string,
): Promise<RbaResponse> {
  const params = new URLSearchParams();
  if (strategy) params.set("strategy", strategy);
  if (setup_type) params.set("setup_type", setup_type);
  const query = params.toString();
  const url = `/api/rba/metrics${query ? `?${query}` : ""}`;
  const res = await fetch(url);
  if (!res.ok) {
    throw new Error(`RBA metrikleri alınamadı: ${res.status}`);
  }
  return res.json();
}

export function useRbaMetrics(options: UseRbaMetricsOptions = {}) {
  const { strategy, setup_type, enabled = true } = options;
  return useQuery({
    queryKey: ["rba-metrics", strategy ?? null, setup_type ?? null],
    queryFn: () => fetchRbaMetrics(strategy, setup_type),
    enabled,
    staleTime: 60_000,  // 1 dk — trade kapanışları sık olmaz
  });
}
