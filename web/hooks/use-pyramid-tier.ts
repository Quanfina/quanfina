"use client";

import { useQuery } from "@tanstack/react-query";
import { parseErrorBody } from "@/lib/api-error";

/**
 * KARAR ADAY #732 (24 May 2026) — Mark Pyramid Tier backend hook.
 *
 * Mark KARAR #487 v20.98 3-Tier hesabi backend'den (DRY).
 * Frontend client-side fallback: web/lib/pyramid-calculator.ts
 *
 * Backend: POST /api/pyramid/tier
 */

export type PyramidTier = "BELOW_PILOT" | "PILOT" | "STANDARD" | "FULL" | "OVER_MAX";
export type PyramidNextTier = "PILOT" | "STANDARD" | "FULL" | null;
export type PyramidSeverity = "ok" | "info" | "warn" | "violation";

export interface PyramidTierResponse {
  tier: PyramidTier;
  position_pct: number;
  severity: PyramidSeverity;
  next_tier: PyramidNextTier;
  mark_says: string;
  pilot_range_pct: [number, number];
  standard_range_pct: [number, number];
  full_range_pct: [number, number];
}

interface UsePyramidTierOptions {
  positionValue: number;
  portfolioValue: number;
  prevTierProfitable?: boolean;
  /** Pozisyon değeri 0 ise sorgu yapılmaz */
  enabled?: boolean;
}

async function fetchPyramidTier(
  positionValue: number,
  portfolioValue: number,
  prevTierProfitable: boolean,
): Promise<PyramidTierResponse> {
  const res = await fetch("/api/pyramid/tier", {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({
      position_value: positionValue,
      portfolio_value: portfolioValue,
      prev_tier_profitable: prevTierProfitable,
    }),
  });
  // P391: Pydantic 422 field-bazli mesaj (P387 position+portfolio gt=0).
  // Jenerik 'HTTP 422' yerine 'portfolio_value: Input should be greater than 0'.
  if (!res.ok) throw new Error(await parseErrorBody(res));
  return res.json();
}

export function usePyramidTier(options: UsePyramidTierOptions) {
  const { positionValue, portfolioValue, prevTierProfitable = false, enabled = true } = options;
  const shouldFetch = enabled && positionValue > 0 && portfolioValue > 0;
  return useQuery({
    queryKey: ["pyramid-tier", positionValue, portfolioValue, prevTierProfitable],
    queryFn: () => fetchPyramidTier(positionValue, portfolioValue, prevTierProfitable),
    enabled: shouldFetch,
    staleTime: 30_000,
  });
}
