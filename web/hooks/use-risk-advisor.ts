"use client";

import { useMutation } from "@tanstack/react-query";
import { parseErrorBody } from "@/lib/api-error";

/**
 * Sprint 4-bis.7 Faz 1 B paket — Mark Risk Advisor hook
 * KARAR ADAY #914 + #969 + #970 (Vizyon v22.00)
 * Detay: notebook/Sprint_4_bis_7_Mark_HASSAS_Tarama.md
 */

export interface RiskAdvisorRequest {
  portfolio_value: number;
  target_risk_pct?: number;
  max_stop_pct?: number;
  total_positions?: number;
  is_best_name?: boolean;
  avg_gain_pct?: number | null;
  avg_loss_pct?: number | null;
  num_trades?: number | null;
}

export interface RiskAdvisorRule {
  rule_no: number;
  rule: string;
  passed: boolean;
  value: number | null;
  message: string;
  mark_says: string;
  critical: boolean;
}

export interface MarkConstants {
  stop_absolute_cap_pct: number;
  equity_risk_min_pct: number;
  equity_risk_max_pct: number;
  position_max_pct: number;
  position_optimal_range: [number, number];
  portfolio_optimal_stocks: [number, number];
  portfolio_max_stocks: number;
}

export interface RiskAdvisorResponse {
  position_dollars: number;
  position_pct: number;
  risk_dollars: number;
  risk_pct: number;
  tier: "pilot_buy" | "optimal" | "aggressive";
  sizing_warnings: string[];
  sizing_says: string;
  recommended_stop_pct: number;
  stop_method: "rba_based" | "fallback";
  stop_absolute_cap_applied: boolean;
  stop_says: string;
  six_rule_all_pass: boolean;
  six_rule_pass_count: number;
  six_rule_critical_violations: number[];
  six_rules: RiskAdvisorRule[];
  mark_constants: MarkConstants;
}

async function postRiskAdvisor(req: RiskAdvisorRequest): Promise<RiskAdvisorResponse> {
  const res = await fetch("/api/risk/advisor", {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify(req),
  });
  // P391: Pydantic 422 field-bazli mesaj (P387 portfolio_value gt=0 vs.).
  // Jenerik "HTTP 422" yerine "portfolio_value: Input should be greater than 0".
  if (!res.ok) throw new Error(await parseErrorBody(res));
  return res.json();
}

export function useRiskAdvisor() {
  return useMutation({
    mutationFn: postRiskAdvisor,
  });
}
