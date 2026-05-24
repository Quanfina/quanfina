export interface SectorChange {
  name: string;
  change_pct: number;
}

// KARAR ADAY #731 (24 May 2026): Mark Regime backend pre-compute (KARAR #488)
export type MarkRegimeType = "HEALTHY" | "CAUTION" | "UNDER_PRESSURE" | "BEAR_PRESSURE";

export interface MarkRegimeInfoBackend {
  regime: MarkRegimeType;
  label: string;
  allocation: string;
  new_buy_allowed: boolean;
  pilot_override: boolean;
}

// KARAR #733 alt-paket (Paket 52, 25 May 2026): Mark+O'Neil A/D Line canon
// (P51 compute_market_breadth helper + P52 backend wire)
export type MarketBreadthHealth = "STRONG" | "NEUTRAL" | "WEAK";

export interface MarketBreadthInfo {
  ad_ratio: number;                 // Bugun advance/decline orani
  ad_line_cumulative: number;       // 20-gun birikimli (advance - decline)
  breadth_health: MarketBreadthHealth;
  mark_says: string;
}

export interface MarketStatus {
  spy_stage: number;
  qqq_stage: number;
  iwm_stage: number;
  vix: number;
  distribution_days: number;
  market_health_score: number;
  market_health_label: string;
  suggested_mode: string;
  top_sectors: SectorChange[];
  bottom_sectors: SectorChange[];
  // KARAR ADAY #731: Mark Regime backend (frontend DRY computeMarketRegime fallback)
  mark_regime?: MarkRegimeInfoBackend | null;
  // KARAR #733 alt-paket (Paket 52): Market Breadth A/D Line backend pre-compute
  market_breadth?: MarketBreadthInfo | null;
}
