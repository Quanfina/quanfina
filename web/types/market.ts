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
}
