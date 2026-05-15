export interface SectorChange {
  name: string;
  change_pct: number;
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
}
