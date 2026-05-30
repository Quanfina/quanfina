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
  is_mock?: boolean;                // P409: True = MOCK fallback, False = gercek scan
}

// KARAR #733 alt-paket (Paket 57, 25 May 2026): Mark+O'Neil divergence canon
// (P56 compute_breadth_divergence helper + P57 backend wire)
export type BreadthDivergenceType =
  | "CONFIRMED_UP"
  | "BEARISH_DIVERGENCE"
  | "BULLISH_DIVERGENCE"
  | "CONFIRMED_DOWN"
  | "NEUTRAL";

export type BreadthDivergenceSeverity = "ok" | "info" | "warn" | "critical";

export interface BreadthDivergenceInfo {
  divergence: BreadthDivergenceType;
  index_change_pct: number;
  ad_trend_delta: number;
  severity: BreadthDivergenceSeverity;
  mark_says: string;
  breadth_is_mock?: boolean;        // P409: A/D MOCK ise True (SPY her zaman gercek)
}

// KARAR #733 alt-paket (Paket 65, 25 May 2026): Mark/O'Neil FTD canon
// (P64 compute_follow_through_day helper + P65 backend wire)
export interface FollowThroughDayInfo {
  ftd_detected: boolean;
  ftd_gain_pct?: number | null;       // FTD günü % değişim
  days_after_low?: number | null;     // Dip'ten kaç gün sonra
  volume_confirmed: boolean;           // Hacim teyit
  previous_low?: number | null;        // Bulunan dip fiyatı
  mark_says: string;
}

// Paket 382: market_health_label clean-room enum (Cift Danisma Kural #20 + #26).
// Eski "YESIL/SARI/KIRMIZI" yabanci platform Bundle CSS class sizmasiydi ->
// jenerik HEALTHY/NEUTRAL + Mark TLSMW Bol. 5 birebir "Under Pressure".
export type MarketHealthLabel = "HEALTHY" | "NEUTRAL" | "UNDER_PRESSURE";

// TR display cevirisi (DRY — 3 ayri sayfada inline ceviri yerine tek kaynak).
// "Baski Altinda" Mark Minervini kitap birebir "Under Pressure" terimi cevirisi.
export const MARKET_HEALTH_LABEL_TR: Record<MarketHealthLabel, string> = {
  HEALTHY: "Sağlıklı",
  NEUTRAL: "Nötr",
  UNDER_PRESSURE: "Baskı Altında",
};

export interface MarketStatus {
  spy_stage: number;
  qqq_stage: number;
  iwm_stage: number;
  vix: number;
  distribution_days: number;
  market_health_score: number;
  market_health_label: MarketHealthLabel;
  suggested_mode: string;
  top_sectors: SectorChange[];
  bottom_sectors: SectorChange[];
  // KARAR ADAY #731: Mark Regime backend (frontend DRY computeMarketRegime fallback)
  mark_regime?: MarkRegimeInfoBackend | null;
  // KARAR #733 alt-paket (Paket 52): Market Breadth A/D Line backend pre-compute
  market_breadth?: MarketBreadthInfo | null;
  // KARAR #733 alt-paket (Paket 57): Index vs A/D Divergence backend pre-compute
  breadth_divergence?: BreadthDivergenceInfo | null;
  // KARAR #733 alt-paket (Paket 65): Mark/O'Neil Follow-Through Day backend
  follow_through?: FollowThroughDayInfo | null;
  // P110 (26 May 2026): DD Severity — O'Neil CLEAN/CAUTION/HEAVY/EXTREME
  dd_severity?: string | null;
  dd_allocation_factor?: number | null;
  dd_severity_mark_says?: string | null;
}
