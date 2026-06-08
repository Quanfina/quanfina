import type { MarkSignals } from "@/components/mark/MarkBadgeStrip";
import type { PivotBreakoutStatus } from "@/hooks/use-pivot-breakout";

export type WatchlistStatus = 'watch' | 'on_deck' | 'focus' | 'buy';
export type WatchlistStrategy = 'minervini' | 'carr';

export interface WatchlistRow {
  symbol: string;
  strategy: WatchlistStrategy;
  status: WatchlistStatus;
  price: number;
  added_date: string;
  setup_type: string | null;
  pivot_price: number | null;
  note: string | null;
  rs_rating: number;
  consensus_count: number;
  consensus_strategies: string[];
  // KARAR ADAY #724 (24 May 2026) — Mark Profili rozetleri (DRY MarkBadgeStrip)
  // Backend /api/watchlist Mark sinyallerini join eder; eski response'lar bozulmaz
  mark_signals?: MarkSignals;
  // KARAR #733 alt-paket (Paket 82, 26 May 2026): Pivot Breakout status
  // P81 Sinyaller paten — Watchlist'te de AL/Zayıf/Yakın/Altı kolon
  pivot_status?: PivotBreakoutStatus | null;
  // Paket 154 (26 May 2026): Watchlist canlı fiyat — paper trading sabah bakışı.
  // Quote endpoint /api/stock/{sym}/quote (yfinance + 5dk cache).
  // FİYAT kolonu (price) eklendi anki fiyat; CANLI $ ise piyasanın o anki fiyatı.
  current_price?: number | null;
  change_pct_today?: number | null;
  quote_source?: "yfinance" | "mock" | null;
  // P416 (31 May 2026): Bağıl Hacim (Mark TLSMW Bol. 6 "Hacim teyit")
  // Bugün / 50-gün ortalama oranı. UI rozet: ≥1.5 yeşil "patlama" / 1.0-1.5
  // nötr / <0.7 kırmızı "sönük". Backend yfinance + compute_relative_volume.
  relative_volume?: number | null;
}

export interface WatchlistRowCreate {
  symbol: string;
  strategy: WatchlistStrategy;
  status: WatchlistStatus;
  setup_type?: string | null;
  pivot_price?: number | null;
  note?: string | null;
}

export interface WatchlistRowUpdate {
  status?: WatchlistStatus;
  note?: string | null;
  setup_type?: string | null;
}

export const STATUS_LABELS: Record<WatchlistStatus | 'all', string> = {
  all:    'Tümü',
  watch:  'Watch',
  on_deck:'On Deck',
  focus:  'Focus',
  buy:    'Buy',
};

export const STRATEGY_LABELS: Record<WatchlistStrategy | 'all', string> = {
  all:        'Tümü',
  minervini:  'Minervini',
  carr:       'Carr',
};
