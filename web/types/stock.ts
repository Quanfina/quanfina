import type { WatchlistRow } from "./watchlist";
import type { MarkSignals } from "@/components/mark/MarkBadgeStrip";

export interface StockInfo {
  symbol: string;
  name: string;
  sector: string;
  industry: string;
  market_cap: string;
  price: number;
  change_pct: number;
  rs_rating: number;
  active_strategies: WatchlistRow[];
  // KARAR ADAY #723 (24 May 2026): Hisse detay Mark Profil rozetleri
  // Backend /api/stock/{symbol}/info Migration 004-007 sonrasi populate eder
  // Optional — eski response'lar bozulmaz, undefined ise rozet gosterilmez
  mark_signals?: MarkSignals;
}

export interface OhlcvBar {
  time: string;  // ISO date "YYYY-MM-DD"
  open: number;
  high: number;
  low: number;
  close: number;
  volume: number;
}
