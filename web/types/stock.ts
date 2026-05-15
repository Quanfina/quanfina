import type { WatchlistRow } from "./watchlist";

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
}

export interface OhlcvBar {
  time: string;  // ISO date "YYYY-MM-DD"
  open: number;
  high: number;
  low: number;
  close: number;
  volume: number;
}
