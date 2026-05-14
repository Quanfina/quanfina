export type ListType = 'watch' | 'on_deck' | 'focus' | 'buy';

export interface MinerviniStock {
  symbol: string;
  company: string;
  sector: string;
  price: number;
  change_pct: number;
  grade: string;
  rs_ibd: number;
  rs_12m: number;
  ma200_slope: number;
  high52: number;
  pct_from_high: number;
  eps_qoq: number;
  sales_qoq: number;
  volume: number;
  market_cap: number;
  confirmations: number;
  violations: number;
  sma50: number;
  atr14: number;
  pivot_price: number | null;
  list_type: ListType;
}

export const LIST_LABELS: Record<ListType | 'all', string> = {
  all: 'Tümü',
  watch: 'Watch',
  on_deck: 'On Deck',
  focus: 'Focus',
  buy: 'Buy',
};
