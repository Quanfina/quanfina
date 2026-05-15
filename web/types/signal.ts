export interface StrategyEntry {
  strategy: string;
  status: string;
  setup_type: string | null;
}

export interface Signal {
  symbol: string;
  strategies: StrategyEntry[];
  consensus_count: number;
  max_status: string;
  avg_rs_rating: number;
  price: number;
  latest_added: string;
  is_new_today: boolean;
}
