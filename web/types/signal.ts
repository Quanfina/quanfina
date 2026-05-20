// KARAR #469 (20 May 2026): konsensus yapısı kaldırıldı.
// Her watchlist satırı = 1 Signal (NVDA-Minervini ayrı kart, NVDA-Carr ayrı kart).
// UX Bölüm 4 madde 5: "Her satır ayrı trade: kendi stop, kendi hedef, kendi R/R"
// KARAR #423 (Sinyaller AG Grid Değil — Kart vitrin) korundu.
export interface Signal {
  symbol: string;
  strategy: string;
  status: string;
  setup_type: string | null;
  rs_rating: number;
  price: number;
  added_date: string;
  is_new_today: boolean;
}
