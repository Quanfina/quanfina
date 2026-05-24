import type { MarkSignals } from "@/components/mark/MarkBadgeStrip";

// KARAR #469 (20 May 2026): konsensus yapısı kaldırıldı.
// Her watchlist satırı = 1 Signal (NVDA-Minervini ayrı kart, NVDA-Carr ayrı kart).
// UX Bölüm 4 madde 5: "Her satır ayrı trade: kendi stop, kendi hedef, kendi R/R"
// KARAR #470 (20 May 2026): AG Grid tablo.
// KARAR #473 (20 May 2026): stop_loss + target_price + risk_reward eklendi
// (UX Bölüm 4 madde 6 "R/R'a göre sıralı").
// KARAR #726 (24 May 2026): Mark Profili rozetleri (DRY MarkBadgeStrip 4. sayfa).
export interface Signal {
  symbol: string;
  strategy: string;
  status: string;
  setup_type: string | null;
  rs_rating: number;
  price: number;
  stop_loss: number | null;
  target_price: number | null;
  risk_reward: number | null;
  added_date: string;
  is_new_today: boolean;
  // KARAR ADAY #726 — Backend signals derive watchlist; mark_signals join
  mark_signals?: MarkSignals;
}
