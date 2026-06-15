import type { MarkSignals } from "@/components/mark/MarkBadgeStrip";
import type { PivotBreakoutStatus } from "@/hooks/use-pivot-breakout";

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
  // KARAR #733 alt-paket (Paket 81, 26 May 2026): Pivot Breakout status
  // P70+P71 helper enrichment — sinyal listesinde AL/Zayıf/Yakın/Altı görünür
  pivot_status?: PivotBreakoutStatus | null;
  // P417 (31 May 2026): Bağıl Hacim (P416 Watchlist paten) — Mark TLSMW Bol. 6
  // "Hacim teyit" canon. Paper trading kararı pivot kırılım anında hacim
  // doğrulaması için (yeşil ≥1.5 "patlama" / kırmızı <0.7 "sönük").
  relative_volume?: number | null;
  // P438 (Kural #28): client-side enrich — canlı yfinance quote. `price` alanı
  // web_watchlist.price snapshot'ı (insert anında set, UPDATE edilmiyor) → bayat.
  // useStockQuotes ile doldurulur, Fiyat kolonu canlıyı gösterir (watchlist paten).
  current_price?: number;
  quote_source?: "yfinance" | "mock";
  // P472 (15 Haz 2026, Kural #28): Stop/Hedef/R-R MOCK-türevi mi (yfinance fail →
  // sentetik volatilite). true → UI Stop/Hedef/R-R amber + uyarı (Fiyat quote_source paten).
  stop_basis_is_mock?: boolean | null;
}
