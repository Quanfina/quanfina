import type { Trade } from "@/types/trade";

export interface ReturnClassification {
  winners: Trade[];
  losers: Trade[];
  winRate: number;       // 0-100 (%)
  avgGainPct: number;    // ortalama kazanan pl_pct (pozitif)
  avgLossPct: number;    // ortalama kaybeden pl_pct (negatif)
}

/**
 * Kapalı trade listesini GETİRİ YÜZDESİNE (pl_pct) göre kazanan/kaybeden sınıflar
 * + win rate + ortalama kazanç/kayıp. Van Tharp / Brandon RBA standardı (pl_pct sign).
 *
 * #24 (denetim DRY, İlke #4): monthly-rba (aylık grup) + return-distribution
 * (genel + bracket) bu bloğu birebir tekrarlıyordu. Tek kaynak — davranış DEĞİŞMEZ
 * (aynı formüller: pl_pct>0 kazanan, pl_pct<0 kaybeden, başabaş hariç).
 *
 * NOT: trade-stats.ts BİLİNÇLİ olarak pl_DOLLAR bazlı (İstatistikler sayfası farklı
 * metrik) — bu util'e dahil EDİLMEDİ (baz farkı; zorla birleştirme davranış değiştirir).
 */
export function classifyByReturnPct(trades: Trade[]): ReturnClassification {
  const winners = trades.filter((t) => (t.pl_pct ?? 0) > 0);
  const losers = trades.filter((t) => (t.pl_pct ?? 0) < 0);
  const winRate = trades.length > 0 ? (winners.length / trades.length) * 100 : 0;
  const avgGainPct =
    winners.length > 0
      ? winners.reduce((s, t) => s + (t.pl_pct ?? 0), 0) / winners.length
      : 0;
  const avgLossPct =
    losers.length > 0
      ? losers.reduce((s, t) => s + (t.pl_pct ?? 0), 0) / losers.length
      : 0;
  return { winners, losers, winRate, avgGainPct, avgLossPct };
}
