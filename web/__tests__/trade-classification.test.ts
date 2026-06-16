/**
 * classifyByReturnPct (#24 denetim DRY) — monthly-rba + return-distribution ortak
 * pl_pct kazanan/kaybeden sınıflama. Davranış-koruyan extract (Van Tharp/Brandon RBA).
 */
import { describe, it, expect } from "vitest";
import { classifyByReturnPct } from "@/lib/trade-classification";
import type { Trade } from "@/types/trade";

function t(pl_pct: number | null): Trade {
  return {
    id: 1, symbol: "X", strategy: "minervini", setup_type: "vcp",
    entry_date: "2026-01-01", entry_price: 100, exit_date: "2026-01-02",
    exit_price: 110, shares: 10, status: "closed", pl_dollar: 100, pl_pct,
    grade: null, exit_reason: null, lessons: null,
  } as Trade;
}

describe("classifyByReturnPct", () => {
  it("boş liste → winRate 0, boş winners/losers", () => {
    const r = classifyByReturnPct([]);
    expect(r.winRate).toBe(0);
    expect(r.winners).toHaveLength(0);
    expect(r.losers).toHaveLength(0);
    expect(r.avgGainPct).toBe(0);
    expect(r.avgLossPct).toBe(0);
  });

  it("kazanan/kaybeden/başabaş ayrımı (pl_pct sign, başabaş hariç)", () => {
    const r = classifyByReturnPct([t(5), t(-3), t(0), t(10)]);
    expect(r.winners).toHaveLength(2); // 5, 10
    expect(r.losers).toHaveLength(1); // -3 (0 başabaş ne winner ne loser)
    expect(r.winRate).toBe(50); // 2/4 × 100
    expect(r.avgGainPct).toBe(7.5); // (5+10)/2
    expect(r.avgLossPct).toBe(-3);
  });

  it("null pl_pct → 0 gibi (winner/loser DEĞİL)", () => {
    const r = classifyByReturnPct([t(null), t(8)]);
    expect(r.winners).toHaveLength(1); // sadece 8
    expect(r.losers).toHaveLength(0);
    expect(r.winRate).toBe(50); // 1/2 (null total'a dahil ama winner değil)
  });

  it("hepsi kazanan → winRate 100, avgLoss 0", () => {
    const r = classifyByReturnPct([t(3), t(7)]);
    expect(r.winRate).toBe(100);
    expect(r.avgLossPct).toBe(0);
  });
});
