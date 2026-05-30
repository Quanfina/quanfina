/**
 * P406: return-distribution helper testleri.
 *
 * Mark TTLC Sec 4 RBA + Van Tharp Beklenti (E) canon — bracket dağılımı
 * + Probabilistic Expectancy hesabı.
 */
import { describe, it, expect } from "vitest";
import {
  RETURN_BRACKETS,
  computeReturnDistribution,
} from "@/lib/return-distribution";
import type { Trade } from "@/types/trade";

function makeClosed(plPct: number, plDollar = 0, overrides: Partial<Trade> = {}): Trade {
  return {
    id: 1,
    symbol: "AAPL",
    strategy: "minervini",
    setup_type: "vcp",
    entry_date: "2026-04-15",
    entry_price: 100,
    exit_date: "2026-05-10",
    exit_price: 100 + plPct,
    shares: 100,
    status: "closed",
    pl_dollar: plDollar,
    pl_pct: plPct,
    grade: null,
    exit_reason: null,
    lessons: null,
    ...overrides,
  } as Trade;
}


describe("RETURN_BRACKETS", () => {
  it("7 bracket: 0-2 / 2-5 / 5-10 / 10-15 / 15-20 / 20-30 / 30+", () => {
    expect(RETURN_BRACKETS).toHaveLength(7);
    expect(RETURN_BRACKETS[0].label).toBe("0-2%");
    expect(RETURN_BRACKETS[6].max).toBe(Infinity);
  });
});


describe("computeReturnDistribution — boş + edge", () => {
  it("Bos liste -> 0 trade, E=0", () => {
    const s = computeReturnDistribution([]);
    expect(s.totalClosedTrades).toBe(0);
    expect(s.expectancy).toBe(0);
    expect(s.rows).toHaveLength(7);
    expect(s.rows.every((r) => r.totalTrades === 0)).toBe(true);
  });

  it("Acik trade ignore edilir (sadece closed)", () => {
    const s = computeReturnDistribution([
      { ...makeClosed(5, 500), status: "open", pl_pct: null } as Trade,
    ]);
    expect(s.totalClosedTrades).toBe(0);
  });
});


describe("computeReturnDistribution — bracket dağılım", () => {
  it("pl_pct=3 -> '2-5%' bracket'a düşer", () => {
    const s = computeReturnDistribution([makeClosed(3, 300)]);
    const bracket25 = s.rows.find((r) => r.bracket.label === "2-5%")!;
    expect(bracket25.totalTrades).toBe(1);
    expect(bracket25.wins).toBe(1);
    expect(bracket25.netPctSum).toBe(3);
  });

  it("Negatif pl_pct mutlak değer ile bracket'a düşer (-7% → 5-10%)", () => {
    const s = computeReturnDistribution([makeClosed(-7, -700)]);
    const bracket510 = s.rows.find((r) => r.bracket.label === "5-10%")!;
    expect(bracket510.totalTrades).toBe(1);
    expect(bracket510.losses).toBe(1);
    expect(bracket510.netPctSum).toBe(-7);  // signed
  });

  it("30%+ son bracket Infinity üst sınır", () => {
    const s = computeReturnDistribution([makeClosed(45, 4500)]);
    const last = s.rows.find((r) => r.bracket.label === "30%+")!;
    expect(last.totalTrades).toBe(1);
    expect(last.wins).toBe(1);
  });

  it("Bracket sınır exclusive üst (5% tam → 5-10 değil 2-5)", () => {
    // 5.0 is the boundary — 2-5 [2, 5) exclusive üst, 5-10 [5, 10) inclusive alt
    const s = computeReturnDistribution([makeClosed(5.0)]);
    const b25 = s.rows.find((r) => r.bracket.label === "2-5%")!;
    const b510 = s.rows.find((r) => r.bracket.label === "5-10%")!;
    expect(b25.totalTrades).toBe(0);  // 5 >= 5 değil 2-5
    expect(b510.totalTrades).toBe(1);  // 5 ∈ [5, 10)
  });
});


describe("computeReturnDistribution — Van Tharp Beklenti (E)", () => {
  it("Sadece kazanan (3 trade %10 ortalama) → E pozitif", () => {
    const trades = [
      makeClosed(10, 1000, { id: 1 }),
      makeClosed(10, 1000, { id: 2 }),
      makeClosed(10, 1000, { id: 3 }),
    ];
    const s = computeReturnDistribution(trades);
    expect(s.overallWinRate).toBe(100);
    expect(s.avgGainPct).toBe(10);
    expect(s.avgLossPct).toBe(0);
    // E = 1.0 × 10 - 0.0 × 0 = 10
    expect(s.expectancy).toBe(10);
  });

  it("Karışık (%60 win × +%8, %40 loss × -%5) → E = 0.6×8 - 0.4×5 = 2.8", () => {
    const trades = [
      makeClosed(8, 800, { id: 1 }),
      makeClosed(8, 800, { id: 2 }),
      makeClosed(8, 800, { id: 3 }),
      makeClosed(-5, -500, { id: 4 }),
      makeClosed(-5, -500, { id: 5 }),
    ];
    const s = computeReturnDistribution(trades);
    expect(s.overallWinRate).toBe(60);
    expect(s.avgGainPct).toBe(8);
    expect(s.avgLossPct).toBe(-5);
    // E = 0.6 × 8 - 0.4 × 5 = 4.8 - 2.0 = 2.8
    expect(s.expectancy).toBeCloseTo(2.8, 2);
  });

  it("Negatif beklenti (düşük win + büyük loss) → E < 0 (sistemli zarar uyarı)", () => {
    const trades = [
      makeClosed(3, 300, { id: 1 }),    // küçük win
      makeClosed(-15, -1500, { id: 2 }),
      makeClosed(-15, -1500, { id: 3 }),
      makeClosed(-15, -1500, { id: 4 }),
    ];
    const s = computeReturnDistribution(trades);
    expect(s.overallWinRate).toBe(25);  // 1/4
    // E = 0.25 × 3 - 0.75 × 15 = 0.75 - 11.25 = -10.5
    expect(s.expectancy).toBeCloseTo(-10.5, 1);
    expect(s.expectancy).toBeLessThan(0);
  });
});
