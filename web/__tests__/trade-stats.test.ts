/**
 * computeTradeStats (İstatistikler sayfası saf hesap).
 *
 * Win rate + P&L + grade dağılımı. Mark RBA disiplini.
 */
import { describe, it, expect } from "vitest";
import { computeTradeStats } from "@/lib/trade-stats";
import type { Trade } from "@/types/trade";

function t(o: Partial<Trade>): Trade {
  return {
    id: 1, symbol: "AAPL", strategy: "minervini", setup_type: "vcp",
    entry_date: "2026-05-01", entry_price: 100, shares: 100,
    status: "closed", pl_dollar: 0, pl_pct: 0, grade: null,
    exit_reason: null, lessons: null,
    ...o,
  } as Trade;
}

describe("computeTradeStats — boş", () => {
  it("Boş liste → sıfır istatistik", () => {
    const s = computeTradeStats([]);
    expect(s.totalClosed).toBe(0);
    expect(s.winRate).toBe(0);
    expect(s.totalPlDollar).toBe(0);
    expect(s.bestPlPct).toBeNull();
  });
});

describe("computeTradeStats — win rate + P&L", () => {
  const trades = [
    t({ id: 1, status: "closed", pl_dollar: 1000, pl_pct: 10, grade: "A" }),
    t({ id: 2, status: "closed", pl_dollar: 500, pl_pct: 5, grade: "B" }),
    t({ id: 3, status: "closed", pl_dollar: -300, pl_pct: -3, grade: "D" }),
    t({ id: 4, status: "open", pl_dollar: null, pl_pct: null }),
  ];

  it("3 kapalı, 1 açık", () => {
    const s = computeTradeStats(trades);
    expect(s.totalClosed).toBe(3);
    expect(s.totalOpen).toBe(1);
  });

  it("2 kazanan, 1 kaybeden → win rate %66.67", () => {
    const s = computeTradeStats(trades);
    expect(s.winners).toBe(2);
    expect(s.losers).toBe(1);
    expect(s.winRate).toBeCloseTo(66.67, 1);
  });

  it("Toplam P&L = 1000+500-300 = 1200", () => {
    const s = computeTradeStats(trades);
    expect(s.totalPlDollar).toBe(1200);
  });

  it("Ortalama P&L% = (10+5-3)/3 = 4", () => {
    const s = computeTradeStats(trades);
    expect(s.avgPlPct).toBeCloseTo(4, 1);
  });

  it("En iyi %10, en kötü -3", () => {
    const s = computeTradeStats(trades);
    expect(s.bestPlPct).toBe(10);
    expect(s.worstPlPct).toBe(-3);
  });
});

describe("computeTradeStats — grade dağılımı", () => {
  it("A=1, B=1, D=1, diğer 0", () => {
    const trades = [
      t({ id: 1, status: "closed", pl_dollar: 100, grade: "A" }),
      t({ id: 2, status: "closed", pl_dollar: 100, grade: "B" }),
      t({ id: 3, status: "closed", pl_dollar: -100, grade: "D" }),
    ];
    const s = computeTradeStats(trades);
    expect(s.gradeDistribution["A"]).toBe(1);
    expect(s.gradeDistribution["B"]).toBe(1);
    expect(s.gradeDistribution["D"]).toBe(1);
    expect(s.gradeDistribution["A+"]).toBe(0);
    expect(s.gradeDistribution["F"]).toBe(0);
  });

  it("grade null → dağılıma sayılmaz", () => {
    const s = computeTradeStats([t({ status: "closed", pl_dollar: 100, grade: null })]);
    const total = Object.values(s.gradeDistribution).reduce((a, b) => a + b, 0);
    expect(total).toBe(0);
  });
});

describe("computeTradeStats — açık trade dışlanır", () => {
  it("Açık trade'ler win rate hesabına girmez", () => {
    const s = computeTradeStats([
      t({ id: 1, status: "open", pl_dollar: null }),
      t({ id: 2, status: "open", pl_dollar: null }),
    ]);
    expect(s.totalClosed).toBe(0);
    expect(s.totalOpen).toBe(2);
    expect(s.winRate).toBe(0);
  });

  it("pl_dollar null kapalı trade dışlanır (savunmacı)", () => {
    const s = computeTradeStats([t({ status: "closed", pl_dollar: null })]);
    expect(s.totalClosed).toBe(0);
  });
});
