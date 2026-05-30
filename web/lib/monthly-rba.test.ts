/**
 * P401: monthly-rba helper testleri.
 *
 * Mark TTLC Sec 4 RBA aggregate canon — kapanan trade'lerden aylık
 * Win Rate / Avg Gain / Avg Loss / Net% / Gain-Loss Ratio hesabı.
 */
import { describe, it, expect } from "vitest";
import { computeMonthlyRba, summarizeMonthlyRba } from "@/lib/monthly-rba";
import type { Trade } from "@/types/trade";

function makeClosed(overrides: Partial<Trade> = {}): Trade {
  return {
    id: 1,
    symbol: "AAPL",
    strategy: "minervini",
    setup_type: "vcp",
    entry_date: "2026-04-15",
    entry_price: 100,
    exit_date: "2026-05-10",
    exit_price: 110,
    shares: 100,
    status: "closed",
    pl_dollar: 1000,
    pl_pct: 10,
    grade: "A",
    exit_reason: "target_hit",
    lessons: null,
    ...overrides,
  } as Trade;
}

function makeOpen(): Trade {
  return {
    id: 99,
    symbol: "NVDA",
    strategy: "minervini",
    setup_type: "vcp",
    entry_date: "2026-05-15",
    entry_price: 100,
    shares: 100,
    status: "open",
    pl_dollar: null,
    pl_pct: null,
    grade: null,
    exit_reason: null,
    lessons: null,
  } as Trade;
}


describe("computeMonthlyRba", () => {
  it("Bos liste -> bos array", () => {
    expect(computeMonthlyRba([])).toEqual([]);
  });

  it("Acik trade ignore edilir (RBA sadece closed)", () => {
    const rows = computeMonthlyRba([makeOpen()]);
    expect(rows).toEqual([]);
  });

  it("Tek closed trade -> 1 ay, win rate %100, net %10", () => {
    const rows = computeMonthlyRba([
      makeClosed({ exit_date: "2026-05-10", pl_pct: 10, pl_dollar: 1000 }),
    ]);
    expect(rows).toHaveLength(1);
    expect(rows[0].month).toBe("2026-05");
    expect(rows[0].monthLabel).toBe("May 2026");
    expect(rows[0].winners).toBe(1);
    expect(rows[0].losers).toBe(0);
    expect(rows[0].winRate).toBe(100);
    expect(rows[0].avgGainPct).toBe(10);
    expect(rows[0].avgLossPct).toBe(0);  // hic kayip yok
    expect(rows[0].netPct).toBe(10);
    expect(rows[0].totalPlDollar).toBe(1000);
    expect(rows[0].gainLossRatio).toBeNull();  // avgLoss=0 -> ratio yok
  });

  it("2 ayda toplam 4 trade (3 win + 1 loss) -> ratio hesabi", () => {
    const trades: Trade[] = [
      makeClosed({ id: 1, exit_date: "2026-05-10", pl_pct: 10, pl_dollar: 1000 }),
      makeClosed({ id: 2, exit_date: "2026-05-20", pl_pct: 5, pl_dollar: 500 }),
      makeClosed({ id: 3, exit_date: "2026-04-15", pl_pct: -3, pl_dollar: -300 }),
      makeClosed({ id: 4, exit_date: "2026-04-25", pl_pct: 8, pl_dollar: 800 }),
    ];
    const rows = computeMonthlyRba(trades);
    expect(rows).toHaveLength(2);
    // En yeni ay önce
    expect(rows[0].month).toBe("2026-05");
    expect(rows[1].month).toBe("2026-04");
    // May 2026: 2 win, 0 loss
    expect(rows[0].winners).toBe(2);
    expect(rows[0].winRate).toBe(100);
    expect(rows[0].avgGainPct).toBe(7.5);
    expect(rows[0].netPct).toBe(7.5);
    expect(rows[0].totalPlDollar).toBe(1500);
    // April 2026: 1 win, 1 loss
    expect(rows[1].winners).toBe(1);
    expect(rows[1].losers).toBe(1);
    expect(rows[1].winRate).toBe(50);
    expect(rows[1].avgGainPct).toBe(8);
    expect(rows[1].avgLossPct).toBe(-3);
    expect(rows[1].netPct).toBe(2.5);  // (-3 + 8) / 2
    // Gain/Loss ratio: 8 / |−3| = 2.67
    expect(rows[1].gainLossRatio).toBeCloseTo(2.67, 1);
  });

  it("pl_pct null olan closed trade atlanir (filter)", () => {
    const rows = computeMonthlyRba([
      makeClosed({ pl_pct: null }),
    ]);
    expect(rows).toEqual([]);
  });
});


describe("summarizeMonthlyRba", () => {
  it("Bos -> null", () => {
    expect(summarizeMonthlyRba([])).toBeNull();
  });

  it("Tek ay -> aynisini doner (weighted = unit)", () => {
    const rows = computeMonthlyRba([
      makeClosed({ exit_date: "2026-05-10", pl_pct: 10, pl_dollar: 1000 }),
    ]);
    const summary = summarizeMonthlyRba(rows);
    expect(summary).not.toBeNull();
    expect(summary!.tradeCount).toBe(1);
    expect(summary!.winRate).toBe(100);
    expect(summary!.monthLabel).toBe("Toplam");
  });

  it("Cok ay -> weighted aggregate (winners/losers/winRate)", () => {
    const trades: Trade[] = [
      makeClosed({ id: 1, exit_date: "2026-05-10", pl_pct: 10, pl_dollar: 1000 }),
      makeClosed({ id: 2, exit_date: "2026-05-20", pl_pct: -5, pl_dollar: -500 }),
      makeClosed({ id: 3, exit_date: "2026-04-15", pl_pct: 8, pl_dollar: 800 }),
      makeClosed({ id: 4, exit_date: "2026-04-25", pl_pct: 6, pl_dollar: 600 }),
    ];
    const rows = computeMonthlyRba(trades);
    const summary = summarizeMonthlyRba(rows);
    expect(summary!.tradeCount).toBe(4);
    expect(summary!.winners).toBe(3);
    expect(summary!.losers).toBe(1);
    expect(summary!.winRate).toBe(75);  // 3/4
    expect(summary!.totalPlDollar).toBe(1900);
  });
});
