/**
 * calcPL + fmtPLDollar + fmtPLPct (web/lib/math.ts).
 *
 * Decimal.js bazlı tam-doğruluk P&L hesabı — float toplama hatasını önler
 * (örn. 0.1 + 0.2 = 0.30000000000000004 değil 0.30). Trade kaydı + UI gösterim.
 */
import { describe, it, expect } from "vitest";
import { calcPL, fmtPLDollar, fmtPLPct } from "@/lib/math";

describe("calcPL — Decimal.js tam-doğruluk P&L", () => {
  it("entry=100 exit=110 shares=100 → +$1000 +%10", () => {
    const r = calcPL(100, 110, 100);
    expect(r.plDollar).toBe(1000);
    expect(r.plPct).toBe(10);
  });

  it("entry=100 exit=92 shares=50 → -$400 -%8 (Mark %7 sınırı aşıldı)", () => {
    const r = calcPL(100, 92, 50);
    expect(r.plDollar).toBe(-400);
    expect(r.plPct).toBe(-8);
  });

  it("entry=exit → break-even (0/0)", () => {
    const r = calcPL(100, 100, 50);
    expect(r.plDollar).toBe(0);
    expect(r.plPct).toBe(0);
  });

  // P539: SHORT (invest_type=2) — fiyat DÜŞÜNCE kâr (yön-farkında)
  it("SHORT entry=100 exit=90 shares=10 → +$100 +%10 (fiyat düştü, kâr)", () => {
    const r = calcPL(100, 90, 10, 2);
    expect(r.plDollar).toBe(100);
    expect(r.plPct).toBe(10);
  });

  it("SHORT entry=100 exit=110 shares=10 → -$100 -%10 (fiyat yükseldi, zarar)", () => {
    const r = calcPL(100, 110, 10, 2);
    expect(r.plDollar).toBe(-100);
    expect(r.plPct).toBe(-10);
  });

  it("invest_type default=1 (LONG) — eski çağrılar değişmez", () => {
    const r = calcPL(100, 110, 10);
    expect(r.plDollar).toBe(100);
  });

  it("String input ('100', '110', '100') de çalışır", () => {
    const r = calcPL("100", "110", "100");
    expect(r.plDollar).toBe(1000);
    expect(r.plPct).toBe(10);
  });

  it("Decimal.js: 0.1+0.2 float hatası YOK", () => {
    // entry=0.1 exit=0.3 shares=1 → 0.20 dolar +%200
    const r = calcPL(0.1, 0.3, 1);
    expect(r.plDollar).toBe(0.2);
    expect(r.plPct).toBe(200);
  });

  it("2 ondalığa yuvarlama: entry=100 exit=100.555 shares=10", () => {
    // exit - entry = 0.555, × 10 = 5.55 → toDecimalPlaces(2) = 5.55
    const r = calcPL(100, 100.555, 10);
    expect(r.plDollar).toBe(5.55);
    expect(r.plPct).toBeCloseTo(0.56, 2);
  });

  it("Büyük sayılar: entry=10 exit=12.5 shares=10000 → +$25000 +%25", () => {
    const r = calcPL(10, 12.5, 10_000);
    expect(r.plDollar).toBe(25_000);
    expect(r.plPct).toBe(25);
  });
});

describe("fmtPLDollar — sign + 2 ondalık", () => {
  it.each<[number, string]>([
    [1000, "+$1000.00"],
    [-400, "-$400.00"],
    [0, "+$0.00"],         // 0 pozitif sayılır (≥0)
    [123.456, "+$123.46"], // yuvarlama
    [-0.5, "-$0.50"],
  ])("fmtPLDollar(%s) → %s", (input, expected) => {
    expect(fmtPLDollar(input)).toBe(expected);
  });
});

describe("fmtPLPct — sign + 2 ondalık + '%'", () => {
  it.each<[number, string]>([
    [10, "+10.00%"],
    [-7.5, "-7.50%"],
    [0, "+0.00%"],
    [100, "+100.00%"],
    [0.005, "+0.01%"], // yuvarlama (0.005 → 0.01)
  ])("fmtPLPct(%s) → %s", (input, expected) => {
    expect(fmtPLPct(input)).toBe(expected);
  });
});
