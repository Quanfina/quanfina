/**
 * computeRMultiple + formatR (KARAR ADAY #734 - 24 May 2026 P22).
 *
 * Mark TTLC R-Multiple disiplin: 5 tier eşik + Long varsayım + validation.
 */
import { describe, it, expect } from "vitest";
import { computeRMultiple, formatR } from "@/lib/r-multiple";

describe("computeRMultiple — Mark R disiplini", () => {
  describe("Validation (geçersiz girdiler null döner)", () => {
    it.each([
      ["entry=0", 0, 95, 110, 100],
      ["stop=0", 100, 0, 110, 100],
      ["exit=0", 100, 95, 0, 100],
      ["shares=0", 100, 95, 110, 0],
      ["entry<=stop (Long imkansız)", 95, 100, 110, 100],
      ["entry=stop (sıfıra bölme)", 100, 100, 110, 100],
      ["negatif fiyat", -100, 95, 110, 100],
    ])("%s → null", (_label, e, s, x, sh) => {
      expect(computeRMultiple(e, s, x, sh)).toBeNull();
    });
  });

  describe("Tier eşikleri (R ≥ 3 excellent, ≥ 2 good, ≥ 1 acceptable, ≥ 0 weak, < 0 loss)", () => {
    it("entry=100 stop=95 exit=120 → R=4.0 → excellent (Mark superperformance)", () => {
      const r = computeRMultiple(100, 95, 120, 100);
      expect(r).not.toBeNull();
      expect(r!.r).toBe(4);
      expect(r!.tier).toBe("excellent");
      expect(r!.label).toBe("Mükemmel");
      expect(r!.markSays).toContain("superperformance");
    });

    it("entry=100 stop=95 exit=115 → R=3.0 (eşik) → excellent (boundary)", () => {
      const r = computeRMultiple(100, 95, 115, 100);
      expect(r!.r).toBe(3);
      expect(r!.tier).toBe("excellent");
    });

    it("entry=100 stop=95 exit=110 → R=2.0 → good", () => {
      const r = computeRMultiple(100, 95, 110, 100);
      expect(r!.r).toBe(2);
      expect(r!.tier).toBe("good");
      expect(r!.label).toBe("İyi");
    });

    it("entry=100 stop=95 exit=105 → R=1.0 → acceptable", () => {
      const r = computeRMultiple(100, 95, 105, 100);
      expect(r!.r).toBe(1);
      expect(r!.tier).toBe("acceptable");
      expect(r!.markSays).toContain("Mark hedefi 2R+");
    });

    it("entry=100 stop=95 exit=102 → R=0.4 → weak (slippage uyarısı)", () => {
      const r = computeRMultiple(100, 95, 102, 100);
      expect(r!.r).toBe(0.4);
      expect(r!.tier).toBe("weak");
      expect(r!.markSays).toContain("slippage");
    });

    it("entry=100 stop=95 exit=90 → R=-2.0 → loss (Mark Wall %10 uyarısı)", () => {
      const r = computeRMultiple(100, 95, 90, 100);
      expect(r!.r).toBe(-2);
      expect(r!.tier).toBe("loss");
      expect(r!.markSays).toContain("Wall %10");
    });
  });

  describe("Risk/PnL dolar hesabı (100 shares baz)", () => {
    it("entry=100 stop=95 → riskPerShare=5 → riskDollars=500", () => {
      const r = computeRMultiple(100, 95, 110, 100);
      expect(r!.riskDollars).toBe(500);
    });

    it("entry=100 exit=110 100 shares → pnlDollars=1000", () => {
      const r = computeRMultiple(100, 95, 110, 100);
      expect(r!.pnlDollars).toBe(1000);
    });

    it("Negatif exit (zarar) → pnlDollars negatif", () => {
      const r = computeRMultiple(100, 95, 90, 100);
      expect(r!.pnlDollars).toBe(-1000);
    });

    it("Decimal yuvarlama (2 ondalık)", () => {
      // entry=100 stop=98 exit=103 → R=(103-100)/(100-98)=1.5
      // pnl=3*100=300, risk=2*100=200
      const r = computeRMultiple(100, 98, 103, 100);
      expect(r!.r).toBe(1.5);
      expect(r!.riskDollars).toBe(200);
      expect(r!.pnlDollars).toBe(300);
    });
  });

  describe("Color CSS değişkeni doğru tier'a eşli", () => {
    it.each([
      [120, "var(--mtp-excellent)"],
      [110, "var(--mtp-good, #4B9CD3)"],
      [105, "var(--mtp-neutral)"],
      [102, "#F59E0B"],
      [90, "var(--mtp-danger)"],
    ])("exit=%i → color=%s", (exit, color) => {
      const r = computeRMultiple(100, 95, exit, 100);
      expect(r!.color).toBe(color);
    });
  });
});

describe("formatR — TR sign format", () => {
  it("+2.5R pozitif", () => {
    expect(formatR(2.5)).toBe("+2.50R");
  });

  it("−0.7R negatif (Türkçe minus karakteri −)", () => {
    expect(formatR(-0.7)).toBe("−0.70R");
  });

  it("0R nötr (işaretsiz)", () => {
    expect(formatR(0)).toBe("0.00R");
  });

  it("3 ondalık → 2'ye yuvarlanır", () => {
    expect(formatR(2.567)).toBe("+2.57R");
  });
});
