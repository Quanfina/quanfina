/**
 * computeMarketRegime + isFollowThroughDay (KARAR ADAY #729 + KARAR #488).
 *
 * Mark Market Regime 4-Katman × 2-Eksen Matris (Mark + O'Neil mekanik DD).
 */
import { describe, it, expect } from "vitest";
import {
  computeMarketRegime,
  isFollowThroughDay,
} from "@/lib/market-regime";

describe("computeMarketRegime — 4 katman DD eşikleri (KARAR #488)", () => {
  describe("HEALTHY (0-2 DD)", () => {
    it.each([0, 1, 2])("DD=%i → HEALTHY 'tam pozisyon'", (dd) => {
      const r = computeMarketRegime(dd);
      expect(r.regime).toBe("HEALTHY");
      expect(r.label).toBe("Sağlıklı");
      expect(r.emoji).toBe("🟢");
      expect(r.allocation).toContain("Tam pozisyon");
      expect(r.newBuyAllowed).toBe(true);
      expect(r.pilotOverride).toBe(true);
    });
  });

  describe("CAUTION (3 DD)", () => {
    it("DD=3 → CAUTION 'A+ setup'", () => {
      const r = computeMarketRegime(3);
      expect(r.regime).toBe("CAUTION");
      expect(r.label).toBe("Dikkat");
      expect(r.emoji).toBe("🟡");
      expect(r.newBuyAllowed).toBe(true);
      expect(r.markSays).toContain("kurumsal satış sinyali");
    });
  });

  describe("UNDER_PRESSURE (4 DD — Mark Hard Filter)", () => {
    it("DD=4 → UNDER_PRESSURE %50 pozisyon", () => {
      const r = computeMarketRegime(4);
      expect(r.regime).toBe("UNDER_PRESSURE");
      expect(r.label).toBe("Baskı Altında");
      expect(r.emoji).toBe("🟠");
      expect(r.allocation).toContain("%50");
      expect(r.newBuyAllowed).toBe(false);
      expect(r.pilotOverride).toBe(true);
      expect(r.markSays).toContain("O'Neil Hard Filter");
    });
  });

  describe("BEAR_PRESSURE (5+ DD)", () => {
    it.each([5, 6, 8, 12])("DD=%i → BEAR_PRESSURE '%25 max veya nakit'", (dd) => {
      const r = computeMarketRegime(dd);
      expect(r.regime).toBe("BEAR_PRESSURE");
      expect(r.label).toBe("Ayı Baskısı");
      expect(r.emoji).toBe("🔴");
      expect(r.allocation).toContain("%25");
      expect(r.newBuyAllowed).toBe(false);
      expect(r.pilotOverride).toBe(true);  // Elit lider %1-2 hâlâ izinli
      expect(r.markSays).toContain("Spot SPY %75 nakit");
    });
  });

  describe("Negatif veya float sanitizasyon", () => {
    it("DD=-3 → 0 olarak yorumlanır → HEALTHY", () => {
      const r = computeMarketRegime(-3);
      expect(r.regime).toBe("HEALTHY");
    });

    it("DD=3.7 → floor=3 → CAUTION", () => {
      const r = computeMarketRegime(3.7);
      expect(r.regime).toBe("CAUTION");
    });

    it("DD=4.99 → floor=4 → UNDER_PRESSURE", () => {
      const r = computeMarketRegime(4.99);
      expect(r.regime).toBe("UNDER_PRESSURE");
    });
  });

  describe("Color + bgColor — UI tutarlılık", () => {
    it("HEALTHY → mtp-excellent renk", () => {
      const r = computeMarketRegime(0);
      expect(r.color).toBe("var(--mtp-excellent)");
    });

    it("BEAR_PRESSURE → mtp-danger renk", () => {
      const r = computeMarketRegime(7);
      expect(r.color).toBe("var(--mtp-danger)");
    });

    it("Tüm regime'lerde bgColor dolu (rgba)", () => {
      [0, 3, 4, 7].forEach((dd) => {
        const r = computeMarketRegime(dd);
        expect(r.bgColor).toMatch(/^rgba/);
      });
    });
  });
});

describe("isFollowThroughDay — FTD reset mekaniği", () => {
  it("prev=4 + cur=0 → FTD true (rally confirmed)", () => {
    expect(isFollowThroughDay(4, 0)).toBe(true);
  });

  it("prev=5 + cur=1 → FTD true (4+ → ≤1)", () => {
    expect(isFollowThroughDay(5, 1)).toBe(true);
  });

  it("prev=3 + cur=0 → FTD false (DD<4 idi, reset eşiği aşılmadı)", () => {
    expect(isFollowThroughDay(3, 0)).toBe(false);
  });

  it("prev=4 + cur=2 → FTD false (cur>1)", () => {
    expect(isFollowThroughDay(4, 2)).toBe(false);
  });

  it("prev=0 + cur=0 → FTD false (DD hep düşüktü)", () => {
    expect(isFollowThroughDay(0, 0)).toBe(false);
  });
});
