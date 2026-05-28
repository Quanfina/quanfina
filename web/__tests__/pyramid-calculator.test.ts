/**
 * evaluatePyramidTier + suggestPositionDollars (KARAR ADAY #732 - 24 May 2026 P15-19).
 *
 * Mark Pyramid 3-Tier disiplini + Mark X "Trades not working = no size increase" kilit.
 * TraderLion (PILOT) + Brandon Video (STANDARD) + Mark Video Kelly 2:1 (FULL).
 */
import { describe, it, expect } from "vitest";
import {
  evaluatePyramidTier,
  suggestPositionDollars,
  TIER_LIMITS,
} from "@/lib/pyramid-calculator";

describe("evaluatePyramidTier — 3 tier eşik tespiti", () => {
  const PORTFOLIO = 100_000;

  describe("Sınır durumlar (edge cases)", () => {
    it("portfolio=0 → BELOW_PILOT info", () => {
      const r = evaluatePyramidTier(1000, 0);
      expect(r.currentTier).toBe("BELOW_PILOT");
      expect(r.severity).toBe("info");
    });

    it("position=0 → BELOW_PILOT 'Pozisyon değeri sıfır'", () => {
      const r = evaluatePyramidTier(0, PORTFOLIO);
      expect(r.currentTier).toBe("BELOW_PILOT");
      expect(r.markSays).toContain("Pozisyon değeri sıfır");
    });

    it("position=$500 (0.5%) → BELOW_PILOT 'Pilot tier altında'", () => {
      const r = evaluatePyramidTier(500, PORTFOLIO);
      expect(r.currentTier).toBe("BELOW_PILOT");
      expect(r.positionPct).toBe(0.5);
      expect(r.markSays).toContain("Pilot tier altında");
    });
  });

  describe("PILOT tier (%1-3)", () => {
    it("position=$2000 (2%) → PILOT ok 'nakitten ilk giriş'", () => {
      const r = evaluatePyramidTier(2000, PORTFOLIO);
      expect(r.currentTier).toBe("PILOT");
      expect(r.severity).toBe("ok");
      expect(r.markSays).toContain("nakitten ilk giriş");
    });

    it("position=$1000 (1% — alt sınır boundary) → PILOT", () => {
      const r = evaluatePyramidTier(1000, PORTFOLIO);
      expect(r.currentTier).toBe("PILOT");
      expect(r.positionPct).toBe(1);
    });

    it("position=$3000 (3% — üst sınır boundary) → PILOT", () => {
      const r = evaluatePyramidTier(3000, PORTFOLIO);
      expect(r.currentTier).toBe("PILOT");
      expect(r.positionPct).toBe(3);
    });

    it("PILOT → nextTier='STANDARD' gap %6.25-12.5", () => {
      const r = evaluatePyramidTier(2000, PORTFOLIO);
      expect(r.nextTier).toBe("STANDARD");
      expect(r.nextTierGap).toEqual({ from: 6.25, to: 12.5 });
    });
  });

  describe("PILOT-STANDARD arası (%3-6.25) — Mark X kilit", () => {
    it("position=$5000 (5%) + prevProfitable=false → warn 'YASAK'", () => {
      const r = evaluatePyramidTier(5000, PORTFOLIO, false);
      expect(r.severity).toBe("warn");
      expect(r.markSays).toContain("pilot kâra geçmeden Standart'a YASAK");
    });

    it("position=$5000 (5%) + prevProfitable=true → info 'güvenli'", () => {
      const r = evaluatePyramidTier(5000, PORTFOLIO, true);
      expect(r.severity).toBe("info");
      expect(r.markSays).toContain("Standart'a (%6.25) güvenli");
    });
  });

  describe("STANDARD tier (%6.25-12.5)", () => {
    it("position=$10000 (10%) + prevProfitable=true → STANDARD ok 'trades working'", () => {
      const r = evaluatePyramidTier(10_000, PORTFOLIO, true);
      expect(r.currentTier).toBe("STANDARD");
      expect(r.severity).toBe("ok");
      expect(r.markSays).toContain("normal piyasa + trades working");
    });

    it("position=$10000 (10%) + prevProfitable=false → STANDARD warn 'kilit'", () => {
      const r = evaluatePyramidTier(10_000, PORTFOLIO, false);
      expect(r.currentTier).toBe("STANDARD");
      expect(r.severity).toBe("warn");
      expect(r.markSays).toContain("kilit");
    });

    it("STANDARD → nextTier='FULL' gap %15-25", () => {
      const r = evaluatePyramidTier(10_000, PORTFOLIO, true);
      expect(r.nextTier).toBe("FULL");
      expect(r.nextTierGap).toEqual({ from: 15, to: 25 });
    });
  });

  describe("STANDARD-FULL arası (%12.5-15) — Mark X 2. kilit", () => {
    it("position=$13000 (13%) + prevProfitable=false → warn 'Full'a YASAK'", () => {
      const r = evaluatePyramidTier(13_000, PORTFOLIO, false);
      expect(r.severity).toBe("warn");
      expect(r.markSays).toContain("standart kâra geçmeden Full'a YASAK");
    });
  });

  describe("FULL tier (%15-25) — Mark Kelly 2:1", () => {
    it("position=$20000 (20%) + prevProfitable=true → FULL ok 'Kelly 2:1'", () => {
      const r = evaluatePyramidTier(20_000, PORTFOLIO, true);
      expect(r.currentTier).toBe("FULL");
      expect(r.severity).toBe("ok");
      expect(r.markSays).toContain("Kelly 2:1");
    });

    it("FULL → nextTier=null (zirve)", () => {
      const r = evaluatePyramidTier(20_000, PORTFOLIO, true);
      expect(r.nextTier).toBeNull();
      expect(r.nextTierGap).toBeNull();
    });
  });

  describe("OVER_MAX (%25+) — Mark sert tavan ihlali", () => {
    it("position=$30000 (30%) → OVER_MAX violation 'MAX_POSITION_MAX_PCT = 50%'", () => {
      const r = evaluatePyramidTier(30_000, PORTFOLIO, true);
      expect(r.currentTier).toBe("OVER_MAX");
      expect(r.severity).toBe("violation");
      expect(r.markSays).toContain("%25 üstü");
      expect(r.tierLabel).toBe("MAX AŞILDI");
    });
  });
});

describe("suggestPositionDollars — tier min/max çevrimi", () => {
  it("portfolio=$100k + PILOT → $1000-$3000", () => {
    const r = suggestPositionDollars(100_000, "PILOT");
    expect(r.min).toBe(1000);
    expect(r.max).toBe(3000);
  });

  it("portfolio=$100k + STANDARD → $6250-$12500", () => {
    const r = suggestPositionDollars(100_000, "STANDARD");
    expect(r.min).toBe(6250);
    expect(r.max).toBe(12500);
  });

  it("portfolio=$100k + FULL → $15000-$25000", () => {
    const r = suggestPositionDollars(100_000, "FULL");
    expect(r.min).toBe(15_000);
    expect(r.max).toBe(25_000);
  });

  it("portfolio=$50k + PILOT → $500-$1500 (orantı korunur)", () => {
    const r = suggestPositionDollars(50_000, "PILOT");
    expect(r.min).toBe(500);
    expect(r.max).toBe(1500);
  });
});

describe("TIER_LIMITS sabitleri (KALICI İLKE #4 — Mark birebir kaynak)", () => {
  it("PILOT %1-3 TraderLion atfı", () => {
    expect(TIER_LIMITS.PILOT.minPct).toBe(1);
    expect(TIER_LIMITS.PILOT.maxPct).toBe(3);
    expect(TIER_LIMITS.PILOT.markSource).toContain("TraderLion");
  });

  it("STANDARD %6.25-12.5 Brandon atfı", () => {
    expect(TIER_LIMITS.STANDARD.minPct).toBe(6.25);
    expect(TIER_LIMITS.STANDARD.maxPct).toBe(12.5);
    expect(TIER_LIMITS.STANDARD.markSource).toContain("Brandon");
  });

  it("FULL %15-25 Kelly 2:1 atfı", () => {
    expect(TIER_LIMITS.FULL.minPct).toBe(15);
    expect(TIER_LIMITS.FULL.maxPct).toBe(25);
    expect(TIER_LIMITS.FULL.markSource).toContain("Kelly 2:1");
  });
});
