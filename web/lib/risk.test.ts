/**
 * B3-01 (05 Tem 2026 — FAZ-2): riskDollar helper testleri.
 *
 * Backend `quanfina_math.risk_dollars` (quanfina_math.py:104-109) BİREBİR aynası:
 *   sign = 1 if LONG else -1;  max((entry - stop) * sign * shares, 0)
 *
 * Kapsam: LONG + SHORT (stop>entry → pozitif) + geçersiz-stop (→0, her iki yön) +
 * invest_type undefined (→LONG davranışı). Canlı SHORT paper trade henüz YOK →
 * component-level test-verisiyle doğrulanır (mock'a düşme yok, prod'da SHORT satır yok).
 */
import { describe, it, expect } from "vitest";
import { riskDollar } from "@/lib/risk";
import { calcPL } from "@/lib/math";

/**
 * B3-01b (05 Tem 2026): OpenPositionsRiskPanel unrealizedR bileşiminin birebir aynası.
 * Component: unrealizedR = calcPL(entry,current,shares,investType).plDollar / dollarRisk,
 * dollarRisk = riskDollar(entry,planStop,shares,investType). Bu yardımcı testte aynı iki
 * kanon helper'ı kullanır (yeni formül üretmez — Kural #26). İşaret doğruluğunu doğrular.
 */
function unrealizedRComposition(
  entry: number,
  current: number,
  planStop: number,
  shares: number,
  investType?: 1 | 2,
): number | null {
  const risk = riskDollar(entry, planStop, shares, investType);
  if (risk <= 0) return null;
  return calcPL(entry, current, shares, investType).plDollar / risk;
}

describe("riskDollar — backend risk_dollars birebir (sign + max-floor)", () => {
  it("LONG (default) entry=100 stop=95 shares=10 → 50", () => {
    // (100-95) * 1 * 10 = 50 — LONG'da (entry>stop) pozitif
    expect(riskDollar(100, 95, 10, 1)).toBe(50);
  });

  it("LONG explicit entry=50 stop=45 shares=20 → 100", () => {
    expect(riskDollar(50, 45, 20, 1)).toBe(100);
  });

  it("SHORT (invest_type=2) entry=100 stop=105 shares=10 → 50 (stop>entry, pozitif)", () => {
    // SHORT: (100-105) * -1 * 10 = 50 — sign olmadan -50 çıkardı (eski bug)
    expect(riskDollar(100, 105, 10, 2)).toBe(50);
  });

  it("SHORT entry=200 stop=210 shares=5 → 50", () => {
    expect(riskDollar(200, 210, 5, 2)).toBe(50);
  });

  it("invest_type undefined → LONG davranışı (entry=100 stop=90 shares=10 → 100)", () => {
    expect(riskDollar(100, 90, 10, undefined)).toBe(100);
    expect(riskDollar(100, 90, 10)).toBe(100); // argüman hiç verilmezse de LONG
  });

  it("geçersiz-stop LONG (stop>entry) → max-floor 0", () => {
    // (100-110) * 1 * 10 = -100 → max(...,0) → 0
    expect(riskDollar(100, 110, 10, 1)).toBe(0);
  });

  it("geçersiz-stop SHORT (stop<entry) → max-floor 0", () => {
    // (100-95) * -1 * 10 = -50 → max(...,0) → 0
    expect(riskDollar(100, 95, 10, 2)).toBe(0);
  });

  it("stop=entry → 0 (risk yok)", () => {
    expect(riskDollar(100, 100, 10, 1)).toBe(0);
    expect(riskDollar(100, 100, 10, 2)).toBe(0);
  });
});

describe("unrealizedR (B3-01b) — unrealized P/L ÷ dolar risk, işaret doğruluğu", () => {
  // LONG entry=100 stop=95 shares=10 → risk=50
  it("LONG kâr: current=110 → +2.0R (fiyat yükseldi, kâr)", () => {
    // plDollar = (110-100)*10 = 100 ; 100/50 = +2
    expect(unrealizedRComposition(100, 110, 95, 10, 1)).toBeCloseTo(2.0, 5);
  });
  it("LONG zarar: current=97 → -0.6R (fiyat düştü, zarar)", () => {
    // plDollar = (97-100)*10 = -30 ; -30/50 = -0.6
    expect(unrealizedRComposition(100, 97, 95, 10, 1)).toBeCloseTo(-0.6, 5);
  });

  // SHORT entry=100 stop=105 shares=10 → risk=50 (sign+max ile pozitif)
  it("SHORT kâr: current=90 → +2.0R (fiyat DÜŞTÜ, kâr) — eski kod null verirdi", () => {
    // plDollar = -1*(90-100)*10 = +100 ; 100/50 = +2 (POZİTİF, doğru)
    expect(unrealizedRComposition(100, 90, 105, 10, 2)).toBeCloseTo(2.0, 5);
  });
  it("SHORT zarar: current=103 → -0.6R (fiyat YÜKSELDİ, zarar)", () => {
    // plDollar = -1*(103-100)*10 = -30 ; -30/50 = -0.6 (NEGATİF, doğru)
    expect(unrealizedRComposition(100, 103, 105, 10, 2)).toBeCloseTo(-0.6, 5);
  });

  it("geçersiz-stop (risk=0) → null (R gösterilmez)", () => {
    expect(unrealizedRComposition(100, 110, 110, 10, 1)).toBeNull(); // LONG stop>entry
    expect(unrealizedRComposition(100, 90, 95, 10, 2)).toBeNull();   // SHORT stop<entry
  });

  it("invest_type undefined → LONG davranışı (current=110 → +2.0R)", () => {
    expect(unrealizedRComposition(100, 110, 95, 10, undefined)).toBeCloseTo(2.0, 5);
  });
});
