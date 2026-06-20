/**
 * MarkRiskAdvisor (P559) — pozisyon % GERÇEK portföy değerine dayanır (Kural #28).
 *
 * Önceki bug: hardcoded $100K placeholder → pozisyon % yanlış. Fix: usePortfolioValue
 * (kullanıcının kayıtlı değeri). Bu test gerçek değerin kullanıldığını kilitler.
 */
import { describe, it, expect, vi } from "vitest";
import { render, screen } from "@testing-library/react";
import { MarkRiskAdvisor } from "@/components/journal/MarkRiskAdvisor";

vi.mock("@/hooks/use-risk-advisor", () => ({
  useRiskAdvisor: () => ({ mutate: vi.fn(), isPending: false, error: null }),
}));
vi.mock("@/hooks/use-rba-metrics", () => ({
  useRbaMetrics: () => ({ data: undefined }),
}));
// Gerçek portföy değeri $50K (placeholder $100K DEĞİL)
vi.mock("@/hooks/use-portfolio-value", () => ({
  usePortfolioValue: () => ({ value: 50000, setValue: vi.fn(), defaultValue: 100000 }),
}));

describe("MarkRiskAdvisor — gerçek portföy değeri (P559)", () => {
  it("pozisyon % gerçek portföy ($50K) ile hesaplanır, $100K placeholder DEĞİL", () => {
    // entry=100 × 100 adet = $10.000 pozisyon. $50K portföyde %20 (placeholder $100K'da %10 olurdu).
    render(<MarkRiskAdvisor entryPrice="100" shares="100" />);
    expect(screen.getByText(/%20\.00/)).toBeInTheDocument();
    expect(screen.queryByText(/%10\.00/)).toBeNull();
  });

  it("explicit portfolioValue prop verilirse onu kullanır (override)", () => {
    // prop $200K → $10K pozisyon = %5 (hook değeri $50K'yı override eder)
    render(<MarkRiskAdvisor entryPrice="100" shares="100" portfolioValue={200000} />);
    expect(screen.getByText(/%5\.00/)).toBeInTheDocument();
  });

  it("entry/adet yoksa danışman pasif (pozisyon değeri 0)", () => {
    render(<MarkRiskAdvisor entryPrice="" shares="" />);
    expect(screen.getByText(/Mark Risk Danışmanı aktif olacak/)).toBeInTheDocument();
  });
});
