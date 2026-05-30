/**
 * P408: Market Breadth + Divergence "MOCK" rozet şeffaflık testleri.
 *
 * Sn. Ferit paper trading'de "bu sayfadaki verilerin hepsi gerçek mi?" sordu.
 * A/D Line yfinance breadth bulk YOK -> _mock_breadth_history MOCK üretir.
 * UI'da net "MOCK" / "YARIM MOCK" rozet ile şeffaflık (İLKE #11 Objektif Ayna
 * Dil + Kural #26 sahte sayı uydurmama disiplini).
 *
 * MarkRegimeCard büyük component (props çok) — minimal mock ile render testi.
 */
import { describe, it, expect, vi } from "vitest";
import { render, screen } from "@testing-library/react";
import { MarkRegimeCard } from "@/components/market/MarkRegimeCard";

// useMarketStatus hook'unu MarkRegimeCard çağırıyor — mock
vi.mock("@/hooks/use-market-status", () => ({
  useMarketStatus: () => ({ data: null }),
}));


describe("MarkRegimeCard — P408+P409 MOCK rozet conditional şeffaflık", () => {
  it("P409: is_mock=true (DB fallback) → 'MOCK' rozet render", () => {
    render(
      <MarkRegimeCard
        distributionDays={3}
        marketBreadth={{
          ad_ratio: 0.98,
          ad_line_cumulative: 1096,
          breadth_health: "NEUTRAL",
          mark_says: "Test mock breadth",
          is_mock: true,
        }}
      />
    );
    const badge = screen.getByTestId("market-breadth-mock-badge");
    expect(badge).toBeInTheDocument();
    expect(badge.textContent).toBe("MOCK");
    expect(badge.getAttribute("title")).toContain("MOCK");
  });

  it("P409: is_mock=false (gercek scan) → rozet GÖSTERME (şeffaflık ileri)", () => {
    render(
      <MarkRegimeCard
        distributionDays={3}
        marketBreadth={{
          ad_ratio: 0.62,
          ad_line_cumulative: 1500,
          breadth_health: "STRONG",
          mark_says: "Test gercek breadth",
          is_mock: false,
        }}
      />
    );
    // Gercek scan: rozet YOK (P408 yanlış pozitif önlendi)
    expect(screen.queryByTestId("market-breadth-mock-badge")).not.toBeInTheDocument();
  });

  it("P409: is_mock undefined (eski backend) → rozet YOK (default false)", () => {
    render(
      <MarkRegimeCard
        distributionDays={3}
        marketBreadth={{
          ad_ratio: 1.0,
          ad_line_cumulative: 0,
          breadth_health: "NEUTRAL",
          mark_says: "Test",
        }}
      />
    );
    expect(screen.queryByTestId("market-breadth-mock-badge")).not.toBeInTheDocument();
  });

  it("P409: breadth_is_mock=true → Divergence 'YARIM MOCK' rozet render", () => {
    render(
      <MarkRegimeCard
        distributionDays={3}
        breadthDivergence={{
          divergence: "BEARISH_DIVERGENCE",
          index_change_pct: 1.11,
          ad_trend_delta: -873,
          severity: "critical",
          mark_says: "Test divergence",
          breadth_is_mock: true,
        }}
      />
    );
    const badge = screen.getByTestId("breadth-divergence-mock-badge");
    expect(badge).toBeInTheDocument();
    expect(badge.textContent).toBe("YARIM MOCK");
  });

  it("P409: breadth_is_mock=false → Divergence rozet YOK (gerçek A/D)", () => {
    render(
      <MarkRegimeCard
        distributionDays={3}
        breadthDivergence={{
          divergence: "CONFIRMED_UP",
          index_change_pct: 1.5,
          ad_trend_delta: 250,
          severity: "ok",
          mark_says: "Test gercek",
          breadth_is_mock: false,
        }}
      />
    );
    expect(screen.queryByTestId("breadth-divergence-mock-badge")).not.toBeInTheDocument();
  });

  it("Market Breadth + Divergence ikisi de yokken rozet YOK", () => {
    render(<MarkRegimeCard distributionDays={0} />);
    expect(screen.queryByTestId("market-breadth-mock-badge")).not.toBeInTheDocument();
    expect(screen.queryByTestId("breadth-divergence-mock-badge")).not.toBeInTheDocument();
  });

  it("Tooltip — İLKE #11 Objektif Ayna Dil, yağcılık YOK", () => {
    render(
      <MarkRegimeCard
        distributionDays={3}
        marketBreadth={{
          ad_ratio: 0.98,
          ad_line_cumulative: 1096,
          breadth_health: "NEUTRAL",
          mark_says: "Test",
          is_mock: true,
        }}
      />
    );
    const title = screen.getByTestId("market-breadth-mock-badge").getAttribute("title") ?? "";
    const yagcilik = ["aferin", "tebrikler", "üzülme", "harika"];
    for (const y of yagcilik) {
      expect(title.toLowerCase()).not.toContain(y);
    }
  });
});
