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


describe("MarkRegimeCard — P408 MOCK rozet şeffaflık", () => {
  it("Market Breadth verisi varsa 'MOCK' rozet render", () => {
    render(
      <MarkRegimeCard
        distributionDays={3}
        marketBreadth={{
          ad_ratio: 0.98,
          ad_line_cumulative: 1096,
          breadth_health: "NEUTRAL",
          mark_says: "Test mock breadth",
        }}
      />
    );
    const badge = screen.getByTestId("market-breadth-mock-badge");
    expect(badge).toBeInTheDocument();
    expect(badge.textContent).toBe("MOCK");
    // title (tooltip) MOCK açıklaması içermeli
    expect(badge.getAttribute("title")).toContain("yfinance");
    expect(badge.getAttribute("title")).toContain("MOCK");
  });

  it("Breadth Divergence verisi varsa 'YARIM MOCK' rozet render", () => {
    render(
      <MarkRegimeCard
        distributionDays={3}
        breadthDivergence={{
          divergence: "BEARISH_DIVERGENCE",
          index_change_pct: 1.11,
          ad_trend_delta: -873,
          severity: "HIGH",
          mark_says: "Test divergence",
        }}
      />
    );
    const badge = screen.getByTestId("breadth-divergence-mock-badge");
    expect(badge).toBeInTheDocument();
    expect(badge.textContent).toBe("YARIM MOCK");
    // tooltip: SPY gerçek + A/D MOCK ayrımı açıklaması
    expect(badge.getAttribute("title")).toContain("MOCK");
  });

  it("Market Breadth + Divergence ikisi de yokken rozet render YOK", () => {
    render(<MarkRegimeCard distributionDays={0} />);
    expect(screen.queryByTestId("market-breadth-mock-badge")).not.toBeInTheDocument();
    expect(screen.queryByTestId("breadth-divergence-mock-badge")).not.toBeInTheDocument();
  });

  it("Tooltip içerikleri Mark 'Objektif Ayna Dil' (İLKE #11) — yağcılık yok", () => {
    render(
      <MarkRegimeCard
        distributionDays={3}
        marketBreadth={{
          ad_ratio: 0.98,
          ad_line_cumulative: 1096,
          breadth_health: "NEUTRAL",
          mark_says: "Test",
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
