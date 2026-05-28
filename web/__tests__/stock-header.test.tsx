/**
 * StockHeader — Hisse detay başlık (symbol + price + change + RS badge + Mark rozet).
 *
 * RS renk eşikleri: ≥90 excellent35 / ≥70 excellent18 / ≥50 neutral / <50 danger.
 * change_pct sign rengi + MarkBadgeStrip koşullu (mark_signals varsa).
 */
import { describe, it, expect } from "vitest";
import { render, screen } from "@testing-library/react";
import { StockHeader } from "@/components/stock/StockHeader";
import type { StockInfo } from "@/types/stock";

function makeInfo(overrides: Partial<StockInfo> = {}): StockInfo {
  return {
    symbol: "AAPL",
    name: "Apple Inc.",
    sector: "Technology",
    industry: "Consumer Electronics",
    market_cap: "$3.0T",
    price: 150.25,
    change_pct: 1.5,
    rs_rating: 85,
    active_strategies: [],
    ...overrides,
  };
}

describe("StockHeader — temel render", () => {
  it("symbol + name + sector + industry render", () => {
    render(<StockHeader info={makeInfo()} />);
    expect(screen.getByText("AAPL")).toBeInTheDocument();
    expect(screen.getByText("Apple Inc.")).toBeInTheDocument();
    expect(screen.getByText("Technology")).toBeInTheDocument();
    expect(screen.getByText("Consumer Electronics")).toBeInTheDocument();
  });

  it("price 2 ondalık + $ prefix", () => {
    render(<StockHeader info={makeInfo({ price: 150.256 })} />);
    expect(screen.getByText("$150.26")).toBeInTheDocument();
  });

  it("market_cap render", () => {
    render(<StockHeader info={makeInfo({ market_cap: "$3.0T" })} />);
    expect(screen.getByText(/Piyasa Değeri: \$3.0T/)).toBeInTheDocument();
  });
});

describe("StockHeader — change_pct sign", () => {
  it("Pozitif change → '+1.50%'", () => {
    render(<StockHeader info={makeInfo({ change_pct: 1.5 })} />);
    expect(screen.getByText("+1.50%")).toBeInTheDocument();
  });

  it("Negatif change → '-2.30%' (sign yok, native -)", () => {
    render(<StockHeader info={makeInfo({ change_pct: -2.3 })} />);
    expect(screen.getByText("-2.30%")).toBeInTheDocument();
  });

  it("Sıfır change → '+0.00%' (>= 0 pozitif)", () => {
    render(<StockHeader info={makeInfo({ change_pct: 0 })} />);
    expect(screen.getByText("+0.00%")).toBeInTheDocument();
  });
});

describe("StockHeader — RS badge", () => {
  it.each([99, 85, 70, 55, 30])("RS=%i değeri badge'de görünür", (rs) => {
    render(<StockHeader info={makeInfo({ rs_rating: rs })} />);
    expect(screen.getByText(String(rs))).toBeInTheDocument();
    expect(screen.getByText("RS")).toBeInTheDocument();
  });

  it("RS=92 (Leader ≥90) → excellent renk uygulanır", () => {
    const { container } = render(<StockHeader info={makeInfo({ rs_rating: 92 })} />);
    // RS badge div'inin background'unda excellent token var
    const html = container.innerHTML;
    expect(html).toContain("mtp-excellent");
  });

  it("RS=30 (Laggard <50) → danger renk", () => {
    const { container } = render(<StockHeader info={makeInfo({ rs_rating: 30 })} />);
    expect(container.innerHTML).toContain("mtp-danger");
  });
});

describe("StockHeader — MarkBadgeStrip koşullu", () => {
  it("mark_signals yok → rozet GÖSTERILMEZ", () => {
    render(<StockHeader info={makeInfo()} />);
    expect(screen.queryByText("VCP A+")).not.toBeInTheDocument();
    expect(screen.queryByText("Stage 2")).not.toBeInTheDocument();
  });

  it("mark_signals VCP EXCELLENT + Stage 2 → rozetler render (density=full)", () => {
    render(
      <StockHeader
        info={makeInfo({
          mark_signals: { vcp_quality_score: "EXCELLENT", carr_stage: 2 },
        })}
      />
    );
    expect(screen.getByText("VCP A+")).toBeInTheDocument();
    expect(screen.getByText("Stage 2")).toBeInTheDocument();
  });

  it("mark_signals Stage 4 → ⛔ uzak dur rozeti", () => {
    render(
      <StockHeader
        info={makeInfo({ mark_signals: { carr_stage: 4 } })}
      />
    );
    expect(screen.getByText("Stage 4")).toBeInTheDocument();
  });
});
