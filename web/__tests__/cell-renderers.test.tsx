/**
 * AG Grid cell renderer testleri:
 *   RMultipleCell (KARAR #734 — R-Multiple görsel) +
 *   SignalRREnrichedCell (P158 — RS + Climax + R/R birleşik).
 *
 * ICellRendererParams { data, value } mock ile render.
 */
import { describe, it, expect } from "vitest";
import { render, screen } from "@testing-library/react";
import type { ICellRendererParams } from "ag-grid-community";
import { RMultipleCell } from "@/components/journal/RMultipleCell";
import { SignalRREnrichedCell } from "@/components/signals/SignalRREnrichedCell";
import type { Trade } from "@/types/trade";
import type { Signal } from "@/types/signal";

function tradeParams(data: Partial<Trade> | null): ICellRendererParams<Trade> {
  return { data: data as Trade } as ICellRendererParams<Trade>;
}

function signalParams(
  value: number | null,
  data: Partial<Signal> = {}
): ICellRendererParams<Signal> {
  return { value, data: data as Signal } as ICellRendererParams<Signal>;
}

describe("RMultipleCell — R-Multiple AG Grid hücre", () => {
  it("data=null → '—'", () => {
    render(<RMultipleCell {...tradeParams(null)} />);
    expect(screen.getByText("—")).toBeInTheDocument();
  });

  it("plan_stop=null → '—' (R hesaplanamaz)", () => {
    render(
      <RMultipleCell
        {...tradeParams({ entry_price: 100, plan_stop: null, exit_price: 110, shares: 100 })}
      />
    );
    expect(screen.getByText("—")).toBeInTheDocument();
  });

  it("exit_price=null (açık trade) → '—'", () => {
    render(
      <RMultipleCell
        {...tradeParams({ entry_price: 100, plan_stop: 95, exit_price: null, shares: 100 })}
      />
    );
    expect(screen.getByText("—")).toBeInTheDocument();
  });

  it("entry=100 stop=95 exit=115 → +3.00R (R-Multiple görsel)", () => {
    render(
      <RMultipleCell
        {...tradeParams({ entry_price: 100, plan_stop: 95, exit_price: 115, shares: 100 })}
      />
    );
    expect(screen.getByText("+3.00R")).toBeInTheDocument();
  });

  it("entry=100 stop=95 exit=90 → -2.00R (zarar, kırmızı)", () => {
    render(
      <RMultipleCell
        {...tradeParams({ entry_price: 100, plan_stop: 95, exit_price: 90, shares: 100 })}
      />
    );
    expect(screen.getByText("−2.00R")).toBeInTheDocument();
  });

  it("Geçersiz (entry <= stop) → computeRMultiple null → '—'", () => {
    render(
      <RMultipleCell
        {...tradeParams({ entry_price: 95, plan_stop: 100, exit_price: 110, shares: 100 })}
      />
    );
    expect(screen.getByText("—")).toBeInTheDocument();
  });

  it("markSays tooltip (title attr) render", () => {
    const { container } = render(
      <RMultipleCell
        {...tradeParams({ entry_price: 100, plan_stop: 95, exit_price: 115, shares: 100 })}
      />
    );
    const span = container.querySelector("span[title]");
    expect(span?.getAttribute("title")).toBeTruthy();
  });
});

describe("SignalRREnrichedCell — RS + Climax + R/R birleşik", () => {
  it("value=null → '—'", () => {
    render(<SignalRREnrichedCell {...signalParams(null)} />);
    expect(screen.getByText("—")).toBeInTheDocument();
  });

  it("R/R=3.5 → değer render", () => {
    render(<SignalRREnrichedCell {...signalParams(3.5, { rs_rating: 85 })} />);
    // R/R sayısı görünür (formatlanmış)
    expect(screen.getByText(/3\.5/)).toBeInTheDocument();
  });

  it("rs_rating=85 → 'L' (LEADER ≥80) rozet", () => {
    render(<SignalRREnrichedCell {...signalParams(2.5, { rs_rating: 85 })} />);
    expect(screen.getByText("L")).toBeInTheDocument();
  });

  it("rs_rating=75 → 'S' (STRONG ≥70)", () => {
    render(<SignalRREnrichedCell {...signalParams(2.5, { rs_rating: 75 })} />);
    expect(screen.getByText("S")).toBeInTheDocument();
  });

  it("rs_rating=55 → 'A' (AVERAGE ≥50)", () => {
    render(<SignalRREnrichedCell {...signalParams(2.5, { rs_rating: 55 })} />);
    expect(screen.getByText("A")).toBeInTheDocument();
  });

  it("rs_rating=30 → '↓' (LAGGARD <50)", () => {
    render(<SignalRREnrichedCell {...signalParams(2.5, { rs_rating: 30 })} />);
    expect(screen.getByText("↓")).toBeInTheDocument();
  });

  it("climax_category='CLIMAX_TOP' → 🔥 ikon (mark_signals)", () => {
    const { container } = render(
      <SignalRREnrichedCell
        {...signalParams(2.5, {
          rs_rating: 85,
          mark_signals: { climax_category: "CLIMAX_TOP" },
        } as Partial<Signal>)}
      />
    );
    // Climax top span (title attr içeren)
    expect(container.querySelector('span[title*="Climax"]')).not.toBeNull();
  });

  it("rs_rating yok → rozet göstermez (sadece R/R)", () => {
    render(<SignalRREnrichedCell {...signalParams(2.5, {})} />);
    expect(screen.getByText(/2\.5/)).toBeInTheDocument();
    expect(screen.queryByText("L")).not.toBeInTheDocument();
  });

  it("stop_basis_is_mock=true → ⚠ amber uyarı (P472, Kural #28)", () => {
    render(
      <SignalRREnrichedCell
        {...signalParams(2.5, { rs_rating: 85, stop_basis_is_mock: true } as Partial<Signal>)}
      />
    );
    expect(screen.getByText("⚠")).toBeInTheDocument();
  });

  it("stop_basis_is_mock yok → ⚠ göstermez (gerçek veri)", () => {
    render(<SignalRREnrichedCell {...signalParams(2.5, { rs_rating: 85 })} />);
    expect(screen.queryByText("⚠")).not.toBeInTheDocument();
  });
});
