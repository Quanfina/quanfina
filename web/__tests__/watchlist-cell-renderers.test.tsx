/**
 * Watchlist AG Grid cell renderer testleri:
 *   SymbolCellRenderer (Link /hisse/) + StrategyCellRenderer (Minervini/Carr) +
 *   SetupCellRenderer (VCP/Pullback + TermTooltip).
 */
import { describe, it, expect, vi } from "vitest";
import { render, screen } from "@testing-library/react";
import { SymbolCellRenderer } from "@/components/watchlist/SymbolCellRenderer";
import { StrategyCellRenderer } from "@/components/watchlist/StrategyCellRenderer";
import { SetupCellRenderer } from "@/components/watchlist/SetupCellRenderer";

// next/link mock
vi.mock("next/link", () => ({
  default: ({ children, href }: { children: React.ReactNode; href: string }) => (
    <a href={href}>{children}</a>
  ),
}));

// useTerms mock — TermTooltip bağımı (boş → children render)
vi.mock("@/hooks/use-terms", () => ({
  useTerms: () => ({ data: [] }),
}));

describe("SymbolCellRenderer — sembol linki", () => {
  it("value='AAPL' → /hisse/AAPL linki", () => {
    const { container } = render(<SymbolCellRenderer value="AAPL" />);
    const link = container.querySelector("a");
    expect(link?.getAttribute("href")).toBe("/hisse/AAPL");
    expect(screen.getByText("AAPL")).toBeInTheDocument();
  });

  it("value boş → '—' (link yok)", () => {
    const { container } = render(<SymbolCellRenderer value="" />);
    expect(screen.getByText("—")).toBeInTheDocument();
    expect(container.querySelector("a")).toBeNull();
  });

  it("undefined → '—'", () => {
    render(<SymbolCellRenderer />);
    expect(screen.getByText("—")).toBeInTheDocument();
  });
});

describe("StrategyCellRenderer — strateji label", () => {
  it("minervini → 'Minervini' (TermTooltip içinde)", () => {
    render(<StrategyCellRenderer value="minervini" />);
    expect(screen.getByText("Minervini")).toBeInTheDocument();
  });

  it("carr → 'Carr'", () => {
    render(<StrategyCellRenderer value="carr" />);
    expect(screen.getByText("Carr")).toBeInTheDocument();
  });

  it("Bilinmeyen strateji → raw value (termKey yok → span)", () => {
    render(<StrategyCellRenderer value="custom" />);
    expect(screen.getByText("custom")).toBeInTheDocument();
  });

  it("undefined → '—'", () => {
    render(<StrategyCellRenderer />);
    expect(screen.getByText("—")).toBeInTheDocument();
  });
});

describe("SetupCellRenderer — setup tipi", () => {
  it.each<string>(["VCP", "Pullback", "Coiled Spring"])(
    "setup='%s' → label render (TermTooltip)",
    (setup) => {
      render(<SetupCellRenderer value={setup} />);
      expect(screen.getByText(setup)).toBeInTheDocument();
    }
  );

  it("Bilinmeyen setup → raw value (termKey yok → span)", () => {
    render(<SetupCellRenderer value="Flag" />);
    expect(screen.getByText("Flag")).toBeInTheDocument();
  });

  it("null → '—' (soluk)", () => {
    render(<SetupCellRenderer value={null} />);
    expect(screen.getByText("—")).toBeInTheDocument();
  });

  it("undefined → '—'", () => {
    render(<SetupCellRenderer />);
    expect(screen.getByText("—")).toBeInTheDocument();
  });
});
