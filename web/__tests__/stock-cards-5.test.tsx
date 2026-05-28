/**
 * Stock detay kartları grup 5: ActiveStrategies + SetupNotes (props-based).
 *
 * Aktif strateji listesi (boş durum + çoklu strateji + setup + pivot) + Notlar.
 */
import { describe, it, expect, vi, beforeEach } from "vitest";
import { render, screen } from "@testing-library/react";
import { ActiveStrategies } from "@/components/stock/ActiveStrategies";
import { SetupNotes } from "@/components/stock/SetupNotes";
import type { WatchlistRow } from "@/types/watchlist";

// useTerms mock — TermTooltip bağımı (boş terms → children render)
vi.mock("@/hooks/use-terms", () => ({
  useTerms: () => ({ data: [] }),
}));

beforeEach(() => {
  vi.clearAllMocks();
});

function makeRow(overrides: Partial<WatchlistRow> = {}): WatchlistRow {
  return {
    symbol: "AAPL",
    strategy: "minervini",
    status: "focus",
    setup_type: "VCP",
    ...overrides,
  } as WatchlistRow;
}

describe("ActiveStrategies — boş durum", () => {
  it("strategies=[] → '{symbol} watchlist'te yok'", () => {
    render(<ActiveStrategies strategies={[]} symbol="AAPL" />);
    expect(screen.getByText(/AAPL watchlist'te yok/)).toBeInTheDocument();
  });

  it("Başlık 'Aktif Stratejiler' her zaman render", () => {
    render(<ActiveStrategies strategies={[]} symbol="AAPL" />);
    expect(screen.getByText("Aktif Stratejiler")).toBeInTheDocument();
  });
});

describe("ActiveStrategies — strateji listesi", () => {
  it("minervini → 'Minervini' label render", () => {
    render(
      <ActiveStrategies strategies={[makeRow({ strategy: "minervini" })]} symbol="AAPL" />
    );
    expect(screen.getByText("Minervini")).toBeInTheDocument();
  });

  it("carr → 'Carr' label render", () => {
    render(
      <ActiveStrategies strategies={[makeRow({ strategy: "carr" })]} symbol="AAPL" />
    );
    expect(screen.getByText("Carr")).toBeInTheDocument();
  });

  it("Çoklu strateji (minervini + carr) → 2 kart", () => {
    render(
      <ActiveStrategies
        strategies={[
          makeRow({ strategy: "minervini" }),
          makeRow({ strategy: "carr" }),
        ]}
        symbol="AAPL"
      />
    );
    expect(screen.getByText("Minervini")).toBeInTheDocument();
    expect(screen.getByText("Carr")).toBeInTheDocument();
  });

  it("setup_type='VCP' → 'Setup: VCP' render", () => {
    render(
      <ActiveStrategies strategies={[makeRow({ setup_type: "VCP" })]} symbol="AAPL" />
    );
    expect(screen.getByText(/Setup:/)).toBeInTheDocument();
    expect(screen.getByText("VCP")).toBeInTheDocument();
  });

  it("pivot_price=150.25 → 'Pivot: $150.25'", () => {
    render(
      <ActiveStrategies
        strategies={[makeRow({ pivot_price: 150.25 } as Partial<WatchlistRow>)]}
        symbol="AAPL"
      />
    );
    expect(screen.getByText(/Pivot: \$150.25/)).toBeInTheDocument();
  });

  it("added_date → 'Eklendi: {tarih}'", () => {
    render(
      <ActiveStrategies
        strategies={[makeRow({ added_date: "2026-05-20" } as Partial<WatchlistRow>)]}
        symbol="AAPL"
      />
    );
    expect(screen.getByText(/Eklendi: 2026-05-20/)).toBeInTheDocument();
  });

  it("Bilinmeyen strateji → raw strategy adı fallback", () => {
    render(
      <ActiveStrategies strategies={[makeRow({ strategy: "custom_xyz" as unknown as WatchlistRow["strategy"] })]} symbol="AAPL" />
    );
    expect(screen.getByText("custom_xyz")).toBeInTheDocument();
  });
});

describe("SetupNotes — props-based notlar", () => {
  it("Hiç not yok → null (render etmez)", () => {
    const { container } = render(
      <SetupNotes strategies={[makeRow({ note: undefined } as Partial<WatchlistRow>)]} />
    );
    expect(container.firstChild).toBeNull();
  });

  it("1 not var → 'Notlar' başlık + not metni", () => {
    render(
      <SetupNotes
        strategies={[
          makeRow({ strategy: "minervini", note: "Pivot $150 izle" } as Partial<WatchlistRow>),
        ]}
      />
    );
    expect(screen.getByText("Notlar")).toBeInTheDocument();
    expect(screen.getByText("Pivot $150 izle")).toBeInTheDocument();
  });

  it("Notu olmayan strateji filtrelenir (sadece notlu olanlar)", () => {
    render(
      <SetupNotes
        strategies={[
          makeRow({ strategy: "minervini", note: "Not A" } as Partial<WatchlistRow>),
          makeRow({ strategy: "carr", note: undefined } as Partial<WatchlistRow>),
        ]}
      />
    );
    expect(screen.getByText("Not A")).toBeInTheDocument();
    // carr notu yok → label görünmez (Minervini: render, Carr: yok)
    expect(screen.getByText(/Minervini:/)).toBeInTheDocument();
    expect(screen.queryByText(/Carr:/)).not.toBeInTheDocument();
  });

  it("Çoklu notlu strateji → hepsi render", () => {
    render(
      <SetupNotes
        strategies={[
          makeRow({ strategy: "minervini", note: "Mark notu" } as Partial<WatchlistRow>),
          makeRow({ strategy: "carr", note: "Carr notu" } as Partial<WatchlistRow>),
        ]}
      />
    );
    expect(screen.getByText("Mark notu")).toBeInTheDocument();
    expect(screen.getByText("Carr notu")).toBeInTheDocument();
  });
});
