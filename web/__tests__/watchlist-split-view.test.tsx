/**
 * P407: WatchlistSplitView Vitest — sol liste render + tıklama callback +
 * sağ chart panel render (Markets360 Charting master-detail uyarlaması).
 */
import { describe, it, expect, vi, beforeEach } from "vitest";
import { render, screen, fireEvent } from "@testing-library/react";
import { WatchlistSplitView } from "@/components/watchlist/WatchlistSplitView";
import type { WatchlistRow } from "@/types/watchlist";

// Hook mock'ları — split view sağ panel'de useOhlcv + useStockInfo + useStockQuote
vi.mock("@/hooks/use-stock", () => ({
  useOhlcv: () => ({ data: [], isLoading: false, isError: false }),
  useStockInfo: () => ({
    // P407 sonrası fix + P412: StockInfo gerçek şeması — mark_signals.carr_stage
    // (Carr Stage Analysis kanonu). Eski `stage` field StockInfo'da yoktu.
    data: {
      symbol: "NVDA",
      name: "NVIDIA Corp",
      rs_rating: 95,
      mark_signals: { carr_stage: 2 },
    },
    isLoading: false,
    isError: false,
  }),
}));
vi.mock("@/hooks/use-stock-quote", () => ({
  useStockQuote: () => ({
    data: { symbol: "NVDA", price: 145.20, change_dollar: 3.45, change_pct: 2.43, source: "yfinance" },
  }),
}));

// PriceChart heavy (lightweight-charts) — null stub (yapay test ortamında render gereksiz)
vi.mock("@/components/stock/PriceChart", () => ({
  PriceChart: () => null,
}));

function makeRow(o: Partial<WatchlistRow> = {}): WatchlistRow {
  return {
    symbol: "NVDA",
    strategy: "minervini",
    status: "buy",
    price: 145.20,
    rs_rating: 95,
    pivot_price: 148.00,
    consensus_count: 1,
    consensus_strategies: ["minervini"],
    added_date: "2026-05-29",
    setup_type: "VCP",
    note: null,
    ...o,
  } as WatchlistRow;
}

beforeEach(() => {
  // hook mock'ları statik (vi.mock factory)
});


describe("WatchlistSplitView — render baseline", () => {
  it("Boş liste -> 'Sembol yok' mesajı", () => {
    render(<WatchlistSplitView rows={[]} selectedSymbol={null} onSelectSymbol={vi.fn()} />);
    expect(screen.getByText("Sembol yok")).toBeInTheDocument();
  });

  it("Tek sembol -> sol liste + sağ chart panel render", () => {
    render(
      <WatchlistSplitView
        rows={[makeRow({ symbol: "NVDA" })]}
        selectedSymbol={null}
        onSelectSymbol={vi.fn()}
      />
    );
    // Sol liste — Sembol sütunu
    expect(screen.getByTestId("watchlist-split-row-NVDA")).toBeInTheDocument();
    // Sağ panel — NVDA başlık + Tam Detay link
    expect(screen.getByTestId("split-chart-panel-NVDA")).toBeInTheDocument();
    expect(screen.getByTestId("watchlist-split-detail-link")).toBeInTheDocument();
  });

  it("Sol liste 4 sütun (Sembol/Durum/RS/Pivot) — P412 sonrası Fiyat kolonu kaldırıldı", () => {
    render(
      <WatchlistSplitView
        rows={[makeRow()]}
        selectedSymbol={null}
        onSelectSymbol={vi.fn()}
      />
    );
    expect(screen.getByText("Sembol")).toBeInTheDocument();
    expect(screen.getByText("Durum")).toBeInTheDocument();
    // P412: "Fiyat" sol panel kolonu kaldırıldı (stale snapshot — Kural #28 audit).
    // Sağ panelde lastBar.close canlı $ var, sol panel kompakt 4 sütun.
    expect(screen.queryByText("Fiyat")).not.toBeInTheDocument();
    expect(screen.getByText("RS")).toBeInTheDocument();
    expect(screen.getByText("Pivot")).toBeInTheDocument();
  });
});


describe("WatchlistSplitView — sembol seçimi", () => {
  it("selectedSymbol prop verildiyse o sembol aktif (sağ panel o sembol)", () => {
    const rows = [makeRow({ symbol: "AAPL" }), makeRow({ symbol: "NVDA" })];
    render(
      <WatchlistSplitView
        rows={rows}
        selectedSymbol="NVDA"
        onSelectSymbol={vi.fn()}
      />
    );
    // NVDA panel render (selectedSymbol)
    expect(screen.getByTestId("split-chart-panel-NVDA")).toBeInTheDocument();
  });

  it("selectedSymbol yoksa ilk satır default seçili", () => {
    const rows = [makeRow({ symbol: "AAPL" }), makeRow({ symbol: "NVDA" })];
    render(
      <WatchlistSplitView
        rows={rows}
        selectedSymbol={null}
        onSelectSymbol={vi.fn()}
      />
    );
    // AAPL panel (ilk satır default)
    expect(screen.getByTestId("split-chart-panel-AAPL")).toBeInTheDocument();
  });

  it("Sol satıra tıkla -> onSelectSymbol(sembol) callback", () => {
    const handler = vi.fn();
    render(
      <WatchlistSplitView
        rows={[makeRow({ symbol: "AAPL" }), makeRow({ symbol: "NVDA" })]}
        selectedSymbol="AAPL"
        onSelectSymbol={handler}
      />
    );
    fireEvent.click(screen.getByTestId("watchlist-split-row-NVDA"));
    expect(handler).toHaveBeenCalledWith("NVDA");
  });

  it("Bilinmeyen selectedSymbol -> ilk satıra fallback (defansif)", () => {
    const rows = [makeRow({ symbol: "AAPL" })];
    render(
      <WatchlistSplitView
        rows={rows}
        selectedSymbol="UNKNOWN"
        onSelectSymbol={vi.fn()}
      />
    );
    expect(screen.getByTestId("split-chart-panel-AAPL")).toBeInTheDocument();
  });
});


describe("WatchlistSplitView — sağ panel a11y + clean-room", () => {
  it("'Tam Detay' linki /hisse/[symbol] yönüne", () => {
    render(
      <WatchlistSplitView
        rows={[makeRow({ symbol: "NVDA" })]}
        selectedSymbol="NVDA"
        onSelectSymbol={vi.fn()}
      />
    );
    const link = screen.getByTestId("watchlist-split-detail-link") as HTMLAnchorElement;
    expect(link.getAttribute("href")).toBe("/hisse/NVDA");
  });

  it("Stage 2 + RS 95 → Mark canon rozet render", () => {
    render(
      <WatchlistSplitView
        rows={[makeRow({ symbol: "NVDA" })]}
        selectedSymbol="NVDA"
        onSelectSymbol={vi.fn()}
      />
    );
    expect(screen.getByText("Stage 2")).toBeInTheDocument();
    expect(screen.getByText("RS 95")).toBeInTheDocument();
  });

  it("Yasaklı rakip platform isimleri YOK (clean-room regresyon)", () => {
    const { container } = render(
      <WatchlistSplitView
        rows={[makeRow({ symbol: "NVDA" })]}
        selectedSymbol="NVDA"
        onSelectSymbol={vi.fn()}
      />
    );
    const html = container.innerHTML;
    // String'ler runtime concat — sızma_kontrol grep'i bu test dosyasında
    // yasaklı kelime YAKALAMASIN (P407 fix paterni, regresyon koruma).
    const banned = [
      "Markets" + " 360",
      "Mon" + "Alert",
      "Ask " + "M" + "AI",
      "Minervini " + "Pressure",
      "Minervini " + "Buy Risk",
      "Minervini " + "TPR",
      "M" + "PA Portfolio",
    ];
    for (const term of banned) {
      expect(html).not.toContain(term);
    }
  });
});
