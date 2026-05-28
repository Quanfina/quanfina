/**
 * OpenPositionsRiskPanel (KARAR ADAY #455 — Risk-Merkez UI).
 *
 * Açık pozisyon dolar riski (entry-stop)×shares + % portföy + unrealized R +
 * toplam risk. Mark TTLC risk disiplini (%2.5 üst, stop yok kırmızı).
 */
import { describe, it, expect, vi, beforeEach } from "vitest";
import { render, screen } from "@testing-library/react";
import { OpenPositionsRiskPanel } from "@/components/journal/OpenPositionsRiskPanel";
import type { Trade } from "@/types/trade";

const mockTrades = vi.fn<[], { data?: Trade[]; isLoading: boolean; isError: boolean }>();

vi.mock("@/hooks/use-trades", () => ({
  useTrades: () => mockTrades(),
}));

beforeEach(() => {
  mockTrades.mockReset();
});

function makeTrade(o: Partial<Trade> = {}): Trade {
  return {
    id: 1, symbol: "AAPL", strategy: "minervini", setup_type: "vcp",
    entry_date: "2026-05-20", entry_price: 100, shares: 100,
    status: "open", plan_stop: 95, current_price: 105,
    pl_dollar: null, pl_pct: null, grade: null, exit_reason: null, lessons: null,
    ...o,
  } as Trade;
}

describe("OpenPositionsRiskPanel — boş / loading", () => {
  it("isLoading → 'yükleniyor'", () => {
    mockTrades.mockReturnValue({ data: undefined, isLoading: true, isError: false });
    render(<OpenPositionsRiskPanel />);
    expect(screen.getByText(/Açık pozisyon riskleri yükleniyor/)).toBeInTheDocument();
  });

  it("Açık pozisyon yok → 'risk taşınmıyor'", () => {
    mockTrades.mockReturnValue({ data: [], isLoading: false, isError: false });
    render(<OpenPositionsRiskPanel />);
    expect(screen.getByText(/risk taşınmıyor/)).toBeInTheDocument();
  });

  it("Sadece kapalı trade → açık pozisyon yok", () => {
    mockTrades.mockReturnValue({
      data: [makeTrade({ status: "closed" })],
      isLoading: false,
      isError: false,
    });
    render(<OpenPositionsRiskPanel />);
    expect(screen.getByText(/risk taşınmıyor/)).toBeInTheDocument();
  });
});

describe("OpenPositionsRiskPanel — risk hesabı", () => {
  it("entry=100 stop=95 shares=100 → dolar risk $500", () => {
    mockTrades.mockReturnValue({
      data: [makeTrade({ entry_price: 100, plan_stop: 95, shares: 100 })],
      isLoading: false,
      isError: false,
    });
    render(<OpenPositionsRiskPanel portfolioValue={100000} />);
    // (100-95)×100 = 500
    expect(screen.getAllByText(/\$500/).length).toBeGreaterThan(0);
  });

  it("% portföy risk = 500/100000 = %0.5", () => {
    mockTrades.mockReturnValue({
      data: [makeTrade({ entry_price: 100, plan_stop: 95, shares: 100 })],
      isLoading: false,
      isError: false,
    });
    render(<OpenPositionsRiskPanel portfolioValue={100000} />);
    // %0.5 hem pozisyon satırı hem toplam risk → en az 1 eşleşme
    expect(screen.getAllByText(/%0\.5/).length).toBeGreaterThan(0);
  });

  it("unrealized R: entry=100 stop=95 current=110 → +2.0R", () => {
    mockTrades.mockReturnValue({
      data: [makeTrade({ entry_price: 100, plan_stop: 95, current_price: 110 })],
      isLoading: false,
      isError: false,
    });
    render(<OpenPositionsRiskPanel />);
    // (110-100)/(100-95) = 2.0
    expect(screen.getByText(/\+2\.0R/)).toBeInTheDocument();
  });

  it("Negatif unrealized R: current=97 → -0.6R", () => {
    mockTrades.mockReturnValue({
      data: [makeTrade({ entry_price: 100, plan_stop: 95, current_price: 97 })],
      isLoading: false,
      isError: false,
    });
    render(<OpenPositionsRiskPanel />);
    // (97-100)/(100-95) = -0.6
    expect(screen.getByText(/-0\.6R/)).toBeInTheDocument();
  });

  it("Sembol render", () => {
    mockTrades.mockReturnValue({
      data: [makeTrade({ symbol: "NVDA" })],
      isLoading: false,
      isError: false,
    });
    render(<OpenPositionsRiskPanel />);
    expect(screen.getByText("NVDA")).toBeInTheDocument();
  });
});

describe("OpenPositionsRiskPanel — plan_stop yok uyarısı (Mark risk first)", () => {
  it("plan_stop null → 'stop yok' + Mark uyarı", () => {
    mockTrades.mockReturnValue({
      data: [makeTrade({ plan_stop: null })],
      isLoading: false,
      isError: false,
    });
    render(<OpenPositionsRiskPanel />);
    // "stop yok" hem pozisyon satırı hem uyarı bandı → en az 1
    expect(screen.getAllByText(/stop yok/).length).toBeGreaterThan(0);
    expect(screen.getByText(/risk first.*ihlali/)).toBeInTheDocument();
  });
});

describe("OpenPositionsRiskPanel — toplam risk + çoklu pozisyon", () => {
  it("2 pozisyon → toplam dolar risk + pozisyon sayısı", () => {
    mockTrades.mockReturnValue({
      data: [
        makeTrade({ id: 1, symbol: "AAPL", entry_price: 100, plan_stop: 95, shares: 100 }), // $500
        makeTrade({ id: 2, symbol: "NVDA", entry_price: 200, plan_stop: 190, shares: 50 }),  // $500
      ],
      isLoading: false,
      isError: false,
    });
    render(<OpenPositionsRiskPanel portfolioValue={100000} />);
    expect(screen.getByText(/2 pozisyon/)).toBeInTheDocument();
    // Toplam 500+500 = 1000, %1.00
    expect(screen.getByText(/Toplam risk %1\.00/)).toBeInTheDocument();
  });

  it("Header 'Açık Pozisyon Riski' her zaman render", () => {
    mockTrades.mockReturnValue({
      data: [makeTrade()],
      isLoading: false,
      isError: false,
    });
    render(<OpenPositionsRiskPanel />);
    expect(screen.getByText("Açık Pozisyon Riski")).toBeInTheDocument();
  });
});
