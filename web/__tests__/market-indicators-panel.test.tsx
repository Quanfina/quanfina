/**
 * P425 (31 May 2026): MarketIndicatorsPanel Vitest — Faber/McClellan/Zweig render
 * + (?) tooltip + Kural #28 "veri birikiyor" gating (MOCK sayı YOK).
 */
import { describe, it, expect, vi } from "vitest";
import { render, screen } from "@testing-library/react";
import { MarketIndicatorsPanel } from "@/components/market/MarketIndicatorsPanel";
import type { ExtraIndicators } from "@/hooks/use-extra-indicators";

let mockData: ExtraIndicators | undefined;
let mockState = { isLoading: false, isError: false };

vi.mock("@/hooks/use-extra-indicators", () => ({
  useExtraIndicators: () => ({ data: mockData, ...mockState }),
}));

function setData(d: ExtraIndicators | undefined) {
  mockData = d;
}

const FULL: ExtraIndicators = {
  faber: { data_sufficient: true, signal: "INVESTED", price: 739.22, sma_10mo: 682.1, pct_vs_sma: 8.4, days_needed: 210, days_have: 251, says: "" },
  mcclellan: { data_sufficient: false, value: null, signal: null, days_needed: 39, days_have: 11, says: "" },
  zweig: { data_sufficient: true, ema_ratio: 0.536, thrust_active: false, zone: "NEUTRAL", days_needed: 10, days_have: 11, says: "" },
  breadth_source: "scans",
};

// P426: backfill ile McClellan canlı senaryo
const BACKFILL: ExtraIndicators = {
  faber: FULL.faber,
  mcclellan: { data_sufficient: true, value: -2.3, signal: "BEARISH", days_needed: 39, days_have: 64, says: "" },
  zweig: { data_sufficient: true, ema_ratio: 0.546, thrust_active: false, zone: "NEUTRAL", days_needed: 10, days_have: 64, says: "" },
  breadth_source: "backfill",
};

describe("MarketIndicatorsPanel", () => {
  it("3 gösterge başlığı render (Faber/McClellan/Zweig)", () => {
    setData(FULL);
    render(<MarketIndicatorsPanel />);
    expect(screen.getByText("Faber 10-Ay SMA")).toBeInTheDocument();
    expect(screen.getByText("McClellan Osilatör")).toBeInTheDocument();
    expect(screen.getByText("Zweig Breadth Thrust")).toBeInTheDocument();
  });

  it("Faber data_sufficient=true → YATIRIMLI chip + fiyat", () => {
    setData(FULL);
    render(<MarketIndicatorsPanel />);
    expect(screen.getByText("YATIRIMLI")).toBeInTheDocument();
    expect(screen.getByText(/739\.22/)).toBeInTheDocument();
  });

  it("McClellan data_sufficient=false → 'veri birikiyor (11/39 gün)' (MOCK sayı YOK)", () => {
    setData(FULL);
    render(<MarketIndicatorsPanel />);
    expect(screen.getByText(/veri birikiyor \(11\/39 gün\)/)).toBeInTheDocument();
    // MOCK bir McClellan değeri görünmemeli
    expect(screen.queryByText(/POZİTİF|NEGATİF/)).not.toBeInTheDocument();
  });

  it("Zweig NEUTRAL zone → NÖTR chip + ema_ratio", () => {
    setData(FULL);
    render(<MarketIndicatorsPanel />);
    expect(screen.getByText("NÖTR")).toBeInTheDocument();
    expect(screen.getByText(/0\.536/)).toBeInTheDocument();
  });

  it("(?) tooltip tetikleyicileri var (her gösterge için help-tip)", () => {
    setData(FULL);
    render(<MarketIndicatorsPanel />);
    const tips = screen.getAllByTestId("help-tip");
    expect(tips.length).toBe(3);
  });

  it("Kural #28 dürüstlük notu (NAAIM/AAII MOCK eklenmedi) görünür", () => {
    setData(FULL);
    render(<MarketIndicatorsPanel />);
    expect(screen.getByText(/MOCK sayı gösterilmez/)).toBeInTheDocument();
  });

  it("P426 backfill → McClellan canlı (POZİTİF/NEGATİF chip) + backfill footnote", () => {
    setData(BACKFILL);
    render(<MarketIndicatorsPanel />);
    // McClellan artık değer gösterir (gating yok)
    expect(screen.getByText(/-2\.3 · NEGATİF/)).toBeInTheDocument();
    expect(screen.queryByText(/veri birikiyor/)).not.toBeInTheDocument();
    // Footnote backfill kaynağını açıklar
    expect(screen.getByText(/backfill/)).toBeInTheDocument();
  });
});
