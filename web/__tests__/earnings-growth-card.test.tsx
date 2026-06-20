/**
 * EarningsGrowthCard (P569) — CANSLIM 'C' kazanç ivmesi (Minervini YoY ≥%25 + hızlanma).
 * both_pass (yeşil) / partial (amber) / veri-yok / hızlanma ▲ / loading-error.
 */
import { describe, it, expect, vi, beforeEach } from "vitest";
import { render, screen } from "@testing-library/react";
import { EarningsGrowthCard } from "@/components/stock/EarningsGrowthCard";
import type { EarningsGrowthResponse } from "@/hooks/use-earnings-growth";

const mockEG = vi.fn<() => { data?: EarningsGrowthResponse; isLoading: boolean; isError: boolean }>();
vi.mock("@/hooks/use-earnings-growth", () => ({ useEarningsGrowth: () => mockEG() }));

beforeEach(() => mockEG.mockReset());

function eg(over: Partial<EarningsGrowthResponse> = {}): EarningsGrowthResponse {
  return {
    symbol: "NVDA",
    available: true,
    revenue_yoy_pct: 85,
    earnings_yoy_pct: 211,
    revenue_accelerating: true,
    earnings_accelerating: true,
    both_pass: true,
    quarters_used: 5,
    mark_says: "Minervini CANSLIM-C: Satis YoY %85 ✓, Kazanc YoY %211 ✓. HIZLANIYOR. Guclu C.",
    ...over,
  };
}

describe("EarningsGrowthCard (CANSLIM C)", () => {
  it("isLoading → yükleniyor", () => {
    mockEG.mockReturnValue({ data: undefined, isLoading: true, isError: false });
    render(<EarningsGrowthCard symbol="NVDA" />);
    expect(screen.getByText(/Kazanç ivmesi yükleniyor/)).toBeInTheDocument();
  });

  it("isError → null", () => {
    mockEG.mockReturnValue({ data: undefined, isLoading: false, isError: true });
    const { container } = render(<EarningsGrowthCard symbol="NVDA" />);
    expect(container.firstChild).toBeNull();
  });

  it("both_pass → Satış/Kazanç YoY + hızlanma ▲", () => {
    mockEG.mockReturnValue({ data: eg(), isLoading: false, isError: false });
    render(<EarningsGrowthCard symbol="NVDA" />);
    expect(screen.getByText("Satış YoY")).toBeInTheDocument();
    expect(screen.getByText("%85")).toBeInTheDocument();
    expect(screen.getAllByText("▲").length).toBeGreaterThan(0); // hızlanma işareti
  });

  it("negatif baz → kazanç n/m gösterir (uydurma yok)", () => {
    mockEG.mockReturnValue({
      data: eg({ earnings_yoy_pct: null, earnings_accelerating: false, both_pass: false }),
      isLoading: false, isError: false,
    });
    render(<EarningsGrowthCard symbol="NVDA" />);
    expect(screen.getByText("n/m")).toBeInTheDocument();
  });

  it("veri yok → mark_says, metrik yok", () => {
    mockEG.mockReturnValue({
      data: eg({
        available: false, revenue_yoy_pct: null, earnings_yoy_pct: null,
        both_pass: false, revenue_accelerating: false, earnings_accelerating: false,
        mark_says: "Yetersiz çeyreklik veri (>=5 gerekir). CANSLIM-C hesaplanamadı.",
      }),
      isLoading: false, isError: false,
    });
    render(<EarningsGrowthCard symbol="NVDA" />);
    expect(screen.getByText(/Yetersiz çeyreklik veri/)).toBeInTheDocument();
    expect(screen.queryByText("Satış YoY")).toBeNull();
  });
});
