/**
 * BullishDivergenceCard (Carr 2.baskı s.258) — P515 strateji inşası.
 * uptrend-dip 2+ gösterge divergence ADAY. detected CANDIDATE (entry=close + diverge listesi) +
 * aday yok (SMA + diverge sayısı) + is_mock + loading/error.
 */
import { describe, it, expect, vi, beforeEach } from "vitest";
import { render, screen } from "@testing-library/react";
import { BullishDivergenceCard } from "@/components/stock/BullishDivergenceCard";
import type { BullishDivergenceResponse } from "@/hooks/use-bullish-divergence";

const mockBD = vi.fn<() => { data?: BullishDivergenceResponse; isLoading: boolean; isError: boolean }>();
vi.mock("@/hooks/use-bullish-divergence", () => ({ useBullishDivergence: () => mockBD() }));

beforeEach(() => mockBD.mockReset());

function bdData(over: Partial<BullishDivergenceResponse> = {}): BullishDivergenceResponse {
  return {
    detected: false,
    direction: null,
    quality: "NONE",
    signal_close: null,
    entry: null,
    stop: null,
    target: null,
    risk_pct: null,
    rr: null,
    sma50: 288,
    sma200: 267,
    divergence_count: 0,
    divergence_indicators: [],
    eyeball_checks: [],
    mark_says: "",
    is_mock: false,
    ...over,
  };
}

describe("BullishDivergenceCard (Carr s.258 uptrend-dip)", () => {
  it("isLoading → 'Bullish Divergence yükleniyor'", () => {
    mockBD.mockReturnValue({ data: undefined, isLoading: true, isError: false });
    render(<BullishDivergenceCard symbol="NVDA" />);
    expect(screen.getByText(/Bullish Divergence yükleniyor/)).toBeInTheDocument();
  });

  it("isError → null", () => {
    mockBD.mockReturnValue({ data: undefined, isLoading: false, isError: true });
    const { container } = render(<BullishDivergenceCard symbol="NVDA" />);
    expect(container.firstChild).toBeNull();
  });

  it("detected=false → 'Aday yok' + Diverge sayısı bağlamı", () => {
    mockBD.mockReturnValue({ data: bdData({ divergence_count: 3 }), isLoading: false, isError: false });
    render(<BullishDivergenceCard symbol="NVDA" />);
    expect(screen.getByText("Aday yok")).toBeInTheDocument();
    expect(screen.getByText("Diverge")).toBeInTheDocument();
    expect(screen.getByText("3/6")).toBeInTheDocument();
  });

  it("detected CANDIDATE → 'LONG ADAYI' + Giriş(close) + eyeball checklist", () => {
    mockBD.mockReturnValue({
      data: bdData({
        detected: true,
        direction: "LONG",
        quality: "CANDIDATE",
        signal_close: 285,
        entry: 285,
        stop: 281.5,
        target: 292,
        risk_pct: 1.23,
        rr: 2.0,
        divergence_count: 4,
        divergence_indicators: ["MACD line", "RSI(5)", "Stochastics %K", "CCI(20)"],
        eyeball_checks: [
          "Divergence gözle teyit: fiyat lower low + gösterge higher low",
          "Diverge gösterge (4): MACD line, RSI(5), Stochastics %K, CCI(20)",
          "Dipler arası en az 5 işlem günü",
        ],
        mark_says: "Bullish Divergence LONG ADAYI",
      }),
      isLoading: false,
      isError: false,
    });
    render(<BullishDivergenceCard symbol="NVDA" />);
    expect(screen.getByText("🟡 LONG ADAYI (göz kararı şart)")).toBeInTheDocument();
    expect(screen.getByText("Giriş (close)")).toBeInTheDocument();
    expect(screen.getByText(/Divergence gözle teyit/)).toBeInTheDocument();
  });

  it("is_mock → sentetik / <200 bar banner (Kural #28)", () => {
    mockBD.mockReturnValue({ data: bdData({ is_mock: true }), isLoading: false, isError: false });
    render(<BullishDivergenceCard symbol="NVDA" />);
    expect(screen.getByText(/Sentetik/)).toBeInTheDocument();
  });
});
