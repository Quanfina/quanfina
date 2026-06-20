/**
 * PullbackReliefCard (P570) — Carr Pullback/Relief Rally oranı (s.280).
 * BULLISH_OVERSOLD (yeşil) / BEARISH_OVERBOUGHT (kırmızı) / NEUTRAL / relief=0 / veri-yok.
 */
import { describe, it, expect, vi, beforeEach } from "vitest";
import { render, screen } from "@testing-library/react";
import { PullbackReliefCard } from "@/components/market/PullbackReliefCard";
import type { PullbackReliefRatioResponse } from "@/hooks/use-pullback-relief-ratio";

const mockPR = vi.fn<() => { data?: PullbackReliefRatioResponse; isLoading: boolean; isError: boolean }>();
vi.mock("@/hooks/use-pullback-relief-ratio", () => ({ usePullbackReliefRatio: () => mockPR() }));

beforeEach(() => mockPR.mockReset());

function pr(over: Partial<PullbackReliefRatioResponse> = {}): PullbackReliefRatioResponse {
  return {
    available: true, ratio: 1.5, pullback_count: 30, relief_count: 20,
    direction: "rising", regime: "BULLISH_OVERSOLD", scan_date: "2026-06-20",
    series: [], mark_says: "Oran 1.5 (>1, yükseliyor) — boğa rejimi, buyable dip.",
    ...over,
  };
}

describe("PullbackReliefCard (Carr s.280)", () => {
  it("isLoading → yükleniyor", () => {
    mockPR.mockReturnValue({ data: undefined, isLoading: true, isError: false });
    render(<PullbackReliefCard />);
    expect(screen.getByText(/oranı yükleniyor/)).toBeInTheDocument();
  });

  it("BULLISH_OVERSOLD → oran + AL dip + sayımlar", () => {
    mockPR.mockReturnValue({ data: pr(), isLoading: false, isError: false });
    render(<PullbackReliefCard />);
    expect(screen.getByText("1.50")).toBeInTheDocument();
    expect(screen.getByText(/AL dip/)).toBeInTheDocument();
    expect(screen.getByText("30")).toBeInTheDocument(); // pullback
    expect(screen.getByText("20")).toBeInTheDocument(); // relief
  });

  it("BEARISH_OVERBOUGHT → SHORT tepki", () => {
    mockPR.mockReturnValue({
      data: pr({ ratio: 0.6, regime: "BEARISH_OVERBOUGHT", direction: "falling", pullback_count: 12 }),
      isLoading: false, isError: false,
    });
    render(<PullbackReliefCard />);
    expect(screen.getByText("0.60")).toBeInTheDocument();
    expect(screen.getByText(/SHORT tepki/)).toBeInTheDocument();
  });

  it("relief=0 (payda) → ratio '—' gösterir", () => {
    mockPR.mockReturnValue({
      data: pr({ ratio: null, relief_count: 0, regime: "NEUTRAL", direction: "flat",
                 mark_says: "Relief Rally adayı yok (payda 0). Pullback 22 — aşırı boğa." }),
      isLoading: false, isError: false,
    });
    render(<PullbackReliefCard />);
    expect(screen.getByText("—")).toBeInTheDocument();
    expect(screen.getByText(/aşırı boğa/)).toBeInTheDocument();
  });

  it("available=false → veri yok", () => {
    mockPR.mockReturnValue({
      data: pr({ available: false }), isLoading: false, isError: false,
    });
    render(<PullbackReliefCard />);
    expect(screen.getByText(/veri yok/)).toBeInTheDocument();
  });
});
