/**
 * MeanReversionCard (Carr 2.baskı s.356 countertrend) — P500 strateji inşası.
 * detected LONG/SHORT + sinyal yok (BB bağlamı) + is_mock banner + loading/error.
 */
import { describe, it, expect, vi, beforeEach } from "vitest";
import { render, screen } from "@testing-library/react";
import { MeanReversionCard } from "@/components/stock/MeanReversionCard";
import type { MeanReversionResponse } from "@/hooks/use-mean-reversion";

const mockMR = vi.fn<() => { data?: MeanReversionResponse; isLoading: boolean; isError: boolean }>();
vi.mock("@/hooks/use-mean-reversion", () => ({ useMeanReversion: () => mockMR() }));

beforeEach(() => mockMR.mockReset());

function mrData(over: Partial<MeanReversionResponse> = {}): MeanReversionResponse {
  return {
    detected: false,
    direction: null,
    quality: "NONE",
    entry: null,
    stop: null,
    target: null,
    hard_cap_pct: 8,
    time_stop_days: 7,
    sma20: 100,
    lower_bb: 90,
    upper_bb: 110,
    mark_says: "",
    is_mock: false,
    ...over,
  };
}

describe("MeanReversionCard (Carr s.356 countertrend)", () => {
  it("isLoading → 'Mean Reversion yükleniyor'", () => {
    mockMR.mockReturnValue({ data: undefined, isLoading: true, isError: false });
    render(<MeanReversionCard symbol="AAPL" />);
    expect(screen.getByText(/Mean Reversion yükleniyor/)).toBeInTheDocument();
  });

  it("isError → null", () => {
    mockMR.mockReturnValue({ data: undefined, isLoading: false, isError: true });
    const { container } = render(<MeanReversionCard symbol="AAPL" />);
    expect(container.firstChild).toBeNull();
  });

  it("detected=false → 'Sinyal yok' + BB bağlamı (Alt BB/SMA20/Üst BB)", () => {
    mockMR.mockReturnValue({ data: mrData(), isLoading: false, isError: false });
    render(<MeanReversionCard symbol="AAPL" />);
    expect(screen.getByText("Sinyal yok")).toBeInTheDocument();
    expect(screen.getByText("Alt BB")).toBeInTheDocument();
    expect(screen.getByText("SMA20")).toBeInTheDocument();
    expect(screen.getByText("Üst BB")).toBeInTheDocument();
  });

  it("detected LONG → '🟢 LONG sinyal' + Giriş/Hedef metrik", () => {
    mockMR.mockReturnValue({
      data: mrData({
        detected: true, direction: "LONG", quality: "GOOD",
        entry: 74, stop: 68.08, target: 96.7, mark_says: "Mean Reversion LONG sinyal",
      }),
      isLoading: false, isError: false,
    });
    render(<MeanReversionCard symbol="AAPL" />);
    expect(screen.getByText("🟢 LONG sinyal")).toBeInTheDocument();
    expect(screen.getByText("Giriş")).toBeInTheDocument();
    expect(screen.getByText("Hedef (SMA20)")).toBeInTheDocument();
  });

  it("detected SHORT → '🔴 SHORT sinyal'", () => {
    mockMR.mockReturnValue({
      data: mrData({
        detected: true, direction: "SHORT", quality: "GOOD",
        entry: 115, stop: 119.77, target: 101.55, mark_says: "MR SHORT",
      }),
      isLoading: false, isError: false,
    });
    render(<MeanReversionCard symbol="AAPL" />);
    expect(screen.getByText("🔴 SHORT sinyal")).toBeInTheDocument();
  });

  it("is_mock → sentetik veri banner (Kural #28)", () => {
    mockMR.mockReturnValue({ data: mrData({ is_mock: true }), isLoading: false, isError: false });
    render(<MeanReversionCard symbol="AAPL" />);
    expect(screen.getByText(/Sentetik veri/)).toBeInTheDocument();
  });
});
