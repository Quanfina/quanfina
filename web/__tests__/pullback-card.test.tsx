/**
 * PullbackCard (Carr 2.baskı s.249 entry + s.321-324 exit) — P506 strateji inşası.
 * detected LONG (entry/stop/target/2R) + sinyal yok (SMA+Stoch bağlamı) + is_mock + loading/error.
 */
import { describe, it, expect, vi, beforeEach } from "vitest";
import { render, screen } from "@testing-library/react";
import { PullbackCard } from "@/components/stock/PullbackCard";
import type { PullbackResponse } from "@/hooks/use-pullback";

const mockPB = vi.fn<() => { data?: PullbackResponse; isLoading: boolean; isError: boolean }>();
vi.mock("@/hooks/use-pullback", () => ({ usePullback: () => mockPB() }));

beforeEach(() => mockPB.mockReset());

function pbData(over: Partial<PullbackResponse> = {}): PullbackResponse {
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
    sma20: 62.22,
    sma50: 61.91,
    sma200: 44.7,
    stoch_k: 15.7,
    mark_says: "",
    is_mock: false,
    ...over,
  };
}

describe("PullbackCard (Carr s.249 trend-takip)", () => {
  it("isLoading → 'Carr Pullback yükleniyor'", () => {
    mockPB.mockReturnValue({ data: undefined, isLoading: true, isError: false });
    render(<PullbackCard symbol="CENX" />);
    expect(screen.getByText(/Carr Pullback yükleniyor/)).toBeInTheDocument();
  });

  it("isError → null", () => {
    mockPB.mockReturnValue({ data: undefined, isLoading: false, isError: true });
    const { container } = render(<PullbackCard symbol="CENX" />);
    expect(container.firstChild).toBeNull();
  });

  it("detected=false → 'Sinyal yok' + SMA/Stoch bağlamı", () => {
    mockPB.mockReturnValue({ data: pbData(), isLoading: false, isError: false });
    render(<PullbackCard symbol="CENX" />);
    expect(screen.getByText("Sinyal yok")).toBeInTheDocument();
    expect(screen.getByText("SMA200")).toBeInTheDocument();
    expect(screen.getByText("Stoch %K")).toBeInTheDocument();
  });

  it("detected LONG → '🟢 LONG sinyal (Pullback)' + Giriş/Stop/Hedef(2R)", () => {
    mockPB.mockReturnValue({
      data: pbData({
        detected: true,
        direction: "LONG",
        quality: "GOOD",
        signal_close: 100,
        entry: 101,
        stop: 92.92,
        target: 117.16,
        risk_pct: 8.0,
        rr: 2.0,
        mark_says: "Carr Pullback LONG",
      }),
      isLoading: false,
      isError: false,
    });
    render(<PullbackCard symbol="CENX" />);
    expect(screen.getByText("🟢 LONG sinyal (Pullback)")).toBeInTheDocument();
    expect(screen.getByText("Giriş (signal high)")).toBeInTheDocument();
    expect(screen.getByText("Hedef (2R)")).toBeInTheDocument();
  });

  it("is_mock → sentetik / <200 bar banner (Kural #28)", () => {
    mockPB.mockReturnValue({ data: pbData({ is_mock: true }), isLoading: false, isError: false });
    render(<PullbackCard symbol="CENX" />);
    expect(screen.getByText(/Sentetik/)).toBeInTheDocument();
  });
});
