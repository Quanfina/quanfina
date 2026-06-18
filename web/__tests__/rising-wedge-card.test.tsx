/**
 * RisingWedgeCard (Carr 2.baskı Böl.19) — P522 (SHORT, son Carr setup).
 * detected SHORT CANDIDATE (entry=close + OBV eyeball) + aday yok (SMA/MACD/OBV) + is_mock + loading/error.
 */
import { describe, it, expect, vi, beforeEach } from "vitest";
import { render, screen } from "@testing-library/react";
import { RisingWedgeCard } from "@/components/stock/RisingWedgeCard";
import type { RisingWedgeResponse } from "@/hooks/use-rising-wedge";

const mockRW = vi.fn<() => { data?: RisingWedgeResponse; isLoading: boolean; isError: boolean }>();
vi.mock("@/hooks/use-rising-wedge", () => ({ useRisingWedge: () => mockRW() }));

beforeEach(() => mockRW.mockReset());

function rwData(over: Partial<RisingWedgeResponse> = {}): RisingWedgeResponse {
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
    sma50: 165,
    obv: 50000,
    macd: -1.0,
    eyeball_checks: [],
    mark_says: "",
    is_mock: false,
    ...over,
  };
}

describe("RisingWedgeCard (Carr Böl.19 SHORT)", () => {
  it("isLoading → 'Rising Wedge yükleniyor'", () => {
    mockRW.mockReturnValue({ data: undefined, isLoading: true, isError: false });
    render(<RisingWedgeCard symbol="VICR" />);
    expect(screen.getByText(/Rising Wedge yükleniyor/)).toBeInTheDocument();
  });

  it("isError → null", () => {
    mockRW.mockReturnValue({ data: undefined, isLoading: false, isError: true });
    const { container } = render(<RisingWedgeCard symbol="VICR" />);
    expect(container.firstChild).toBeNull();
  });

  it("detected=false → 'Aday yok' + SMA/MACD/OBV bağlamı", () => {
    mockRW.mockReturnValue({ data: rwData(), isLoading: false, isError: false });
    render(<RisingWedgeCard symbol="VICR" />);
    expect(screen.getByText("Aday yok")).toBeInTheDocument();
    expect(screen.getByText("SMA50")).toBeInTheDocument();
    expect(screen.getByText("MACD")).toBeInTheDocument();
  });

  it("detected SHORT → '🔴 SHORT ADAYI' + Giriş(close) + OBV eyeball", () => {
    mockRW.mockReturnValue({
      data: rwData({
        detected: true,
        direction: "SHORT",
        quality: "CANDIDATE",
        signal_close: 170,
        entry: 170,
        stop: 180.2,
        target: 149.6,
        risk_pct: 6.0,
        rr: 2.0,
        eyeball_checks: [
          "Kama trendline: destek+direnç ≥3 dokunuş",
          "Tetik: OBV trendline aşağı kırılımı → ilk kırmızı mum",
        ],
        mark_says: "Rising Wedge Breakdown SHORT ADAYI",
      }),
      isLoading: false,
      isError: false,
    });
    render(<RisingWedgeCard symbol="VICR" />);
    expect(screen.getByText("🔴 SHORT ADAYI (göz kararı şart)")).toBeInTheDocument();
    expect(screen.getByText("Giriş (close)")).toBeInTheDocument();
    expect(screen.getByText(/OBV trendline/)).toBeInTheDocument();
  });

  it("is_mock → sentetik / <90 bar banner (Kural #28)", () => {
    mockRW.mockReturnValue({ data: rwData({ is_mock: true }), isLoading: false, isError: false });
    render(<RisingWedgeCard symbol="VICR" />);
    expect(screen.getByText(/Sentetik/)).toBeInTheDocument();
  });
});
