/**
 * BlueSeaCard (Carr 2.baskı s.289) — P518 strateji inşası (SHORT).
 * detected SHORT (entry=signal low + stop üstte/target aşağı) + sinyal yok (40g/52h bağlamı) +
 * is_mock + loading/error.
 */
import { describe, it, expect, vi, beforeEach } from "vitest";
import { render, screen } from "@testing-library/react";
import { BlueSeaCard } from "@/components/stock/BlueSeaCard";
import type { BlueSeaResponse } from "@/hooks/use-blue-sea";

const mockBS = vi.fn<() => { data?: BlueSeaResponse; isLoading: boolean; isError: boolean }>();
vi.mock("@/hooks/use-blue-sea", () => ({ useBlueSea: () => mockBS() }));

beforeEach(() => mockBS.mockReset());

function bsData(over: Partial<BlueSeaResponse> = {}): BlueSeaResponse {
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
    low_40d: 250,
    low_260d: 124,
    high_260d: 300,
    obv: -1000,
    macd: -2.5,
    mark_says: "",
    is_mock: false,
    ...over,
  };
}

describe("BlueSeaCard (Carr s.289 SHORT)", () => {
  it("isLoading → 'Blue Sea Breakdown yükleniyor'", () => {
    mockBS.mockReturnValue({ data: undefined, isLoading: true, isError: false });
    render(<BlueSeaCard symbol="SLB" />);
    expect(screen.getByText(/Blue Sea Breakdown yükleniyor/)).toBeInTheDocument();
  });

  it("isError → null", () => {
    mockBS.mockReturnValue({ data: undefined, isLoading: false, isError: true });
    const { container } = render(<BlueSeaCard symbol="SLB" />);
    expect(container.firstChild).toBeNull();
  });

  it("detected=false → 'Sinyal yok' + 40g/52h bağlamı", () => {
    mockBS.mockReturnValue({ data: bsData(), isLoading: false, isError: false });
    render(<BlueSeaCard symbol="SLB" />);
    expect(screen.getByText("Sinyal yok")).toBeInTheDocument();
    expect(screen.getByText("40g Düşük")).toBeInTheDocument();
    expect(screen.getByText("52h Zirve")).toBeInTheDocument();
  });

  it("detected SHORT → '🔴 SHORT sinyal (Blue Sea)' + Giriş(signal low)/Hedef(aşağı)", () => {
    mockBS.mockReturnValue({
      data: bsData({
        detected: true,
        direction: "SHORT",
        quality: "GOOD",
        signal_close: 250,
        entry: 249.5,
        stop: 264.47,
        target: 219.56,
        risk_pct: 6.0,
        rr: 2.0,
        mark_says: "Blue Sea Breakdown SHORT",
      }),
      isLoading: false,
      isError: false,
    });
    render(<BlueSeaCard symbol="SLB" />);
    expect(screen.getByText("🔴 SHORT sinyal (Blue Sea)")).toBeInTheDocument();
    expect(screen.getByText("Giriş (signal low)")).toBeInTheDocument();
    expect(screen.getByText("Hedef (2R aşağı)")).toBeInTheDocument();
  });

  it("is_mock → sentetik / <261 bar banner (Kural #28)", () => {
    mockBS.mockReturnValue({ data: bsData({ is_mock: true }), isLoading: false, isError: false });
    render(<BlueSeaCard symbol="SLB" />);
    expect(screen.getByText(/Sentetik/)).toBeInTheDocument();
  });
});
