/**
 * PocketPivotCard (Minervini.md s.167 + Kacher/Morales) — P533.
 * Bazın içinde kurumsal güç dönüşü (on-deck sinyali, birincil AL değil).
 * detected GOOD (10-DMA dönüşü) / CANDIDATE (extended) / sinyal yok / is_mock / loading-error.
 */
import { describe, it, expect, vi, beforeEach } from "vitest";
import { render, screen } from "@testing-library/react";
import { PocketPivotCard } from "@/components/stock/PocketPivotCard";
import type { PocketPivotResponse } from "@/hooks/use-pocket-pivot";

const mockPP = vi.fn<() => { data?: PocketPivotResponse; isLoading: boolean; isError: boolean }>();
vi.mock("@/hooks/use-pocket-pivot", () => ({ usePocketPivot: () => mockPP() }));

beforeEach(() => mockPP.mockReset());

function ppData(over: Partial<PocketPivotResponse> = {}): PocketPivotResponse {
  return {
    detected: false,
    quality: "NONE",
    up_volume: null,
    max_down_volume_10d: null,
    volume_ratio: null,
    sma10: 122.5,
    sma50: 118.0,
    off_ten_dma: false,
    stage2: false,
    mark_says: "",
    is_mock: false,
    ...over,
  };
}

describe("PocketPivotCard (Minervini s.167 on-deck)", () => {
  it("isLoading → 'Pocket Pivot yükleniyor'", () => {
    mockPP.mockReturnValue({ data: undefined, isLoading: true, isError: false });
    render(<PocketPivotCard symbol="NVDA" />);
    expect(screen.getByText(/Pocket Pivot yükleniyor/)).toBeInTheDocument();
  });

  it("isError → null", () => {
    mockPP.mockReturnValue({ data: undefined, isLoading: false, isError: true });
    const { container } = render(<PocketPivotCard symbol="NVDA" />);
    expect(container.firstChild).toBeNull();
  });

  it("detected=false → 'Sinyal yok' + SMA bağlamı", () => {
    mockPP.mockReturnValue({ data: ppData(), isLoading: false, isError: false });
    render(<PocketPivotCard symbol="NVDA" />);
    expect(screen.getByText("Sinyal yok")).toBeInTheDocument();
    expect(screen.getByText("10-DMA")).toBeInTheDocument();
  });

  it("detected GOOD → 'Kurumsal güç' + Hacim Oranı + 10-DMA dönüşü", () => {
    mockPP.mockReturnValue({
      data: ppData({
        detected: true, quality: "GOOD", volume_ratio: 2.78,
        up_volume: 2_000_000, max_down_volume_10d: 720_000,
        off_ten_dma: true, stage2: true,
        mark_says: "GOOD Pocket Pivot — yukarı gün hacmi (Minervini s.167)",
      }),
      isLoading: false, isError: false,
    });
    render(<PocketPivotCard symbol="NVDA" />);
    expect(screen.getByText("🟢 Kurumsal güç (on-deck)")).toBeInTheDocument();
    expect(screen.getByText("Hacim Oranı")).toBeInTheDocument();
    expect(screen.getByText("2.78x")).toBeInTheDocument();
    expect(screen.getByText(/10-DMA dönüşü/)).toBeInTheDocument();
  });

  it("detected CANDIDATE → 'Aday (extended)'", () => {
    mockPP.mockReturnValue({
      data: ppData({
        detected: true, quality: "CANDIDATE", volume_ratio: 1.5,
        off_ten_dma: false, stage2: true,
        mark_says: "CANDIDATE Pocket Pivot — extended (Minervini s.167)",
      }),
      isLoading: false, isError: false,
    });
    render(<PocketPivotCard symbol="NVDA" />);
    expect(screen.getByText("🟡 Aday (extended — göz kararı)")).toBeInTheDocument();
    expect(screen.getByText(/extended \(10-DMA temassız\)/)).toBeInTheDocument();
  });

  it("is_mock → sentetik / <70 bar banner (Kural #28)", () => {
    mockPP.mockReturnValue({ data: ppData({ is_mock: true }), isLoading: false, isError: false });
    render(<PocketPivotCard symbol="NVDA" />);
    expect(screen.getByText(/Sentetik/)).toBeInTheDocument();
  });
});
