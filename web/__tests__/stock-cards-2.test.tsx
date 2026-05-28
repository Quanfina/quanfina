/**
 * Stock detay kartları grup 2: PivotBreakoutCard + OverheadSupplyCard + ClimaxRunCard.
 *
 * Mark TLSMW Ch 9-10 canon — pivot kırılım / overhead direnç / climax exhaustion.
 */
import { describe, it, expect, vi, beforeEach } from "vitest";
import { render, screen } from "@testing-library/react";
import { PivotBreakoutCard } from "@/components/stock/PivotBreakoutCard";
import { OverheadSupplyCard } from "@/components/stock/OverheadSupplyCard";
import { ClimaxRunCard } from "@/components/stock/ClimaxRunCard";
import type { PivotBreakoutInfo } from "@/hooks/use-pivot-breakout";
import type { OverheadSupplyInfo } from "@/hooks/use-overhead-supply";
import type { ClimaxRunInfo } from "@/hooks/use-climax-run";

const mockPivot = vi.fn<() => { data?: PivotBreakoutInfo; isLoading: boolean; isError: boolean }>();
const mockOverhead = vi.fn<() => { data?: OverheadSupplyInfo; isLoading: boolean; isError: boolean }>();
const mockClimax = vi.fn<() => { data?: ClimaxRunInfo; isLoading: boolean; isError: boolean }>();

vi.mock("@/hooks/use-pivot-breakout", () => ({ usePivotBreakout: () => mockPivot() }));
vi.mock("@/hooks/use-overhead-supply", () => ({ useOverheadSupply: () => mockOverhead() }));
vi.mock("@/hooks/use-climax-run", () => ({ useClimaxRun: () => mockClimax() }));

beforeEach(() => {
  mockPivot.mockReset();
  mockOverhead.mockReset();
  mockClimax.mockReset();
});

describe("PivotBreakoutCard — 4 durum (Mark TLSMW Ch 10)", () => {
  function pivotData(status: PivotBreakoutInfo["status"]): PivotBreakoutInfo {
    return {
      status,
      pivot_price: 150,
      current_price: 152,
      breakout_pct: 1.3,
      volume_multiplier: 1.8,
      volume_confirmed: true,
      mark_says: `${status} Mark felsefe`,
    };
  }

  it("isLoading → 'Pivot Breakout yükleniyor...'", () => {
    mockPivot.mockReturnValue({ data: undefined, isLoading: true, isError: false });
    render(<PivotBreakoutCard symbol="AAPL" />);
    expect(screen.getByText(/Pivot Breakout yükleniyor/)).toBeInTheDocument();
  });

  it("isError → null", () => {
    mockPivot.mockReturnValue({ data: undefined, isLoading: false, isError: true });
    const { container } = render(<PivotBreakoutCard symbol="AAPL" />);
    expect(container.firstChild).toBeNull();
  });

  it("status=null → null", () => {
    mockPivot.mockReturnValue({
      data: { ...pivotData("CONFIRMED"), status: null },
      isLoading: false,
      isError: false,
    });
    const { container } = render(<PivotBreakoutCard symbol="AAPL" />);
    expect(container.firstChild).toBeNull();
  });

  it.each<[PivotBreakoutInfo["status"], string]>([
    ["CONFIRMED", "AL Sinyali"],
    ["WEAK", "Zayıf Kırılım"],
    ["NEAR_PIVOT", "Yakın İzleme"],
    ["BELOW_PIVOT", "Pivot Altı"],
  ])("status=%s → '%s' rozet + Pivot/Sapma/Hacim metrik", (status, label) => {
    mockPivot.mockReturnValue({ data: pivotData(status), isLoading: false, isError: false });
    render(<PivotBreakoutCard symbol="AAPL" />);
    expect(screen.getByText(label)).toBeInTheDocument();
    expect(screen.getByText("Pivot")).toBeInTheDocument();
    expect(screen.getByText("Hacim")).toBeInTheDocument();
  });
});

describe("OverheadSupplyCard — 3 kategori (loading=null, skeleton yok)", () => {
  function overheadData(category: OverheadSupplyInfo["category"]): OverheadSupplyInfo {
    return {
      category,
      overhead_price: 160,
      drop_pct: -12,
      proximity_pct: 5,
      mark_says: `${category} Mark felsefe`,
    };
  }

  it("isLoading → null (skeleton yok)", () => {
    mockOverhead.mockReturnValue({ data: undefined, isLoading: true, isError: false });
    const { container } = render(<OverheadSupplyCard symbol="AAPL" />);
    expect(container.firstChild).toBeNull();
  });

  it("category=null → null", () => {
    mockOverhead.mockReturnValue({
      data: { ...overheadData("HEAVY"), category: null },
      isLoading: false,
      isError: false,
    });
    const { container } = render(<OverheadSupplyCard symbol="AAPL" />);
    expect(container.firstChild).toBeNull();
  });

  it.each<[OverheadSupplyInfo["category"], string]>([
    ["HEAVY", "Ağır Direnç"],
    ["MODERATE", "Orta Direnç"],
    ["NONE", "Temiz"],
  ])("category=%s → '%s' label", (category, label) => {
    mockOverhead.mockReturnValue({ data: overheadData(category), isLoading: false, isError: false });
    render(<OverheadSupplyCard symbol="AAPL" />);
    expect(screen.getByText(label)).toBeInTheDocument();
  });
});

describe("ClimaxRunCard — 4 kategori (Mark TLSMW Ch 9)", () => {
  function climaxData(category: ClimaxRunInfo["category"]): ClimaxRunInfo {
    return {
      category,
      gain_pct: 85,
      gap_up_days: 3,
      avg_volume_ratio: 2.5,
      mark_says: `${category} Mark felsefe`,
    };
  }

  it("isLoading → 'Climax Run yükleniyor...'", () => {
    mockClimax.mockReturnValue({ data: undefined, isLoading: true, isError: false });
    render(<ClimaxRunCard symbol="AAPL" />);
    expect(screen.getByText(/Climax Run yükleniyor/)).toBeInTheDocument();
  });

  it("isError → null", () => {
    mockClimax.mockReturnValue({ data: undefined, isLoading: false, isError: true });
    const { container } = render(<ClimaxRunCard symbol="AAPL" />);
    expect(container.firstChild).toBeNull();
  });

  it("category=null → null", () => {
    mockClimax.mockReturnValue({
      data: { ...climaxData("CLIMAX_TOP"), category: null },
      isLoading: false,
      isError: false,
    });
    const { container } = render(<ClimaxRunCard symbol="AAPL" />);
    expect(container.firstChild).toBeNull();
  });

  it.each<[NonNullable<ClimaxRunInfo["category"]>, string]>([
    ["CLIMAX_TOP", "SAT/Çıkış"],
    ["POTENTIAL_CLIMAX", "Dikkat"],
    ["HEALTHY_ADVANCE", "Sağlıklı Trend"],
    ["NONE", "Trend Yok"],
  ])("category=%s → '%s' label", (category, label) => {
    mockClimax.mockReturnValue({ data: climaxData(category), isLoading: false, isError: false });
    render(<ClimaxRunCard symbol="AAPL" />);
    expect(screen.getByText(label)).toBeInTheDocument();
  });
});
