/**
 * Stock detay kartları grup 1: CarrStageCard + RsRatingCard.
 *
 * Ortak pattern: useX hook → isLoading skeleton / isError|null → null / data → render.
 */
import { describe, it, expect, vi, beforeEach } from "vitest";
import { render, screen } from "@testing-library/react";
import { CarrStageCard } from "@/components/stock/CarrStageCard";
import { RsRatingCard } from "@/components/stock/RsRatingCard";
import type { CarrStageResponse } from "@/hooks/use-carr-stage";
import type { RsRatingInfo } from "@/hooks/use-rs-rating";

const mockCarr = vi.fn<() => { data?: CarrStageResponse; isLoading: boolean; isError: boolean }>();
const mockRs = vi.fn<() => { data?: RsRatingInfo; isLoading: boolean; isError: boolean }>();

vi.mock("@/hooks/use-carr-stage", () => ({
  useCarrStage: () => mockCarr(),
}));
vi.mock("@/hooks/use-rs-rating", () => ({
  useRsRating: () => mockRs(),
}));

beforeEach(() => {
  mockCarr.mockReset();
  mockRs.mockReset();
});

describe("CarrStageCard — 4 stage + loading/error/null", () => {
  function carrData(stage: 1 | 2 | 3 | 4 | null): CarrStageResponse {
    const labels: Record<string, string> = {
      "1": "Stage 1 (Basing)", "2": "Stage 2 (Advancing)",
      "3": "Stage 3 (Topping)", "4": "Stage 4 (Declining)",
    };
    return {
      symbol: "AAPL",
      stage,
      stage_label: stage ? labels[String(stage)] : "Belirsiz",
      ma_value: 150,
      price_vs_ma_pct: stage === 2 ? 5 : -8,
      slope_pct_per_year: stage === 2 ? 12 : -5,
      mark_says: `Stage ${stage} Mark felsefe`,
      ma_window: 150,
    };
  }

  it("isLoading → 'Carr Stage yükleniyor...'", () => {
    mockCarr.mockReturnValue({ data: undefined, isLoading: true, isError: false });
    render(<CarrStageCard symbol="AAPL" />);
    expect(screen.getByText(/Carr Stage yükleniyor/)).toBeInTheDocument();
  });

  it("isError → null", () => {
    mockCarr.mockReturnValue({ data: undefined, isLoading: false, isError: true });
    const { container } = render(<CarrStageCard symbol="AAPL" />);
    expect(container.firstChild).toBeNull();
  });

  it("stage=null → 'Belirsiz' fallback satırı", () => {
    mockCarr.mockReturnValue({ data: carrData(null), isLoading: false, isError: false });
    render(<CarrStageCard symbol="AAPL" />);
    expect(screen.getByText(/Belirsiz/)).toBeInTheDocument();
  });

  it.each<[1 | 2 | 3 | 4, string]>([
    [1, "Basing"],
    [2, "Advancing"],
    [3, "Topping"],
    [4, "Declining"],
  ])("stage=%i → 'Stage %i — %s' label + metrikler", (stage, label) => {
    mockCarr.mockReturnValue({ data: carrData(stage), isLoading: false, isError: false });
    render(<CarrStageCard symbol="AAPL" />);
    expect(screen.getByText(new RegExp(`Stage ${stage} — ${label}`))).toBeInTheDocument();
    expect(screen.getByText("30W MA")).toBeInTheDocument();
    expect(screen.getByText("Yıllık Eğim")).toBeInTheDocument();
  });
});

describe("RsRatingCard — 4 kategori + loading/error/null", () => {
  function rsData(category: RsRatingInfo["category"], rs: number): RsRatingInfo {
    return {
      rs_rating: rs,
      category,
      stock_return_pct: 45,
      benchmark_return_pct: 12,
      outperform_pct: 33,
      mark_says: `${category} Mark felsefe`,
    };
  }

  it("isLoading → 'RS Rating yükleniyor...'", () => {
    mockRs.mockReturnValue({ data: undefined, isLoading: true, isError: false });
    render(<RsRatingCard symbol="AAPL" />);
    expect(screen.getByText(/RS Rating yükleniyor/)).toBeInTheDocument();
  });

  it("isError → null", () => {
    mockRs.mockReturnValue({ data: undefined, isLoading: false, isError: true });
    const { container } = render(<RsRatingCard symbol="AAPL" />);
    expect(container.firstChild).toBeNull();
  });

  it("category=null → null (yetersiz veri)", () => {
    mockRs.mockReturnValue({
      data: { ...rsData("LEADER", 0), category: null, rs_rating: null },
      isLoading: false,
      isError: false,
    });
    const { container } = render(<RsRatingCard symbol="AAPL" />);
    expect(container.firstChild).toBeNull();
  });

  it.each<[RsRatingInfo["category"], number, string]>([
    ["LEADER", 92, "IBD LEADER"],
    ["STRONG", 75, "STRONG"],
    ["AVERAGE", 60, "AVERAGE"],
    ["LAGGARD", 30, "LAGGARD"],
  ])("category=%s (RS=%i) → '%s' rozet + RS rakam", (category, rs, label) => {
    mockRs.mockReturnValue({ data: rsData(category, rs), isLoading: false, isError: false });
    render(<RsRatingCard symbol="AAPL" />);
    expect(screen.getByText(label)).toBeInTheDocument();
    expect(screen.getByText(String(rs))).toBeInTheDocument();
  });

  it("LEADER → Hisse/SPY/Outperform 3 metrik kolon", () => {
    mockRs.mockReturnValue({ data: rsData("LEADER", 92), isLoading: false, isError: false });
    render(<RsRatingCard symbol="AAPL" />);
    expect(screen.getByText("Hisse")).toBeInTheDocument();
    expect(screen.getByText("SPY")).toBeInTheDocument();
    expect(screen.getByText("Outperform")).toBeInTheDocument();
  });
});
