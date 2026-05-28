/**
 * Stock detay kartları grup 4: RelativeVolumeCard + BreakoutQualityCard.
 *
 * Mark TLSMW Ch 6/10 — bağıl hacim (V-Dry/Surge) + kırılım kalite skoru.
 */
import { describe, it, expect, vi, beforeEach } from "vitest";
import { render, screen } from "@testing-library/react";
import { RelativeVolumeCard } from "@/components/stock/RelativeVolumeCard";
import { BreakoutQualityCard } from "@/components/stock/BreakoutQualityCard";
import type { RelativeVolumeInfo, RelativeVolumeCategory } from "@/hooks/use-relative-volume";
import type { BreakoutQualityInfo, BreakoutQualityCategory } from "@/hooks/use-breakout-quality";

const mockRelVol = vi.fn<() => { data?: RelativeVolumeInfo; isLoading: boolean; isError: boolean }>();
const mockBQ = vi.fn<() => { data?: BreakoutQualityInfo; isLoading: boolean; isError: boolean }>();

vi.mock("@/hooks/use-relative-volume", () => ({ useRelativeVolume: () => mockRelVol() }));
vi.mock("@/hooks/use-breakout-quality", () => ({ useBreakoutQuality: () => mockBQ() }));

beforeEach(() => {
  mockRelVol.mockReset();
  mockBQ.mockReset();
});

describe("RelativeVolumeCard — 5 kategori (Mark TLSMW Ch 6)", () => {
  function relVolData(category: RelativeVolumeCategory): RelativeVolumeInfo {
    return {
      rel_vol: 1.5,
      today_volume: 60_000_000,
      avg_volume: 40_000_000,
      category,
      mark_says: `${category} Mark felsefe`,
    };
  }

  it("isLoading → 'Hacim oranı yükleniyor...'", () => {
    mockRelVol.mockReturnValue({ data: undefined, isLoading: true, isError: false });
    render(<RelativeVolumeCard symbol="AAPL" />);
    expect(screen.getByText(/Hacim oranı yükleniyor/)).toBeInTheDocument();
  });

  it("isError → null", () => {
    mockRelVol.mockReturnValue({ data: undefined, isLoading: false, isError: true });
    const { container } = render(<RelativeVolumeCard symbol="AAPL" />);
    expect(container.firstChild).toBeNull();
  });

  it("category=null → null", () => {
    mockRelVol.mockReturnValue({
      data: { ...relVolData("NORMAL"), category: null },
      isLoading: false,
      isError: false,
    });
    const { container } = render(<RelativeVolumeCard symbol="AAPL" />);
    expect(container.firstChild).toBeNull();
  });

  it.each<[RelativeVolumeCategory, string]>([
    ["DRYING", "Kuruyor"],
    ["QUIET", "Sessiz"],
    ["NORMAL", "Normal"],
    ["HIGH", "Yüksek"],
    ["SURGE", "Patlama"],
  ])("category=%s → '%s' label", (category, label) => {
    mockRelVol.mockReturnValue({ data: relVolData(category), isLoading: false, isError: false });
    render(<RelativeVolumeCard symbol="AAPL" />);
    expect(screen.getByText(label)).toBeInTheDocument();
  });
});

describe("BreakoutQualityCard — 4 kategori + skor (Mark TLSMW Ch 10)", () => {
  function bqData(category: BreakoutQualityCategory, score: number): BreakoutQualityInfo {
    return {
      score,
      category,
      breakdown: {
        volume: 28,
        gap_up: 18,
        breakout_strength: 22,
        prior_contraction: 12,
        overhead_clean: 8,
      },
      mark_says: `${category} Mark felsefe`,
    };
  }

  it("isLoading → 'Kırılım kalitesi yükleniyor...'", () => {
    mockBQ.mockReturnValue({ data: undefined, isLoading: true, isError: false });
    render(<BreakoutQualityCard symbol="AAPL" />);
    expect(screen.getByText(/Kırılım kalitesi yükleniyor/)).toBeInTheDocument();
  });

  it("isError → null", () => {
    mockBQ.mockReturnValue({ data: undefined, isLoading: false, isError: true });
    const { container } = render(<BreakoutQualityCard symbol="AAPL" />);
    expect(container.firstChild).toBeNull();
  });

  it("category=null → null", () => {
    mockBQ.mockReturnValue({
      data: { ...bqData("GOOD", 70), category: null },
      isLoading: false,
      isError: false,
    });
    const { container } = render(<BreakoutQualityCard symbol="AAPL" />);
    expect(container.firstChild).toBeNull();
  });

  it.each<[BreakoutQualityCategory, number, string]>([
    ["EXCELLENT", 95, "A++ Kırılım"],
    ["GOOD", 75, "Standart"],
    ["MARGINAL", 55, "Zayıf Teyit"],
    ["POOR", 30, "Fake Riski"],
  ])("category=%s (skor=%i) → '%s' label", (category, score, label) => {
    mockBQ.mockReturnValue({ data: bqData(category, score), isLoading: false, isError: false });
    render(<BreakoutQualityCard symbol="AAPL" />);
    expect(screen.getByText(label)).toBeInTheDocument();
  });

  it("breakdown kriterleri render (Hacim/Gap-Up/Güç/VCP)", () => {
    mockBQ.mockReturnValue({ data: bqData("EXCELLENT", 95), isLoading: false, isError: false });
    render(<BreakoutQualityCard symbol="AAPL" />);
    expect(screen.getByText("Hacim")).toBeInTheDocument();
    expect(screen.getByText("Gap-Up")).toBeInTheDocument();
  });
});
