/**
 * FlatBaseCard — O'Neil/IBD Flat Base later-stage base (Paket 458).
 * IBD-canon esikler. NONE/null/error -> kart gizli.
 */
import { describe, it, expect, vi, beforeEach } from "vitest";
import { render, screen } from "@testing-library/react";
import { FlatBaseCard } from "@/components/stock/FlatBaseCard";
import type { FlatBaseInfo, FlatBaseQuality } from "@/hooks/use-flat-base";

const mockFB = vi.fn<() => { data?: FlatBaseInfo; isLoading: boolean; isError: boolean }>();
vi.mock("@/hooks/use-flat-base", () => ({ useFlatBase: () => mockFB() }));

beforeEach(() => mockFB.mockReset());

function fbData(quality: FlatBaseQuality, faults: string[] = []): FlatBaseInfo {
  return {
    detected: quality === "EXCELLENT" || quality === "GOOD",
    quality,
    pivot_price: 100.5,
    prior_advance_pct: 44.6,
    base_depth_pct: 5.0,
    base_duration_days: 40,
    is_sideways: true,
    volume_dryup: true,
    faults,
    mark_says: `${quality} O'Neil flat base felsefe`,
  };
}

describe("FlatBaseCard — O'Neil/IBD later-stage base", () => {
  it("isLoading → 'Flat Base taranıyor...'", () => {
    mockFB.mockReturnValue({ data: undefined, isLoading: true, isError: false });
    render(<FlatBaseCard symbol="AAPL" />);
    expect(screen.getByText(/Flat Base taranıyor/)).toBeInTheDocument();
  });

  it("isError → null", () => {
    mockFB.mockReturnValue({ data: undefined, isLoading: false, isError: true });
    const { container } = render(<FlatBaseCard symbol="AAPL" />);
    expect(container.firstChild).toBeNull();
  });

  it("quality=null → null", () => {
    mockFB.mockReturnValue({
      data: { ...fbData("GOOD"), quality: null },
      isLoading: false,
      isError: false,
    });
    const { container } = render(<FlatBaseCard symbol="AAPL" />);
    expect(container.firstChild).toBeNull();
  });

  it("quality=NONE → null (her hisse flat base olusturmaz)", () => {
    mockFB.mockReturnValue({ data: fbData("NONE"), isLoading: false, isError: false });
    const { container } = render(<FlatBaseCard symbol="AAPL" />);
    expect(container.firstChild).toBeNull();
  });

  it.each<[Exclude<FlatBaseQuality, "NONE">, string]>([
    ["EXCELLENT", "Tam Canon"],
    ["GOOD", "Geçerli Baz"],
    ["MARGINAL", "Kusurlu"],
  ])("quality=%s → '%s' label", (quality, label) => {
    mockFB.mockReturnValue({ data: fbData(quality), isLoading: false, isError: false });
    render(<FlatBaseCard symbol="AAPL" />);
    expect(screen.getByText(label)).toBeInTheDocument();
  });

  it("pivot + olculen parametreler render", () => {
    mockFB.mockReturnValue({ data: fbData("EXCELLENT"), isLoading: false, isError: false });
    render(<FlatBaseCard symbol="AAPL" />);
    expect(screen.getByText("$100.50")).toBeInTheDocument();
    expect(screen.getByText("Pivot (Buy Point)")).toBeInTheDocument();
    expect(screen.getByText("Süre")).toBeInTheDocument();
    expect(screen.getByText("Derinlik")).toBeInTheDocument();
    expect(screen.getByText("Ön Trend")).toBeInTheDocument();
  });

  it("MARGINAL → kusur listesi render", () => {
    mockFB.mockReturnValue({
      data: fbData("MARGINAL", ["derinlik %18.0 (>%15 genis/gevsek, IBD)"]),
      isLoading: false,
      isError: false,
    });
    render(<FlatBaseCard symbol="AAPL" />);
    expect(screen.getByText(/Kusur\(lar\)/)).toBeInTheDocument();
    expect(screen.getByText(/genis\/gevsek/)).toBeInTheDocument();
  });
});
