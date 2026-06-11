/**
 * DoubleBottomCard — O'Neil/IBD Double Bottom (W) (Paket 460).
 * Undercut + orta tepe pivot. NONE/null/error -> kart gizli.
 */
import { describe, it, expect, vi, beforeEach } from "vitest";
import { render, screen } from "@testing-library/react";
import { DoubleBottomCard } from "@/components/stock/DoubleBottomCard";
import type { DoubleBottomInfo, DoubleBottomQuality } from "@/hooks/use-double-bottom";

const mockDB = vi.fn<() => { data?: DoubleBottomInfo; isLoading: boolean; isError: boolean }>();
vi.mock("@/hooks/use-double-bottom", () => ({ useDoubleBottom: () => mockDB() }));

beforeEach(() => mockDB.mockReset());

function dbData(quality: DoubleBottomQuality, faults: string[] = []): DoubleBottomInfo {
  return {
    detected: quality === "EXCELLENT" || quality === "GOOD",
    quality,
    pivot_price: 95.5,
    prior_advance_pct: 44.6,
    base_depth_pct: 17.9,
    base_duration_days: 41,
    undercut: true,
    first_low: 84.5,
    second_low: 82.5,
    middle_peak: 95.5,
    faults,
    mark_says: `${quality} O'Neil double bottom felsefe`,
  };
}

describe("DoubleBottomCard — O'Neil/IBD (W)", () => {
  it("isLoading → 'Double Bottom (W) taranıyor...'", () => {
    mockDB.mockReturnValue({ data: undefined, isLoading: true, isError: false });
    render(<DoubleBottomCard symbol="AAPL" />);
    expect(screen.getByText(/Double Bottom \(W\) taranıyor/)).toBeInTheDocument();
  });

  it("isError → null", () => {
    mockDB.mockReturnValue({ data: undefined, isLoading: false, isError: true });
    const { container } = render(<DoubleBottomCard symbol="AAPL" />);
    expect(container.firstChild).toBeNull();
  });

  it("quality=null → null", () => {
    mockDB.mockReturnValue({
      data: { ...dbData("GOOD"), quality: null },
      isLoading: false,
      isError: false,
    });
    const { container } = render(<DoubleBottomCard symbol="AAPL" />);
    expect(container.firstChild).toBeNull();
  });

  it("quality=NONE → null (undercut yoksa zaten NONE)", () => {
    mockDB.mockReturnValue({ data: dbData("NONE"), isLoading: false, isError: false });
    const { container } = render(<DoubleBottomCard symbol="AAPL" />);
    expect(container.firstChild).toBeNull();
  });

  it.each<[Exclude<DoubleBottomQuality, "NONE">, string]>([
    ["EXCELLENT", "Tam Canon"],
    ["GOOD", "Geçerli Baz"],
    ["MARGINAL", "Kusurlu"],
  ])("quality=%s → '%s' label", (quality, label) => {
    mockDB.mockReturnValue({ data: dbData(quality), isLoading: false, isError: false });
    render(<DoubleBottomCard symbol="AAPL" />);
    expect(screen.getByText(label)).toBeInTheDocument();
  });

  it("pivot (orta tepe) + undercut görseli render", () => {
    mockDB.mockReturnValue({ data: dbData("EXCELLENT"), isLoading: false, isError: false });
    render(<DoubleBottomCard symbol="AAPL" />);
    expect(screen.getByText("$95.50")).toBeInTheDocument();
    expect(screen.getByText("Pivot (Orta Tepe)")).toBeInTheDocument();
    expect(screen.getByText("$84.50")).toBeInTheDocument();
    expect(screen.getByText(/undercut/)).toBeInTheDocument();
  });

  it("MARGINAL → kusur listesi render", () => {
    mockDB.mockReturnValue({
      data: dbData("MARGINAL", ["orta tepe bazin alt yarisinda (zayif W)"]),
      isLoading: false,
      isError: false,
    });
    render(<DoubleBottomCard symbol="AAPL" />);
    expect(screen.getByText(/Kusur\(lar\)/)).toBeInTheDocument();
    expect(screen.getByText(/alt yarisinda/)).toBeInTheDocument();
  });
});
