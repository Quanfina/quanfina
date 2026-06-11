/**
 * HighTightFlagCard — O'Neil HTF = Minervini Power Play (Paket 462).
 * Flagpole >=%100 + tight flag. NONE/null/error -> kart gizli.
 */
import { describe, it, expect, vi, beforeEach } from "vitest";
import { render, screen } from "@testing-library/react";
import { HighTightFlagCard } from "@/components/stock/HighTightFlagCard";
import type { HighTightFlagInfo, HighTightFlagQuality } from "@/hooks/use-high-tight-flag";

const mockHTF = vi.fn<() => { data?: HighTightFlagInfo; isLoading: boolean; isError: boolean }>();
vi.mock("@/hooks/use-high-tight-flag", () => ({ useHighTightFlag: () => mockHTF() }));

beforeEach(() => mockHTF.mockReset());

function htfData(quality: HighTightFlagQuality, faults: string[] = []): HighTightFlagInfo {
  return {
    detected: quality === "EXCELLENT" || quality === "GOOD",
    quality,
    pivot_price: 105.5,
    flagpole_pct: 113.1,
    flagpole_weeks: 5.8,
    flag_depth_pct: 17.1,
    flag_duration_days: 20,
    volume_dryup: true,
    faults,
    mark_says: `${quality} O'Neil HTF felsefe`,
  };
}

describe("HighTightFlagCard — O'Neil/Minervini", () => {
  it("isLoading → 'High Tight Flag taranıyor...'", () => {
    mockHTF.mockReturnValue({ data: undefined, isLoading: true, isError: false });
    render(<HighTightFlagCard symbol="AAPL" />);
    expect(screen.getByText(/High Tight Flag taranıyor/)).toBeInTheDocument();
  });

  it("isError → null", () => {
    mockHTF.mockReturnValue({ data: undefined, isLoading: false, isError: true });
    const { container } = render(<HighTightFlagCard symbol="AAPL" />);
    expect(container.firstChild).toBeNull();
  });

  it("quality=null → null", () => {
    mockHTF.mockReturnValue({
      data: { ...htfData("GOOD"), quality: null },
      isLoading: false,
      isError: false,
    });
    const { container } = render(<HighTightFlagCard symbol="AAPL" />);
    expect(container.firstChild).toBeNull();
  });

  it("quality=NONE → null (HTF cok nadir / pole<%100)", () => {
    mockHTF.mockReturnValue({ data: htfData("NONE"), isLoading: false, isError: false });
    const { container } = render(<HighTightFlagCard symbol="AAPL" />);
    expect(container.firstChild).toBeNull();
  });

  it.each<[Exclude<HighTightFlagQuality, "NONE">, string]>([
    ["EXCELLENT", "Tam Canon"],
    ["GOOD", "Geçerli"],
    ["MARGINAL", "Kusurlu"],
  ])("quality=%s → '%s' label", (quality, label) => {
    mockHTF.mockReturnValue({ data: htfData(quality), isLoading: false, isError: false });
    render(<HighTightFlagCard symbol="AAPL" />);
    expect(screen.getByText(label)).toBeInTheDocument();
  });

  it("pivot + flagpole/bayrak metrikleri render", () => {
    mockHTF.mockReturnValue({ data: htfData("EXCELLENT"), isLoading: false, isError: false });
    render(<HighTightFlagCard symbol="AAPL" />);
    expect(screen.getByText("$105.50")).toBeInTheDocument();
    expect(screen.getByText("Pivot (Bayrak Zirvesi)")).toBeInTheDocument();
    expect(screen.getByText("Flagpole")).toBeInTheDocument();
    expect(screen.getByText(/\+113%/)).toBeInTheDocument();
  });

  it("MARGINAL → kusur listesi render", () => {
    mockHTF.mockReturnValue({
      data: htfData("MARGINAL", ["bayrak %32.0 derin (>%25 — O'Neil/Minervini)"]),
      isLoading: false,
      isError: false,
    });
    render(<HighTightFlagCard symbol="AAPL" />);
    expect(screen.getByText(/Kusur\(lar\)/)).toBeInTheDocument();
    expect(screen.getByText(/derin/)).toBeInTheDocument();
  });
});
