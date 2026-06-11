/**
 * CupHandleCard — O'Neil CAN SLIM cup-with-handle base (Paket 456).
 * Kitap-birebir esikler (HTMMIS Bol.15). NONE/null/error -> kart gizli.
 */
import { describe, it, expect, vi, beforeEach } from "vitest";
import { render, screen } from "@testing-library/react";
import { CupHandleCard } from "@/components/stock/CupHandleCard";
import type { CupHandleInfo, CupHandleQuality } from "@/hooks/use-cup-handle";

const mockCH = vi.fn<() => { data?: CupHandleInfo; isLoading: boolean; isError: boolean }>();
vi.mock("@/hooks/use-cup-handle", () => ({ useCupHandle: () => mockCH() }));

beforeEach(() => mockCH.mockReset());

function chData(quality: CupHandleQuality, faults: string[] = []): CupHandleInfo {
  return {
    detected: quality === "EXCELLENT" || quality === "GOOD",
    quality,
    pivot_price: 98.5,
    prior_uptrend_pct: 44.6,
    cup_depth_pct: 20.9,
    cup_duration_days: 78,
    handle_depth_pct: 7.1,
    handle_in_upper_half: true,
    shakeout: true,
    faults,
    mark_says: `${quality} O'Neil felsefe`,
  };
}

describe("CupHandleCard — O'Neil CAN SLIM (Bol.15)", () => {
  it("isLoading → 'Cup-with-Handle taranıyor...'", () => {
    mockCH.mockReturnValue({ data: undefined, isLoading: true, isError: false });
    render(<CupHandleCard symbol="AAPL" />);
    expect(screen.getByText(/Cup-with-Handle taranıyor/)).toBeInTheDocument();
  });

  it("isError → null", () => {
    mockCH.mockReturnValue({ data: undefined, isLoading: false, isError: true });
    const { container } = render(<CupHandleCard symbol="AAPL" />);
    expect(container.firstChild).toBeNull();
  });

  it("quality=null → null (yapı yok)", () => {
    mockCH.mockReturnValue({
      data: { ...chData("GOOD"), quality: null },
      isLoading: false,
      isError: false,
    });
    const { container } = render(<CupHandleCard symbol="AAPL" />);
    expect(container.firstChild).toBeNull();
  });

  it("quality=NONE → null (her hisse cup-handle olusturmaz)", () => {
    mockCH.mockReturnValue({ data: chData("NONE"), isLoading: false, isError: false });
    const { container } = render(<CupHandleCard symbol="AAPL" />);
    expect(container.firstChild).toBeNull();
  });

  it.each<[Exclude<CupHandleQuality, "NONE">, string]>([
    ["EXCELLENT", "Tam Canon"],
    ["GOOD", "Geçerli Baz"],
    ["MARGINAL", "Kusurlu"],
  ])("quality=%s → '%s' label", (quality, label) => {
    mockCH.mockReturnValue({ data: chData(quality), isLoading: false, isError: false });
    render(<CupHandleCard symbol="AAPL" />);
    expect(screen.getByText(label)).toBeInTheDocument();
  });

  it("pivot (buy point) ve olculen parametreler render", () => {
    mockCH.mockReturnValue({ data: chData("EXCELLENT"), isLoading: false, isError: false });
    render(<CupHandleCard symbol="AAPL" />);
    expect(screen.getByText("$98.50")).toBeInTheDocument();
    expect(screen.getByText("Pivot (Buy Point)")).toBeInTheDocument();
    expect(screen.getByText("Kupa")).toBeInTheDocument();
    expect(screen.getByText("Kulp")).toBeInTheDocument();
  });

  it("MARGINAL → kusur listesi render", () => {
    mockCH.mockReturnValue({
      data: chData("MARGINAL", ["kulp yukari kama — dipler yukseliyor, shakeout yok (s.164,178)"]),
      isLoading: false,
      isError: false,
    });
    render(<CupHandleCard symbol="AAPL" />);
    expect(screen.getByText(/Kusur\(lar\)/)).toBeInTheDocument();
    expect(screen.getByText(/yukari kama/)).toBeInTheDocument();
  });
});
