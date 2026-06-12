/**
 * BaseSummaryCard — 4 O'Neil base detector konsolide özeti (Paket 467).
 * En güçlü baz (EXCELLENT>GOOD>MARGINAL) veya "baz yok".
 */
import { describe, it, expect, vi, beforeEach } from "vitest";
import { render, screen } from "@testing-library/react";
import { BaseSummaryCard } from "@/components/stock/BaseSummaryCard";

const mCup = vi.fn();
const mFlat = vi.fn();
const mDb = vi.fn();
const mHtf = vi.fn();
vi.mock("@/hooks/use-cup-handle", () => ({ useCupHandle: () => mCup() }));
vi.mock("@/hooks/use-flat-base", () => ({ useFlatBase: () => mFlat() }));
vi.mock("@/hooks/use-double-bottom", () => ({ useDoubleBottom: () => mDb() }));
vi.mock("@/hooks/use-high-tight-flag", () => ({ useHighTightFlag: () => mHtf() }));

function res(quality: string | null, pivot: number | null = null) {
  return { data: quality === undefined ? undefined : { quality, pivot_price: pivot }, isLoading: false, isError: false };
}
const NONE = res("NONE");

beforeEach(() => {
  mCup.mockReturnValue(NONE);
  mFlat.mockReturnValue(NONE);
  mDb.mockReturnValue(NONE);
  mHtf.mockReturnValue(NONE);
});

describe("BaseSummaryCard", () => {
  it("herhangi biri loading → 'taranıyor'", () => {
    mFlat.mockReturnValue({ data: undefined, isLoading: true, isError: false });
    render(<BaseSummaryCard symbol="AAPL" />);
    expect(screen.getByText(/taranıyor/)).toBeInTheDocument();
  });

  it("hepsi NONE → 'geçerli O'Neil baz paterni yok'", () => {
    render(<BaseSummaryCard symbol="AAPL" />);
    expect(screen.getByText(/geçerli O'Neil baz paterni yok/)).toBeInTheDocument();
  });

  it("tek baz (Flat GOOD) → 'Flat Base · GOOD' + pivot", () => {
    mFlat.mockReturnValue(res("GOOD", 100.5));
    render(<BaseSummaryCard symbol="AAPL" />);
    expect(screen.getByText(/Flat Base · GOOD/)).toBeInTheDocument();
    expect(screen.getByText(/pivot \$100\.50/)).toBeInTheDocument();
  });

  it("birden çok → en güçlü (EXCELLENT > GOOD) seçilir + '+N diğer'", () => {
    mFlat.mockReturnValue(res("GOOD", 100));
    mCup.mockReturnValue(res("EXCELLENT", 50));
    render(<BaseSummaryCard symbol="AAPL" />);
    expect(screen.getByText(/Cup-with-Handle · EXCELLENT/)).toBeInTheDocument();
    expect(screen.getByText(/\+1 diğer/)).toBeInTheDocument();
  });

  it("MARGINAL tek başına da gösterilir", () => {
    mDb.mockReturnValue(res("MARGINAL", 80));
    render(<BaseSummaryCard symbol="AAPL" />);
    expect(screen.getByText(/Double Bottom \(W\) · MARGINAL/)).toBeInTheDocument();
  });
});
