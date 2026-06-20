/**
 * FundamentalCard (P542) — Minervini CANSLIM C/A (s.26 ≥%25 Q/Q soft teyit).
 * both-strong (yeşil ✓) / partial (amber ✗) / veri-yok / loading-error.
 */
import { describe, it, expect, vi, beforeEach } from "vitest";
import { render, screen } from "@testing-library/react";
import { FundamentalCard } from "@/components/stock/FundamentalCard";
import type { FundamentalResponse } from "@/hooks/use-fundamental";

const mockFund = vi.fn<() => { data?: FundamentalResponse; isLoading: boolean; isError: boolean }>();
vi.mock("@/hooks/use-fundamental", () => ({ useFundamental: () => mockFund() }));

beforeEach(() => mockFund.mockReset());

function fund(over: Partial<FundamentalResponse> = {}): FundamentalResponse {
  return {
    symbol: "NVDA",
    has_data: true,
    eps_qoq: 88,
    sales_qoq: 122,
    eps_pass: true,
    sales_pass: true,
    threshold_pct: 25,
    mark_says: "Minervini C/A (s.26 ≥%25 Q/Q): EPS Q/Q %88 ✓, Satış Q/Q %122 ✓. güçlü temel ivme.",
    ...over,
  };
}

describe("FundamentalCard (Minervini C/A)", () => {
  it("isLoading → yükleniyor", () => {
    mockFund.mockReturnValue({ data: undefined, isLoading: true, isError: false });
    render(<FundamentalCard symbol="NVDA" />);
    expect(screen.getByText(/Temel veri yükleniyor/)).toBeInTheDocument();
  });

  it("isError → null", () => {
    mockFund.mockReturnValue({ data: undefined, isLoading: false, isError: true });
    const { container } = render(<FundamentalCard symbol="NVDA" />);
    expect(container.firstChild).toBeNull();
  });

  it("both strong → EPS/Satış Q/Q + ✓", () => {
    mockFund.mockReturnValue({ data: fund(), isLoading: false, isError: false });
    render(<FundamentalCard symbol="NVDA" />);
    expect(screen.getByText("EPS Q/Q")).toBeInTheDocument();
    // %88 ✓ hem metrik hücresinde hem mark_says paragrafında geçer → getAllByText
    expect(screen.getAllByText(/%88 ✓/).length).toBeGreaterThan(0);
    expect(screen.getAllByText(/%122 ✓/).length).toBeGreaterThan(0);
  });

  it("zayıf EPS → ✗ gösterir", () => {
    mockFund.mockReturnValue({
      data: fund({ eps_qoq: 10, eps_pass: false, sales_qoq: 30, sales_pass: true }),
      isLoading: false, isError: false,
    });
    render(<FundamentalCard symbol="NVDA" />);
    expect(screen.getByText(/%10 ✗/)).toBeInTheDocument();
  });

  it("veri yok → mark_says (soft teyit) gösterir, metrik yok", () => {
    mockFund.mockReturnValue({
      data: fund({
        has_data: false, eps_qoq: null, sales_qoq: null, eps_pass: null, sales_pass: null,
        mark_says: "Temel veri yok. Minervini: fundamental SOFT teyit (s.1152).",
      }),
      isLoading: false, isError: false,
    });
    render(<FundamentalCard symbol="NVDA" />);
    expect(screen.getByText(/Temel veri yok/)).toBeInTheDocument();
    expect(screen.queryByText("EPS Q/Q")).toBeNull();
  });
});
