/**
 * SellStrengthCard (KARAR ADAY #976, P475) — Mark satış sinyalleri kategori + sinyal listesi.
 * useSellStrength mock — loading/error/null/SELL/HOLD durumları.
 */
import { describe, it, expect, vi, beforeEach } from "vitest";
import { render, screen } from "@testing-library/react";
import { SellStrengthCard } from "@/components/stock/SellStrengthCard";

const mSell = vi.fn();
vi.mock("@/hooks/use-sell-strength", () => ({ useSellStrength: () => mSell() }));

function res(data: unknown, { isLoading = false, isError = false } = {}) {
  return { data, isLoading, isError };
}

beforeEach(() => {
  mSell.mockReset();
});

describe("SellStrengthCard", () => {
  it("loading → 'taranıyor'", () => {
    mSell.mockReturnValue(res(undefined, { isLoading: true }));
    render(<SellStrengthCard symbol="AAPL" />);
    expect(screen.getByText(/taranıyor/)).toBeInTheDocument();
  });

  it("isError → null", () => {
    mSell.mockReturnValue(res(undefined, { isError: true }));
    const { container } = render(<SellStrengthCard symbol="AAPL" />);
    expect(container.firstChild).toBeNull();
  });

  it("category=null → null", () => {
    mSell.mockReturnValue(
      res({ category: null, signals: [], defensive: [], offensive: [], sell_strength: 0, mark_says: "" })
    );
    const { container } = render(<SellStrengthCard symbol="AAPL" />);
    expect(container.firstChild).toBeNull();
  });

  it("SELL → 'SAT · n/10' rozet + defansif sinyal listesi", () => {
    mSell.mockReturnValue(
      res({
        detected: true, category: "SELL", sell_strength: 6,
        signals: ["200-MA kirilim — kesin sat"],
        defensive: ["200-MA kirilim — kesin sat"], offensive: [],
        pct_above_200ma: -5, mark_says: "SAT — teknik bozuldu",
      })
    );
    render(<SellStrengthCard symbol="AAPL" />);
    expect(screen.getByText(/SAT · 6\/10/)).toBeInTheDocument();
    expect(screen.getByText(/200-MA kirilim/)).toBeInTheDocument();
  });

  it("HOLD → 'TUT · 0/10' rozet + sağlıklı mesaj", () => {
    mSell.mockReturnValue(
      res({
        detected: false, category: "HOLD", sell_strength: 0,
        signals: [], defensive: [], offensive: [],
        pct_above_200ma: 12, mark_says: "Aktif satış sinyali yok — sağlıklı",
      })
    );
    render(<SellStrengthCard symbol="AAPL" />);
    expect(screen.getByText(/TUT · 0\/10/)).toBeInTheDocument();
    expect(screen.getByText(/sağlıklı/)).toBeInTheDocument();
  });
});
