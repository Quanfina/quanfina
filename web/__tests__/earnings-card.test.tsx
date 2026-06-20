/**
 * EarningsCard (P543) — Minervini s.249 earnings 5g blackout ("crapshoot").
 * blackout (≤5g kırmızı uyarı) / blackout-dışı (yeşil) / veri-yok (gri) / loading-error.
 */
import { describe, it, expect, vi, beforeEach } from "vitest";
import { render, screen } from "@testing-library/react";
import { EarningsCard } from "@/components/stock/EarningsCard";
import type { EarningsResponse } from "@/hooks/use-earnings";

const mockEarn = vi.fn<() => { data?: EarningsResponse; isLoading: boolean; isError: boolean }>();
vi.mock("@/hooks/use-earnings", () => ({ useEarnings: () => mockEarn() }));

beforeEach(() => mockEarn.mockReset());

function earn(over: Partial<EarningsResponse> = {}): EarningsResponse {
  return {
    symbol: "AAPL",
    has_data: true,
    earnings_date: "2026-06-25",
    days_to_earnings: 12,
    within_blackout: false,
    blackout_days: 5,
    mark_says: "Earnings 12 gün sonra (2026-06-25). Blackout dışı (>5 gün) — Minervini s.249 alım serbest.",
    ...over,
  };
}

describe("EarningsCard (Minervini s.249 blackout)", () => {
  it("isLoading → yükleniyor", () => {
    mockEarn.mockReturnValue({ data: undefined, isLoading: true, isError: false });
    render(<EarningsCard symbol="AAPL" />);
    expect(screen.getByText(/Earnings tarihi yükleniyor/)).toBeInTheDocument();
  });

  it("isError → null", () => {
    mockEarn.mockReturnValue({ data: undefined, isLoading: false, isError: true });
    const { container } = render(<EarningsCard symbol="AAPL" />);
    expect(container.firstChild).toBeNull();
  });

  it("blackout dışı → tarih + kalan gün + serbest", () => {
    mockEarn.mockReturnValue({ data: earn(), isLoading: false, isError: false });
    render(<EarningsCard symbol="AAPL" />);
    expect(screen.getByText("Tarih")).toBeInTheDocument();
    expect(screen.getByText("12g")).toBeInTheDocument();
    expect(screen.getByText(/alım serbest/)).toBeInTheDocument();
  });

  it("blackout (3 gün) → uyarı (s.249 breakout YAPMA)", () => {
    mockEarn.mockReturnValue({
      data: earn({
        days_to_earnings: 3, within_blackout: true,
        mark_says: "⚠️ Earnings 3 gün sonra — Minervini s.249: YENİ breakout alımı YAPMA (crapshoot).",
      }),
      isLoading: false, isError: false,
    });
    render(<EarningsCard symbol="AAPL" />);
    expect(screen.getByText("3g")).toBeInTheDocument();
    expect(screen.getByText(/breakout alımı YAPMA/)).toBeInTheDocument();
  });

  it("veri yok → manuel teyit, metrik yok", () => {
    mockEarn.mockReturnValue({
      data: earn({
        has_data: false, earnings_date: null, days_to_earnings: null, within_blackout: false,
        mark_says: "Earnings tarihi bilinmiyor (yfinance veri yok). Trade öncesi manuel teyit önerilir.",
      }),
      isLoading: false, isError: false,
    });
    render(<EarningsCard symbol="AAPL" />);
    expect(screen.getByText(/manuel teyit/)).toBeInTheDocument();
    expect(screen.queryByText("Tarih")).toBeNull();
  });
});
