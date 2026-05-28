/**
 * MarkRegimeCard (KARAR #731 — backend tercih + client fallback) +
 * MarketStatusBadge (Sprint 4-bis.7 ABD borsa takvim, useMarketCalendar).
 */
import { describe, it, expect, vi, beforeEach } from "vitest";
import { render, screen } from "@testing-library/react";
import { MarkRegimeCard } from "@/components/market/MarkRegimeCard";
import { MarketStatusBadge } from "@/components/market/MarketStatusBadge";
import type { MarkRegimeInfoBackend } from "@/types/market";
import type { MarketCalendarStatus, MarketSession } from "@/hooks/use-market-calendar";

const mockCalendar = vi.fn<() => { data?: MarketCalendarStatus; isLoading: boolean; isError: boolean }>();

vi.mock("@/hooks/use-market-calendar", async () => {
  const actual = await vi.importActual<typeof import("@/hooks/use-market-calendar")>(
    "@/hooks/use-market-calendar"
  );
  return { ...actual, useMarketCalendar: () => mockCalendar() };
});

beforeEach(() => {
  mockCalendar.mockReset();
});

describe("MarkRegimeCard — client fallback (backendRegime yok)", () => {
  it("'Mark Market Regime' başlık render", () => {
    render(<MarkRegimeCard distributionDays={1} />);
    expect(screen.getByText("Mark Market Regime")).toBeInTheDocument();
  });

  it("distributionDays düşük → client computeMarketRegime fallback render", () => {
    const { container } = render(<MarkRegimeCard distributionDays={0} />);
    // Kart render edilir (regime label + allocation)
    expect(container.firstChild).not.toBeNull();
  });
});

describe("MarkRegimeCard — backend tercih (KARAR #731)", () => {
  it("backendRegime verilince label backend'den gelir", () => {
    const backend: MarkRegimeInfoBackend = {
      regime: "UNDER_PRESSURE",
      label: "Baskı Altında (Backend)",
      allocation: "%50 pozisyon",
      new_buy_allowed: false,
      pilot_override: true,
    };
    render(<MarkRegimeCard distributionDays={4} backendRegime={backend} />);
    expect(screen.getByText("Baskı Altında (Backend)")).toBeInTheDocument();
  });

  it.each<MarkRegimeInfoBackend["regime"]>([
    "HEALTHY",
    "CAUTION",
    "UNDER_PRESSURE",
    "BEAR_PRESSURE",
  ])("regime=%s → backend label render", (regime) => {
    const backend: MarkRegimeInfoBackend = {
      regime,
      label: `${regime} test`,
      allocation: "test allocation",
      new_buy_allowed: regime === "HEALTHY",
      pilot_override: false,
    };
    render(<MarkRegimeCard distributionDays={2} backendRegime={backend} />);
    expect(screen.getByText(`${regime} test`)).toBeInTheDocument();
  });
});

describe("MarketStatusBadge — ABD borsa session", () => {
  function calData(session: MarketSession, is_open: boolean): MarketCalendarStatus {
    return {
      is_open,
      session,
      reason: null,
      is_early_close: false,
      now_et: "2026-05-28 10:00 -04",
      now_tr: "2026-05-28 17:00 +03",
      next_open_et: "2026-05-29 09:30 -04",
      next_open_tr: "2026-05-29 16:30 +03",
      last_trading_day: "2026-05-28",
    };
  }

  it("isLoading → 'Yükleniyor...'", () => {
    mockCalendar.mockReturnValue({ data: undefined, isLoading: true, isError: false });
    render(<MarketStatusBadge />);
    expect(screen.getByText(/Yükleniyor/)).toBeInTheDocument();
  });

  it("isError → 'Takvim verisi alınamadı'", () => {
    mockCalendar.mockReturnValue({ data: undefined, isLoading: false, isError: true });
    render(<MarketStatusBadge />);
    expect(screen.getByText(/Takvim verisi alınamadı/)).toBeInTheDocument();
  });

  it("session='regular' is_open=true → 'ABD Borsa: Açık'", () => {
    mockCalendar.mockReturnValue({
      data: calData("regular", true),
      isLoading: false,
      isError: false,
    });
    render(<MarketStatusBadge />);
    expect(screen.getByText(/ABD Borsa: Açık/)).toBeInTheDocument();
  });

  it("session='pre_market' → 'Pre-Market'", () => {
    mockCalendar.mockReturnValue({
      data: calData("pre_market", false),
      isLoading: false,
      isError: false,
    });
    render(<MarketStatusBadge />);
    expect(screen.getByText(/Pre-Market/)).toBeInTheDocument();
  });

  it("session='post_market' → 'After-Hours'", () => {
    mockCalendar.mockReturnValue({
      data: calData("post_market", false),
      isLoading: false,
      isError: false,
    });
    render(<MarketStatusBadge />);
    expect(screen.getByText(/After-Hours/)).toBeInTheDocument();
  });

  it("session='closed' → 'Kapalı'", () => {
    mockCalendar.mockReturnValue({
      data: calData("closed", false),
      isLoading: false,
      isError: false,
    });
    render(<MarketStatusBadge />);
    expect(screen.getByText(/ABD Borsa: Kapalı/)).toBeInTheDocument();
  });
});
