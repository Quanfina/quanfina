/**
 * MarkRegimeBanner (KARAR #733 alt P36) — Piyasa rejimi üst-uyarı banner.
 *
 * useMarketStatus mock ile: 4 regime + hideOnHealthy + stage4 + divergence + FTD + climax + compact.
 */
import { describe, it, expect, vi, beforeEach } from "vitest";
import { render, screen } from "@testing-library/react";
import { MarkRegimeBanner } from "@/components/mark/MarkRegimeBanner";
import type { MarketStatus, MarkRegimeType } from "@/types/market";

const mockMarketStatus = vi.fn<() => { data?: Partial<MarketStatus>; isLoading: boolean; isError: boolean }>();

vi.mock("@/hooks/use-market-status", () => ({
  useMarketStatus: () => mockMarketStatus(),
}));

function makeData(
  regime: MarkRegimeType,
  overrides: Partial<MarketStatus> = {}
): Partial<MarketStatus> {
  const labels: Record<MarkRegimeType, string> = {
    HEALTHY: "Sağlıklı",
    CAUTION: "Dikkat",
    UNDER_PRESSURE: "Baskı Altında",
    BEAR_PRESSURE: "Ayı Baskısı",
  };
  return {
    distribution_days: 2,
    mark_regime: {
      regime,
      label: labels[regime],
      allocation: regime === "HEALTHY" ? "Tam pozisyon (100%)" : "%50 pozisyon",
      new_buy_allowed: regime === "HEALTHY" || regime === "CAUTION",
      pilot_override: true,
    },
    ...overrides,
  };
}

beforeEach(() => {
  mockMarketStatus.mockReset();
});

describe("MarkRegimeBanner — yükleme/hata/veri yok → null", () => {
  it("isLoading=true → null", () => {
    mockMarketStatus.mockReturnValue({ data: undefined, isLoading: true, isError: false });
    const { container } = render(<MarkRegimeBanner />);
    expect(container.firstChild).toBeNull();
  });

  it("isError=true → null", () => {
    mockMarketStatus.mockReturnValue({ data: undefined, isLoading: false, isError: true });
    const { container } = render(<MarkRegimeBanner />);
    expect(container.firstChild).toBeNull();
  });

  it("mark_regime yok → null", () => {
    mockMarketStatus.mockReturnValue({
      data: { distribution_days: 1 },
      isLoading: false,
      isError: false,
    });
    const { container } = render(<MarkRegimeBanner />);
    expect(container.firstChild).toBeNull();
  });
});

describe("MarkRegimeBanner — hideOnHealthy davranışı", () => {
  it("HEALTHY + hideOnHealthy(default) + stage4=0 → null (sessiz)", () => {
    mockMarketStatus.mockReturnValue({
      data: makeData("HEALTHY"),
      isLoading: false,
      isError: false,
    });
    const { container } = render(<MarkRegimeBanner />);
    expect(container.firstChild).toBeNull();
  });

  it("HEALTHY + stage4Count>0 → banner görünür (UZAK DUR uyarısı)", () => {
    mockMarketStatus.mockReturnValue({
      data: makeData("HEALTHY"),
      isLoading: false,
      isError: false,
    });
    render(<MarkRegimeBanner stage4Count={3} totalCount={10} />);
    expect(screen.getByText(/Sağlıklı/)).toBeInTheDocument();
    expect(screen.getByText(/3 hisse Carr Stage 4/)).toBeInTheDocument();
  });

  it("HEALTHY + hideOnHealthy=false → banner her zaman görünür", () => {
    mockMarketStatus.mockReturnValue({
      data: makeData("HEALTHY"),
      isLoading: false,
      isError: false,
    });
    render(<MarkRegimeBanner hideOnHealthy={false} />);
    expect(screen.getByText(/Sağlıklı/)).toBeInTheDocument();
  });
});

describe("MarkRegimeBanner — 4 regime + Yeni Alım YASAK", () => {
  it("UNDER_PRESSURE → banner + 'Yeni Alım YASAK' (new_buy_allowed=false)", () => {
    mockMarketStatus.mockReturnValue({
      data: makeData("UNDER_PRESSURE"),
      isLoading: false,
      isError: false,
    });
    render(<MarkRegimeBanner />);
    expect(screen.getByText(/Baskı Altında/)).toBeInTheDocument();
    expect(screen.getByText("Yeni Alım YASAK")).toBeInTheDocument();
  });

  it("BEAR_PRESSURE → role='alert' (critical severity)", () => {
    mockMarketStatus.mockReturnValue({
      data: makeData("BEAR_PRESSURE"),
      isLoading: false,
      isError: false,
    });
    render(<MarkRegimeBanner />);
    expect(screen.getByRole("alert")).toBeInTheDocument();
  });

  it("CAUTION → banner görünür (new_buy_allowed=true → YASAK etiketi yok)", () => {
    mockMarketStatus.mockReturnValue({
      data: makeData("CAUTION"),
      isLoading: false,
      isError: false,
    });
    render(<MarkRegimeBanner hideOnHealthy={false} />);
    expect(screen.getByText(/Dikkat/)).toBeInTheDocument();
    expect(screen.queryByText("Yeni Alım YASAK")).not.toBeInTheDocument();
  });
});

describe("MarkRegimeBanner — compact mode", () => {
  it("compact=true → tek satır (role='status')", () => {
    mockMarketStatus.mockReturnValue({
      data: makeData("UNDER_PRESSURE"),
      isLoading: false,
      isError: false,
    });
    render(<MarkRegimeBanner compact />);
    const status = screen.getByRole("status");
    expect(status).toBeInTheDocument();
    expect(status.textContent).toContain("Baskı Altında");
    expect(status.textContent).toMatch(/DD 2\/20/);
  });
});

describe("MarkRegimeBanner — özel uyarı bantları (P61/P68/P95)", () => {
  it("Critical divergence → BEARISH DIVERGENCE bandı (HEALTHY'de bile görünür)", () => {
    mockMarketStatus.mockReturnValue({
      data: makeData("HEALTHY", {
        breadth_divergence: {
          divergence: "BEARISH_DIVERGENCE",
          severity: "critical",
          index_change_pct: 0.5,
          ad_trend_delta: -1500,
          mark_says: "1999 dot-com paterni — erken uyarı",
        },
      }),
      isLoading: false,
      isError: false,
    });
    render(<MarkRegimeBanner />);
    expect(screen.getByText(/BEARISH DIVERGENCE/)).toBeInTheDocument();
  });

  it("FTD onaylı → FOLLOW-THROUGH DAY ✓ bandı (HEALTHY'de bile)", () => {
    mockMarketStatus.mockReturnValue({
      data: makeData("HEALTHY", {
        follow_through: {
          ftd_detected: true,
          volume_confirmed: true,
          ftd_gain_pct: 1.8,
          days_after_low: 5,
          mark_says: "2009 dip recovery paterni",
        },
      }),
      isLoading: false,
      isError: false,
    });
    render(<MarkRegimeBanner />);
    expect(screen.getByText(/FOLLOW-THROUGH DAY ✓ ONAYLI/)).toBeInTheDocument();
  });

  it("climaxTopCount>0 → CLIMAX RUN SAT bandı (HEALTHY'de bile)", () => {
    mockMarketStatus.mockReturnValue({
      data: makeData("HEALTHY"),
      isLoading: false,
      isError: false,
    });
    render(<MarkRegimeBanner climaxTopCount={2} />);
    expect(screen.getByText(/CLIMAX RUN 🔴 SAT\/ÇIKIŞ SİNYALİ/)).toBeInTheDocument();
  });
});
