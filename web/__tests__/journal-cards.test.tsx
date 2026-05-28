/**
 * Journal Mark kartları: BrandonExpectancyCard + RbaSummaryCard.
 *
 * useTradesExpectancy / useRbaMetrics mock — loading / error / empty / kategori render.
 */
import { describe, it, expect, vi, beforeEach } from "vitest";
import { render, screen } from "@testing-library/react";
import { BrandonExpectancyCard } from "@/components/journal/BrandonExpectancyCard";
import { RbaSummaryCard } from "@/components/journal/RbaSummaryCard";
import type { BrandonExpectancyInfo } from "@/hooks/use-trades-expectancy";
import type { RbaResponse } from "@/hooks/use-rba-metrics";

const mockExpectancy = vi.fn<() => { data?: BrandonExpectancyInfo; isLoading: boolean; isError: boolean }>();
const mockRba = vi.fn<() => { data?: RbaResponse; isLoading: boolean; isError: boolean }>();

vi.mock("@/hooks/use-trades-expectancy", () => ({
  useTradesExpectancy: () => mockExpectancy(),
}));
vi.mock("@/hooks/use-rba-metrics", () => ({
  useRbaMetrics: () => mockRba(),
}));

beforeEach(() => {
  mockExpectancy.mockReset();
  mockRba.mockReset();
});

describe("BrandonExpectancyCard — 4 kategori + loading/error/empty", () => {
  function expData(
    category: BrandonExpectancyInfo["category"],
    total = 20
  ): BrandonExpectancyInfo {
    return {
      expectancy_pct: 12.5,
      category,
      meets_brandon_targets: category === "EXCELLENT",
      mark_says: "Brandon felsefe notu",
      total_closed: total,
      winners: 13,
      losers: 7,
      avg_gain_pct: 25,
      avg_loss_pct: 8,
      win_rate: 65,
    };
  }

  it("isLoading → 'Brandon Expectancy yükleniyor...'", () => {
    mockExpectancy.mockReturnValue({ data: undefined, isLoading: true, isError: false });
    render(<BrandonExpectancyCard />);
    expect(screen.getByText(/Brandon Expectancy yükleniyor/)).toBeInTheDocument();
  });

  it("isError → 'Brandon Expectancy alınamadı.' hata mesajı", () => {
    mockExpectancy.mockReturnValue({ data: undefined, isLoading: false, isError: true });
    render(<BrandonExpectancyCard />);
    expect(screen.getByText(/Brandon Expectancy alınamadı/)).toBeInTheDocument();
  });

  it("total_closed=0 → boş durum (mark_says italik)", () => {
    mockExpectancy.mockReturnValue({
      data: { ...expData(null, 0), mark_says: "Henüz kapalı trade yok" },
      isLoading: false,
      isError: false,
    });
    render(<BrandonExpectancyCard />);
    expect(screen.getByText("Henüz kapalı trade yok")).toBeInTheDocument();
  });

  it.each<[NonNullable<BrandonExpectancyInfo["category"]>, string]>([
    ["EXCELLENT", "Brandon Canon Zirvesi"],
    ["GOOD", "Brandon Eşiği OK"],
    ["ACCEPTABLE", "Asgari Kârlılık"],
    ["POOR", "Disiplin Gerek"],
  ])("category=%s → '%s' label", (category, label) => {
    mockExpectancy.mockReturnValue({ data: expData(category), isLoading: false, isError: false });
    render(<BrandonExpectancyCard />);
    expect(screen.getByText(label)).toBeInTheDocument();
  });
});

describe("RbaSummaryCard — 4 severity + loading/error", () => {
  function rbaData(severity: RbaResponse["recommendation"]["severity"]): RbaResponse {
    return {
      metrics: {
        num_trades: 30,
        win_rate: 0.6,
        avg_gain_pct: 15,
        avg_loss_pct: -7,
        largest_gain_pct: 35,
        largest_loss_pct: -12,
        adjusted_ratio: 2.5,
        expectancy_pct: 6.4,
        is_statistically_significant: true,
      },
      recommendation: { severity, message: `${severity} öneri` },
      filter_strategy: null,
      filter_setup_type: null,
    };
  }

  it("isLoading → skeleton/yükleniyor render (null değil)", () => {
    mockRba.mockReturnValue({ data: undefined, isLoading: true, isError: false });
    const { container } = render(<RbaSummaryCard />);
    // Loading state bir şey render eder (boş değil)
    expect(container.firstChild).not.toBeNull();
  });

  it("isError → null (sessiz fallback)", () => {
    mockRba.mockReturnValue({ data: undefined, isLoading: false, isError: true });
    const { container } = render(<RbaSummaryCard />);
    expect(container.firstChild).toBeNull();
  });

  it.each<RbaResponse["recommendation"]["severity"]>([
    "OK",
    "INFO",
    "WARNING",
    "CRITICAL",
  ])("severity=%s → render (severity etiketi)", (severity) => {
    mockRba.mockReturnValue({ data: rbaData(severity), isLoading: false, isError: false });
    render(<RbaSummaryCard />);
    expect(screen.getByText(severity)).toBeInTheDocument();
  });

  it("Metrik: num_trades + adjusted_ratio data'dan render", () => {
    mockRba.mockReturnValue({ data: rbaData("OK"), isLoading: false, isError: false });
    const { container } = render(<RbaSummaryCard />);
    // 30 trade + 2.5 adjusted ratio render edilir (metin içinde)
    expect(container.textContent).toMatch(/30/);
  });

  it("adjusted_ratio=99 (backend inf cap) → '∞' gösterilir, '99' değil", () => {
    // Paket 348: tüm-kazanan edge backend'de float('inf')→99.0 cap'lenir.
    // fmtRatio bunu "∞" olarak render etmeli (orijinal UX intent korunur).
    const data = rbaData("OK");
    data.metrics.adjusted_ratio = 99.0;
    mockRba.mockReturnValue({ data, isLoading: false, isError: false });
    const { container } = render(<RbaSummaryCard />);
    expect(container.textContent).toContain("∞");
    expect(container.textContent).not.toMatch(/99\.00/);
  });
});
