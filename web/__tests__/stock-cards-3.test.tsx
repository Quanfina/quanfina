/**
 * Stock detay kartları grup 3: AtrVolatilityCard + StageTransitionCard.
 *
 * Mark TLSMW Ch 4/11 + Weinstein/Wilder — ATR volatilite + Stage 1→2 geçiş.
 */
import { describe, it, expect, vi, beforeEach } from "vitest";
import { render, screen } from "@testing-library/react";
import { AtrVolatilityCard } from "@/components/stock/AtrVolatilityCard";
import { StageTransitionCard } from "@/components/stock/StageTransitionCard";
import type { AtrVolatilityInfo, AtrCategory } from "@/hooks/use-atr-volatility";
import type { StageTransitionInfo, StageCategory } from "@/hooks/use-stage-transition";

const mockAtr = vi.fn<[], { data?: AtrVolatilityInfo; isLoading: boolean; isError: boolean }>();
const mockStage = vi.fn<[], { data?: StageTransitionInfo; isLoading: boolean; isError: boolean }>();

vi.mock("@/hooks/use-atr-volatility", () => ({ useAtrVolatility: () => mockAtr() }));
vi.mock("@/hooks/use-stage-transition", () => ({ useStageTransition: () => mockStage() }));

beforeEach(() => {
  mockAtr.mockReset();
  mockStage.mockReset();
});

describe("AtrVolatilityCard — 4 kategori (Mark TLSMW Ch 11 / Wilder)", () => {
  function atrData(category: AtrCategory): AtrVolatilityInfo {
    return {
      atr: 3.5,
      atr_pct: 2.3,
      category,
      suggested_stop_tight: 145,
      suggested_stop_normal: 142,
      suggested_stop_loose: 138,
      mark_says: `${category} Mark felsefe`,
    };
  }

  it("isLoading → 'ATR yükleniyor...'", () => {
    mockAtr.mockReturnValue({ data: undefined, isLoading: true, isError: false });
    render(<AtrVolatilityCard symbol="AAPL" />);
    expect(screen.getByText(/ATR yükleniyor/)).toBeInTheDocument();
  });

  it("isError → null", () => {
    mockAtr.mockReturnValue({ data: undefined, isLoading: false, isError: true });
    const { container } = render(<AtrVolatilityCard symbol="AAPL" />);
    expect(container.firstChild).toBeNull();
  });

  it("category=null → null", () => {
    mockAtr.mockReturnValue({
      data: { ...atrData("NORMAL"), category: null },
      isLoading: false,
      isError: false,
    });
    const { container } = render(<AtrVolatilityCard symbol="AAPL" />);
    expect(container.firstChild).toBeNull();
  });

  it.each<[AtrCategory, string]>([
    ["LOW", "Sıkı"],
    ["NORMAL", "Sağlıklı"],
    ["HIGH", "Yüksek"],
    ["EXTREME", "Aşırı"],
  ])("category=%s → '%s' label", (category, label) => {
    mockAtr.mockReturnValue({ data: atrData(category), isLoading: false, isError: false });
    render(<AtrVolatilityCard symbol="AAPL" />);
    expect(screen.getByText(label)).toBeInTheDocument();
  });
});

describe("StageTransitionCard — 4 kategori (Mark TLSMW Ch 4 / Weinstein)", () => {
  function stageData(category: StageCategory): StageTransitionInfo {
    return {
      category,
      ma_value: 150,
      price_above_ma_pct: 5,
      slope_pct: 12,
      days_above_ma: 30,
      volume_trend: "RISING",
      mark_says: `${category} Mark felsefe`,
    };
  }

  it("isLoading → 'Stage Transition yükleniyor...'", () => {
    mockStage.mockReturnValue({ data: undefined, isLoading: true, isError: false });
    render(<StageTransitionCard symbol="AAPL" />);
    expect(screen.getByText(/Stage Transition yükleniyor/)).toBeInTheDocument();
  });

  it("isError → null", () => {
    mockStage.mockReturnValue({ data: undefined, isLoading: false, isError: true });
    const { container } = render(<StageTransitionCard symbol="AAPL" />);
    expect(container.firstChild).toBeNull();
  });

  it("category=null → null", () => {
    mockStage.mockReturnValue({
      data: { ...stageData("EARLY_STAGE_2"), category: null },
      isLoading: false,
      isError: false,
    });
    const { container } = render(<StageTransitionCard symbol="AAPL" />);
    expect(container.firstChild).toBeNull();
  });

  it.each<[StageCategory, string]>([
    ["NO_TRANSITION", "Kırılım Yok"],
    ["EARLY_STAGE_2", "Erken Stage 2"],
    ["CONFIRMED_STAGE_2", "Stage 2 Onaylı"],
    ["STAGE_2_MATURE", "Olgun Trend"],
  ])("category=%s → '%s' label", (category, label) => {
    mockStage.mockReturnValue({ data: stageData(category), isLoading: false, isError: false });
    render(<StageTransitionCard symbol="AAPL" />);
    expect(screen.getByText(label)).toBeInTheDocument();
  });

  it("volume_trend='RISING' → '↑ Artıyor' göstergesi", () => {
    mockStage.mockReturnValue({
      data: stageData("CONFIRMED_STAGE_2"),
      isLoading: false,
      isError: false,
    });
    render(<StageTransitionCard symbol="AAPL" />);
    expect(screen.getByText(/↑ Artıyor/)).toBeInTheDocument();
  });
});
