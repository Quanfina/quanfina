/**
 * BullishBaseCard (Carr 2.baskı s.291) — P513 strateji inşası.
 * CONTRARIAN downtrend-sonu ADAY. detected CANDIDATE (entry=close + eyeball checklist) +
 * aday yok (SMA bağlamı) + is_mock + loading/error.
 */
import { describe, it, expect, vi, beforeEach } from "vitest";
import { render, screen } from "@testing-library/react";
import { BullishBaseCard } from "@/components/stock/BullishBaseCard";
import type { BullishBaseResponse } from "@/hooks/use-bullish-base";

const mockBB = vi.fn<() => { data?: BullishBaseResponse; isLoading: boolean; isError: boolean }>();
vi.mock("@/hooks/use-bullish-base", () => ({ useBullishBase: () => mockBB() }));

beforeEach(() => mockBB.mockReset());

function bbData(over: Partial<BullishBaseResponse> = {}): BullishBaseResponse {
  return {
    detected: false,
    direction: null,
    quality: "NONE",
    signal_close: null,
    entry: null,
    stop: null,
    target: null,
    risk_pct: null,
    rr: null,
    sma20: 138,
    sma50: 140,
    sma200: 189,
    obv: 1000,
    macd: -1.2,
    eyeball_checks: [],
    mark_says: "",
    is_mock: false,
    ...over,
  };
}

describe("BullishBaseCard (Carr s.291 contrarian)", () => {
  it("isLoading → 'Bullish Base yükleniyor'", () => {
    mockBB.mockReturnValue({ data: undefined, isLoading: true, isError: false });
    render(<BullishBaseCard symbol="PFE" />);
    expect(screen.getByText(/Bullish Base yükleniyor/)).toBeInTheDocument();
  });

  it("isError → null", () => {
    mockBB.mockReturnValue({ data: undefined, isLoading: false, isError: true });
    const { container } = render(<BullishBaseCard symbol="PFE" />);
    expect(container.firstChild).toBeNull();
  });

  it("detected=false → 'Aday yok' + SMA200 bağlamı", () => {
    mockBB.mockReturnValue({ data: bbData(), isLoading: false, isError: false });
    render(<BullishBaseCard symbol="PFE" />);
    expect(screen.getByText("Aday yok")).toBeInTheDocument();
    expect(screen.getByText("SMA200")).toBeInTheDocument();
  });

  it("detected CANDIDATE → 'LONG ADAYI' + Giriş (close) + eyeball checklist", () => {
    mockBB.mockReturnValue({
      data: bbData({
        detected: true,
        direction: "LONG",
        quality: "CANDIDATE",
        signal_close: 138,
        entry: 138,
        stop: 129.72,
        target: 154.56,
        risk_pct: 6.0,
        rr: 2.0,
        eyeball_checks: [
          "Base tipi: falling wedge / expanding triangle / rectangle olmalı",
          "RISING WEDGE OLMAZ: destek+direnç ikisi birden yukarı bakmamalı",
          "Konsolidasyon en az 30 gün",
        ],
        mark_says: "Bullish Base Breakout LONG ADAYI",
      }),
      isLoading: false,
      isError: false,
    });
    render(<BullishBaseCard symbol="PFE" />);
    expect(screen.getByText("🟡 LONG ADAYI (contrarian — göz kararı şart)")).toBeInTheDocument();
    expect(screen.getByText("Giriş (close)")).toBeInTheDocument(); // entry=close (kırılım beklenmez)
    expect(screen.getByText(/RISING WEDGE/)).toBeInTheDocument(); // eyeball checklist
  });

  it("is_mock → sentetik / <200 bar banner (Kural #28)", () => {
    mockBB.mockReturnValue({ data: bbData({ is_mock: true }), isLoading: false, isError: false });
    render(<BullishBaseCard symbol="PFE" />);
    expect(screen.getByText(/Sentetik/)).toBeInTheDocument();
  });

  // P531 (Kural #28): paper trade köprü wiring — is_mock=data.is_mock geçiyor mu
  it("detected + gerçek veri + onPaperTrade → paper trade butonu görünür", () => {
    mockBB.mockReturnValue({
      data: bbData({ detected: true, direction: "LONG", quality: "CANDIDATE",
        entry: 138, stop: 129.72, target: 154.56, risk_pct: 6.0, rr: 2.0, is_mock: false }),
      isLoading: false, isError: false,
    });
    render(<BullishBaseCard symbol="PFE" onPaperTrade={vi.fn()} />);
    expect(screen.getByTestId("carr-paper-trade-btn")).toBeInTheDocument();
  });

  it("detected + is_mock + onPaperTrade → paper trade butonu GİZLİ (Kural #28)", () => {
    mockBB.mockReturnValue({
      data: bbData({ detected: true, direction: "LONG", quality: "CANDIDATE",
        entry: 138, stop: 129.72, target: 154.56, risk_pct: 6.0, rr: 2.0, is_mock: true }),
      isLoading: false, isError: false,
    });
    render(<BullishBaseCard symbol="PFE" onPaperTrade={vi.fn()} />);
    expect(screen.queryByTestId("carr-paper-trade-btn")).toBeNull();
  });
});
