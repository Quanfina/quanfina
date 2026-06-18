/**
 * GapDownCard (Carr 2.baskı s.273-274) — P520 strateji inşası (SHORT reversal).
 * detected SHORT CANDIDATE (entry=close + haber eyeball) + aday yok (SMA/gap bağlamı) +
 * is_mock + loading/error.
 */
import { describe, it, expect, vi, beforeEach } from "vitest";
import { render, screen } from "@testing-library/react";
import { GapDownCard } from "@/components/stock/GapDownCard";
import type { GapDownResponse } from "@/hooks/use-gap-down";

const mockGD = vi.fn<() => { data?: GapDownResponse; isLoading: boolean; isError: boolean }>();
vi.mock("@/hooks/use-gap-down", () => ({ useGapDown: () => mockGD() }));

beforeEach(() => mockGD.mockReset());

function gdData(over: Partial<GapDownResponse> = {}): GapDownResponse {
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
    sma50: 150,
    gap_pct: -2.0,
    eyeball_checks: [],
    mark_says: "",
    is_mock: false,
    ...over,
  };
}

describe("GapDownCard (Carr s.273 SHORT reversal)", () => {
  it("isLoading → 'Gap Down yükleniyor'", () => {
    mockGD.mockReturnValue({ data: undefined, isLoading: true, isError: false });
    render(<GapDownCard symbol="STLD" />);
    expect(screen.getByText(/Gap Down yükleniyor/)).toBeInTheDocument();
  });

  it("isError → null", () => {
    mockGD.mockReturnValue({ data: undefined, isLoading: false, isError: true });
    const { container } = render(<GapDownCard symbol="STLD" />);
    expect(container.firstChild).toBeNull();
  });

  it("detected=false → 'Aday yok' + SMA/gap bağlamı", () => {
    mockGD.mockReturnValue({ data: gdData(), isLoading: false, isError: false });
    render(<GapDownCard symbol="STLD" />);
    expect(screen.getByText("Aday yok")).toBeInTheDocument();
    expect(screen.getByText("SMA50")).toBeInTheDocument();
    expect(screen.getByText("Gap %")).toBeInTheDocument();
  });

  it("detected SHORT → '🔴 SHORT ADAYI' + Giriş(close) + haber eyeball", () => {
    mockGD.mockReturnValue({
      data: gdData({
        detected: true,
        direction: "SHORT",
        quality: "CANDIDATE",
        signal_close: 160,
        entry: 160,
        stop: 169.6,
        target: 140.8,
        risk_pct: 6.0,
        rr: 2.0,
        gap_pct: 1.3,
        eyeball_checks: [
          "HABER TEYİDİ şart: gap şirkete özel KÖTÜ haberden mi?",
          "Uzun ralli sonu reversal",
        ],
        mark_says: "Gap Down SHORT ADAYI",
      }),
      isLoading: false,
      isError: false,
    });
    render(<GapDownCard symbol="STLD" />);
    expect(screen.getByText("🔴 SHORT ADAYI (haber teyidi şart)")).toBeInTheDocument();
    expect(screen.getByText("Giriş (close)")).toBeInTheDocument();
    expect(screen.getByText(/HABER TEYİDİ/)).toBeInTheDocument();
  });

  it("is_mock → sentetik / <110 bar banner (Kural #28)", () => {
    mockGD.mockReturnValue({ data: gdData({ is_mock: true }), isLoading: false, isError: false });
    render(<GapDownCard symbol="STLD" />);
    expect(screen.getByText(/Sentetik/)).toBeInTheDocument();
  });
});
