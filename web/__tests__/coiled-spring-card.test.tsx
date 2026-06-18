/**
 * CoiledSpringCard (Carr 2.baskı s.250) — P510 strateji inşası.
 * detected CANDIDATE (entry/stop/target/2R + eyeball checklist) + aday yok (SMA bağlamı) +
 * is_mock + loading/error. TIER-2 eyeball ayırt edici.
 */
import { describe, it, expect, vi, beforeEach } from "vitest";
import { render, screen } from "@testing-library/react";
import { CoiledSpringCard } from "@/components/stock/CoiledSpringCard";
import type { CoiledSpringResponse } from "@/hooks/use-coiled-spring";

const mockCS = vi.fn<() => { data?: CoiledSpringResponse; isLoading: boolean; isError: boolean }>();
vi.mock("@/hooks/use-coiled-spring", () => ({ useCoiledSpring: () => mockCS() }));

beforeEach(() => mockCS.mockReset());

function csData(over: Partial<CoiledSpringResponse> = {}): CoiledSpringResponse {
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
    sma20: 159,
    sma50: 150.3,
    eyeball_checks: [],
    mark_says: "",
    is_mock: false,
    ...over,
  };
}

describe("CoiledSpringCard (Carr s.250 daralan yay)", () => {
  it("isLoading → 'Coiled Spring yükleniyor'", () => {
    mockCS.mockReturnValue({ data: undefined, isLoading: true, isError: false });
    render(<CoiledSpringCard symbol="AAPL" />);
    expect(screen.getByText(/Coiled Spring yükleniyor/)).toBeInTheDocument();
  });

  it("isError → null", () => {
    mockCS.mockReturnValue({ data: undefined, isLoading: false, isError: true });
    const { container } = render(<CoiledSpringCard symbol="AAPL" />);
    expect(container.firstChild).toBeNull();
  });

  it("detected=false → 'Aday yok' + SMA bağlamı", () => {
    mockCS.mockReturnValue({ data: csData(), isLoading: false, isError: false });
    render(<CoiledSpringCard symbol="AAPL" />);
    expect(screen.getByText("Aday yok")).toBeInTheDocument();
    expect(screen.getByText("SMA20")).toBeInTheDocument();
    expect(screen.getByText("SMA50")).toBeInTheDocument();
  });

  it("detected CANDIDATE → 'LONG ADAYI' + eyeball checklist + Giriş/Hedef", () => {
    mockCS.mockReturnValue({
      data: csData({
        detected: true,
        direction: "LONG",
        quality: "CANDIDATE",
        signal_close: 159.1,
        entry: 159.15,
        stop: 147.3,
        target: 182.85,
        risk_pct: 7.44,
        rr: 2.0,
        eyeball_checks: [
          "Daralma: fiyat aralığı sağa doğru daralıyor mu?",
          "Açı: formasyon YUKARI eğimli OLMAMALI",
          "50MA teması: hiçbir kısım 50MA'ya değmemeli",
        ],
        mark_says: "Coiled Spring LONG ADAYI",
      }),
      isLoading: false,
      isError: false,
    });
    render(<CoiledSpringCard symbol="AAPL" />);
    expect(screen.getByText("🟡 LONG ADAYI (göz kararı şart)")).toBeInTheDocument();
    expect(screen.getByText("Giriş (signal high)")).toBeInTheDocument();
    expect(screen.getByText(/Daralma:/)).toBeInTheDocument(); // eyeball checklist render
  });

  it("is_mock → sentetik / <60 bar banner (Kural #28)", () => {
    mockCS.mockReturnValue({ data: csData({ is_mock: true }), isLoading: false, isError: false });
    render(<CoiledSpringCard symbol="AAPL" />);
    expect(screen.getByText(/Sentetik/)).toBeInTheDocument();
  });
});
