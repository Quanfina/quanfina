/**
 * BlueSkyCard (Carr 2.baskı s.264-265 entry + s.324-325 exit) — P508 strateji inşası.
 * detected LONG (entry/stop/target/2R) + sinyal yok (40g/52h yüksek bağlamı) + is_mock + loading/error.
 */
import { describe, it, expect, vi, beforeEach } from "vitest";
import { render, screen } from "@testing-library/react";
import { BlueSkyCard } from "@/components/stock/BlueSkyCard";
import type { BlueSkyResponse } from "@/hooks/use-blue-sky";

const mockBS = vi.fn<() => { data?: BlueSkyResponse; isLoading: boolean; isError: boolean }>();
vi.mock("@/hooks/use-blue-sky", () => ({ useBlueSky: () => mockBS() }));

beforeEach(() => mockBS.mockReset());

function bsData(over: Partial<BlueSkyResponse> = {}): BlueSkyResponse {
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
    high_40d: 280,
    high_260d: 299,
    low_260d: 219.5,
    obv: 1000,
    macd: 1.2,
    mark_says: "",
    is_mock: false,
    ...over,
  };
}

describe("BlueSkyCard (Carr s.264 breakout)", () => {
  it("isLoading → 'Blue Sky Breakout yükleniyor'", () => {
    mockBS.mockReturnValue({ data: undefined, isLoading: true, isError: false });
    render(<BlueSkyCard symbol="PLTR" />);
    expect(screen.getByText(/Blue Sky Breakout yükleniyor/)).toBeInTheDocument();
  });

  it("isError → null", () => {
    mockBS.mockReturnValue({ data: undefined, isLoading: false, isError: true });
    const { container } = render(<BlueSkyCard symbol="PLTR" />);
    expect(container.firstChild).toBeNull();
  });

  it("detected=false → 'Sinyal yok' + 40g/52h yüksek bağlamı", () => {
    mockBS.mockReturnValue({ data: bsData(), isLoading: false, isError: false });
    render(<BlueSkyCard symbol="PLTR" />);
    expect(screen.getByText("Sinyal yok")).toBeInTheDocument();
    expect(screen.getByText("40g Yüksek")).toBeInTheDocument();
    expect(screen.getByText("52h Yüksek")).toBeInTheDocument();
  });

  it("detected LONG → '🟢 LONG sinyal (Blue Sky)' + Giriş/Stop/Hedef(2R)", () => {
    mockBS.mockReturnValue({
      data: bsData({
        detected: true,
        direction: "LONG",
        quality: "GOOD",
        signal_close: 288,
        entry: 288.5,
        stop: 271.19,
        target: 323.12,
        risk_pct: 6.0,
        rr: 2.0,
        mark_says: "Blue Sky Breakout LONG",
      }),
      isLoading: false,
      isError: false,
    });
    render(<BlueSkyCard symbol="PLTR" />);
    expect(screen.getByText("🟢 LONG sinyal (Blue Sky)")).toBeInTheDocument();
    expect(screen.getByText("Giriş (signal high)")).toBeInTheDocument();
    expect(screen.getByText("Hedef (2R)")).toBeInTheDocument();
  });

  it("is_mock → sentetik / <261 bar banner (Kural #28)", () => {
    mockBS.mockReturnValue({ data: bsData({ is_mock: true }), isLoading: false, isError: false });
    render(<BlueSkyCard symbol="PLTR" />);
    expect(screen.getByText(/Sentetik/)).toBeInTheDocument();
  });
});
