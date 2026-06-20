/**
 * GroupRSCard (P553) — Grup/Endüstri RS onayı (Minervini s.95 + O'Neil 'L').
 * LEADING (yeşil) / LAGGING "yalnız kurt" (kırmızı) / NEUTRAL (amber) / veri-yok / loading-error.
 */
import { describe, it, expect, vi, beforeEach } from "vitest";
import { render, screen } from "@testing-library/react";
import { GroupRSCard } from "@/components/stock/GroupRSCard";
import type { GroupRSResponse } from "@/hooks/use-group-rs";

const mockGR = vi.fn<() => { data?: GroupRSResponse; isLoading: boolean; isError: boolean }>();
vi.mock("@/hooks/use-group-rs", () => ({ useGroupRS: () => mockGR() }));

beforeEach(() => mockGR.mockReset());

function gr(over: Partial<GroupRSResponse> = {}): GroupRSResponse {
  return {
    symbol: "NVDA",
    available: true,
    sector: "Technology",
    rs_rank: 1,
    total: 11,
    tier: "LEADING",
    is_confirmed: true,
    is_lone_wolf: false,
    mark_says: "Sektor LIDER (Technology #1/11). Guclu hisse + guclu grup = Minervini s.95 teyidi.",
    ...over,
  };
}

describe("GroupRSCard (Grup RS — sektör teyidi)", () => {
  it("isLoading → yükleniyor", () => {
    mockGR.mockReturnValue({ data: undefined, isLoading: true, isError: false });
    render(<GroupRSCard symbol="NVDA" />);
    expect(screen.getByText(/Sektör RS yükleniyor/)).toBeInTheDocument();
  });

  it("isError → null", () => {
    mockGR.mockReturnValue({ data: undefined, isLoading: false, isError: true });
    const { container } = render(<GroupRSCard symbol="NVDA" />);
    expect(container.firstChild).toBeNull();
  });

  it("LEADING → lider sektör + rank gösterir", () => {
    mockGR.mockReturnValue({ data: gr(), isLoading: false, isError: false });
    render(<GroupRSCard symbol="NVDA" />);
    expect(screen.getByText(/LİDER SEKTÖR/)).toBeInTheDocument();
    // rank hem span hem mark_says'te geçer → getAllByText
    expect(screen.getAllByText(/Technology #1\/11/).length).toBeGreaterThan(0);
  });

  it("LAGGING → 'yalnız kurt' uyarısı", () => {
    mockGR.mockReturnValue({
      data: gr({
        sector: "Communication Services", rs_rank: 11, tier: "LAGGING",
        is_confirmed: false, is_lone_wolf: true,
        mark_says: "Sektor ZAYIF (Communication Services #11/11). 'Yalniz kurt' riski (s.95).",
      }),
      isLoading: false, isError: false,
    });
    render(<GroupRSCard symbol="GOOGL" />);
    expect(screen.getByText(/YALNIZ KURT/)).toBeInTheDocument();
    expect(screen.getAllByText(/#11\/11/).length).toBeGreaterThan(0);
  });

  it("NEUTRAL → nötr sektör", () => {
    mockGR.mockReturnValue({
      data: gr({ sector: "Real Estate", rs_rank: 6, tier: "NEUTRAL", is_confirmed: false }),
      isLoading: false, isError: false,
    });
    render(<GroupRSCard symbol="O" />);
    expect(screen.getByText(/NÖTR SEKTÖR/)).toBeInTheDocument();
  });

  it("veri yok → mark_says gösterir, rank yok (MOCK YOK)", () => {
    mockGR.mockReturnValue({
      data: gr({
        available: false, sector: null, rs_rank: null, tier: null,
        is_confirmed: false, is_lone_wolf: false,
        mark_says: "Sektor RS verisi yok (sektor bilinmiyor veya sector_rotation erisilmez).",
      }),
      isLoading: false, isError: false,
    });
    render(<GroupRSCard symbol="ZZZZ" />);
    expect(screen.getByText(/Sektor RS verisi yok/)).toBeInTheDocument();
    expect(screen.queryByText(/#/)).toBeNull();
  });
});
