/**
 * DataFreshnessBanner (P375) — tarama veri bayatlık uyarısı.
 *
 * Sn. Ferit "14 gün eski veri" acısı: is_stale=true → kırmızı banner; taze/yükleniyor
 * → gizli (DbStatusBanner pateni). useScanFreshness mock.
 */
import { describe, it, expect, vi, beforeEach } from "vitest";
import { render, screen } from "@testing-library/react";
import { DataFreshnessBanner } from "@/components/layout/DataFreshnessBanner";
import type { ScanFreshness } from "@/hooks/use-scan-freshness";

const mockHook = vi.fn<() => { data?: ScanFreshness }>();
vi.mock("@/hooks/use-scan-freshness", () => ({
  useScanFreshness: () => mockHook(),
}));

beforeEach(() => mockHook.mockReset());

function fresh(o: Partial<ScanFreshness> = {}): ScanFreshness {
  return {
    latest_scan_date: "2026-05-29", is_stale: false, calendar_days_old: 0,
    threshold_days: 4, message: "Tarama guncel", ...o,
  };
}

describe("DataFreshnessBanner", () => {
  it("is_stale=true → kırmızı banner + tarih + gün gösterilir", () => {
    mockHook.mockReturnValue({
      data: fresh({ is_stale: true, latest_scan_date: "2026-05-22", calendar_days_old: 7 }),
    });
    render(<DataFreshnessBanner />);
    expect(screen.getByRole("alert")).toBeInTheDocument();
    expect(screen.getByText(/Tarama verisi bayat/)).toBeInTheDocument();
    expect(screen.getByText(/2026-05-22/)).toBeInTheDocument();
    expect(screen.getByText(/7 gün önce/)).toBeInTheDocument();
  });

  it("is_stale=false (taze) → banner GİZLİ (null)", () => {
    mockHook.mockReturnValue({ data: fresh({ is_stale: false }) });
    const { container } = render(<DataFreshnessBanner />);
    expect(container.firstChild).toBeNull();
  });

  it("data yok (yükleniyor) → banner gizli (flicker önleme)", () => {
    mockHook.mockReturnValue({ data: undefined });
    const { container } = render(<DataFreshnessBanner />);
    expect(container.firstChild).toBeNull();
  });

  it("objektif ayna dil — yağcılık/his yok, somut tarih+gün", () => {
    mockHook.mockReturnValue({
      data: fresh({ is_stale: true, latest_scan_date: "2026-05-20", calendar_days_old: 9 }),
    });
    render(<DataFreshnessBanner />);
    const text = document.body.textContent ?? "";
    expect(text).toMatch(/trade kararı vermeyin/);   // direktif (aksiyon), his değil
    expect(text).not.toMatch(/üzülme|merak etme/i);
  });
});
