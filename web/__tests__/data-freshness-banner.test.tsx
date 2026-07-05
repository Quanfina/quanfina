/**
 * DataFreshnessBanner (P375 + B1-03) — tarama veri bayatlık uyarısı.
 *
 * Sn. Ferit "14 gün eski veri" acısı: any_stale=true → kırmızı banner (kaynak-adlı
 * backend mesajı); taze/yükleniyor → gizli (DbStatusBanner pateni). useScanFreshness mock.
 *
 * B1-03 (05 Tem 2026): çok-tablo. Banner artık `any_stale` (herhangi biri bayat) +
 * backend `message` (kaynak-adlı) sürücüsü. Kritik senaryo: yalnız sector bayat →
 * minervini taze olsa bile banner AÇIK, minervini'nin taze tarihi problem gibi gösterilmez.
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
    threshold_days: 4, message: "Tarama guncel: Hisse taraması 2026-05-29 (0 gün önce).",
    any_stale: false, ...o,
  };
}

describe("DataFreshnessBanner", () => {
  it("any_stale=true (minervini bayat) → kırmızı banner + kaynak-adlı mesaj", () => {
    mockHook.mockReturnValue({
      data: fresh({
        is_stale: true, any_stale: true,
        latest_scan_date: "2026-05-22", calendar_days_old: 7,
        message: "Hisse taraması BAYAT (2026-05-22, 7 gün önce) — esik 4 gun. Cloud Run scanner kontrol et.",
      }),
    });
    render(<DataFreshnessBanner />);
    expect(screen.getByRole("alert")).toBeInTheDocument();
    expect(screen.getByText(/Tarama verisi bayat/)).toBeInTheDocument();
    expect(screen.getByText(/Hisse taraması BAYAT \(2026-05-22, 7 gün önce\)/)).toBeInTheDocument();
  });

  it("YALNIZ sector bayat (minervini TAZE) → any_stale sürücüsü ile RED, sector-adlı mesaj", () => {
    // Kritik B1-03: is_stale=false (minervini taze) olsa bile any_stale=true → banner AÇIK.
    // Eski davranış (is_stale sürücüsü) bu durumu GİZLERDİ → sector ölümü sessiz kalırdı (H#17).
    mockHook.mockReturnValue({
      data: fresh({
        is_stale: false,                 // minervini TAZE
        latest_scan_date: "2026-07-03", calendar_days_old: 2,
        any_stale: true,                 // sector bayat
        message: "Sektör rotasyonu BAYAT (2026-06-15, 20 gün önce) — esik 4 gun. Cloud Run scanner kontrol et.",
        sources: [
          { table: "minervini_scans", label: "Hisse taraması", latest_scan_date: "2026-07-03", calendar_days_old: 2, is_stale: false },
          { table: "sector_rotation", label: "Sektör rotasyonu", latest_scan_date: "2026-06-15", calendar_days_old: 20, is_stale: true },
        ],
      }),
    });
    render(<DataFreshnessBanner />);
    expect(screen.getByRole("alert")).toBeInTheDocument();
    expect(screen.getByText(/Sektör rotasyonu BAYAT/)).toBeInTheDocument();
    // minervini'nin TAZE tarihi (2026-07-03) problem gibi sunulmaz (kafa karışıklığı önleme)
    expect(document.body.textContent ?? "").not.toMatch(/2026-07-03/);
  });

  it("any_stale=false (hepsi taze) → banner GİZLİ (null)", () => {
    mockHook.mockReturnValue({ data: fresh({ is_stale: false, any_stale: false }) });
    const { container } = render(<DataFreshnessBanner />);
    expect(container.firstChild).toBeNull();
  });

  it("geriye-uyum: eski API (any_stale undefined) + is_stale=true → RED (is_stale fallback)", () => {
    // api deploy beklerken frontend eski response alabilir → any_stale yok → is_stale sürücü.
    const old = fresh({ is_stale: true, calendar_days_old: 8,
      message: "Tarama BAYAT: son tarama 2026-05-20, 8 gun once." });
    delete (old as Partial<ScanFreshness>).any_stale;
    mockHook.mockReturnValue({ data: old });
    render(<DataFreshnessBanner />);
    expect(screen.getByRole("alert")).toBeInTheDocument();
  });

  it("data yok (yükleniyor) → banner gizli (flicker önleme)", () => {
    mockHook.mockReturnValue({ data: undefined });
    const { container } = render(<DataFreshnessBanner />);
    expect(container.firstChild).toBeNull();
  });

  it("objektif ayna dil — yağcılık/his yok, aksiyon direktifi var", () => {
    mockHook.mockReturnValue({
      data: fresh({ is_stale: true, any_stale: true,
        message: "Hisse taraması BAYAT (2026-05-20, 9 gün önce) — esik 4 gun. Cloud Run scanner kontrol et." }),
    });
    render(<DataFreshnessBanner />);
    const text = document.body.textContent ?? "";
    expect(text).toMatch(/trade kararı vermeyin/);   // direktif (aksiyon), his değil
    expect(text).not.toMatch(/üzülme|merak etme/i);
  });
});
