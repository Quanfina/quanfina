/**
 * VDryCard — V-Dry hacim kuruması sınıflandırması (Minervini s.150 "Quiet Action").
 * 3 seviye: STRONG (Güçlü) / IDEAL (İdeal) / WEAK (Zayıf).
 */
import { describe, it, expect, vi, beforeEach } from "vitest";
import { render, screen } from "@testing-library/react";
import { VDryCard } from "@/components/stock/VDryCard";
import type { VDryInfo, VDryClass } from "@/hooks/use-v-dry";

const mockVDry = vi.fn<() => { data?: VDryInfo; isLoading: boolean; isError: boolean }>();

vi.mock("@/hooks/use-v-dry", () => ({ useVDry: () => mockVDry() }));

beforeEach(() => {
  mockVDry.mockReset();
});

function vDryData(cls: VDryClass, label: string, ratio: number): VDryInfo {
  return {
    v_dry_class: cls,
    label,
    vol_ratio: ratio,
    says: `${label} kuruma — son 5g ort. hacim 50g ort.'nin %${(ratio * 100).toFixed(0)}'i.`,
  };
}

describe("VDryCard — 3 seviye (Minervini s.150)", () => {
  it("isLoading → 'Hacim kuruması yükleniyor...'", () => {
    mockVDry.mockReturnValue({ data: undefined, isLoading: true, isError: false });
    render(<VDryCard symbol="AAPL" />);
    expect(screen.getByText(/Hacim kuruması yükleniyor/)).toBeInTheDocument();
  });

  it("isError → null", () => {
    mockVDry.mockReturnValue({ data: undefined, isLoading: false, isError: true });
    const { container } = render(<VDryCard symbol="AAPL" />);
    expect(container.firstChild).toBeNull();
  });

  it("v_dry_class null → null", () => {
    mockVDry.mockReturnValue({
      data: { v_dry_class: null, label: null, vol_ratio: null, says: "Yetersiz veri." },
      isLoading: false,
      isError: false,
    });
    const { container } = render(<VDryCard symbol="AAPL" />);
    expect(container.firstChild).toBeNull();
  });

  it("STRONG → 'Güçlü' rozet + %43 oran", () => {
    mockVDry.mockReturnValue({ data: vDryData("STRONG", "Güçlü", 0.43), isLoading: false, isError: false });
    render(<VDryCard symbol="AAPL" />);
    expect(screen.getByText("Güçlü")).toBeInTheDocument();
    expect(screen.getByText("43%")).toBeInTheDocument();
    expect(screen.getByText(/Quiet Action/)).toBeInTheDocument();
  });

  it("IDEAL → 'İdeal' rozet", () => {
    mockVDry.mockReturnValue({ data: vDryData("IDEAL", "İdeal", 0.62), isLoading: false, isError: false });
    render(<VDryCard symbol="AAPL" />);
    expect(screen.getByText("İdeal")).toBeInTheDocument();
    expect(screen.getByText("62%")).toBeInTheDocument();
  });

  it("WEAK → 'Zayıf' rozet", () => {
    mockVDry.mockReturnValue({ data: vDryData("WEAK", "Zayıf", 0.94), isLoading: false, isError: false });
    render(<VDryCard symbol="AAPL" />);
    expect(screen.getByText("Zayıf")).toBeInTheDocument();
    expect(screen.getByText("94%")).toBeInTheDocument();
  });
});
