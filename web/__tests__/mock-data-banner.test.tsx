/**
 * P418 (31 May 2026): MockDataBanner DRY component testleri.
 * 4 sayfada paylaşılan banner — render kontratı + conditional davranış.
 */
import { describe, it, expect } from "vitest";
import { render, screen } from "@testing-library/react";
import { MockDataBanner } from "@/components/shared/MockDataBanner";

describe("MockDataBanner — conditional render", () => {
  it("isMock=false → null (render etmez)", () => {
    const { container } = render(<MockDataBanner isMock={false} />);
    expect(container.firstChild).toBeNull();
  });

  it("isMock=undefined → null (loading / hata durumu)", () => {
    const { container } = render(<MockDataBanner isMock={undefined} />);
    expect(container.firstChild).toBeNull();
  });

  it("isMock=true → sarı banner görünür", () => {
    render(<MockDataBanner isMock={true} />);
    expect(screen.getByTestId("mock-data-banner")).toBeInTheDocument();
    expect(screen.getByText(/MOCK VERİ:/i)).toBeInTheDocument();
    expect(screen.getByText(/Cloud SQL erişilemez/)).toBeInTheDocument();
  });

  it("context prop banner metnine eklenir", () => {
    render(<MockDataBanner isMock={true} context="Win Rate" />);
    expect(screen.getByText(/Win Rate hesabı yanıltıcı/)).toBeInTheDocument();
  });

  it("testId override çalışır (sayfa-bazlı seçici)", () => {
    render(<MockDataBanner isMock={true} testId="journal-mock-banner" />);
    expect(screen.getByTestId("journal-mock-banner")).toBeInTheDocument();
  });

  it("role='status' a11y attr (screen reader anonsu)", () => {
    render(<MockDataBanner isMock={true} />);
    const banner = screen.getByRole("status");
    expect(banner).toBeInTheDocument();
  });

  it("Kural #28 referansı banner metninde", () => {
    render(<MockDataBanner isMock={true} />);
    expect(screen.getByText(/Kural #28 audit P418/)).toBeInTheDocument();
  });
});
