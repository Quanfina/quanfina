/**
 * MarketDataMockBanner (P483 #1, Kural #28) — karar-kritik şerit MOCK uyarısı.
 * Sadece index/VIX/sektör canlı alınamadığında görünür; canlı veride null.
 */
import { describe, it, expect } from "vitest";
import { render, screen } from "@testing-library/react";
import { MarketDataMockBanner } from "@/components/market/MarketDataMockBanner";

describe("MarketDataMockBanner", () => {
  it("canlı veri (3 flag false) → hiçbir şey render etmez", () => {
    const { container } = render(
      <MarketDataMockBanner
        status={{ index_is_mock: false, vix_is_mock: false, sectors_is_mock: false }}
      />,
    );
    expect(container.firstChild).toBeNull();
    expect(screen.queryByTestId("market-mock-banner")).toBeNull();
  });

  it("flag'ler undefined → render etmez (geriye uyum)", () => {
    const { container } = render(<MarketDataMockBanner status={{}} />);
    expect(container.firstChild).toBeNull();
  });

  it("index_is_mock → banner görünür, sadece 'endeks' parçası", () => {
    render(<MarketDataMockBanner status={{ index_is_mock: true }} />);
    const el = screen.getByTestId("market-mock-banner");
    expect(el.textContent).toContain("endeks");
    expect(el.textContent).not.toContain("VIX");
    expect(el.textContent).not.toContain("sektörler");
  });

  it("3 flag de true → 3 parça da listelenir", () => {
    render(
      <MarketDataMockBanner
        status={{ index_is_mock: true, vix_is_mock: true, sectors_is_mock: true }}
      />,
    );
    const el = screen.getByTestId("market-mock-banner");
    expect(el.textContent).toContain("endeks");
    expect(el.textContent).toContain("VIX");
    expect(el.textContent).toContain("sektörler");
  });
});
