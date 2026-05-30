/**
 * P400: usePortfolioValue hook + PortfolioValueEditor UI uctan uca testi.
 *
 * localStorage persist + state senkron + UI edit/cancel/commit akisi.
 */
import { describe, it, expect, beforeEach } from "vitest";
import { render, screen, fireEvent } from "@testing-library/react";
import { renderHook, act } from "@testing-library/react";
import { usePortfolioValue } from "@/hooks/use-portfolio-value";
import { PortfolioValueEditor } from "@/components/journal/PortfolioValueEditor";
import {
  DEFAULT_PORTFOLIO_VALUE,
  setPortfolioValue,
} from "@/lib/portfolio-settings";

beforeEach(() => {
  window.localStorage.clear();
});


describe("usePortfolioValue hook", () => {
  it("Bos localStorage -> default $100K initial", () => {
    const { result } = renderHook(() => usePortfolioValue());
    expect(result.current.value).toBe(DEFAULT_PORTFOLIO_VALUE);
    expect(result.current.defaultValue).toBe(DEFAULT_PORTFOLIO_VALUE);
  });

  it("Kayitli deger initial state'e geliyor", () => {
    setPortfolioValue(60000);
    const { result } = renderHook(() => usePortfolioValue());
    expect(result.current.value).toBe(60000);
  });

  it("setValue cagirisi -> state + localStorage senkron", () => {
    const { result } = renderHook(() => usePortfolioValue());
    act(() => result.current.setValue(45000));
    expect(result.current.value).toBe(45000);
    expect(parseFloat(window.localStorage.getItem("quanfina:portfolio_value")!)).toBe(45000);
  });

  it("Negatif/sifir reddedilir (state degismez)", () => {
    const { result } = renderHook(() => usePortfolioValue());
    act(() => result.current.setValue(100000));
    act(() => result.current.setValue(-50));
    expect(result.current.value).toBe(100000);
  });
});


describe("PortfolioValueEditor — UI render", () => {
  it("Default modda 'Portföy: $100,000 (varsayılan)' goster", () => {
    render(<PortfolioValueEditor />);
    const btn = screen.getByTestId("portfolio-value-editor");
    expect(btn.textContent).toContain("Portföy:");
    expect(btn.textContent).toContain("$100,000");
    expect(btn.textContent).toContain("varsayılan");
  });

  it("Kayitli deger varsa varsayilan etiketi gozukmemeli", () => {
    setPortfolioValue(75000);
    render(<PortfolioValueEditor />);
    const btn = screen.getByTestId("portfolio-value-editor");
    expect(btn.textContent).toContain("$75,000");
    expect(btn.textContent).not.toContain("varsayılan");
  });
});


describe("PortfolioValueEditor — edit/commit/cancel", () => {
  it("Tıklayinca input gozukur, yeni deger gir + check tikla -> kaydet", () => {
    render(<PortfolioValueEditor />);
    fireEvent.click(screen.getByTestId("portfolio-value-editor"));
    const input = screen.getByLabelText("Portföy büyüklüğü ($)") as HTMLInputElement;
    fireEvent.change(input, { target: { value: "50000" } });
    fireEvent.click(screen.getByLabelText("Kaydet"));
    // localStorage persist + UI yeni deger
    expect(parseFloat(window.localStorage.getItem("quanfina:portfolio_value")!)).toBe(50000);
    expect(screen.getByTestId("portfolio-value-editor").textContent).toContain("$50,000");
  });

  it("Enter tusu -> commit", () => {
    render(<PortfolioValueEditor />);
    fireEvent.click(screen.getByTestId("portfolio-value-editor"));
    const input = screen.getByLabelText("Portföy büyüklüğü ($)") as HTMLInputElement;
    fireEvent.change(input, { target: { value: "200000" } });
    fireEvent.keyDown(input, { key: "Enter" });
    expect(screen.getByTestId("portfolio-value-editor").textContent).toContain("$200,000");
  });

  it("ESC tusu -> cancel, deger degismez", () => {
    setPortfolioValue(80000);
    render(<PortfolioValueEditor />);
    fireEvent.click(screen.getByTestId("portfolio-value-editor"));
    const input = screen.getByLabelText("Portföy büyüklüğü ($)") as HTMLInputElement;
    fireEvent.change(input, { target: { value: "999" } });
    fireEvent.keyDown(input, { key: "Escape" });
    expect(screen.getByTestId("portfolio-value-editor").textContent).toContain("$80,000");
  });

  it("Bozuk deger (negatif) -> commit reddedilir, eski deger korunur", () => {
    setPortfolioValue(100000);
    render(<PortfolioValueEditor />);
    fireEvent.click(screen.getByTestId("portfolio-value-editor"));
    const input = screen.getByLabelText("Portföy büyüklüğü ($)") as HTMLInputElement;
    fireEvent.change(input, { target: { value: "-50" } });
    fireEvent.click(screen.getByLabelText("Kaydet"));
    // localStorage degismeli (negatif reddedildi)
    expect(parseFloat(window.localStorage.getItem("quanfina:portfolio_value")!)).toBe(100000);
  });
});
