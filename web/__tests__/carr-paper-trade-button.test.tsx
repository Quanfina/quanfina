/**
 * CarrPaperTradeButton (P529 köprü + P531 is_mock guard).
 *
 * Buton mantığının tek-kaynak testi (DRY — 6 kart tek tek test etmek yerine):
 * - onPaperTrade yoksa görünmez (backward-compatible)
 * - onPaperTrade + gerçek veri → görünür, tıklayınca data ile çağrılır
 * - isMock=true → görünmez (Kural #28: sentetik veriyle paper trade açılmaz)
 */
import { describe, it, expect, vi } from "vitest";
import { render, screen, fireEvent } from "@testing-library/react";
import { CarrPaperTradeButton } from "@/components/stock/CarrPaperTradeButton";
import type { InitialData } from "@/components/journal/AddTradeDialog";

const sampleData: InitialData = {
  symbol: "RXO",
  strategy: "carr",
  setup_type: "pullback",
  entry_price: 25.32,
  plan_stop: 23.97,
  plan_target: 30.22,
  plan_entry_trigger: "Carr Pullback (s.249)",
};

const BTN = "carr-paper-trade-btn";

describe("CarrPaperTradeButton", () => {
  it("onPaperTrade yoksa görünmez (backward-compatible)", () => {
    render(<CarrPaperTradeButton data={sampleData} />);
    expect(screen.queryByTestId(BTN)).toBeNull();
  });

  it("onPaperTrade + gerçek veri → görünür, tıklayınca data ile çağrılır", () => {
    const onPaperTrade = vi.fn();
    render(<CarrPaperTradeButton data={sampleData} onPaperTrade={onPaperTrade} />);
    const btn = screen.getByTestId(BTN);
    expect(btn).toBeInTheDocument();
    fireEvent.click(btn);
    expect(onPaperTrade).toHaveBeenCalledWith(sampleData);
  });

  it("isMock=true → görünmez (Kural #28 — sentetik veriyle paper trade YASAK)", () => {
    const onPaperTrade = vi.fn();
    render(<CarrPaperTradeButton data={sampleData} onPaperTrade={onPaperTrade} isMock />);
    expect(screen.queryByTestId(BTN)).toBeNull();
  });
});
