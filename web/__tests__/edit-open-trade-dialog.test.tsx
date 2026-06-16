/**
 * EditOpenTradeDialog (Migration 011 / KARAR ADAY #960) — açık pozisyon aktif stop/hedef düzenleme.
 *
 * Mark "audible" disiplini: stop/hedef DEĞİŞİRSE sebep ZORUNLU (yoksa mutate çağrılmaz).
 * useUpdateTrade mock — form validation + handleSubmit → mutate payload.
 */
import { describe, it, expect, vi, beforeEach } from "vitest";
import { render, screen, fireEvent } from "@testing-library/react";
import { EditOpenTradeDialog } from "@/components/journal/EditOpenTradeDialog";
import type { Trade } from "@/types/trade";

const mockMutate = vi.fn();
vi.mock("@/hooks/use-trades", () => ({
  useUpdateTrade: () => ({ mutate: mockMutate, isPending: false }),
}));

beforeEach(() => {
  mockMutate.mockReset();
});

function makeTrade(o: Partial<Trade> = {}): Trade {
  return {
    id: 7, symbol: "AAPL", strategy: "minervini", setup_type: "vcp",
    entry_date: "2026-05-20", entry_price: 100, shares: 100,
    status: "open", pl_dollar: null, pl_pct: null, grade: null,
    exit_reason: null, lessons: null,
    plan_stop: 95, plan_target: 120, stop_loss: 95, target_price: 120,
    ...o,
  } as Trade;
}

describe("EditOpenTradeDialog — render", () => {
  it("open=false → içerik görünmez", () => {
    render(<EditOpenTradeDialog trade={makeTrade()} open={false} onOpenChange={vi.fn()} />);
    expect(screen.queryByText(/Stop \/ Hedef Düzenle/)).not.toBeInTheDocument();
  });

  it("open=true → 'Stop / Hedef Düzenle — {symbol}' başlık + plan referansı", () => {
    render(<EditOpenTradeDialog trade={makeTrade()} open onOpenChange={vi.fn()} />);
    expect(screen.getByText("Stop / Hedef Düzenle — AAPL")).toBeInTheDocument();
    expect(screen.getByText(/Plan \(değişmez\)/)).toBeInTheDocument();
  });
});

describe("EditOpenTradeDialog — audible disiplini (#960)", () => {
  it("değişiklik yok + submit → 'değişiklik yok', mutate çağrılmaz", () => {
    render(<EditOpenTradeDialog trade={makeTrade()} open onOpenChange={vi.fn()} />);
    fireEvent.submit(document.querySelector("form")!);
    // data-testid ile hedefle — "değişiklik yoksa" label'i de eşleşiyor (çoklu match önle)
    expect(screen.getByTestId("edit-trade-error")).toHaveTextContent(/değişiklik yok/i);
    expect(mockMutate).not.toHaveBeenCalled();
  });

  it("stop değişir + sebep YOK → 'sebep zorunlu', mutate çağrılmaz", () => {
    render(<EditOpenTradeDialog trade={makeTrade()} open onOpenChange={vi.fn()} />);
    fireEvent.change(screen.getByLabelText(/Aktif Stop/), { target: { value: "98" } });
    fireEvent.submit(document.querySelector("form")!);
    expect(screen.getByText(/sebep zorunlu/i)).toBeInTheDocument();
    expect(mockMutate).not.toHaveBeenCalled();
  });

  it("stop değişir + sebep VAR → mutate doğru payload ile çağrılır", () => {
    render(<EditOpenTradeDialog trade={makeTrade()} open onOpenChange={vi.fn()} />);
    fireEvent.change(screen.getByLabelText(/Aktif Stop/), { target: { value: "98" } });
    fireEvent.change(screen.getByLabelText(/Sebep/), { target: { value: "trailing breakeven üstü" } });
    fireEvent.submit(document.querySelector("form")!);
    expect(mockMutate).toHaveBeenCalledTimes(1);
    const arg = mockMutate.mock.calls[0][0];
    expect(arg.id).toBe(7);
    expect(arg.update.stop_loss).toBe(98);
    expect(arg.update.audible_reason).toBe("trailing breakeven üstü");
  });
});
