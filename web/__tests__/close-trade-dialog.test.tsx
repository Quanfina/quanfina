/**
 * CloseTradeDialog (KARAR #475 + #725) — trade kapama + P&L + validation.
 *
 * useUpdateTrade / useTradingMode / useTrades mock. Form: exit date+price
 * validation + handleSubmit → mutate. Plan vs Reality (PlanVsRealityCard).
 */
import { describe, it, expect, vi, beforeEach } from "vitest";
import { render, screen, fireEvent } from "@testing-library/react";
import { CloseTradeDialog } from "@/components/journal/CloseTradeDialog";
import type { Trade } from "@/types/trade";

const mockMutate = vi.fn();
const mockUpdate = { mutate: mockMutate, isPending: false };
const mockTradingMode = {
  mode: "normal", reason: "", emoji: "●", color: "", recommendedSizingPct: 1,
  uiBehavior: "", consecutiveWins: 0, consecutiveLosses: 0, totalClosedTrades: 0,
};

vi.mock("@/hooks/use-trades", () => ({
  useUpdateTrade: () => mockUpdate,
  useTrades: () => ({ data: [], isLoading: false, isError: false }),
}));
vi.mock("@/hooks/use-trading-mode", () => ({
  useTradingMode: () => mockTradingMode,
}));

beforeEach(() => {
  mockMutate.mockReset();
});

function makeTrade(o: Partial<Trade> = {}): Trade {
  return {
    id: 1, symbol: "AAPL", strategy: "minervini", setup_type: "vcp",
    entry_date: "2026-05-20", entry_price: 100, shares: 100,
    status: "open", pl_dollar: null, pl_pct: null, grade: null,
    exit_reason: null, lessons: null, plan_stop: 95, plan_target: 120,
    ...o,
  } as Trade;
}

describe("CloseTradeDialog — render", () => {
  it("open=false → dialog içeriği görünmez", () => {
    render(<CloseTradeDialog trade={makeTrade()} open={false} onOpenChange={vi.fn()} />);
    expect(screen.queryByText(/Kapat — AAPL/)).not.toBeInTheDocument();
  });

  it("open=true + açık trade → 'Kapat — {symbol}' başlık", () => {
    render(<CloseTradeDialog trade={makeTrade()} open onOpenChange={vi.fn()} />);
    expect(screen.getByText("Kapat — AAPL")).toBeInTheDocument();
  });

  it("kapalı trade → 'Düzenle — {symbol}' başlık", () => {
    render(<CloseTradeDialog trade={makeTrade({ status: "closed" })} open onOpenChange={vi.fn()} />);
    expect(screen.getByText("Düzenle — AAPL")).toBeInTheDocument();
  });

  it("Çıkış Tarihi + Çıkış Fiyatı alanları render", () => {
    render(<CloseTradeDialog trade={makeTrade()} open onOpenChange={vi.fn()} />);
    expect(screen.getByText(/Çıkış Tarihi/)).toBeInTheDocument();
    expect(screen.getByText(/Çıkış Fiyatı/)).toBeInTheDocument();
  });
});

describe("CloseTradeDialog — validation", () => {
  it("exit date + price boş → submit error mesajı, mutate çağrılmaz", () => {
    render(<CloseTradeDialog trade={makeTrade()} open onOpenChange={vi.fn()} />);
    const form = document.querySelector("form")!;
    fireEvent.submit(form);
    expect(screen.getByText(/Çıkış tarihi ve fiyatı gerekli/)).toBeInTheDocument();
    expect(mockMutate).not.toHaveBeenCalled();
  });

  it("geçersiz fiyat (0) → 'Geçerli çıkış fiyatı gerekli'", () => {
    render(<CloseTradeDialog trade={makeTrade()} open onOpenChange={vi.fn()} />);
    fireEvent.change(screen.getByLabelText(/Çıkış Tarihi/), { target: { value: "2026-05-29" } });
    fireEvent.change(screen.getByLabelText(/Çıkış Fiyatı/), { target: { value: "0" } });
    fireEvent.submit(document.querySelector("form")!);
    expect(screen.getByText(/Geçerli çıkış fiyatı gerekli/)).toBeInTheDocument();
    expect(mockMutate).not.toHaveBeenCalled();
  });
});

describe("CloseTradeDialog — başarılı kapama", () => {
  it("geçerli form → updateMutation.mutate (id + exit_price + status closed)", () => {
    render(<CloseTradeDialog trade={makeTrade({ id: 42 })} open onOpenChange={vi.fn()} />);
    fireEvent.change(screen.getByLabelText(/Çıkış Tarihi/), { target: { value: "2026-05-29" } });
    fireEvent.change(screen.getByLabelText(/Çıkış Fiyatı/), { target: { value: "110" } });
    fireEvent.submit(document.querySelector("form")!);
    expect(mockMutate).toHaveBeenCalledTimes(1);
    const [payload] = mockMutate.mock.calls[0];
    expect(payload.id).toBe(42);
    expect(payload.update.exit_price).toBe(110);
    expect(payload.update.status).toBe("closed");
  });

  it("P&L preview: exit=110 entry=100 shares=100 → +$1000 görünür", () => {
    render(<CloseTradeDialog trade={makeTrade({ entry_price: 100, shares: 100 })} open onOpenChange={vi.fn()} />);
    fireEvent.change(screen.getByLabelText(/Çıkış Fiyatı/), { target: { value: "110" } });
    // plPreview render (P&L hesap görünür — $1000 veya 1000)
    expect(screen.getByText(/1[.,]?000/)).toBeInTheDocument();
  });
});

describe("CloseTradeDialog — P393 422 hata uctan uca UI (AddTradeDialog P392 pateni)", () => {
  function fillValidForm() {
    fireEvent.change(screen.getByLabelText(/Çıkış Tarihi/), { target: { value: "2026-05-29" } });
    fireEvent.change(screen.getByLabelText(/Çıkış Fiyatı/), { target: { value: "110" } });
  }

  it("Backend 422 -> 'exit_price: ...' field-bazli mesaj render eder", () => {
    // P388 hook field-bazli mesaj atar, P393 role='alert' a11y gosterir.
    mockMutate.mockImplementation((_payload, options) => {
      options?.onError?.(new Error("exit_price: Input should be greater than or equal to 0"));
    });
    render(<CloseTradeDialog trade={makeTrade()} open onOpenChange={vi.fn()} />);
    fillValidForm();
    fireEvent.submit(document.querySelector("form")!);
    const errEl = screen.getByTestId("close-trade-error");
    expect(errEl.textContent).toContain("exit_price");
    expect(errEl).toHaveAttribute("role", "alert");
  });

  it("Multi-field 422 -> semicolon join + '[object Object]' YOK (regresyon)", () => {
    mockMutate.mockImplementation((_payload, options) => {
      options?.onError?.(new Error("exit_price: must be >= 0; status: invalid value"));
    });
    render(<CloseTradeDialog trade={makeTrade()} open onOpenChange={vi.fn()} />);
    fillValidForm();
    fireEvent.submit(document.querySelector("form")!);
    const errEl = screen.getByTestId("close-trade-error");
    expect(errEl.textContent).toContain("exit_price");
    expect(errEl.textContent).toContain("status");
    expect(errEl.textContent).not.toContain("[object Object]");
  });

  it("Cloud SQL down 503 -> 'Cloud SQL' string mesaji render", () => {
    mockMutate.mockImplementation((_payload, options) => {
      options?.onError?.(new Error("Veritabanına ulaşılamıyor (Cloud SQL)."));
    });
    render(<CloseTradeDialog trade={makeTrade()} open onOpenChange={vi.fn()} />);
    fillValidForm();
    fireEvent.submit(document.querySelector("form")!);
    expect(screen.getByTestId("close-trade-error").textContent).toContain("Cloud SQL");
  });

  it("404 Trade bulunamadı -> direkt mesaj", () => {
    // PATCH yapilirken trade DB'den silinmis olabilir -> 404
    mockMutate.mockImplementation((_payload, options) => {
      options?.onError?.(new Error("Trade 1 bulunamadı"));
    });
    render(<CloseTradeDialog trade={makeTrade()} open onOpenChange={vi.fn()} />);
    fillValidForm();
    fireEvent.submit(document.querySelector("form")!);
    expect(screen.getByTestId("close-trade-error").textContent).toContain("bulunamadı");
  });
});
