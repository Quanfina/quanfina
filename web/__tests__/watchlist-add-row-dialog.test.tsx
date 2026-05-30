/**
 * Watchlist AddRowDialog — Paket 394 (Add/CloseTrade P392/P393 pateni).
 *
 * P387 backend WatchlistRowCreate `pivot_price gt=0` + `symbol min/max_length`
 * ekledim -> 422 senaryosu gercek. Dialog UI hata zinciri (hook -> form ->
 * role="alert" + screen reader) uctan uca test edilmemisti.
 *
 * Mevcut: AddRowDialog'in hic UI testi yoktu (sadece mutation hook level
 * use-watchlist-mutations testleri var). P394 ile baseline + 422 zinciri.
 */
import { describe, it, expect, vi, beforeEach } from "vitest";
import { render, screen, fireEvent } from "@testing-library/react";
import { AddRowDialog } from "@/components/watchlist/AddRowDialog";

const mockMutate = vi.fn();
const mockAdd = { mutate: mockMutate, isPending: false };

vi.mock("@/hooks/use-watchlist-mutations", () => ({
  useAddWatchlistRow: () => mockAdd,
}));
// Symbol search autocomplete — boş stub (uctan uca test'te suggestions bizi ilgilendirmez)
vi.mock("@/hooks/use-symbol-search", () => ({
  useSymbolSearch: () => ({ data: undefined }),
}));

beforeEach(() => {
  mockMutate.mockReset();
});


describe("AddRowDialog — render baseline", () => {
  it("open=false → dialog gorunmez", () => {
    render(<AddRowDialog open={false} onOpenChange={vi.fn()} />);
    expect(screen.queryByText("Hisse Ekle")).not.toBeInTheDocument();
  });

  it("open=true → 'Hisse Ekle' baslik gorunur", () => {
    render(<AddRowDialog open onOpenChange={vi.fn()} />);
    expect(screen.getByText("Hisse Ekle")).toBeInTheDocument();
  });
});


describe("AddRowDialog — client-side validation", () => {
  it("Bos sembol -> 'Hisse sembolu gerekli' mesaj, mutate cagrilmaz", () => {
    render(<AddRowDialog open onOpenChange={vi.fn()} />);
    fireEvent.submit(document.querySelector("form")!);
    expect(screen.getByTestId("watchlist-add-row-error").textContent).toContain("gerekli");
    expect(mockMutate).not.toHaveBeenCalled();
  });

  it("Sembol kucuk harf -> uppercase normalize body", () => {
    render(<AddRowDialog open onOpenChange={vi.fn()} />);
    fireEvent.change(screen.getByLabelText(/Hisse \*/), { target: { value: "nvda" } });
    fireEvent.submit(document.querySelector("form")!);
    expect(mockMutate).toHaveBeenCalledTimes(1);
    const [payload] = mockMutate.mock.calls[0];
    expect(payload.symbol).toBe("NVDA");
  });
});


describe("AddRowDialog — P394 422 hata uctan uca UI (P392 pateni)", () => {
  function fillValidForm() {
    fireEvent.change(screen.getByLabelText(/Hisse \*/), { target: { value: "AAPL" } });
  }

  it("Backend 422 pivot_price -> 'pivot_price: ...' mesaj + role=alert", () => {
    // P387 pivot_price gt=0 -> Sn. Ferit 0 girip submit ederse bu senaryo gercek.
    mockMutate.mockImplementation((_body, options) => {
      options?.onError?.(new Error("pivot_price: Input should be greater than 0"));
    });
    render(<AddRowDialog open onOpenChange={vi.fn()} />);
    fillValidForm();
    fireEvent.submit(document.querySelector("form")!);
    const errEl = screen.getByTestId("watchlist-add-row-error");
    expect(errEl.textContent).toContain("pivot_price");
    expect(errEl.textContent).toContain("greater than 0");
    expect(errEl).toHaveAttribute("role", "alert");
  });

  it("Multi-field 422 -> semicolon join + '[object Object]' YOK regresyon", () => {
    mockMutate.mockImplementation((_body, options) => {
      options?.onError?.(new Error("symbol: too long; pivot_price: must be > 0"));
    });
    render(<AddRowDialog open onOpenChange={vi.fn()} />);
    fillValidForm();
    fireEvent.submit(document.querySelector("form")!);
    const errEl = screen.getByTestId("watchlist-add-row-error");
    expect(errEl.textContent).toContain("symbol");
    expect(errEl.textContent).toContain("pivot_price");
    expect(errEl.textContent).not.toContain("[object Object]");
  });

  it("409 duplicate symbol-strategy -> string mesaj direkt", () => {
    // Backend AAPL-minervini zaten varsa 409 doner (add_watchlist_row).
    mockMutate.mockImplementation((_body, options) => {
      options?.onError?.(new Error("AAPL-minervini zaten watchlist'te"));
    });
    render(<AddRowDialog open onOpenChange={vi.fn()} />);
    fillValidForm();
    fireEvent.submit(document.querySelector("form")!);
    expect(screen.getByTestId("watchlist-add-row-error").textContent).toContain("zaten watchlist");
  });

  it("503 Cloud SQL down -> 'Cloud SQL' mesaj render", () => {
    mockMutate.mockImplementation((_body, options) => {
      options?.onError?.(new Error("Veritabanına ulaşılamıyor (Cloud SQL)."));
    });
    render(<AddRowDialog open onOpenChange={vi.fn()} />);
    fillValidForm();
    fireEvent.submit(document.querySelector("form")!);
    expect(screen.getByTestId("watchlist-add-row-error").textContent).toContain("Cloud SQL");
  });
});
