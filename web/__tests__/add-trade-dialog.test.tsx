/**
 * AddTradeDialog (KARAR ADAY #717 + #477 + İLKE #10) — yeni trade giriş akışı.
 *
 * Paper trading'in çekirdek yolu: 6 zorunlu Mark plan alanı + validation +
 * Defansif mod BLOK (override checkbox) + başarılı submit payload + P&L preview.
 *
 * 14 hook + 3 child card mock'lanır (MarkRiskAdvisor/MarkPyramidCard/PreTradeChecklist
 * null stub — kendi hook'larını test etmeyiz, AddTradeDialog mantığına odaklanırız).
 */
import { describe, it, expect, vi, beforeEach } from "vitest";
import { render, screen, fireEvent } from "@testing-library/react";
import { AddTradeDialog } from "@/components/journal/AddTradeDialog";

// Hoisted paylaşılan mock state (vi.mock factory'leri hoist edildiği için zorunlu)
const h = vi.hoisted(() => ({
  mockMutate: vi.fn(),
  tradingMode: {
    mode: "normal" as string,
    reason: "",
    emoji: "●",
    color: "#888888",
    recommendedSizingPct: 1,
    uiBehavior: "",
    consecutiveWins: 0,
    consecutiveLosses: 0,
    totalClosedTrades: 0,
  },
}));

vi.mock("@/hooks/use-trades", () => ({
  useAddTrade: () => ({ mutate: h.mockMutate, isPending: false }),
  useSetupTypes: () => ({ data: [] }),
  useTrades: () => ({ data: [], isLoading: false, isError: false }),
}));
vi.mock("@/hooks/use-trading-mode", () => ({
  useTradingMode: () => h.tradingMode,
  getDefansifBlockMessage: () => "Defansif modda yeni AL pozisyonu BLOK (Mark TTLC s.187).",
}));
vi.mock("@/hooks/use-carr-stage", () => ({ useCarrStage: () => ({ data: undefined }) }));
vi.mock("@/hooks/use-pivot-breakout", () => ({ usePivotBreakout: () => ({ data: undefined }) }));
vi.mock("@/hooks/use-climax-run", () => ({ useClimaxRun: () => ({ data: undefined }) }));
vi.mock("@/hooks/use-rs-rating", () => ({ useRsRating: () => ({ data: undefined }) }));
vi.mock("@/hooks/use-stage-transition", () => ({ useStageTransition: () => ({ data: undefined }) }));
vi.mock("@/hooks/use-atr-volatility", () => ({ useAtrVolatility: () => ({ data: undefined }) }));
vi.mock("@/hooks/use-symbol-search", () => ({ useSymbolSearch: () => ({ data: undefined }) }));
vi.mock("@/hooks/use-stock-quote", () => ({
  useStockQuote: () => ({ data: undefined }),
  useStockQuotes: () => [],
}));
vi.mock("@/lib/mindset-read-state", () => ({ isReadTodayAny: () => true }));

// Child kartlar — kendi hook'ları var, null stub (AddTradeDialog mantığına odak)
vi.mock("@/components/journal/MarkRiskAdvisor", () => ({ MarkRiskAdvisor: () => null }));
vi.mock("@/components/journal/MarkPyramidCard", () => ({ MarkPyramidCard: () => null }));
vi.mock("@/components/journal/PreTradeChecklist", () => ({ PreTradeChecklist: () => null }));

beforeEach(() => {
  h.mockMutate.mockReset();
  h.tradingMode.mode = "normal";
  h.tradingMode.recommendedSizingPct = 1;
});

/** Geçerli açık trade için tüm zorunlu alanları doldurur. */
function fillRequired() {
  fireEvent.change(screen.getByLabelText(/Hisse/), { target: { value: "NVDA" } });
  fireEvent.change(screen.getByLabelText(/Giriş Tarihi/), { target: { value: "2026-05-28" } });
  fireEvent.change(screen.getByLabelText(/Giriş \$/), { target: { value: "100" } });
  fireEvent.change(screen.getByLabelText(/Adet/), { target: { value: "50" } });
  fireEvent.change(screen.getByLabelText(/Giriş Tetikleyicisi/), { target: { value: "VCP pivot kirilimi" } });
  fireEvent.change(screen.getByLabelText(/Stop \$/), { target: { value: "94" } });
  fireEvent.change(screen.getByLabelText(/Hedef \$/), { target: { value: "112" } });
  fireEvent.change(screen.getByLabelText(/Pozisyon %/), { target: { value: "6.25" } });
  fireEvent.change(screen.getByLabelText(/Çıkış Stratejisi/), { target: { value: "2R yari sat trail" } });
}

describe("AddTradeDialog — render", () => {
  it("open=false → 'Yeni Trade' başlık görünmez", () => {
    render(<AddTradeDialog open={false} onOpenChange={vi.fn()} />);
    expect(screen.queryByText("Yeni Trade")).not.toBeInTheDocument();
  });

  it("open=true → 'Yeni Trade' başlık görünür", () => {
    render(<AddTradeDialog open onOpenChange={vi.fn()} />);
    expect(screen.getByText("Yeni Trade")).toBeInTheDocument();
  });

  it("Mark Trade Plan bölümü (6 alan ZORUNLU) render", () => {
    render(<AddTradeDialog open onOpenChange={vi.fn()} />);
    expect(screen.getByText(/Mark Trade Plan/)).toBeInTheDocument();
    expect(screen.getByText(/6 alan ZORUNLU/)).toBeInTheDocument();
  });

  it("normal mod → Trade Modu uyarı banner'ı yok", () => {
    render(<AddTradeDialog open onOpenChange={vi.fn()} />);
    expect(screen.queryByText(/Trade Modu:/)).not.toBeInTheDocument();
  });
});

describe("AddTradeDialog — validation (mutate çağrılmaz)", () => {
  it("boş submit → 'Hisse sembolü gerekli'", () => {
    render(<AddTradeDialog open onOpenChange={vi.fn()} />);
    fireEvent.submit(document.querySelector("form")!);
    expect(screen.getByText(/Hisse sembolü gerekli/)).toBeInTheDocument();
    expect(h.mockMutate).not.toHaveBeenCalled();
  });

  it("sembol var, tarih yok → 'Giriş tarihi gerekli'", () => {
    render(<AddTradeDialog open onOpenChange={vi.fn()} />);
    fireEvent.change(screen.getByLabelText(/Hisse/), { target: { value: "NVDA" } });
    fireEvent.submit(document.querySelector("form")!);
    expect(screen.getByText(/Giriş tarihi gerekli/)).toBeInTheDocument();
    expect(h.mockMutate).not.toHaveBeenCalled();
  });

  it("geçersiz giriş fiyatı (0) → 'Geçerli giriş fiyatı gerekli'", () => {
    render(<AddTradeDialog open onOpenChange={vi.fn()} />);
    fireEvent.change(screen.getByLabelText(/Hisse/), { target: { value: "NVDA" } });
    fireEvent.change(screen.getByLabelText(/Giriş Tarihi/), { target: { value: "2026-05-28" } });
    fireEvent.change(screen.getByLabelText(/Giriş \$/), { target: { value: "0" } });
    fireEvent.submit(document.querySelector("form")!);
    expect(screen.getByText(/Geçerli giriş fiyatı gerekli/)).toBeInTheDocument();
    expect(h.mockMutate).not.toHaveBeenCalled();
  });

  it("plan giriş tetikleyicisi boş → Mark TTLC Sec 1 hatası", () => {
    render(<AddTradeDialog open onOpenChange={vi.fn()} />);
    fireEvent.change(screen.getByLabelText(/Hisse/), { target: { value: "NVDA" } });
    fireEvent.change(screen.getByLabelText(/Giriş Tarihi/), { target: { value: "2026-05-28" } });
    fireEvent.change(screen.getByLabelText(/Giriş \$/), { target: { value: "100" } });
    fireEvent.change(screen.getByLabelText(/Adet/), { target: { value: "50" } });
    fireEvent.submit(document.querySelector("form")!);
    expect(screen.getByText(/Plan: Giriş tetikleyicisi gerekli/)).toBeInTheDocument();
    expect(h.mockMutate).not.toHaveBeenCalled();
  });

  it("plan stop boş → 'Plan: Geçerli stop $ gerekli'", () => {
    render(<AddTradeDialog open onOpenChange={vi.fn()} />);
    fireEvent.change(screen.getByLabelText(/Hisse/), { target: { value: "NVDA" } });
    fireEvent.change(screen.getByLabelText(/Giriş Tarihi/), { target: { value: "2026-05-28" } });
    fireEvent.change(screen.getByLabelText(/Giriş \$/), { target: { value: "100" } });
    fireEvent.change(screen.getByLabelText(/Adet/), { target: { value: "50" } });
    fireEvent.change(screen.getByLabelText(/Giriş Tetikleyicisi/), { target: { value: "VCP pivot" } });
    fireEvent.submit(document.querySelector("form")!);
    expect(screen.getByText(/Plan: Geçerli stop \$ gerekli/)).toBeInTheDocument();
    expect(h.mockMutate).not.toHaveBeenCalled();
  });
});

describe("AddTradeDialog — başarılı submit", () => {
  it("tüm zorunlu alanlar → mutate doğru payload ile çağrılır", () => {
    render(<AddTradeDialog open onOpenChange={vi.fn()} />);
    fillRequired();
    fireEvent.submit(document.querySelector("form")!);
    expect(h.mockMutate).toHaveBeenCalledTimes(1);
    const [body] = h.mockMutate.mock.calls[0];
    expect(body.symbol).toBe("NVDA");
    expect(body.strategy).toBe("minervini");
    expect(body.setup_type).toBe("vcp");
    expect(body.signal_source).toBe("strategy");
    expect(body.entry_price).toBe(100);
    expect(body.shares).toBe(50);
    expect(body.status).toBe("open");
    expect(body.plan_stop).toBe(94);
    expect(body.plan_target).toBe(112);
    expect(body.plan_size_pct).toBe(6.25);
    expect(body.plan_entry_trigger).toBe("VCP pivot kirilimi");
    expect(body.plan_exit_strategy).toBe("2R yari sat trail");
    expect(body.plan_time_horizon).toBe("swing");
  });

  it("küçük harf sembol → uppercase normalize", () => {
    render(<AddTradeDialog open onOpenChange={vi.fn()} />);
    fillRequired();
    fireEvent.change(screen.getByLabelText(/Hisse/), { target: { value: "aapl" } });
    fireEvent.submit(document.querySelector("form")!);
    const [body] = h.mockMutate.mock.calls[0];
    expect(body.symbol).toBe("AAPL");
  });
});

describe("AddTradeDialog — P388/P392 422 hata uctan uca UI", () => {
  it("Backend 422 -> form 'pivot_price: Input should be...' tipli mesaj render eder", () => {
    // P388 hook field-bazli mesaj atar, P392 role='alert' a11y gosterir.
    // Mock mutate hata simulasyonu: onError callback'ini cagir.
    h.mockMutate.mockImplementation((_body, options) => {
      options?.onError?.(new Error("plan_stop: Input should be greater than 0"));
    });
    render(<AddTradeDialog open onOpenChange={vi.fn()} />);
    fillRequired();
    fireEvent.submit(document.querySelector("form")!);
    // Spesifik field mesaji
    const errEl = screen.getByTestId("add-trade-error");
    expect(errEl.textContent).toContain("plan_stop");
    expect(errEl.textContent).toContain("greater than 0");
    // role='alert' a11y (screen reader fark eder)
    expect(errEl).toHaveAttribute("role", "alert");
  });

  it("Multi-field 422 -> semicolon join goster", () => {
    h.mockMutate.mockImplementation((_body, options) => {
      options?.onError?.(new Error("entry_price: must be > 0; shares: must be > 0"));
    });
    render(<AddTradeDialog open onOpenChange={vi.fn()} />);
    fillRequired();
    fireEvent.submit(document.querySelector("form")!);
    const errEl = screen.getByTestId("add-trade-error");
    expect(errEl.textContent).toContain("entry_price");
    expect(errEl.textContent).toContain("shares");
    expect(errEl.textContent).toContain(";");
    // ASLA "[object Object]" gosterme (P388 ana hedef)
    expect(errEl.textContent).not.toContain("[object Object]");
  });

  it("Cloud SQL down 503 -> 'Cloud SQL' mesaji render", () => {
    h.mockMutate.mockImplementation((_body, options) => {
      options?.onError?.(new Error("Veritabanına ulaşılamıyor (Cloud SQL)."));
    });
    render(<AddTradeDialog open onOpenChange={vi.fn()} />);
    fillRequired();
    fireEvent.submit(document.querySelector("form")!);
    expect(screen.getByTestId("add-trade-error").textContent).toContain("Cloud SQL");
  });
});

describe("AddTradeDialog — Defansif mod BLOK (Vizyon İLKE #10)", () => {
  it("defansif + override yok → submit BLOK, mutate çağrılmaz", () => {
    h.tradingMode.mode = "defansif";
    h.tradingMode.recommendedSizingPct = 0;
    render(<AddTradeDialog open onOpenChange={vi.fn()} />);
    fillRequired();
    fireEvent.submit(document.querySelector("form")!);
    expect(screen.getByText(/Riski biliyorum/)).toBeInTheDocument();
    expect(h.mockMutate).not.toHaveBeenCalled();
  });

  it("defansif + 'Riski biliyorum' işaretli → mutate çağrılır", () => {
    h.tradingMode.mode = "defansif";
    h.tradingMode.recommendedSizingPct = 0;
    render(<AddTradeDialog open onOpenChange={vi.fn()} />);
    fillRequired();
    fireEvent.click(screen.getByRole("checkbox"));
    fireEvent.submit(document.querySelector("form")!);
    expect(h.mockMutate).toHaveBeenCalledTimes(1);
  });

  it("defansif mod → Trade Modu uyarı banner'ı görünür", () => {
    h.tradingMode.mode = "defansif";
    render(<AddTradeDialog open onOpenChange={vi.fn()} />);
    expect(screen.getByText(/Trade Modu: DEFANSIF/)).toBeInTheDocument();
  });
});

describe("AddTradeDialog — P&L preview (kapalı trade)", () => {
  it("status=closed + entry=100 exit=110 shares=100 → P&L hesap görünür", () => {
    render(<AddTradeDialog open onOpenChange={vi.fn()} />);
    fireEvent.change(screen.getByLabelText(/Giriş \$/), { target: { value: "100" } });
    fireEvent.change(screen.getByLabelText(/Adet/), { target: { value: "100" } });
    fireEvent.change(screen.getByLabelText(/Statü/), { target: { value: "closed" } });
    fireEvent.change(screen.getByLabelText(/Çıkış \$/), { target: { value: "110" } });
    // calcPL(100, 110, 100) = +$1000 / +%10
    expect(screen.getByText(/1[.,]?000/)).toBeInTheDocument();
  });
});
