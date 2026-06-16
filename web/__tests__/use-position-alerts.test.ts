/**
 * usePositionAlerts (P160 + P161 + P168) — Mark TTLC s.131 + TLSMW Ch 12 stop disiplini.
 *
 * 4 alert tipi: STOP_HIT (critical), STOP_NEAR (warning), MINERVINI_7PCT
 * (warning), TARGET_NEAR (info). Dismiss localStorage 24h TTL, history FIFO 50.
 */
import { describe, it, expect, vi, beforeEach } from "vitest";
import { renderHook } from "@testing-library/react";
import {
  usePositionAlerts,
  getAlertHistory,
  type PositionAlert,
} from "@/hooks/use-position-alerts";
import type { Trade } from "@/types/trade";
import type { StockQuote } from "@/hooks/use-stock-quote";

// sonner toast mock — sadece çağrı kayıt et
const toastError = vi.fn();
const toastWarning = vi.fn();
const toastInfo = vi.fn();
vi.mock("sonner", () => ({
  toast: {
    error: (...args: unknown[]) => toastError(...args),
    warning: (...args: unknown[]) => toastWarning(...args),
    info: (...args: unknown[]) => toastInfo(...args),
  },
}));

function makeTrade(overrides: Partial<Trade> = {}): Trade {
  const base: Trade = {
    id: 1,
    symbol: "AAPL",
    strategy: "minervini",
    setup_type: "vcp",
    entry_date: "2026-05-20",
    entry_price: 100,
    exit_date: null,
    exit_price: null,
    shares: 100,
    status: "open",
    pl_dollar: null,
    pl_pct: null,
    grade: null,
    exit_reason: null,
    lessons: null,
    plan_stop: 93,
    plan_target: 120,
  };
  return { ...base, ...overrides };
}

function makeQuote(symbol: string, price: number): { data: StockQuote } {
  return {
    data: { symbol, price, change_dollar: 0, change_pct: 0, source: "yfinance" },
  };
}

beforeEach(() => {
  localStorage.clear();
  toastError.mockClear();
  toastWarning.mockClear();
  toastInfo.mockClear();
});

describe("usePositionAlerts — alert hesabı (saf useMemo)", () => {
  it("boş trade listesi → 0 alert", () => {
    const { result } = renderHook(() => usePositionAlerts([], []));
    expect(result.current).toHaveLength(0);
  });

  it("status='closed' trade → alert üretmez", () => {
    const trades = [makeTrade({ status: "closed", exit_price: 90 })];
    const { result } = renderHook(() =>
      usePositionAlerts(trades, [makeQuote("AAPL", 85)])
    );
    expect(result.current).toHaveLength(0);
  });

  it("quote yok → alert üretmez (skip)", () => {
    const { result } = renderHook(() =>
      usePositionAlerts([makeTrade()], [])
    );
    expect(result.current).toHaveLength(0);
  });

  it("#9 (Kural #28): MOCK-source quote → alert ÜRETMEZ (sahte kritik stop alarmı engeli)", () => {
    // price=92 <= plan_stop=93 normalde STOP_HIT (kritik "pozisyonu kapat!") üretir.
    // MOCK fiyat ile bu YANILTICI -> source!="yfinance" ise alarm yok (paper trading güvenliği).
    const mockQuote = {
      data: { symbol: "AAPL", price: 92, change_dollar: 0, change_pct: 0, source: "mock" as const },
    };
    const { result } = renderHook(() =>
      usePositionAlerts([makeTrade({ plan_stop: 93 })], [mockQuote])
    );
    expect(result.current).toHaveLength(0);
  });

  describe("STOP_HIT (critical) — current <= plan_stop", () => {
    it("price=92 + plan_stop=93 → STOP_HIT", () => {
      const trades = [makeTrade({ plan_stop: 93 })];
      const { result } = renderHook(() =>
        usePositionAlerts(trades, [makeQuote("AAPL", 92)])
      );
      const stopHit = result.current.find((a) => a.type === "stop_hit");
      expect(stopHit).toBeDefined();
      expect(stopHit?.severity).toBe("critical");
      expect(stopHit?.title).toContain("STOP'A DEĞDİ");
    });

    it("price=93 (eşit) → STOP_HIT (boundary)", () => {
      const trades = [makeTrade({ plan_stop: 93 })];
      const { result } = renderHook(() =>
        usePositionAlerts(trades, [makeQuote("AAPL", 93)])
      );
      expect(result.current.some((a) => a.type === "stop_hit")).toBe(true);
    });

    it("Alert id format: '{tradeId}-stop_hit-YYYY-MM-DD'", () => {
      const trades = [makeTrade({ id: 42, plan_stop: 93 })];
      const { result } = renderHook(() =>
        usePositionAlerts(trades, [makeQuote("AAPL", 90)])
      );
      const today = new Date().toISOString().slice(0, 10);
      expect(result.current[0].id).toBe(`42-stop_hit-${today}`);
    });
  });

  describe("STOP_NEAR (warning) — plan_stop * 1.01 yakın", () => {
    it("price=93.5, plan_stop=93 → STOP_NEAR (price ≤ 93*1.01=93.93)", () => {
      const trades = [makeTrade({ plan_stop: 93 })];
      const { result } = renderHook(() =>
        usePositionAlerts(trades, [makeQuote("AAPL", 93.5)])
      );
      const stopNear = result.current.find((a) => a.type === "stop_near");
      expect(stopNear).toBeDefined();
      expect(stopNear?.severity).toBe("warning");
    });

    it("price=95 (1% dışı) → STOP_NEAR YOK", () => {
      const trades = [makeTrade({ plan_stop: 93 })];
      const { result } = renderHook(() =>
        usePositionAlerts(trades, [makeQuote("AAPL", 95)])
      );
      expect(result.current.some((a) => a.type === "stop_near")).toBe(false);
    });
  });

  describe("MINERVINI_7PCT (warning) — Mark TTLC s.131 mutlak limit", () => {
    it("entry=100 price=92 (loss=-8%) plan_stop=93 (<93 → 92 değil) → MINERVINI_7PCT", () => {
      // plan_stop=93 ama 7%stop=93 → eşit; %7 alarmı tetik: plan_stop < sevenPctStop OR plan_stop=null
      const trades = [
        makeTrade({ entry_price: 100, plan_stop: null }),
      ];
      const { result } = renderHook(() =>
        usePositionAlerts(trades, [makeQuote("AAPL", 92)])
      );
      const mrk7 = result.current.find((a) => a.type === "minervini_7pct");
      expect(mrk7).toBeDefined();
      expect(mrk7?.severity).toBe("warning");
      expect(mrk7?.title).toContain("%7 KURALI");
    });

    it("entry=100 price=95 (loss=-5%) → MINERVINI_7PCT YOK (limit aşılmadı)", () => {
      const trades = [makeTrade({ entry_price: 100, plan_stop: null })];
      const { result } = renderHook(() =>
        usePositionAlerts(trades, [makeQuote("AAPL", 95)])
      );
      expect(result.current.some((a) => a.type === "minervini_7pct")).toBe(false);
    });

    it("entry=100 price=92 + plan_stop=94 (94 > 93 yani sevenPctStop=93'ten büyük) → MINERVINI_7PCT YOK", () => {
      // plan_stop daha sıkı (yukarıda) → 7pct alarmı bastırılır (sadece daha gevşek planlı ya da null'da çalışır)
      const trades = [makeTrade({ entry_price: 100, plan_stop: 94 })];
      const { result } = renderHook(() =>
        usePositionAlerts(trades, [makeQuote("AAPL", 92)])
      );
      expect(result.current.some((a) => a.type === "minervini_7pct")).toBe(false);
    });
  });

  describe("TARGET_NEAR (info) — plan_target * 0.98 yakın", () => {
    it("price=118 plan_target=120 (price=120*0.983) → TARGET_NEAR", () => {
      const trades = [makeTrade({ plan_target: 120 })];
      const { result } = renderHook(() =>
        usePositionAlerts(trades, [makeQuote("AAPL", 118)])
      );
      const target = result.current.find((a) => a.type === "target_near");
      expect(target).toBeDefined();
      expect(target?.severity).toBe("info");
    });

    it("price=110 (98% dışı) → TARGET_NEAR YOK", () => {
      const trades = [makeTrade({ plan_target: 120 })];
      const { result } = renderHook(() =>
        usePositionAlerts(trades, [makeQuote("AAPL", 110)])
      );
      expect(result.current.some((a) => a.type === "target_near")).toBe(false);
    });

    it("price=120 (hedefte) → TARGET_NEAR YOK (price < target koşulu)", () => {
      const trades = [makeTrade({ plan_target: 120 })];
      const { result } = renderHook(() =>
        usePositionAlerts(trades, [makeQuote("AAPL", 120)])
      );
      expect(result.current.some((a) => a.type === "target_near")).toBe(false);
    });
  });
});

describe("usePositionAlerts — toast tetik (useEffect)", () => {
  it("STOP_HIT alert → toast.error çağrılır", () => {
    const trades = [makeTrade({ plan_stop: 93 })];
    renderHook(() =>
      usePositionAlerts(trades, [makeQuote("AAPL", 90)])
    );
    expect(toastError).toHaveBeenCalled();
    expect(toastError.mock.calls[0][0]).toContain("STOP'A DEĞDİ");
  });

  it("TARGET_NEAR → toast.info, STOP_NEAR → toast.warning", () => {
    const trades = [
      makeTrade({ id: 1, symbol: "AAPL", plan_target: 120, plan_stop: 50 }),  // target near
      makeTrade({ id: 2, symbol: "NVDA", entry_price: 100, plan_stop: 90 }),  // stop near
    ];
    renderHook(() =>
      usePositionAlerts(trades, [
        makeQuote("AAPL", 118),
        makeQuote("NVDA", 90.5),
      ])
    );
    expect(toastInfo).toHaveBeenCalled();
    expect(toastWarning).toHaveBeenCalled();
  });
});

describe("getAlertHistory — Paket 168 timeline (24h FIFO 50)", () => {
  it("boş localStorage → []", () => {
    expect(getAlertHistory()).toEqual([]);
  });

  it("toast atıldıktan sonra history'e yazılır", () => {
    const trades = [makeTrade({ plan_stop: 93 })];
    renderHook(() =>
      usePositionAlerts(trades, [makeQuote("AAPL", 90)])
    );
    const history = getAlertHistory();
    expect(history.length).toBeGreaterThan(0);
    expect(history[0].type).toBe("stop_hit");
    expect(history[0].timestamp).toBeGreaterThan(Date.now() - 5000);
  });

  it("25 saat eski entry filtrelenir (24h TTL)", () => {
    const oldEntry: PositionAlert & { timestamp: number } = {
      id: "old-stop_hit-2026-01-01",
      tradeId: 99,
      symbol: "OLD",
      severity: "critical",
      type: "stop_hit",
      title: "Eski",
      message: "Eski",
      current_price: 50,
      ref_price: 60,
      timestamp: Date.now() - 25 * 3600 * 1000,
    };
    localStorage.setItem("position-alerts-history", JSON.stringify([oldEntry]));
    expect(getAlertHistory()).toEqual([]);
  });
});
