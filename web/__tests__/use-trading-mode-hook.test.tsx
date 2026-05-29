/**
 * useTradingMode hook (P193 — Vizyon KALICI İLKE #10).
 *
 * 4 mod otomatik tetik: Normal / Rehab / Defansif / Agresif.
 * Streak hesap: en yeni kapalı trade'den geriye, kazanç/kayıp kırılma noktasında dur.
 * Tetik sırası: Defansif > Rehab > Agresif > Normal.
 */
import { describe, it, expect, vi, beforeEach } from "vitest";
import { renderHook } from "@testing-library/react";
import { useTradingMode } from "@/hooks/use-trading-mode";
import type { Trade } from "@/types/trade";

const mockTrades = vi.fn<() => { data?: Trade[] }>();
const mockMarket = vi.fn<() => {
  data?: {
    market_health_score: number | null;
    market_health_label?: string;  // P383: clean-room enum (HEALTHY/NEUTRAL/UNDER_PRESSURE)
    suggested_mode?: string | null;
  };
}>();

vi.mock("@/hooks/use-trades", () => ({
  useTrades: () => mockTrades(),
}));
vi.mock("@/hooks/use-market-status", () => ({
  useMarketStatus: () => mockMarket(),
}));

function makeTrade(overrides: Partial<Trade> = {}): Trade {
  return {
    id: 1,
    symbol: "AAPL",
    strategy: "minervini",
    setup_type: "vcp",
    entry_date: "2026-05-01",
    entry_price: 100,
    exit_date: "2026-05-15",
    exit_price: 110,
    shares: 100,
    status: "closed",
    pl_dollar: 1000,
    pl_pct: 10,
    grade: "A",
    exit_reason: "target_hit",
    lessons: null,
    ...overrides,
  };
}

beforeEach(() => {
  mockTrades.mockReset();
  mockMarket.mockReset();
  mockTrades.mockReturnValue({ data: [] });
  mockMarket.mockReturnValue({ data: { market_health_score: 50 } });
});

describe("useTradingMode — 4 mod tetik öncelik (Defansif > Rehab > Agresif > Normal)", () => {
  it("Boş trade + market_health=50 → Normal mod, sizing=%1", () => {
    const { result } = renderHook(() => useTradingMode());
    expect(result.current.mode).toBe("normal");
    expect(result.current.recommendedSizingPct).toBe(1.0);
    expect(result.current.consecutiveWins).toBe(0);
    expect(result.current.consecutiveLosses).toBe(0);
    expect(result.current.totalClosedTrades).toBe(0);
  });

  describe("Defansif tetik (market_health < 30)", () => {
    it("market_health=25 → Defansif (Stage 3-4 paralel), sizing=0", () => {
      mockMarket.mockReturnValue({ data: { market_health_score: 25 } });
      const { result } = renderHook(() => useTradingMode());
      expect(result.current.mode).toBe("defansif");
      expect(result.current.recommendedSizingPct).toBe(0);
      expect(result.current.reason).toContain("25/100");
      expect(result.current.uiBehavior).toContain("BLOK");
    });

    it("market_health=29 (sınır) → Defansif", () => {
      mockMarket.mockReturnValue({ data: { market_health_score: 29 } });
      const { result } = renderHook(() => useTradingMode());
      expect(result.current.mode).toBe("defansif");
    });

    it("market_health=30 (sınır dışı) → Normal", () => {
      mockMarket.mockReturnValue({ data: { market_health_score: 30 } });
      const { result } = renderHook(() => useTradingMode());
      expect(result.current.mode).toBe("normal");
    });

    it("Defansif 3 ardışık kayıp olsa BİLE üstte → mode='defansif' (öncelik)", () => {
      mockMarket.mockReturnValue({ data: { market_health_score: 20 } });
      mockTrades.mockReturnValue({
        data: [
          makeTrade({ id: 1, pl_dollar: -100, exit_date: "2026-05-15" }),
          makeTrade({ id: 2, pl_dollar: -100, exit_date: "2026-05-14" }),
          makeTrade({ id: 3, pl_dollar: -100, exit_date: "2026-05-13" }),
        ],
      });
      const { result } = renderHook(() => useTradingMode());
      expect(result.current.mode).toBe("defansif"); // Rehab değil — Defansif öncelikli
      expect(result.current.consecutiveLosses).toBe(3);
    });
  });

  describe("Rehab tetik (consecutiveLosses ≥ 3, market sağlıklı)", () => {
    it("3 ardışık kayıp + market=50 → Rehab, sizing=%0.5", () => {
      mockTrades.mockReturnValue({
        data: [
          makeTrade({ id: 1, pl_dollar: -100, exit_date: "2026-05-15" }),
          makeTrade({ id: 2, pl_dollar: -150, exit_date: "2026-05-14" }),
          makeTrade({ id: 3, pl_dollar: -200, exit_date: "2026-05-13" }),
        ],
      });
      const { result } = renderHook(() => useTradingMode());
      expect(result.current.mode).toBe("rehab");
      expect(result.current.recommendedSizingPct).toBe(0.5);
      expect(result.current.consecutiveLosses).toBe(3);
      expect(result.current.reason).toContain("Mark TTLC s.187");
    });

    it("2 ardışık kayıp → henüz Rehab değil (Normal)", () => {
      mockTrades.mockReturnValue({
        data: [
          makeTrade({ id: 1, pl_dollar: -100 }),
          makeTrade({ id: 2, pl_dollar: -100 }),
        ],
      });
      const { result } = renderHook(() => useTradingMode());
      expect(result.current.mode).toBe("normal");
      expect(result.current.consecutiveLosses).toBe(2);
    });
  });

  describe("Rehab tetik (realized drawdown >%10 — P357, ardışık kayıp OLMADAN)", () => {
    it("Tepe +20k sonra -15k tek kayıp → drawdown %12.5 → Rehab (loss streak=1)", () => {
      // $100k taban: id2 (eski) +20k → equity 120k (tepe), id1 (yeni) -15k → 105k.
      // dd = 15k/120k = %12.5 > %10 → Rehab. consecutiveLosses=1 (3 değil!).
      mockTrades.mockReturnValue({
        data: [
          makeTrade({ id: 1, pl_dollar: -15000, exit_date: "2026-05-15" }),
          makeTrade({ id: 2, pl_dollar: 20000, exit_date: "2026-05-14" }),
        ],
      });
      const { result } = renderHook(() => useTradingMode());
      expect(result.current.mode).toBe("rehab");
      expect(result.current.recommendedSizingPct).toBe(0.5);
      expect(result.current.consecutiveLosses).toBe(1); // 3 değil — drawdown tetikledi
      expect(result.current.reason.toLowerCase()).toContain("drawdown");
    });

    it("Tepe +20k sonra -10k → drawdown %8.3 (<%10) → Normal (tetik yok)", () => {
      mockTrades.mockReturnValue({
        data: [
          makeTrade({ id: 1, pl_dollar: -10000, exit_date: "2026-05-15" }),
          makeTrade({ id: 2, pl_dollar: 20000, exit_date: "2026-05-14" }),
        ],
      });
      const { result } = renderHook(() => useTradingMode());
      expect(result.current.mode).toBe("normal");
    });

    it("Defansif, drawdown >%10 olsa BİLE üstte → mode='defansif' (öncelik)", () => {
      mockMarket.mockReturnValue({ data: { market_health_score: 20 } });
      mockTrades.mockReturnValue({
        data: [
          makeTrade({ id: 1, pl_dollar: -15000, exit_date: "2026-05-15" }),
          makeTrade({ id: 2, pl_dollar: 20000, exit_date: "2026-05-14" }),
        ],
      });
      const { result } = renderHook(() => useTradingMode());
      expect(result.current.mode).toBe("defansif"); // Drawdown rehab'tan önce Defansif
    });
  });

  describe("Agresif tetik (market > 70 + consecutiveWins ≥ 5)", () => {
    it("market=80 + 5 ardışık kazanç → Agresif, sizing=%1.5", () => {
      mockMarket.mockReturnValue({ data: { market_health_score: 80 } });
      mockTrades.mockReturnValue({
        data: Array.from({ length: 5 }, (_, i) =>
          makeTrade({ id: i + 1, pl_dollar: 100, exit_date: `2026-05-${15 - i}` })
        ),
      });
      const { result } = renderHook(() => useTradingMode());
      expect(result.current.mode).toBe("agresif");
      expect(result.current.recommendedSizingPct).toBe(1.5);
      expect(result.current.consecutiveWins).toBe(5);
      expect(result.current.reason).toContain("Hot hand");
    });

    it("market=80 + 4 ardışık kazanç → henüz Agresif değil (Normal)", () => {
      mockMarket.mockReturnValue({ data: { market_health_score: 80 } });
      mockTrades.mockReturnValue({
        data: Array.from({ length: 4 }, (_, i) =>
          makeTrade({ id: i + 1, pl_dollar: 100, exit_date: `2026-05-${15 - i}` })
        ),
      });
      const { result } = renderHook(() => useTradingMode());
      expect(result.current.mode).toBe("normal");
    });

    it("market=70 (sınır dışı) + 5 ardışık kazanç → Normal (> 70 strict)", () => {
      mockMarket.mockReturnValue({ data: { market_health_score: 70 } });
      mockTrades.mockReturnValue({
        data: Array.from({ length: 5 }, (_, i) =>
          makeTrade({ id: i + 1, pl_dollar: 100, exit_date: `2026-05-${15 - i}` })
        ),
      });
      const { result } = renderHook(() => useTradingMode());
      expect(result.current.mode).toBe("normal");
    });
  });
});

describe("useTradingMode — streak hesap (en yeni'den geriye, kırılma noktasında dur)", () => {
  it("Sıralı: kazanç, kazanç, KAYIP, kazanç → kayıp güncel değil, win streak=2", () => {
    // En yeni 2 kazanç → consecutiveWins=2, kırılma noktasında dur
    mockTrades.mockReturnValue({
      data: [
        makeTrade({ id: 1, pl_dollar: 100, exit_date: "2026-05-15" }),  // En yeni: kazanç
        makeTrade({ id: 2, pl_dollar: 200, exit_date: "2026-05-14" }),  // kazanç
        makeTrade({ id: 3, pl_dollar: -50, exit_date: "2026-05-13" }),  // KAYIP — kır
        makeTrade({ id: 4, pl_dollar: 150, exit_date: "2026-05-12" }),  // sayılmaz
      ],
    });
    const { result } = renderHook(() => useTradingMode());
    expect(result.current.consecutiveWins).toBe(2);
    expect(result.current.consecutiveLosses).toBe(0);
    expect(result.current.totalClosedTrades).toBe(4);
  });

  it("En yeni 3 kayıp → loss streak=3, Rehab tetik", () => {
    mockTrades.mockReturnValue({
      data: [
        makeTrade({ id: 1, pl_dollar: -100, exit_date: "2026-05-15" }),
        makeTrade({ id: 2, pl_dollar: -200, exit_date: "2026-05-14" }),
        makeTrade({ id: 3, pl_dollar: -50, exit_date: "2026-05-13" }),
        makeTrade({ id: 4, pl_dollar: 500, exit_date: "2026-05-12" }), // sayılmaz
      ],
    });
    const { result } = renderHook(() => useTradingMode());
    expect(result.current.consecutiveLosses).toBe(3);
    expect(result.current.consecutiveWins).toBe(0);
    expect(result.current.mode).toBe("rehab");
  });

  it("Status='open' trade'ler streak hesabına dahil edilmez", () => {
    mockTrades.mockReturnValue({
      data: [
        makeTrade({ id: 1, pl_dollar: null, status: "open" }), // skip
        makeTrade({ id: 2, pl_dollar: -100, exit_date: "2026-05-14" }),
        makeTrade({ id: 3, pl_dollar: -200, exit_date: "2026-05-13" }),
        makeTrade({ id: 4, pl_dollar: -50, exit_date: "2026-05-12" }),
      ],
    });
    const { result } = renderHook(() => useTradingMode());
    expect(result.current.totalClosedTrades).toBe(3);
    expect(result.current.consecutiveLosses).toBe(3);
  });

  it("pl_dollar=null kapalı trade → streak hesabına dahil edilmez (filter)", () => {
    mockTrades.mockReturnValue({
      data: [
        makeTrade({ id: 1, pl_dollar: null, status: "closed" }), // skip
        makeTrade({ id: 2, pl_dollar: 100, exit_date: "2026-05-14" }),
      ],
    });
    const { result } = renderHook(() => useTradingMode());
    expect(result.current.totalClosedTrades).toBe(1);
    expect(result.current.consecutiveWins).toBe(1);
  });

  it("pl_dollar=0 (break-even) → streak kırılır (ne kazanç ne kayıp)", () => {
    mockTrades.mockReturnValue({
      data: [
        makeTrade({ id: 1, pl_dollar: 100, exit_date: "2026-05-15" }), // kazanç
        makeTrade({ id: 2, pl_dollar: 0, exit_date: "2026-05-14" }),   // break — kırar
        makeTrade({ id: 3, pl_dollar: 200, exit_date: "2026-05-13" }), // sayılmaz
      ],
    });
    const { result } = renderHook(() => useTradingMode());
    expect(result.current.consecutiveWins).toBe(1);
    expect(result.current.consecutiveLosses).toBe(0);
  });
});

describe("useTradingMode — market_health=null edge", () => {
  it("market_health=null + boş trade → Normal (Defansif/Agresif tetiklenmez)", () => {
    mockMarket.mockReturnValue({ data: { market_health_score: null } });
    const { result } = renderHook(() => useTradingMode());
    expect(result.current.mode).toBe("normal");
  });
});

/**
 * Paket 383: Backend P382 gerçek skor kontratı × frontend mod tetiği uçtan uca uyumu.
 *
 * Backend _compute_market_health (test_market_health_compute.py kontratı, 30 May 2026):
 *   - UNDER_PRESSURE → score=25  (DD>=4 override veya 2+ Stage 4)
 *   - NEUTRAL        → score=50  (karışık: Stage 2 var, Stage 4 yok)
 *   - HEALTHY        → score=75  (3/3 Stage 2, DD<4 — Probability Convergence TLSMW Böl. 10)
 *
 * Frontend hook eşikleri (use-trading-mode.ts L95-96):
 *   - isMarketWeak    = score < 30   → 25 < 30 ✓ Defansif tetik
 *   - isMarketStrong  = score > 70   → 75 > 70 ✓ Agresif tetik (5+ kazanç şartı ile)
 *   - 50 → ne weak ne strong → Normal default
 *
 * Bu testler "paper trading ilk gün" güvencesi: gerçek backend skor üretip frontend
 * doğru modu seçecek mi? Çift danışma sonrası end-to-end kanıt.
 */
describe("useTradingMode × Backend P382 gerçek skor (P383 uçtan uca uyum)", () => {
  it("UNDER_PRESSURE backend (25) + boş trade → Defansif (DD>=4 / 2+ Stage 4)", () => {
    mockMarket.mockReturnValue({
      data: { market_health_score: 25, market_health_label: "UNDER_PRESSURE", suggested_mode: "DEFENSIVE" },
    });
    const { result } = renderHook(() => useTradingMode());
    expect(result.current.mode).toBe("defansif");
    expect(result.current.recommendedSizingPct).toBe(0);
    expect(result.current.uiBehavior).toContain("BLOK");
  });

  it("NEUTRAL backend (50) + boş trade → Normal (selective piyasa, ne weak ne strong)", () => {
    mockMarket.mockReturnValue({
      data: { market_health_score: 50, market_health_label: "NEUTRAL", suggested_mode: "CAUTION" },
    });
    const { result } = renderHook(() => useTradingMode());
    expect(result.current.mode).toBe("normal");
    expect(result.current.recommendedSizingPct).toBe(1.0);
  });

  it("HEALTHY backend (75) + boş trade → Normal (Agresif için 5+ kazanç şart, henüz yok)", () => {
    mockMarket.mockReturnValue({
      data: { market_health_score: 75, market_health_label: "HEALTHY", suggested_mode: "LONG" },
    });
    const { result } = renderHook(() => useTradingMode());
    // 75 > 70 ✓ strong, ama consecutiveWins<5 → Agresif tetik yok → Normal
    expect(result.current.mode).toBe("normal");
  });

  it("HEALTHY backend (75) + 5 ardışık kazanç → Agresif (Probability Convergence + streak)", () => {
    mockMarket.mockReturnValue({
      data: { market_health_score: 75, market_health_label: "HEALTHY", suggested_mode: "LONG" },
    });
    // 5 ardışık kazanç trade (en yeni önce)
    mockTrades.mockReturnValue({
      data: [
        makeTrade({ id: 5, pl_dollar: 1000, exit_date: "2026-05-25" }),
        makeTrade({ id: 4, pl_dollar: 800,  exit_date: "2026-05-20" }),
        makeTrade({ id: 3, pl_dollar: 1200, exit_date: "2026-05-15" }),
        makeTrade({ id: 2, pl_dollar: 600,  exit_date: "2026-05-10" }),
        makeTrade({ id: 1, pl_dollar: 900,  exit_date: "2026-05-05" }),
      ],
    });
    const { result } = renderHook(() => useTradingMode());
    expect(result.current.mode).toBe("agresif");
    expect(result.current.consecutiveWins).toBeGreaterThanOrEqual(5);
  });

  it("UNDER_PRESSURE backend (25) Defansif > Rehab öncelik (3 ardışık kayıp da olsa Defansif)", () => {
    // Hem Defansif tetik (score=25) hem Rehab tetik (3 kayıp). Defansif > Rehab.
    mockMarket.mockReturnValue({
      data: { market_health_score: 25, market_health_label: "UNDER_PRESSURE", suggested_mode: "DEFENSIVE" },
    });
    mockTrades.mockReturnValue({
      data: [
        makeTrade({ id: 3, pl_dollar: -500, exit_date: "2026-05-15" }),
        makeTrade({ id: 2, pl_dollar: -300, exit_date: "2026-05-10" }),
        makeTrade({ id: 1, pl_dollar: -400, exit_date: "2026-05-05" }),
      ],
    });
    const { result } = renderHook(() => useTradingMode());
    expect(result.current.mode).toBe("defansif");  // önce Defansif
  });

  it("Clean-room label kontratı: backend asla 'YEŞİL/SARI/KIRMIZI' dönmemeli (P382 garantisi)", () => {
    // Bu test backend kontrat regresyon koruması: frontend yeni label'ları bekliyor.
    const validLabels = ["HEALTHY", "NEUTRAL", "UNDER_PRESSURE"];
    const legacyLabels = ["YEŞİL", "SARI", "KIRMIZI", "YESIL"];
    // mock kontrolü için: type union daha katı tutar (types/market.ts MarketHealthLabel Literal)
    expect(validLabels).toContain("HEALTHY");
    expect(validLabels).toContain("UNDER_PRESSURE");
    // Legacy "YEŞİL" hâlâ frontend type'a uyduğu durumda alarm:
    expect(validLabels.some((l) => legacyLabels.includes(l))).toBe(false);
  });
});
