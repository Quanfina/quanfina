/**
 * PortfolioSummaryCard (P156 + P190 + P202) — Mark TTLC s.85 sektör konsantrasyon.
 *
 * Dashboard portfolio özet kartı: açık trade + canlı quote + sektör %25/30 uyarı.
 */
import { describe, it, expect, vi, beforeEach } from "vitest";
import { render, screen } from "@testing-library/react";
import { PortfolioSummaryCard } from "@/components/dashboard/PortfolioSummaryCard";
import type { Trade } from "@/types/trade";
import type { StockQuote } from "@/hooks/use-stock-quote";

// next/link mock
vi.mock("next/link", () => ({
  default: ({ children, href, ...rest }: { children: React.ReactNode; href: string; [k: string]: unknown }) => (
    <a href={href} {...rest}>
      {children}
    </a>
  ),
}));

// useTrades mock
const mockTradesResult = vi.fn<() => { data?: Trade[]; isLoading: boolean }>();
vi.mock("@/hooks/use-trades", () => ({
  useTrades: () => mockTradesResult(),
}));

// useStockQuotes mock
const mockQuotes = vi.fn<(symbols: string[]) => Array<{ data?: StockQuote }>>();
vi.mock("@/hooks/use-stock-quote", () => ({
  useStockQuotes: (symbols: string[]) => mockQuotes(symbols),
}));

// usePositionAlerts side-effect only mock
vi.mock("@/hooks/use-position-alerts", () => ({
  usePositionAlerts: vi.fn(),
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
    sector: "Technology",
    current_price: 110,
  };
  return { ...base, ...overrides };
}

function makeQuote(symbol: string, price: number, changePct = 1.5): { data: StockQuote } {
  return {
    data: { symbol, price, change_dollar: 0, change_pct: changePct, source: "yfinance" },
  };
}

beforeEach(() => {
  mockTradesResult.mockReset();
  mockQuotes.mockReset();
  mockQuotes.mockReturnValue([]);
});

describe("PortfolioSummaryCard — Loading + boş durum", () => {
  it("isLoading=true → 'Portfolio yükleniyor...'", () => {
    mockTradesResult.mockReturnValue({ data: undefined, isLoading: true });
    render(<PortfolioSummaryCard />);
    expect(screen.getByText(/Portfolio yükleniyor/)).toBeInTheDocument();
  });

  it("openCount=0 → 'Henüz açık trade yok' + /journal link", () => {
    mockTradesResult.mockReturnValue({ data: [], isLoading: false });
    const { container } = render(<PortfolioSummaryCard />);
    expect(screen.getByText(/Henüz açık trade yok/)).toBeInTheDocument();
    expect(container.querySelector("a")?.getAttribute("href")).toBe("/journal");
  });

  it("status='closed' tek trade → boş durum render", () => {
    mockTradesResult.mockReturnValue({
      data: [makeTrade({ status: "closed", exit_price: 110 })],
      isLoading: false,
    });
    render(<PortfolioSummaryCard />);
    expect(screen.getByText(/Henüz açık trade yok/)).toBeInTheDocument();
  });
});

describe("PortfolioSummaryCard — Sektör konsantrasyon (P190 + Mark TTLC s.85)", () => {
  it("Tek sektör (Technology %100) → kırmızı uyarı 'max 25-30%'", () => {
    // 100% Technology → >30% → sectorWarning=true
    mockTradesResult.mockReturnValue({
      data: [makeTrade({ sector: "Technology", current_price: 110 })],
      isLoading: false,
    });
    mockQuotes.mockReturnValue([makeQuote("AAPL", 110)]);
    render(<PortfolioSummaryCard />);
    expect(screen.getByText(/Technology/)).toBeInTheDocument();
    expect(screen.getByText(/100%/)).toBeInTheDocument();
    expect(screen.getByText(/Mark TTLC s\.85.*max 25-30%/)).toBeInTheDocument();
  });

  it("Map agregasyon: 3 trade aynı Technology sektör → toplam %", () => {
    const trades = [
      makeTrade({ id: 1, symbol: "AAPL", sector: "Technology", current_price: 110 }),
      makeTrade({ id: 2, symbol: "NVDA", sector: "Technology", current_price: 200 }),
      makeTrade({ id: 3, symbol: "MSFT", sector: "Technology", current_price: 300 }),
    ];
    mockTradesResult.mockReturnValue({ data: trades, isLoading: false });
    mockQuotes.mockReturnValue([
      makeQuote("AAPL", 110),
      makeQuote("NVDA", 200),
      makeQuote("MSFT", 300),
    ]);
    render(<PortfolioSummaryCard />);
    // 3 trade aynı sektörde toplam → topSectorPct = 100
    expect(screen.getByText(/Technology/)).toBeInTheDocument();
  });

  it("Çoklu sektör Technology %20 + Energy %80 → 'En yoğun: Energy' 80%", () => {
    const trades = [
      // Technology 1 × 100 × 100 = 10000 → %20
      makeTrade({
        id: 1, symbol: "AAPL", sector: "Technology",
        entry_price: 100, current_price: 100, shares: 100,
      }),
      // Energy 1 × 400 × 100 = 40000 → %80
      makeTrade({
        id: 2, symbol: "XOM", sector: "Energy",
        entry_price: 400, current_price: 400, shares: 100,
      }),
    ];
    mockTradesResult.mockReturnValue({ data: trades, isLoading: false });
    mockQuotes.mockReturnValue([
      makeQuote("AAPL", 100),
      makeQuote("XOM", 400),
    ]);
    render(<PortfolioSummaryCard />);
    expect(screen.getByText(/Energy/)).toBeInTheDocument();
    expect(screen.getByText(/80%/)).toBeInTheDocument();
  });

  it("sector=null → 'Bilinmiyor' fallback bucket'a düşer", () => {
    mockTradesResult.mockReturnValue({
      data: [makeTrade({ sector: null })],
      isLoading: false,
    });
    mockQuotes.mockReturnValue([makeQuote("AAPL", 110)]);
    render(<PortfolioSummaryCard />);
    expect(screen.getByText(/Bilinmiyor/)).toBeInTheDocument();
  });
});

describe("PortfolioSummaryCard — Ana metrikler (canlı değer + P&L)", () => {
  it("entry=100 × 100 shares, current=110 → +$1000 unrealized P&L (%10)", () => {
    mockTradesResult.mockReturnValue({
      data: [makeTrade({ entry_price: 100, shares: 100, current_price: 110 })],
      isLoading: false,
    });
    mockQuotes.mockReturnValue([makeQuote("AAPL", 110)]);
    render(<PortfolioSummaryCard />);
    // Canlı değer = 110 × 100 = $11,000
    expect(screen.getByText(/\$11,000/)).toBeInTheDocument();
    // Unrealized P&L = +$1,000 (+10.00%)
    expect(screen.getByText(/\+10\.00%/)).toBeInTheDocument();
  });

  it("zarar pozisyon: entry=100 current=92 → kırmızı '−$800 (−8%)'", () => {
    mockTradesResult.mockReturnValue({
      data: [makeTrade({ entry_price: 100, shares: 100, current_price: 92 })],
      isLoading: false,
    });
    mockQuotes.mockReturnValue([makeQuote("AAPL", 92, -2)]);
    render(<PortfolioSummaryCard />);
    // Unrealized = -800 → component "-" prefix + abs gösterir
    expect(screen.getByText(/-\$800/)).toBeInTheDocument();
  });

  it("Footer: '{openCount} açık • ${investedCapital} yatırım'", () => {
    mockTradesResult.mockReturnValue({
      data: [
        makeTrade({ id: 1, symbol: "AAPL", entry_price: 100, shares: 100 }),
        makeTrade({ id: 2, symbol: "NVDA", entry_price: 200, shares: 50 }),
      ],
      isLoading: false,
    });
    mockQuotes.mockReturnValue([
      makeQuote("AAPL", 110),
      makeQuote("NVDA", 210),
    ]);
    render(<PortfolioSummaryCard />);
    // 100×100 + 200×50 = 20000
    expect(screen.getByText(/2 açık.*\$20,000\.00 yatırım/)).toBeInTheDocument();
  });

  it("En İyi/En Kötü: AAPL +%10 (best), NVDA -%5 (worst)", () => {
    mockTradesResult.mockReturnValue({
      data: [
        makeTrade({ id: 1, symbol: "AAPL", entry_price: 100, shares: 100 }),
        makeTrade({ id: 2, symbol: "NVDA", entry_price: 200, shares: 100 }),
      ],
      isLoading: false,
    });
    mockQuotes.mockReturnValue([
      makeQuote("AAPL", 110),
      makeQuote("NVDA", 190),
    ]);
    render(<PortfolioSummaryCard />);
    expect(screen.getByText(/AAPL \+10\.0%/)).toBeInTheDocument();
    expect(screen.getByText(/NVDA -5\.0%/)).toBeInTheDocument();
  });
});
