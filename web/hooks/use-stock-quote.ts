"use client";

import { useQueries, useQuery } from "@tanstack/react-query";

/**
 * Paket 150-151 (26 May 2026): Stock quote canlı fiyat hook.
 *
 * Backend: GET /api/stock/{symbol}/quote (yfinance + 5dk cache)
 * Kullanım: Paper trading açık trade unrealized P&L hesabı.
 */
export interface StockQuote {
  symbol: string;
  price: number;
  change_dollar: number | null;
  change_pct: number | null;
  source: "yfinance" | "mock";
}

async function fetchQuote(symbol: string): Promise<StockQuote> {
  const res = await fetch(`/api/stock/${symbol}/quote`, {
    signal: AbortSignal.timeout(10000),
  });
  if (!res.ok) throw new Error(`HTTP ${res.status}`);
  return res.json();
}

/** Tek sembol için canlı fiyat. */
export function useStockQuote(symbol: string) {
  return useQuery({
    queryKey: ["stock-quote", symbol.toUpperCase()],
    queryFn: () => fetchQuote(symbol),
    enabled: symbol.length > 0,
    staleTime: 60_000,   // 1 dk — backend cache 5dk, frontend daha kısa
    refetchInterval: 60_000,  // Açık trade satırı için otomatik yenile
    retry: 0,
  });
}

/** Çoklu sembol için paralel canlı fiyatlar (TradeTable batch). */
export function useStockQuotes(symbols: string[]) {
  const upper = symbols.map((s) => s.toUpperCase());
  const unique = Array.from(new Set(upper));
  return useQueries({
    queries: unique.map((sym) => ({
      queryKey: ["stock-quote", sym],
      queryFn: () => fetchQuote(sym),
      staleTime: 60_000,
      refetchInterval: 60_000,
      retry: 0,
    })),
  });
}
