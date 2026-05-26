"use client";

import { useQuery } from "@tanstack/react-query";

/**
 * Paket 149 (26 May 2026): Sembol arama autocomplete hook.
 * Backend: /api/symbols/search?q=...&limit=10 (Paket 148)
 * Quanfina hisse evreni (_STOCK_META 56 sembol) içinde arama.
 */
export interface SymbolSearchResult {
  symbol: string;
  name: string;
  sector: string;
  /**
   * Paket 211 (27 May 2026): Kaynak ayrımı — kullanıcı bilgi.
   * - "universe": Quanfina bilinen evren (_STOCK_META 106 sembol)
   * - "yfinance": Evren dışı, yfinance Ticker.info ile doğrulandı
   * Backward compat: alanı olmayan eski response'larda "universe" varsayılır.
   */
  source?: "universe" | "yfinance";
}

async function fetchSymbolSearch(q: string, limit: number): Promise<SymbolSearchResult[]> {
  if (!q || q.length < 1) return [];
  const url = `/api/symbols/search?q=${encodeURIComponent(q)}&limit=${limit}`;
  const res = await fetch(url, { signal: AbortSignal.timeout(5000) });
  if (!res.ok) return [];
  return res.json();
}

export function useSymbolSearch(q: string, limit: number = 10) {
  return useQuery({
    queryKey: ["symbol-search", q.toUpperCase(), limit],
    queryFn: () => fetchSymbolSearch(q, limit),
    enabled: q.length >= 1,
    staleTime: 60_000, // Sembol evreni nadiren değişir
    retry: 0,
  });
}
