/**
 * useStockQuote + useStockQuotes (P150-151).
 *
 * Tekli + çoklu paralel quote (TradeTable batch). yfinance API + 5dk backend cache.
 */
import { describe, it, expect, vi, beforeEach, afterEach } from "vitest";
import { renderHook, waitFor } from "@testing-library/react";
import {
  useStockQuote,
  useStockQuotes,
  type StockQuote,
} from "@/hooks/use-stock-quote";
import { withQueryClient } from "./_test-utils";

const fetchMock = vi.fn<[RequestInfo | URL, RequestInit?], Promise<Response>>();

beforeEach(() => {
  fetchMock.mockReset();
  vi.stubGlobal("fetch", fetchMock);
});

afterEach(() => {
  vi.unstubAllGlobals();
});

function jsonResponse(body: unknown, status = 200): Response {
  return new Response(JSON.stringify(body), {
    status,
    headers: { "Content-Type": "application/json" },
  });
}

function makeQuote(symbol: string, price: number): StockQuote {
  return {
    symbol,
    price,
    change_dollar: price * 0.01,
    change_pct: 1.0,
    source: "yfinance",
  };
}

describe("useStockQuote — tekli quote", () => {
  it("symbol='AAPL' → /api/stock/AAPL/quote fetch", async () => {
    fetchMock.mockResolvedValue(jsonResponse(makeQuote("AAPL", 150)));
    renderHook(() => useStockQuote("AAPL"), { wrapper: withQueryClient() });
    await waitFor(() => expect(fetchMock).toHaveBeenCalled());
    expect(fetchMock.mock.calls[0][0]).toBe("/api/stock/AAPL/quote");
  });

  it("Boş symbol='' → fetch tetiklenmez (enabled=false)", async () => {
    fetchMock.mockResolvedValue(jsonResponse(makeQuote("AAPL", 150)));
    renderHook(() => useStockQuote(""), { wrapper: withQueryClient() });
    await new Promise((r) => setTimeout(r, 50));
    expect(fetchMock).not.toHaveBeenCalled();
  });

  it("Quote → data döner (price + change)", async () => {
    fetchMock.mockResolvedValue(jsonResponse(makeQuote("NVDA", 215.57)));
    const { result } = renderHook(() => useStockQuote("NVDA"), {
      wrapper: withQueryClient(),
    });
    await waitFor(() => expect(result.current.isSuccess).toBe(true));
    expect(result.current.data?.price).toBe(215.57);
    expect(result.current.data?.source).toBe("yfinance");
  });

  it("HTTP 404 (sembol bulunmadı) → error", async () => {
    fetchMock.mockResolvedValue(new Response("not found", { status: 404 }));
    const { result } = renderHook(() => useStockQuote("INVALID"), {
      wrapper: withQueryClient(),
    });
    await waitFor(() => expect(result.current.isError).toBe(true));
    expect((result.current.error as Error).message).toContain("404");
  });

  it("AbortSignal.timeout(10000) — fetch init'te signal var", async () => {
    fetchMock.mockResolvedValue(jsonResponse(makeQuote("AAPL", 150)));
    renderHook(() => useStockQuote("AAPL"), { wrapper: withQueryClient() });
    await waitFor(() => expect(fetchMock).toHaveBeenCalled());
    const init = fetchMock.mock.calls[0][1];
    expect(init?.signal).toBeDefined();
  });
});

describe("useStockQuotes — çoklu paralel (TradeTable batch)", () => {
  it("Boş array → 0 query", async () => {
    fetchMock.mockResolvedValue(jsonResponse(makeQuote("AAPL", 150)));
    const { result } = renderHook(() => useStockQuotes([]), {
      wrapper: withQueryClient(),
    });
    expect(result.current).toHaveLength(0);
    await new Promise((r) => setTimeout(r, 50));
    expect(fetchMock).not.toHaveBeenCalled();
  });

  it("['AAPL','NVDA','MSFT'] → 3 paralel fetch çağrısı", async () => {
    fetchMock.mockImplementation(async (url) => {
      const u = url.toString();
      const sym = u.split("/")[3];
      return jsonResponse(makeQuote(sym, 100));
    });
    const { result } = renderHook(
      () => useStockQuotes(["AAPL", "NVDA", "MSFT"]),
      { wrapper: withQueryClient() }
    );
    await waitFor(() => {
      expect(result.current.every((r) => r.isSuccess)).toBe(true);
    });
    expect(fetchMock).toHaveBeenCalledTimes(3);
    const urls = fetchMock.mock.calls.map((c) => c[0]);
    expect(urls).toContain("/api/stock/AAPL/quote");
    expect(urls).toContain("/api/stock/NVDA/quote");
    expect(urls).toContain("/api/stock/MSFT/quote");
  });

  it("Dedup: ['AAPL','aapl','AAPL'] → 1 unique fetch (upper case + Set)", async () => {
    fetchMock.mockResolvedValue(jsonResponse(makeQuote("AAPL", 150)));
    renderHook(() => useStockQuotes(["AAPL", "aapl", "AAPL"]), {
      wrapper: withQueryClient(),
    });
    await waitFor(() => expect(fetchMock).toHaveBeenCalled());
    // 1 unique sembol → 1 fetch
    expect(fetchMock).toHaveBeenCalledTimes(1);
    expect(fetchMock.mock.calls[0][0]).toBe("/api/stock/AAPL/quote");
  });

  it("Lower case input → upper case query (case normalize)", async () => {
    fetchMock.mockResolvedValue(jsonResponse(makeQuote("AAPL", 150)));
    renderHook(() => useStockQuotes(["aapl"]), {
      wrapper: withQueryClient(),
    });
    await waitFor(() => expect(fetchMock).toHaveBeenCalled());
    expect(fetchMock.mock.calls[0][0]).toBe("/api/stock/AAPL/quote");
  });

  it("Bir sembol hata, diğerleri başarılı → kısmi sonuç (her query izole)", async () => {
    fetchMock.mockImplementation(async (url) => {
      const u = url.toString();
      if (u.includes("INVALID")) {
        return new Response("not found", { status: 404 });
      }
      const sym = u.split("/")[3];
      return jsonResponse(makeQuote(sym, 100));
    });
    const { result } = renderHook(
      () => useStockQuotes(["AAPL", "INVALID"]),
      { wrapper: withQueryClient() }
    );
    await waitFor(() => {
      expect(result.current.every((r) => !r.isFetching)).toBe(true);
    });
    const successes = result.current.filter((r) => r.isSuccess);
    const errors = result.current.filter((r) => r.isError);
    expect(successes).toHaveLength(1);
    expect(errors).toHaveLength(1);
  });
});
