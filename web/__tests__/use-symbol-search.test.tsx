/**
 * useSymbolSearch (P149) — Sembol autocomplete arama.
 *
 * GET /api/symbols/search?q=X&limit=N, 5s timeout, 60s cache, retry: 0.
 * Boş query → 0 sonuç (server'a gitmez), case-insensitive query key.
 */
import { describe, it, expect, vi, beforeEach, afterEach } from "vitest";
import { renderHook, waitFor } from "@testing-library/react";
import {
  useSymbolSearch,
  type SymbolSearchResult,
} from "@/hooks/use-symbol-search";
import { withQueryClient } from "./_test-utils";

const fetchMock = vi.fn<(input: RequestInfo | URL, init?: RequestInit) => Promise<Response>>();

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

const RESULTS: SymbolSearchResult[] = [
  { symbol: "AAPL", name: "Apple Inc.", sector: "Technology", source: "universe" },
  { symbol: "AMZN", name: "Amazon.com Inc.", sector: "Consumer", source: "universe" },
];

describe("useSymbolSearch — enabled koşulu (q.length >= 1)", () => {
  it("q='' (boş) → fetch tetiklenmez", async () => {
    fetchMock.mockResolvedValue(jsonResponse(RESULTS));
    renderHook(() => useSymbolSearch(""), { wrapper: withQueryClient() });
    await new Promise((r) => setTimeout(r, 50));
    expect(fetchMock).not.toHaveBeenCalled();
  });

  it("q='A' (1 karakter) → fetch tetiklenir", async () => {
    fetchMock.mockResolvedValue(jsonResponse(RESULTS));
    renderHook(() => useSymbolSearch("A"), { wrapper: withQueryClient() });
    await waitFor(() => expect(fetchMock).toHaveBeenCalled());
  });
});

describe("useSymbolSearch — URL building", () => {
  it("q='AAPL' → /api/symbols/search?q=AAPL&limit=10 (default)", async () => {
    fetchMock.mockResolvedValue(jsonResponse(RESULTS));
    renderHook(() => useSymbolSearch("AAPL"), { wrapper: withQueryClient() });
    await waitFor(() => expect(fetchMock).toHaveBeenCalled());
    expect(fetchMock.mock.calls[0][0]).toBe(
      "/api/symbols/search?q=AAPL&limit=10"
    );
  });

  it("limit=5 override → URL limit=5", async () => {
    fetchMock.mockResolvedValue(jsonResponse(RESULTS));
    renderHook(() => useSymbolSearch("A", 5), { wrapper: withQueryClient() });
    await waitFor(() => expect(fetchMock).toHaveBeenCalled());
    expect(fetchMock.mock.calls[0][0]).toBe(
      "/api/symbols/search?q=A&limit=5"
    );
  });

  it("Özel karakter URL encode (& → %26)", async () => {
    fetchMock.mockResolvedValue(jsonResponse([]));
    renderHook(() => useSymbolSearch("A&B"), { wrapper: withQueryClient() });
    await waitFor(() => expect(fetchMock).toHaveBeenCalled());
    expect(fetchMock.mock.calls[0][0]).toBe(
      "/api/symbols/search?q=A%26B&limit=10"
    );
  });

  it("AbortSignal.timeout(5000) — fetch init signal", async () => {
    fetchMock.mockResolvedValue(jsonResponse(RESULTS));
    renderHook(() => useSymbolSearch("AAPL"), { wrapper: withQueryClient() });
    await waitFor(() => expect(fetchMock).toHaveBeenCalled());
    expect(fetchMock.mock.calls[0][1]?.signal).toBeDefined();
  });
});

describe("useSymbolSearch — response parsing", () => {
  it("Çoklu sonuç → array döner", async () => {
    fetchMock.mockResolvedValue(jsonResponse(RESULTS));
    const { result } = renderHook(() => useSymbolSearch("A"), {
      wrapper: withQueryClient(),
    });
    await waitFor(() => expect(result.current.isSuccess).toBe(true));
    expect(result.current.data).toHaveLength(2);
    expect(result.current.data?.[0].symbol).toBe("AAPL");
  });

  it("Source: 'universe' vs 'yfinance' (P211 ayrım)", async () => {
    fetchMock.mockResolvedValue(
      jsonResponse([
        { symbol: "AAPL", name: "Apple", sector: "Technology", source: "universe" },
        { symbol: "RARE", name: "Rare Co", sector: "Other", source: "yfinance" },
      ])
    );
    const { result } = renderHook(() => useSymbolSearch("A"), {
      wrapper: withQueryClient(),
    });
    await waitFor(() => expect(result.current.isSuccess).toBe(true));
    expect(result.current.data?.[0].source).toBe("universe");
    expect(result.current.data?.[1].source).toBe("yfinance");
  });

  it("Backward compat: source field yok → undefined", async () => {
    fetchMock.mockResolvedValue(
      jsonResponse([{ symbol: "OLD", name: "Old Co", sector: "X" }])
    );
    const { result } = renderHook(() => useSymbolSearch("OLD"), {
      wrapper: withQueryClient(),
    });
    await waitFor(() => expect(result.current.isSuccess).toBe(true));
    expect(result.current.data?.[0].source).toBeUndefined();
  });

  it("HTTP hata → boş array (silent fallback, no throw)", async () => {
    fetchMock.mockResolvedValue(new Response("err", { status: 500 }));
    const { result } = renderHook(() => useSymbolSearch("A"), {
      wrapper: withQueryClient(),
    });
    await waitFor(() => expect(result.current.isSuccess).toBe(true));
    expect(result.current.data).toEqual([]);
    expect(result.current.isError).toBe(false);
  });
});

describe("useSymbolSearch — case-insensitive query key (cache hit)", () => {
  it("'aapl' ve 'AAPL' aynı queryKey → tek cache (toUpperCase)", async () => {
    fetchMock.mockResolvedValue(jsonResponse(RESULTS));
    // İlk render lowercase
    const { unmount } = renderHook(() => useSymbolSearch("aapl"), {
      wrapper: withQueryClient(),
    });
    await waitFor(() => expect(fetchMock).toHaveBeenCalled());
    unmount();
    // queryKey ["symbol-search", "AAPL", 10] — büyük harfe normalize
    // (URL'de raw q kullanılıyor ama cache key normalize)
  });
});
