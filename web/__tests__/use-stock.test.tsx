/**
 * useStockInfo + useOhlcv — Hisse detay sayfası (info + OHLCV bar grafiği).
 *
 * GET /api/stock/{symbol}/info ve .../ohlcv, 60s cache, retry: 1.
 */
import { describe, it, expect, vi, beforeEach, afterEach } from "vitest";
import { renderHook, waitFor } from "@testing-library/react";
import { useStockInfo, useOhlcv } from "@/hooks/use-stock";
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

const INFO_HAPPY = {
  symbol: "AAPL",
  name: "Apple Inc.",
  sector: "Technology",
  industry: "Consumer Electronics",
  market_cap: 3_000_000_000,
};

const OHLCV_HAPPY = [
  { date: "2026-05-26", open: 145, high: 152, low: 144, close: 150, volume: 50_000_000 },
  { date: "2026-05-27", open: 150, high: 155, low: 149, close: 152, volume: 48_000_000 },
];

describe("useStockInfo — Hisse meta info", () => {
  it("Fetch URL: /api/stock/AAPL/info", async () => {
    fetchMock.mockResolvedValue(jsonResponse(INFO_HAPPY));
    renderHook(() => useStockInfo("AAPL"), { wrapper: withQueryClient() });
    await waitFor(() => expect(fetchMock).toHaveBeenCalled());
    expect(fetchMock.mock.calls[0][0]).toBe("/api/stock/AAPL/info");
  });

  it("Happy → data döner (name + sector)", async () => {
    fetchMock.mockResolvedValue(jsonResponse(INFO_HAPPY));
    const { result } = renderHook(() => useStockInfo("AAPL"), {
      wrapper: withQueryClient(),
    });
    await waitFor(() => expect(result.current.isSuccess).toBe(true));
    expect(result.current.data?.symbol).toBe("AAPL");
    expect(result.current.data?.name).toBe("Apple Inc.");
  });

  it("HTTP 404 → error 'HTTP 404'", async () => {
    fetchMock.mockResolvedValue(new Response("", { status: 404 }));
    const { result } = renderHook(() => useStockInfo("UNKNOWN"), {
      wrapper: withQueryClient(),
    });
    await waitFor(() => expect(result.current.isError).toBe(true), {
      timeout: 3000,
    });
    expect((result.current.error as Error).message).toBe("HTTP 404");
  });
});

describe("useOhlcv — Lightweight Charts data", () => {
  it("Fetch URL: /api/stock/AAPL/ohlcv", async () => {
    fetchMock.mockResolvedValue(jsonResponse(OHLCV_HAPPY));
    renderHook(() => useOhlcv("AAPL"), { wrapper: withQueryClient() });
    await waitFor(() => expect(fetchMock).toHaveBeenCalled());
    expect(fetchMock.mock.calls[0][0]).toBe("/api/stock/AAPL/ohlcv");
  });

  it("Happy → bar array döner", async () => {
    fetchMock.mockResolvedValue(jsonResponse(OHLCV_HAPPY));
    const { result } = renderHook(() => useOhlcv("AAPL"), {
      wrapper: withQueryClient(),
    });
    await waitFor(() => expect(result.current.isSuccess).toBe(true));
    expect(result.current.data).toHaveLength(2);
    expect(result.current.data?.[0].close).toBe(150);
    expect(result.current.data?.[1].close).toBe(152);
  });

  it("Boş array → tarihsel veri yok (yeni IPO)", async () => {
    fetchMock.mockResolvedValue(jsonResponse([]));
    const { result } = renderHook(() => useOhlcv("NEW"), {
      wrapper: withQueryClient(),
    });
    await waitFor(() => expect(result.current.isSuccess).toBe(true));
    expect(result.current.data).toEqual([]);
  });

  it("HTTP 500 → error 'HTTP 500'", async () => {
    fetchMock.mockResolvedValue(new Response("", { status: 500 }));
    const { result } = renderHook(() => useOhlcv("AAPL"), {
      wrapper: withQueryClient(),
    });
    await waitFor(() => expect(result.current.isError).toBe(true), {
      timeout: 3000,
    });
    expect((result.current.error as Error).message).toBe("HTTP 500");
  });
});

describe("useStockInfo + useOhlcv — queryKey ayrımı (cache izolasyon)", () => {
  it("Aynı symbol farklı endpoint → 2 ayrı queryKey", async () => {
    fetchMock.mockImplementation(async (url) => {
      const u = url.toString();
      if (u.endsWith("/info")) return jsonResponse(INFO_HAPPY);
      if (u.endsWith("/ohlcv")) return jsonResponse(OHLCV_HAPPY);
      return new Response("", { status: 404 });
    });
    const { result: infoR } = renderHook(() => useStockInfo("AAPL"), {
      wrapper: withQueryClient(),
    });
    const { result: ohlcvR } = renderHook(() => useOhlcv("AAPL"), {
      wrapper: withQueryClient(),
    });
    await waitFor(() => expect(infoR.current.isSuccess).toBe(true));
    await waitFor(() => expect(ohlcvR.current.isSuccess).toBe(true));
    // İki hook bağımsız cache, info data != ohlcv data
    expect(infoR.current.data).toEqual(INFO_HAPPY);
    expect(Array.isArray(ohlcvR.current.data)).toBe(true);
  });
});
