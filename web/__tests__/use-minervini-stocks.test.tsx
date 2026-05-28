/**
 * useMinerviniStocks — Minervini sayfa stock listesi.
 *
 * GET /api/minervini/stocks, 60s cache, retry: 1.
 */
import { describe, it, expect, vi, beforeEach, afterEach } from "vitest";
import { renderHook, waitFor } from "@testing-library/react";
import { useMinerviniStocks } from "@/hooks/use-minervini-stocks";
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

const STOCKS = [
  { symbol: "AAPL", rs_rating: 92, stage: 2 },
  { symbol: "NVDA", rs_rating: 99, stage: 2 },
  { symbol: "MSFT", rs_rating: 88, stage: 2 },
];

describe("useMinerviniStocks", () => {
  it("Fetch URL: /api/minervini/stocks", async () => {
    fetchMock.mockResolvedValue(jsonResponse(STOCKS));
    renderHook(() => useMinerviniStocks(), { wrapper: withQueryClient() });
    await waitFor(() => expect(fetchMock).toHaveBeenCalled());
    expect(fetchMock.mock.calls[0][0]).toBe("/api/minervini/stocks");
  });

  it("Happy → 3 stock array", async () => {
    fetchMock.mockResolvedValue(jsonResponse(STOCKS));
    const { result } = renderHook(() => useMinerviniStocks(), {
      wrapper: withQueryClient(),
    });
    await waitFor(() => expect(result.current.isSuccess).toBe(true));
    expect(result.current.data).toHaveLength(3);
  });

  it("Boş array → tarama sonucu yok", async () => {
    fetchMock.mockResolvedValue(jsonResponse([]));
    const { result } = renderHook(() => useMinerviniStocks(), {
      wrapper: withQueryClient(),
    });
    await waitFor(() => expect(result.current.isSuccess).toBe(true));
    expect(result.current.data).toEqual([]);
  });

  it("HTTP 500 → error 'HTTP 500'", async () => {
    fetchMock.mockResolvedValue(new Response("", { status: 500 }));
    const { result } = renderHook(() => useMinerviniStocks(), {
      wrapper: withQueryClient(),
    });
    await waitFor(() => expect(result.current.isError).toBe(true), {
      timeout: 3000,
    });
    expect((result.current.error as Error).message).toBe("HTTP 500");
  });
});
