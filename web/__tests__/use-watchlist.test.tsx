/**
 * useWatchlist — Watchlist 4-katman (watch/on_deck/focus/buy) data fetch.
 *
 * GET /api/watchlist, 8s timeout, 60s cache, retry: 1.
 */
import { describe, it, expect, vi, beforeEach, afterEach } from "vitest";
import { renderHook, waitFor } from "@testing-library/react";
import { useWatchlist } from "@/hooks/use-watchlist";
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

describe("useWatchlist — fetch + AbortSignal", () => {
  it("Fetch URL: /api/watchlist + AbortSignal", async () => {
    fetchMock.mockResolvedValue(jsonResponse([]));
    renderHook(() => useWatchlist(), { wrapper: withQueryClient() });
    await waitFor(() => expect(fetchMock).toHaveBeenCalled());
    expect(fetchMock.mock.calls[0][0]).toBe("/api/watchlist");
    expect(fetchMock.mock.calls[0][1]?.signal).toBeDefined();
  });

  it("Boş array → data=[]", async () => {
    fetchMock.mockResolvedValue(jsonResponse([]));
    const { result } = renderHook(() => useWatchlist(), {
      wrapper: withQueryClient(),
    });
    await waitFor(() => expect(result.current.isSuccess).toBe(true));
    expect(result.current.data).toEqual([]);
  });

  it("Çoklu satır → array döner", async () => {
    const rows = [
      { symbol: "AAPL", strategy: "minervini", status: "focus" },
      { symbol: "NVDA", strategy: "carr", status: "buy" },
      { symbol: "MSFT", strategy: "minervini", status: "watch" },
    ];
    fetchMock.mockResolvedValue(jsonResponse(rows));
    const { result } = renderHook(() => useWatchlist(), {
      wrapper: withQueryClient(),
    });
    await waitFor(() => expect(result.current.isSuccess).toBe(true));
    expect(result.current.data).toHaveLength(3);
  });
});

describe("useWatchlist — Hata + 160 char slice", () => {
  it("500 + uzun body → error mesajında ilk 160 char", async () => {
    const longBody = "X".repeat(500);
    fetchMock.mockImplementation(async () =>
      new Response(longBody, { status: 500 })
    );
    const { result } = renderHook(() => useWatchlist(), {
      wrapper: withQueryClient(),
    });
    await waitFor(() => expect(result.current.isError).toBe(true), {
      timeout: 3000,
    });
    const msg = (result.current.error as Error).message;
    expect(msg).toContain("500");
    // Mesajda body 160 karakteri var (HTTP 500 prefix sonrası)
    expect(msg).toContain("X".repeat(160));
    // 161. karakter yok (slice cutoff)
    expect(msg.includes("X".repeat(170))).toBe(false);
  });

  it("503 + boş body → 'HTTP 503' (detail eklenmez)", async () => {
    fetchMock.mockImplementation(async () => new Response("", { status: 503 }));
    const { result } = renderHook(() => useWatchlist(), {
      wrapper: withQueryClient(),
    });
    await waitFor(() => expect(result.current.isError).toBe(true), {
      timeout: 3000,
    });
    expect((result.current.error as Error).message).toBe("HTTP 503");
  });

  it("Network fail → error", async () => {
    fetchMock.mockRejectedValue(new Error("offline"));
    const { result } = renderHook(() => useWatchlist(), {
      wrapper: withQueryClient(),
    });
    await waitFor(() => expect(result.current.isError).toBe(true), {
      timeout: 3000,
    });
    expect((result.current.error as Error).message).toBe("offline");
  });
});
