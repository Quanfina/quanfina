/**
 * Watchlist CRUD mutation hook ailesi:
 *   useAddWatchlistRow / useUpdateWatchlistRow / useDeleteWatchlistRow / usePromoteWatchlistRow
 *
 * POST/PATCH/DELETE/POST endpoint'leri + invalidate ["watchlist"] cache pattern.
 */
import { describe, it, expect, vi, beforeEach, afterEach } from "vitest";
import { renderHook, waitFor, act } from "@testing-library/react";
import {
  useAddWatchlistRow,
  useUpdateWatchlistRow,
  useDeleteWatchlistRow,
  usePromoteWatchlistRow,
} from "@/hooks/use-watchlist-mutations";
import { withQueryClient, makeQueryClient } from "./_test-utils";

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

describe("useAddWatchlistRow — POST /api/watchlist", () => {
  it("URL + method POST + body JSON", async () => {
    fetchMock.mockResolvedValue(jsonResponse({ symbol: "AAPL", strategy: "minervini" }));
    const { result } = renderHook(() => useAddWatchlistRow(), {
      wrapper: withQueryClient(),
    });
    act(() => {
      result.current.mutate({ symbol: "AAPL", strategy: "minervini", status: "watch" });
    });
    await waitFor(() => expect(result.current.isSuccess).toBe(true));
    expect(fetchMock.mock.calls[0][0]).toBe("/api/watchlist");
    expect(fetchMock.mock.calls[0][1]?.method).toBe("POST");
    const body = JSON.parse(fetchMock.mock.calls[0][1]!.body as string);
    expect(body.symbol).toBe("AAPL");
  });

  it("422 + FastAPI detail → error mesajı", async () => {
    fetchMock.mockResolvedValue(
      new Response(JSON.stringify({ detail: "PRIMARY KEY çakışması" }), {
        status: 422,
      })
    );
    const { result } = renderHook(() => useAddWatchlistRow(), {
      wrapper: withQueryClient(),
    });
    act(() => {
      result.current.mutate({ symbol: "X", strategy: "minervini", status: "watch" });
    });
    await waitFor(() => expect(result.current.isError).toBe(true));
    expect((result.current.error as Error).message).toContain("PRIMARY KEY");
  });

  it("onSuccess → ['watchlist'] cache invalidate", async () => {
    fetchMock.mockResolvedValue(jsonResponse({ symbol: "AAPL" }));
    const qc = makeQueryClient();
    const spy = vi.spyOn(qc, "invalidateQueries");
    const { result } = renderHook(() => useAddWatchlistRow(), {
      wrapper: withQueryClient(qc),
    });
    act(() => {
      result.current.mutate({ symbol: "A", strategy: "minervini", status: "watch" });
    });
    await waitFor(() => expect(result.current.isSuccess).toBe(true));
    expect(spy).toHaveBeenCalledWith({ queryKey: ["watchlist"] });
  });
});

describe("useUpdateWatchlistRow — PATCH /api/watchlist/{symbol}/{strategy}", () => {
  it("URL composite + body update", async () => {
    fetchMock.mockResolvedValue(jsonResponse({ symbol: "AAPL", status: "focus" }));
    const { result } = renderHook(() => useUpdateWatchlistRow(), {
      wrapper: withQueryClient(),
    });
    act(() => {
      result.current.mutate({
        symbol: "AAPL",
        strategy: "minervini",
        update: { status: "focus" },
      });
    });
    await waitFor(() => expect(result.current.isSuccess).toBe(true));
    expect(fetchMock.mock.calls[0][0]).toBe(
      "/api/watchlist/AAPL/minervini"
    );
    expect(fetchMock.mock.calls[0][1]?.method).toBe("PATCH");
  });

  it("404 + JSON parse fail → fallback 'HTTP 404'", async () => {
    fetchMock.mockResolvedValue(new Response("not-json", { status: 404 }));
    const { result } = renderHook(() => useUpdateWatchlistRow(), {
      wrapper: withQueryClient(),
    });
    act(() => {
      result.current.mutate({
        symbol: "X",
        strategy: "y",
        update: { status: "buy" },
      });
    });
    await waitFor(() => expect(result.current.isError).toBe(true));
    expect((result.current.error as Error).message).toBe("HTTP 404");
  });
});

describe("useDeleteWatchlistRow — DELETE /api/watchlist/{symbol}/{strategy}", () => {
  it("DELETE URL + invalidate", async () => {
    fetchMock.mockResolvedValue(jsonResponse({ ok: true }));
    const qc = makeQueryClient();
    const spy = vi.spyOn(qc, "invalidateQueries");
    const { result } = renderHook(() => useDeleteWatchlistRow(), {
      wrapper: withQueryClient(qc),
    });
    act(() => {
      result.current.mutate({ symbol: "AAPL", strategy: "minervini" });
    });
    await waitFor(() => expect(result.current.isSuccess).toBe(true));
    expect(fetchMock.mock.calls[0][0]).toBe(
      "/api/watchlist/AAPL/minervini"
    );
    expect(fetchMock.mock.calls[0][1]?.method).toBe("DELETE");
    expect(spy).toHaveBeenCalledWith({ queryKey: ["watchlist"] });
  });
});

describe("usePromoteWatchlistRow — POST /api/watchlist/{symbol}/{strategy}/promote", () => {
  it("Promote URL + POST + invalidate (watch → on_deck → focus → buy 4 katman)", async () => {
    fetchMock.mockResolvedValue(jsonResponse({ symbol: "AAPL", status: "on_deck" }));
    const qc = makeQueryClient();
    const spy = vi.spyOn(qc, "invalidateQueries");
    const { result } = renderHook(() => usePromoteWatchlistRow(), {
      wrapper: withQueryClient(qc),
    });
    act(() => {
      result.current.mutate({ symbol: "AAPL", strategy: "minervini" });
    });
    await waitFor(() => expect(result.current.isSuccess).toBe(true));
    expect(fetchMock.mock.calls[0][0]).toBe(
      "/api/watchlist/AAPL/minervini/promote"
    );
    expect(fetchMock.mock.calls[0][1]?.method).toBe("POST");
    expect(spy).toHaveBeenCalledWith({ queryKey: ["watchlist"] });
  });

  it("400 + FastAPI detail ('Tepe seviyede') → error", async () => {
    fetchMock.mockResolvedValue(
      new Response(JSON.stringify({ detail: "buy tier'da promote yapılamaz" }), {
        status: 400,
      })
    );
    const { result } = renderHook(() => usePromoteWatchlistRow(), {
      wrapper: withQueryClient(),
    });
    act(() => {
      result.current.mutate({ symbol: "X", strategy: "y" });
    });
    await waitFor(() => expect(result.current.isError).toBe(true));
    expect((result.current.error as Error).message).toContain("buy tier");
  });
});
