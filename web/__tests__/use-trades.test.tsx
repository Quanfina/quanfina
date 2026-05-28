/**
 * useTrades + useAddTrade + useUpdateTrade + useDeleteTrade + useSetupTypes.
 *
 * Trade CRUD: GET /api/trades, POST/PATCH/DELETE /api/trades[/{id}]
 * + invalidateQueries cache yenileme + setup-types Infinity cache.
 */
import { describe, it, expect, vi, beforeEach, afterEach } from "vitest";
import { renderHook, waitFor, act } from "@testing-library/react";
import {
  useTrades,
  useSetupTypes,
  useAddTrade,
  useUpdateTrade,
  useDeleteTrade,
} from "@/hooks/use-trades";
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

describe("useTrades — GET /api/trades", () => {
  it("Fetch URL + AbortSignal", async () => {
    fetchMock.mockResolvedValue(jsonResponse([]));
    renderHook(() => useTrades(), { wrapper: withQueryClient() });
    await waitFor(() => expect(fetchMock).toHaveBeenCalled());
    expect(fetchMock.mock.calls[0][0]).toBe("/api/trades");
    expect(fetchMock.mock.calls[0][1]?.signal).toBeDefined();
  });

  it("Çoklu trade array döner", async () => {
    fetchMock.mockResolvedValue(
      jsonResponse([
        { id: 1, symbol: "AAPL", status: "open" },
        { id: 2, symbol: "NVDA", status: "closed" },
      ])
    );
    const { result } = renderHook(() => useTrades(), {
      wrapper: withQueryClient(),
    });
    await waitFor(() => expect(result.current.isSuccess).toBe(true));
    expect(result.current.data).toHaveLength(2);
  });

  it("503 + FastAPI detail → error mesajında detail", async () => {
    fetchMock.mockImplementation(async () =>
      new Response(JSON.stringify({ detail: "DB unreachable" }), { status: 503 })
    );
    const { result } = renderHook(() => useTrades(), {
      wrapper: withQueryClient(),
    });
    await waitFor(() => expect(result.current.isError).toBe(true), {
      timeout: 3000,
    });
    expect((result.current.error as Error).message).toContain("DB unreachable");
  });
});

describe("useSetupTypes — Infinity cache, no auto-refetch", () => {
  it("Fetch URL /api/setup-types", async () => {
    fetchMock.mockResolvedValue(jsonResponse([{ key: "vcp", label: "VCP" }]));
    renderHook(() => useSetupTypes(), { wrapper: withQueryClient() });
    await waitFor(() => expect(fetchMock).toHaveBeenCalled());
    expect(fetchMock.mock.calls[0][0]).toBe("/api/setup-types");
  });

  it("404 → error 'HTTP 404'", async () => {
    fetchMock.mockResolvedValue(new Response("", { status: 404 }));
    const { result } = renderHook(() => useSetupTypes(), {
      wrapper: withQueryClient(),
    });
    await waitFor(() => expect(result.current.isError).toBe(true));
    expect((result.current.error as Error).message).toBe("HTTP 404");
  });
});

describe("useAddTrade — POST + invalidate", () => {
  it("mutate → POST /api/trades body", async () => {
    fetchMock.mockResolvedValue(jsonResponse({ id: 99, symbol: "AAPL" }));
    const { result } = renderHook(() => useAddTrade(), {
      wrapper: withQueryClient(),
    });
    act(() => {
      result.current.mutate({
        symbol: "AAPL",
        strategy: "minervini",
        setup_type: "vcp",
        signal_source: "strategy",
        entry_date: "2026-05-28",
        entry_price: 100,
        shares: 100,
        plan_entry_trigger: "pivot $100 üstü",
        plan_stop: 95,
        plan_target: 120,
        plan_size_pct: 5,
        plan_exit_strategy: "trailing stop",
        plan_time_horizon: "position",
      });
    });
    await waitFor(() => expect(result.current.isSuccess).toBe(true));
    expect(fetchMock.mock.calls[0][0]).toBe("/api/trades");
    expect(fetchMock.mock.calls[0][1]?.method).toBe("POST");
  });

  it("Hata: 422 + FastAPI detail JSON → error mesajında", async () => {
    fetchMock.mockResolvedValue(
      new Response(JSON.stringify({ detail: "plan_stop ZORUNLU" }), { status: 422 })
    );
    const { result } = renderHook(() => useAddTrade(), {
      wrapper: withQueryClient(),
    });
    act(() => {
      result.current.mutate({
        symbol: "AAPL",
        strategy: "minervini",
        setup_type: "vcp",
        signal_source: "strategy",
        entry_date: "2026-05-28",
        entry_price: 100,
        shares: 100,
        plan_entry_trigger: "pivot $100 üstü",
        plan_stop: 95,
        plan_target: 120,
        plan_size_pct: 5,
        plan_exit_strategy: "trailing stop",
        plan_time_horizon: "position",
      });
    });
    await waitFor(() => expect(result.current.isError).toBe(true));
    expect((result.current.error as Error).message).toBe("plan_stop ZORUNLU");
  });

  it("onSuccess → trades query invalidate (cache yenilenir)", async () => {
    fetchMock.mockResolvedValue(jsonResponse({ id: 1 }));
    const qc = makeQueryClient();
    const invalidateSpy = vi.spyOn(qc, "invalidateQueries");
    const { result } = renderHook(() => useAddTrade(), {
      wrapper: withQueryClient(qc),
    });
    act(() => {
      result.current.mutate({
        symbol: "X",
        strategy: "minervini",
        setup_type: "vcp",
        signal_source: "strategy",
        entry_date: "2026-05-28",
        entry_price: 10,
        shares: 10,
        plan_entry_trigger: "pivot $10 üstü",
        plan_stop: 9,
        plan_target: 12,
        plan_size_pct: 5,
        plan_exit_strategy: "trailing stop",
        plan_time_horizon: "position",
      });
    });
    await waitFor(() => expect(result.current.isSuccess).toBe(true));
    expect(invalidateSpy).toHaveBeenCalledWith({ queryKey: ["trades"] });
  });
});

describe("useUpdateTrade — PATCH /api/trades/{id}", () => {
  it("PATCH URL doğru + body güncelleme", async () => {
    fetchMock.mockResolvedValue(jsonResponse({ id: 42, status: "closed" }));
    const { result } = renderHook(() => useUpdateTrade(), {
      wrapper: withQueryClient(),
    });
    act(() => {
      result.current.mutate({
        id: 42,
        update: { exit_price: 110, status: "closed" },
      });
    });
    await waitFor(() => expect(result.current.isSuccess).toBe(true));
    expect(fetchMock.mock.calls[0][0]).toBe("/api/trades/42");
    expect(fetchMock.mock.calls[0][1]?.method).toBe("PATCH");
    const body = JSON.parse(fetchMock.mock.calls[0][1]!.body as string);
    expect(body).toEqual({ exit_price: 110, status: "closed" });
  });
});

describe("useDeleteTrade — DELETE /api/trades/{id}", () => {
  it("DELETE URL + no body + invalidate", async () => {
    // 204 No Content — body null (HTTP spec gereği)
    fetchMock.mockResolvedValue(new Response(null, { status: 204 }));
    const qc = makeQueryClient();
    const invalidateSpy = vi.spyOn(qc, "invalidateQueries");
    const { result } = renderHook(() => useDeleteTrade(), {
      wrapper: withQueryClient(qc),
    });
    act(() => result.current.mutate(99));
    await waitFor(() => expect(result.current.isSuccess).toBe(true));
    expect(fetchMock.mock.calls[0][0]).toBe("/api/trades/99");
    expect(fetchMock.mock.calls[0][1]?.method).toBe("DELETE");
    expect(invalidateSpy).toHaveBeenCalledWith({ queryKey: ["trades"] });
  });

  it("404 + FastAPI detail → error", async () => {
    fetchMock.mockResolvedValue(
      new Response(JSON.stringify({ detail: "trade not found" }), { status: 404 })
    );
    const { result } = renderHook(() => useDeleteTrade(), {
      wrapper: withQueryClient(),
    });
    act(() => result.current.mutate(99));
    await waitFor(() => expect(result.current.isError).toBe(true));
    expect((result.current.error as Error).message).toBe("trade not found");
  });
});
