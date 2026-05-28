/**
 * useSignals — Sinyaller sayfası watchlist'ten türetilmiş sinyaller.
 *
 * GET /api/signals, 8s timeout, 503 informative body parse,
 * FastAPI {"detail":...} format.
 */
import { describe, it, expect, vi, beforeEach, afterEach } from "vitest";
import { renderHook, waitFor } from "@testing-library/react";
import { useSignals } from "@/hooks/use-signals";
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

describe("useSignals — fetch + parsing", () => {
  it("Fetch URL: /api/signals + AbortSignal", async () => {
    fetchMock.mockResolvedValue(jsonResponse([]));
    renderHook(() => useSignals(), { wrapper: withQueryClient() });
    await waitFor(() => expect(fetchMock).toHaveBeenCalled());
    expect(fetchMock.mock.calls[0][0]).toBe("/api/signals");
    expect(fetchMock.mock.calls[0][1]?.signal).toBeDefined();
  });

  it("Boş array → data=[] başarılı", async () => {
    fetchMock.mockResolvedValue(jsonResponse([]));
    const { result } = renderHook(() => useSignals(), {
      wrapper: withQueryClient(),
    });
    await waitFor(() => expect(result.current.isSuccess).toBe(true));
    expect(result.current.data).toEqual([]);
  });

  it("Çoklu sinyal → array döner", async () => {
    const signals = [
      { symbol: "AAPL", strategy: "minervini", price: 150 },
      { symbol: "NVDA", strategy: "carr", price: 215 },
    ];
    fetchMock.mockResolvedValue(jsonResponse(signals));
    const { result } = renderHook(() => useSignals(), {
      wrapper: withQueryClient(),
    });
    await waitFor(() => expect(result.current.isSuccess).toBe(true));
    expect(result.current.data).toHaveLength(2);
  });
});

describe("useSignals — 503 informative body parse (FastAPI detail)", () => {
  it("503 + FastAPI {\"detail\":\"DB unreachable\"} → error message detail içerir", async () => {
    // retry: 1 nedeniyle Response her çağrıda yeni (body tek seferlik tüketilir)
    fetchMock.mockImplementation(async () =>
      new Response(JSON.stringify({ detail: "DB unreachable" }), {
        status: 503,
        headers: { "Content-Type": "application/json" },
      })
    );
    const { result } = renderHook(() => useSignals(), {
      wrapper: withQueryClient(),
    });
    await waitFor(() => expect(result.current.isError).toBe(true), {
      timeout: 3000,
    });
    expect((result.current.error as Error).message).toContain("503");
    expect((result.current.error as Error).message).toContain("DB unreachable");
  });

  it("500 + plain text body (JSON değil) → text slice 160 char", async () => {
    fetchMock.mockImplementation(async () =>
      new Response("Server is down for maintenance", { status: 500 })
    );
    const { result } = renderHook(() => useSignals(), {
      wrapper: withQueryClient(),
    });
    await waitFor(() => expect(result.current.isError).toBe(true), {
      timeout: 3000,
    });
    expect((result.current.error as Error).message).toContain("500");
    expect((result.current.error as Error).message).toContain("Server is down");
  });

  it("404 + boş body → sadece 'HTTP 404'", async () => {
    fetchMock.mockImplementation(async () => new Response("", { status: 404 }));
    const { result } = renderHook(() => useSignals(), {
      wrapper: withQueryClient(),
    });
    await waitFor(() => expect(result.current.isError).toBe(true), {
      timeout: 3000,
    });
    expect((result.current.error as Error).message).toBe("HTTP 404");
  });

  it("503 + JSON detail uzun string → 'detail' alanı kullanılır", async () => {
    fetchMock.mockImplementation(async () =>
      new Response(
        JSON.stringify({
          detail: "Cloud SQL instance paused — Sn. Ferit GCP eli gerek",
        }),
        { status: 503 }
      )
    );
    const { result } = renderHook(() => useSignals(), {
      wrapper: withQueryClient(),
    });
    await waitFor(() => expect(result.current.isError).toBe(true), {
      timeout: 3000,
    });
    expect((result.current.error as Error).message).toContain(
      "Cloud SQL instance paused"
    );
  });
});

describe("useSignals — Network fail", () => {
  it("fetch reject → error state", async () => {
    fetchMock.mockRejectedValue(new Error("network offline"));
    const { result } = renderHook(() => useSignals(), {
      wrapper: withQueryClient(),
    });
    await waitFor(() => expect(result.current.isError).toBe(true), {
      timeout: 3000,
    });
    expect((result.current.error as Error).message).toBe("network offline");
  });
});
