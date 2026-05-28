/**
 * useScreenResults — Tarama sayfası screen sonuçları.
 *
 * GET /api/screens/{slug}?limit=N, 12s timeout, 60s cache, retry: 1.
 */
import { describe, it, expect, vi, beforeEach, afterEach } from "vitest";
import { renderHook, waitFor } from "@testing-library/react";
import { useScreenResults } from "@/hooks/use-screen-results";
import { withQueryClient } from "./_test-utils";
import type { ScreenSlug } from "@/types/screens";

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

describe("useScreenResults — enabled koşulu", () => {
  it("slug=null → fetch tetiklenmez", async () => {
    fetchMock.mockResolvedValue(jsonResponse([]));
    renderHook(() => useScreenResults(null), { wrapper: withQueryClient() });
    await new Promise((r) => setTimeout(r, 50));
    expect(fetchMock).not.toHaveBeenCalled();
  });

  it("slug='trend-template' → /api/screens/trend-template?limit=500", async () => {
    fetchMock.mockResolvedValue(jsonResponse([]));
    renderHook(
      () => useScreenResults("trend-template" as ScreenSlug),
      { wrapper: withQueryClient() }
    );
    await waitFor(() => expect(fetchMock).toHaveBeenCalled());
    expect(fetchMock.mock.calls[0][0]).toBe(
      "/api/screens/trend-template?limit=500"
    );
  });

  it("limit=100 override → URL limit=100", async () => {
    fetchMock.mockResolvedValue(jsonResponse([]));
    renderHook(
      () => useScreenResults("vcp" as ScreenSlug, 100),
      { wrapper: withQueryClient() }
    );
    await waitFor(() => expect(fetchMock).toHaveBeenCalled());
    expect(fetchMock.mock.calls[0][0]).toBe(
      "/api/screens/vcp?limit=100"
    );
  });

  it("AbortSignal.timeout(12000) — fetch init signal var", async () => {
    fetchMock.mockResolvedValue(jsonResponse([]));
    renderHook(
      () => useScreenResults("trend-template" as ScreenSlug),
      { wrapper: withQueryClient() }
    );
    await waitFor(() => expect(fetchMock).toHaveBeenCalled());
    expect(fetchMock.mock.calls[0][1]?.signal).toBeDefined();
  });
});

describe("useScreenResults — response parsing", () => {
  it("Çoklu satır → array döner", async () => {
    fetchMock.mockResolvedValue(
      jsonResponse([
        { ticker: "AAPL", price: 150, rs_ibd: 92 },
        { ticker: "NVDA", price: 215, rs_ibd: 99 },
      ])
    );
    const { result } = renderHook(
      () => useScreenResults("trend-template" as ScreenSlug),
      { wrapper: withQueryClient() }
    );
    await waitFor(() => expect(result.current.isSuccess).toBe(true));
    expect(result.current.data).toHaveLength(2);
  });

  it("Boş array → tarama sonucu yok", async () => {
    fetchMock.mockResolvedValue(jsonResponse([]));
    const { result } = renderHook(
      () => useScreenResults("low-cheat" as ScreenSlug),
      { wrapper: withQueryClient() }
    );
    await waitFor(() => expect(result.current.isSuccess).toBe(true));
    expect(result.current.data).toEqual([]);
  });
});

describe("useScreenResults — hata + 160 char body slice", () => {
  it("500 + uzun body → mesaj ilk 160 char", async () => {
    fetchMock.mockImplementation(async () =>
      new Response("ERROR".repeat(100), { status: 500 })
    );
    const { result } = renderHook(
      () => useScreenResults("vcp" as ScreenSlug),
      { wrapper: withQueryClient() }
    );
    await waitFor(() => expect(result.current.isError).toBe(true), {
      timeout: 3000,
    });
    const msg = (result.current.error as Error).message;
    expect(msg).toContain("500");
    // 160 char slice → "ERROR" 32 kez
    expect(msg).toContain("ERROR".repeat(32));
  });

  it("503 + boş body → 'HTTP 503'", async () => {
    fetchMock.mockImplementation(async () => new Response("", { status: 503 }));
    const { result } = renderHook(
      () => useScreenResults("vcp" as ScreenSlug),
      { wrapper: withQueryClient() }
    );
    await waitFor(() => expect(result.current.isError).toBe(true), {
      timeout: 3000,
    });
    expect((result.current.error as Error).message).toBe("HTTP 503");
  });
});
