/**
 * Statik liste hook ailesi — DRY parametrik test:
 *   useScreenMeta (P175 — 8 ready screen dropdown)
 *   useTerms (Sözlük sayfası, KARAR #480 alt)
 *   useMinerviniStocks zaten ayrı test edildi (kompleks data farklı pattern)
 *
 * Hepsi: GET /api/X, basit array döner, retry: 1.
 */
import { describe, it, expect, vi, beforeEach, afterEach } from "vitest";
import { renderHook, waitFor } from "@testing-library/react";
import { useScreenMeta } from "@/hooks/use-screen-meta";
import { useTerms } from "@/hooks/use-terms";
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

interface HookSpec {
  name: string;
  url: string;
  hook: () => { isSuccess: boolean; isError: boolean; data?: unknown; error?: unknown };
  withSignal: boolean;
}

const HOOKS: HookSpec[] = [
  {
    name: "useScreenMeta",
    url: "/api/screens",
    hook: useScreenMeta as HookSpec["hook"],
    withSignal: true,
  },
  {
    name: "useTerms",
    url: "/api/terms",
    hook: useTerms as HookSpec["hook"],
    withSignal: false,
  },
];

describe.each(HOOKS)("$name — $url", ({ url, hook, withSignal }) => {
  it(`Fetch URL: ${url}`, async () => {
    fetchMock.mockResolvedValue(jsonResponse([]));
    renderHook(() => hook(), { wrapper: withQueryClient() });
    await waitFor(() => expect(fetchMock).toHaveBeenCalled());
    expect(fetchMock.mock.calls[0][0]).toBe(url);
  });

  if (withSignal) {
    it("AbortSignal.timeout var (5s)", async () => {
      fetchMock.mockResolvedValue(jsonResponse([]));
      renderHook(() => hook(), { wrapper: withQueryClient() });
      await waitFor(() => expect(fetchMock).toHaveBeenCalled());
      expect(fetchMock.mock.calls[0][1]?.signal).toBeDefined();
    });
  }

  it("Çoklu satır array döner (statik liste)", async () => {
    fetchMock.mockResolvedValue(
      jsonResponse([
        { key: "a", label: "A" },
        { key: "b", label: "B" },
      ])
    );
    const { result } = renderHook(() => hook(), {
      wrapper: withQueryClient(),
    });
    await waitFor(() => expect(result.current.isSuccess).toBe(true));
    expect(Array.isArray(result.current.data)).toBe(true);
    expect((result.current.data as unknown[])).toHaveLength(2);
  });

  it("Boş array → fallback", async () => {
    fetchMock.mockResolvedValue(jsonResponse([]));
    const { result } = renderHook(() => hook(), {
      wrapper: withQueryClient(),
    });
    await waitFor(() => expect(result.current.isSuccess).toBe(true));
    expect(result.current.data).toEqual([]);
  });

  it("HTTP 500 → error 'HTTP 500'", async () => {
    fetchMock.mockResolvedValue(new Response("", { status: 500 }));
    const { result } = renderHook(() => hook(), {
      wrapper: withQueryClient(),
    });
    await waitFor(() => expect(result.current.isError).toBe(true), {
      timeout: 3000,
    });
    expect((result.current.error as Error).message).toBe("HTTP 500");
  });
});
