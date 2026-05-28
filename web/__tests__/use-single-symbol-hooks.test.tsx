/**
 * Single-symbol Mark canon hook ailesi — DRY parametrik test.
 *
 * Hepsi /api/stock/{symbol}/{endpoint} pattern (useQuery + enabled + 5dk cache).
 *   - usePivotBreakout (P71)
 *   - useOverheadSupply (P78)
 *   - useClimaxRun
 *   - useStageTransition
 *   - useRelativeVolume
 *   - useAtrVolatility
 *   - useBreakoutQuality
 *
 * Her hook için 4 standart senaryo: undefined → no fetch / URL doğru / happy / 404 error.
 */
import { describe, it, expect, vi, beforeEach, afterEach } from "vitest";
import { renderHook, waitFor } from "@testing-library/react";
import { usePivotBreakout } from "@/hooks/use-pivot-breakout";
import { useOverheadSupply } from "@/hooks/use-overhead-supply";
import { useClimaxRun } from "@/hooks/use-climax-run";
import { useStageTransition } from "@/hooks/use-stage-transition";
import { useRelativeVolume } from "@/hooks/use-relative-volume";
import { useAtrVolatility } from "@/hooks/use-atr-volatility";
import { useBreakoutQuality } from "@/hooks/use-breakout-quality";
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

interface HookSpec {
  name: string;
  endpoint: string;
  hook: (symbol: string | undefined) => { isSuccess: boolean; isError: boolean; data?: unknown; error?: unknown };
}

const HOOKS: HookSpec[] = [
  { name: "usePivotBreakout", endpoint: "pivot", hook: usePivotBreakout as HookSpec["hook"] },
  { name: "useOverheadSupply", endpoint: "overhead", hook: useOverheadSupply as HookSpec["hook"] },
  { name: "useClimaxRun", endpoint: "climax", hook: useClimaxRun as HookSpec["hook"] },
  { name: "useStageTransition", endpoint: "stage", hook: useStageTransition as HookSpec["hook"] },
  { name: "useRelativeVolume", endpoint: "relative-volume", hook: useRelativeVolume as HookSpec["hook"] },
  { name: "useAtrVolatility", endpoint: "atr", hook: useAtrVolatility as HookSpec["hook"] },
  { name: "useBreakoutQuality", endpoint: "breakout-quality", hook: useBreakoutQuality as HookSpec["hook"] },
];

describe.each(HOOKS)("$name — /api/stock/{symbol}/$endpoint", ({ endpoint, hook }) => {
  it("symbol=undefined → fetch tetiklenmez (enabled=false)", async () => {
    fetchMock.mockResolvedValue(jsonResponse({}));
    renderHook(() => hook(undefined), { wrapper: withQueryClient() });
    await new Promise((r) => setTimeout(r, 50));
    expect(fetchMock).not.toHaveBeenCalled();
  });

  it(`symbol='AAPL' → fetch /api/stock/AAPL/${endpoint}`, async () => {
    fetchMock.mockResolvedValue(jsonResponse({ symbol: "AAPL" }));
    renderHook(() => hook("AAPL"), { wrapper: withQueryClient() });
    await waitFor(() => expect(fetchMock).toHaveBeenCalled());
    expect(fetchMock.mock.calls[0][0]).toBe(`/api/stock/AAPL/${endpoint}`);
  });

  it("Happy response → data döner (isSuccess=true)", async () => {
    fetchMock.mockResolvedValue(jsonResponse({ symbol: "NVDA", value: 42 }));
    const { result } = renderHook(() => hook("NVDA"), {
      wrapper: withQueryClient(),
    });
    await waitFor(() => expect(result.current.isSuccess).toBe(true));
    expect(result.current.data).toBeDefined();
  });

  it("HTTP 404 → error mesajında sembol + status", async () => {
    fetchMock.mockResolvedValue(new Response("", { status: 404 }));
    const { result } = renderHook(() => hook("UNKNOWN"), {
      wrapper: withQueryClient(),
    });
    await waitFor(() => expect(result.current.isError).toBe(true));
    const msg = (result.current.error as Error).message;
    expect(msg).toContain("UNKNOWN");
    expect(msg).toContain("404");
  });
});

describe("Single-symbol hook ailesi — URL encoding (özel karakter)", () => {
  it("usePivotBreakout('BRK.B') → /api/stock/BRK.B/pivot (nokta safe)", async () => {
    fetchMock.mockResolvedValue(jsonResponse({}));
    renderHook(() => usePivotBreakout("BRK.B"), { wrapper: withQueryClient() });
    await waitFor(() => expect(fetchMock).toHaveBeenCalled());
    expect(fetchMock.mock.calls[0][0]).toBe("/api/stock/BRK.B/pivot");
  });

  it("useOverheadSupply boş string → fetch yok", async () => {
    fetchMock.mockResolvedValue(jsonResponse({}));
    renderHook(() => useOverheadSupply(""), { wrapper: withQueryClient() });
    await new Promise((r) => setTimeout(r, 50));
    expect(fetchMock).not.toHaveBeenCalled();
  });
});
