/**
 * useCarrStage (KARAR #733 alt P32 — Stan Weinstein 4-Stage).
 *
 * GET /api/carr/stage/{symbol}, undefined → enabled=false, 4 stage tipi.
 */
import { describe, it, expect, vi, beforeEach, afterEach } from "vitest";
import { renderHook, waitFor } from "@testing-library/react";
import {
  useCarrStage,
  type CarrStage,
  type CarrStageResponse,
} from "@/hooks/use-carr-stage";
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

function makeResp(symbol: string, stage: CarrStage): CarrStageResponse {
  const labels: Record<NonNullable<CarrStage>, string> = {
    1: "Stage 1 (Basing)",
    2: "Stage 2 (Advancing)",
    3: "Stage 3 (Topping)",
    4: "Stage 4 (Declining)",
  };
  return {
    symbol,
    stage,
    stage_label: stage ? labels[stage] : "Belirsiz",
    ma_value: 150,
    price_vs_ma_pct: stage === 2 ? 5 : stage === 4 ? -10 : 0,
    slope_pct_per_year: stage === 2 ? 12 : stage === 4 ? -8 : 0,
    mark_says: `Stage ${stage} test`,
    ma_window: 150,
  };
}

describe("useCarrStage — enabled koşulları", () => {
  it("symbol=undefined → fetch tetiklenmez", async () => {
    fetchMock.mockResolvedValue(jsonResponse(makeResp("AAPL", 2)));
    renderHook(() => useCarrStage(undefined), { wrapper: withQueryClient() });
    await new Promise((r) => setTimeout(r, 50));
    expect(fetchMock).not.toHaveBeenCalled();
  });

  it("symbol='' (boş) → fetch tetiklenmez", async () => {
    fetchMock.mockResolvedValue(jsonResponse(makeResp("AAPL", 2)));
    renderHook(() => useCarrStage(""), { wrapper: withQueryClient() });
    await new Promise((r) => setTimeout(r, 50));
    expect(fetchMock).not.toHaveBeenCalled();
  });

  it("symbol='AAPL' → fetch /api/carr/stage/AAPL", async () => {
    fetchMock.mockResolvedValue(jsonResponse(makeResp("AAPL", 2)));
    renderHook(() => useCarrStage("AAPL"), { wrapper: withQueryClient() });
    await waitFor(() => expect(fetchMock).toHaveBeenCalled());
    expect(fetchMock.mock.calls[0][0]).toBe("/api/carr/stage/AAPL");
  });

  it("Özel karakter encode (BRK.B → BRK.B URL-safe)", async () => {
    fetchMock.mockResolvedValue(jsonResponse(makeResp("BRK.B", 2)));
    renderHook(() => useCarrStage("BRK.B"), { wrapper: withQueryClient() });
    await waitFor(() => expect(fetchMock).toHaveBeenCalled());
    // encodeURIComponent("BRK.B") = "BRK.B" (nokta safe), beklenir aynı
    expect(fetchMock.mock.calls[0][0]).toBe("/api/carr/stage/BRK.B");
  });
});

describe("useCarrStage — 4 stage tipi", () => {
  it.each<NonNullable<CarrStage>>([1, 2, 3, 4])(
    "stage=%i → data döner, label doğru",
    async (stage) => {
      fetchMock.mockResolvedValue(jsonResponse(makeResp("AAPL", stage)));
      const { result } = renderHook(() => useCarrStage("AAPL"), {
        wrapper: withQueryClient(),
      });
      await waitFor(() => expect(result.current.isSuccess).toBe(true));
      expect(result.current.data?.stage).toBe(stage);
      expect(result.current.data?.stage_label).toContain(`Stage ${stage}`);
    }
  );

  it("stage=null (yetersiz veri) → label='Belirsiz'", async () => {
    fetchMock.mockResolvedValue(jsonResponse(makeResp("AAPL", null)));
    const { result } = renderHook(() => useCarrStage("AAPL"), {
      wrapper: withQueryClient(),
    });
    await waitFor(() => expect(result.current.isSuccess).toBe(true));
    expect(result.current.data?.stage).toBeNull();
    expect(result.current.data?.stage_label).toBe("Belirsiz");
  });

  it("Stage 2: price_vs_ma_pct > 0 + slope > 0 (Mark advancing)", async () => {
    fetchMock.mockResolvedValue(jsonResponse(makeResp("NVDA", 2)));
    const { result } = renderHook(() => useCarrStage("NVDA"), {
      wrapper: withQueryClient(),
    });
    await waitFor(() => expect(result.current.isSuccess).toBe(true));
    expect(result.current.data?.price_vs_ma_pct).toBeGreaterThan(0);
    expect(result.current.data?.slope_pct_per_year).toBeGreaterThan(0);
  });

  it("Stage 4: price_vs_ma_pct < 0 + slope < 0 (Mark uzak dur)", async () => {
    fetchMock.mockResolvedValue(jsonResponse(makeResp("BADCO", 4)));
    const { result } = renderHook(() => useCarrStage("BADCO"), {
      wrapper: withQueryClient(),
    });
    await waitFor(() => expect(result.current.isSuccess).toBe(true));
    expect(result.current.data?.price_vs_ma_pct).toBeLessThan(0);
    expect(result.current.data?.slope_pct_per_year).toBeLessThan(0);
  });
});

describe("useCarrStage — hata yolu", () => {
  it("HTTP 404 → error mesajında sembol + status", async () => {
    fetchMock.mockResolvedValue(new Response("not found", { status: 404 }));
    const { result } = renderHook(() => useCarrStage("UNKNOWN"), {
      wrapper: withQueryClient(),
    });
    await waitFor(() => expect(result.current.isError).toBe(true));
    expect((result.current.error as Error).message).toContain("UNKNOWN");
    expect((result.current.error as Error).message).toContain("404");
  });
});
