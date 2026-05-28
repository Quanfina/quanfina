/**
 * useMarketStatus — Dashboard piyasa sağlık + 3 ana index Stage + DD severity.
 *
 * GET /api/market/status, 60s cache, retry: 1.
 */
import { describe, it, expect, vi, beforeEach, afterEach } from "vitest";
import { renderHook, waitFor } from "@testing-library/react";
import { useMarketStatus } from "@/hooks/use-market-status";
import { withQueryClient } from "./_test-utils";
import type { MarketStatus } from "@/types/market";

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

const HEALTHY: MarketStatus = {
  spy_stage: 2,
  qqq_stage: 2,
  iwm_stage: 2,
  vix: 14.5,
  distribution_days: 1,
  market_health_score: 75,
  market_health_label: "Sağlıklı",
  suggested_mode: "LONG",
  top_sectors: [],
  bottom_sectors: [],
};

const BEAR: MarketStatus = {
  ...HEALTHY,
  spy_stage: 4,
  qqq_stage: 4,
  iwm_stage: 4,
  vix: 28,
  distribution_days: 6,
  market_health_score: 20,
  market_health_label: "Ayı Baskısı",
  suggested_mode: "CASH",
  dd_severity: "EXTREME",
  dd_allocation_factor: 0.25,
};

describe("useMarketStatus — fetch + parsing", () => {
  it("Fetch URL: /api/market/status (no query)", async () => {
    fetchMock.mockResolvedValue(jsonResponse(HEALTHY));
    renderHook(() => useMarketStatus(), { wrapper: withQueryClient() });
    await waitFor(() => expect(fetchMock).toHaveBeenCalled());
    expect(fetchMock.mock.calls[0][0]).toBe("/api/market/status");
  });

  it("HEALTHY response → MarketHealth 75, Stage 2", async () => {
    fetchMock.mockResolvedValue(jsonResponse(HEALTHY));
    const { result } = renderHook(() => useMarketStatus(), {
      wrapper: withQueryClient(),
    });
    await waitFor(() => expect(result.current.isSuccess).toBe(true));
    expect(result.current.data?.market_health_score).toBe(75);
    expect(result.current.data?.spy_stage).toBe(2);
    expect(result.current.data?.market_health_label).toBe("Sağlıklı");
  });

  it("BEAR response → Stage 4, DD 6, CASH mode", async () => {
    fetchMock.mockResolvedValue(jsonResponse(BEAR));
    const { result } = renderHook(() => useMarketStatus(), {
      wrapper: withQueryClient(),
    });
    await waitFor(() => expect(result.current.isSuccess).toBe(true));
    expect(result.current.data?.spy_stage).toBe(4);
    expect(result.current.data?.distribution_days).toBe(6);
    expect(result.current.data?.suggested_mode).toBe("CASH");
    expect(result.current.data?.dd_severity).toBe("EXTREME");
    expect(result.current.data?.dd_allocation_factor).toBe(0.25);
  });
});

describe("useMarketStatus — 3 index Stage farklı", () => {
  it("SPY=2 QQQ=2 IWM=4 → small-cap divergence (lider yok)", async () => {
    fetchMock.mockResolvedValue(
      jsonResponse({ ...HEALTHY, spy_stage: 2, qqq_stage: 2, iwm_stage: 4 })
    );
    const { result } = renderHook(() => useMarketStatus(), {
      wrapper: withQueryClient(),
    });
    await waitFor(() => expect(result.current.isSuccess).toBe(true));
    expect(result.current.data?.spy_stage).toBe(2);
    expect(result.current.data?.qqq_stage).toBe(2);
    expect(result.current.data?.iwm_stage).toBe(4);
  });
});

describe("useMarketStatus — DD severity 4 katman (P110)", () => {
  it.each<[string, number]>([
    ["CLEAN", 1.0],
    ["CAUTION", 0.75],
    ["HEAVY", 0.5],
    ["EXTREME", 0.25],
  ])("dd_severity='%s' → allocation_factor=%s", async (severity, factor) => {
    fetchMock.mockResolvedValue(
      jsonResponse({
        ...HEALTHY,
        dd_severity: severity,
        dd_allocation_factor: factor,
      })
    );
    const { result } = renderHook(() => useMarketStatus(), {
      wrapper: withQueryClient(),
    });
    await waitFor(() => expect(result.current.isSuccess).toBe(true));
    expect(result.current.data?.dd_severity).toBe(severity);
    expect(result.current.data?.dd_allocation_factor).toBe(factor);
  });
});

describe("useMarketStatus — hata yolu (retry: 1)", () => {
  it("HTTP 500 → retry sonrası error (timeout 3s)", async () => {
    fetchMock.mockResolvedValue(new Response("err", { status: 500 }));
    const { result } = renderHook(() => useMarketStatus(), {
      wrapper: withQueryClient(),
    });
    // Hook kendi retry: 1 ayarı QC default'u override eder, retry delay ~1s var → 3s timeout
    await waitFor(() => expect(result.current.isError).toBe(true), { timeout: 3000 });
    expect((result.current.error as Error).message).toContain("500");
    // retry: 1 → toplam 2 çağrı (ilk + 1 retry)
    expect(fetchMock).toHaveBeenCalledTimes(2);
  });
});
