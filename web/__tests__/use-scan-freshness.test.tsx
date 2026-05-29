/**
 * useScanFreshness — tarama veri tazeligi polling (5dk stale + 10dk poll, P375).
 *
 * Sn. Ferit "14 gun eski veri" acisi: scanner Cloud Run'da durursa minervini_scans
 * bayat kalir. Bu hook /api/scan/freshness'ten is_stale + son tarama tarihini ceker.
 * use-db-status.test.tsx pateninin esi.
 */
import { describe, it, expect, vi, beforeEach, afterEach } from "vitest";
import { renderHook, waitFor } from "@testing-library/react";
import { useScanFreshness, type ScanFreshness } from "@/hooks/use-scan-freshness";
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

const FRESH: ScanFreshness = {
  latest_scan_date: "2026-05-29",
  is_stale: false,
  calendar_days_old: 1,
  threshold_days: 4,
  message: "Tarama verisi guncel (1 gun once).",
};

describe("useScanFreshness", () => {
  it("Fetch URL: /api/scan/freshness", async () => {
    fetchMock.mockResolvedValue(jsonResponse(FRESH));
    renderHook(() => useScanFreshness(), { wrapper: withQueryClient() });
    await waitFor(() => expect(fetchMock).toHaveBeenCalled());
    expect(fetchMock.mock.calls[0][0]).toBe("/api/scan/freshness");
  });

  it("is_stale=false → banner gizli (veri taze)", async () => {
    fetchMock.mockResolvedValue(jsonResponse(FRESH));
    const { result } = renderHook(() => useScanFreshness(), {
      wrapper: withQueryClient(),
    });
    await waitFor(() => expect(result.current.isSuccess).toBe(true));
    expect(result.current.data?.is_stale).toBe(false);
    expect(result.current.data?.calendar_days_old).toBe(1);
  });

  it("is_stale=true → banner tetik (scanner bayat)", async () => {
    fetchMock.mockResolvedValue(
      jsonResponse({
        ...FRESH,
        is_stale: true,
        calendar_days_old: 7,
        message: "Tarama verisi 7 gun bayat (esik 4).",
      })
    );
    const { result } = renderHook(() => useScanFreshness(), {
      wrapper: withQueryClient(),
    });
    await waitFor(() => expect(result.current.isSuccess).toBe(true));
    expect(result.current.data?.is_stale).toBe(true);
    expect(result.current.data?.calendar_days_old).toBe(7);
  });

  it("latest_scan_date=null → hic tarama yok (is_stale beklenir)", async () => {
    fetchMock.mockResolvedValue(
      jsonResponse({
        latest_scan_date: null,
        is_stale: true,
        calendar_days_old: null,
        threshold_days: 4,
        message: "Hic tarama bulunamadi.",
      })
    );
    const { result } = renderHook(() => useScanFreshness(), {
      wrapper: withQueryClient(),
    });
    await waitFor(() => expect(result.current.isSuccess).toBe(true));
    expect(result.current.data?.latest_scan_date).toBeNull();
    expect(result.current.data?.calendar_days_old).toBeNull();
    expect(result.current.data?.is_stale).toBe(true);
  });

  it("HTTP 500 → error 'HTTP 500' (API down)", async () => {
    fetchMock.mockResolvedValue(new Response("", { status: 500 }));
    const { result } = renderHook(() => useScanFreshness(), {
      wrapper: withQueryClient(),
    });
    await waitFor(() => expect(result.current.isError).toBe(true), {
      timeout: 3000,
    });
    expect((result.current.error as Error).message).toBe("HTTP 500");
  });

  it("threshold_days alani meta bilgi olarak gelir", async () => {
    fetchMock.mockResolvedValue(jsonResponse(FRESH));
    const { result } = renderHook(() => useScanFreshness(), {
      wrapper: withQueryClient(),
    });
    await waitFor(() => expect(result.current.isSuccess).toBe(true));
    expect(result.current.data?.threshold_days).toBe(4);
    expect(result.current.data?.message).toBeDefined();
  });
});
