/**
 * useDbStatus — DB sağlık polling (30s refetch + 15s stale).
 *
 * Sn. Ferit talimat: "Mockdan gerçek veriye geçmeyi ayarla" — frontend banner.
 */
import { describe, it, expect, vi, beforeEach, afterEach } from "vitest";
import { renderHook, waitFor } from "@testing-library/react";
import { useDbStatus, type DbStatus } from "@/hooks/use-db-status";
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

const HEALTHY: DbStatus = {
  status: "ok",
  service: "quanfina-api",
  timestamp: "2026-05-28T05:00:00Z",
  db_connected: true,
};

describe("useDbStatus", () => {
  it("Fetch URL: /api/health", async () => {
    fetchMock.mockResolvedValue(jsonResponse(HEALTHY));
    renderHook(() => useDbStatus(), { wrapper: withQueryClient() });
    await waitFor(() => expect(fetchMock).toHaveBeenCalled());
    expect(fetchMock.mock.calls[0][0]).toBe("/api/health");
  });

  it("db_connected=true → MOCK feed yerine gerçek DB", async () => {
    fetchMock.mockResolvedValue(jsonResponse(HEALTHY));
    const { result } = renderHook(() => useDbStatus(), {
      wrapper: withQueryClient(),
    });
    await waitFor(() => expect(result.current.isSuccess).toBe(true));
    expect(result.current.data?.db_connected).toBe(true);
    expect(result.current.data?.status).toBe("ok");
  });

  it("db_connected=false → frontend banner tetik (MOCK uyarısı)", async () => {
    fetchMock.mockResolvedValue(
      jsonResponse({ ...HEALTHY, db_connected: false, status: "degraded" })
    );
    const { result } = renderHook(() => useDbStatus(), {
      wrapper: withQueryClient(),
    });
    await waitFor(() => expect(result.current.isSuccess).toBe(true));
    expect(result.current.data?.db_connected).toBe(false);
    expect(result.current.data?.status).toBe("degraded");
  });

  it("HTTP 500 → error 'HTTP 500' (API down)", async () => {
    fetchMock.mockResolvedValue(new Response("", { status: 500 }));
    const { result } = renderHook(() => useDbStatus(), {
      wrapper: withQueryClient(),
    });
    await waitFor(() => expect(result.current.isError).toBe(true), {
      timeout: 3000,
    });
    expect((result.current.error as Error).message).toBe("HTTP 500");
  });

  it("Service field: 'quanfina-api' meta bilgi", async () => {
    fetchMock.mockResolvedValue(jsonResponse(HEALTHY));
    const { result } = renderHook(() => useDbStatus(), {
      wrapper: withQueryClient(),
    });
    await waitFor(() => expect(result.current.isSuccess).toBe(true));
    expect(result.current.data?.service).toBe("quanfina-api");
    expect(result.current.data?.timestamp).toBeDefined();
  });
});
