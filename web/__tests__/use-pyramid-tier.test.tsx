/**
 * usePyramidTier hook (P19 — KARAR ADAY #732 backend wire).
 *
 * POST /api/pyramid/tier çağrısı, enabled mantığı, error path.
 */
import { describe, it, expect, vi, beforeEach, afterEach } from "vitest";
import { renderHook, waitFor } from "@testing-library/react";
import { usePyramidTier, type PyramidTierResponse } from "@/hooks/use-pyramid-tier";
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

const HAPPY: PyramidTierResponse = {
  tier: "PILOT",
  position_pct: 2.0,
  severity: "ok",
  next_tier: "STANDARD",
  mark_says: "Pilot tier — nakitten ilk giriş.",
  pilot_range_pct: [1.0, 3.0],
  standard_range_pct: [6.25, 12.5],
  full_range_pct: [15.0, 25.0],
};

describe("usePyramidTier — enabled koşulları", () => {
  it("positionValue=0 → fetch tetiklenmez (enabled=false)", async () => {
    fetchMock.mockResolvedValue(jsonResponse(HAPPY));
    const { result } = renderHook(
      () => usePyramidTier({ positionValue: 0, portfolioValue: 100_000 }),
      { wrapper: withQueryClient() }
    );
    // Beklemeden kontrol — useQuery enabled=false → idle/disabled
    await new Promise((r) => setTimeout(r, 50));
    expect(fetchMock).not.toHaveBeenCalled();
    expect(result.current.data).toBeUndefined();
  });

  it("portfolioValue=0 → fetch tetiklenmez", async () => {
    fetchMock.mockResolvedValue(jsonResponse(HAPPY));
    renderHook(
      () => usePyramidTier({ positionValue: 2000, portfolioValue: 0 }),
      { wrapper: withQueryClient() }
    );
    await new Promise((r) => setTimeout(r, 50));
    expect(fetchMock).not.toHaveBeenCalled();
  });

  it("enabled=false → pozitif değerlerle bile fetch tetiklenmez", async () => {
    fetchMock.mockResolvedValue(jsonResponse(HAPPY));
    renderHook(
      () =>
        usePyramidTier({
          positionValue: 2000,
          portfolioValue: 100_000,
          enabled: false,
        }),
      { wrapper: withQueryClient() }
    );
    await new Promise((r) => setTimeout(r, 50));
    expect(fetchMock).not.toHaveBeenCalled();
  });

  it("Pozitif değerler + enabled default → fetch tetiklenir, data döner", async () => {
    fetchMock.mockResolvedValue(jsonResponse(HAPPY));
    const { result } = renderHook(
      () => usePyramidTier({ positionValue: 2000, portfolioValue: 100_000 }),
      { wrapper: withQueryClient() }
    );
    await waitFor(() => {
      expect(result.current.isSuccess).toBe(true);
    });
    expect(result.current.data).toEqual(HAPPY);
  });
});

describe("usePyramidTier — POST body içeriği", () => {
  it("position_value + portfolio_value + prev_tier_profitable JSON gönderilir", async () => {
    fetchMock.mockResolvedValue(jsonResponse(HAPPY));
    renderHook(
      () =>
        usePyramidTier({
          positionValue: 5000,
          portfolioValue: 50_000,
          prevTierProfitable: true,
        }),
      { wrapper: withQueryClient() }
    );
    await waitFor(() => expect(fetchMock).toHaveBeenCalled());
    const [url, init] = fetchMock.mock.calls[0];
    expect(url).toBe("/api/pyramid/tier");
    expect(init?.method).toBe("POST");
    const body = JSON.parse(init!.body as string);
    expect(body).toEqual({
      position_value: 5000,
      portfolio_value: 50_000,
      prev_tier_profitable: true,
    });
  });

  it("prevTierProfitable default false → JSON'da false olarak gider", async () => {
    fetchMock.mockResolvedValue(jsonResponse(HAPPY));
    renderHook(
      () => usePyramidTier({ positionValue: 2000, portfolioValue: 100_000 }),
      { wrapper: withQueryClient() }
    );
    await waitFor(() => expect(fetchMock).toHaveBeenCalled());
    const body = JSON.parse(fetchMock.mock.calls[0][1]!.body as string);
    expect(body.prev_tier_profitable).toBe(false);
  });

  it("Content-Type: application/json header set", async () => {
    fetchMock.mockResolvedValue(jsonResponse(HAPPY));
    renderHook(
      () => usePyramidTier({ positionValue: 2000, portfolioValue: 100_000 }),
      { wrapper: withQueryClient() }
    );
    await waitFor(() => expect(fetchMock).toHaveBeenCalled());
    const headers = fetchMock.mock.calls[0][1]?.headers as Record<string, string>;
    expect(headers["Content-Type"]).toBe("application/json");
  });
});

describe("usePyramidTier — hata yolu", () => {
  it("HTTP 500 → error state, mesajda status", async () => {
    fetchMock.mockResolvedValue(new Response("server error", { status: 500 }));
    const { result } = renderHook(
      () => usePyramidTier({ positionValue: 2000, portfolioValue: 100_000 }),
      { wrapper: withQueryClient() }
    );
    await waitFor(() => expect(result.current.isError).toBe(true));
    expect((result.current.error as Error).message).toContain("500");
  });

  it("HTTP 404 → error", async () => {
    fetchMock.mockResolvedValue(new Response("not found", { status: 404 }));
    const { result } = renderHook(
      () => usePyramidTier({ positionValue: 2000, portfolioValue: 100_000 }),
      { wrapper: withQueryClient() }
    );
    await waitFor(() => expect(result.current.isError).toBe(true));
    expect((result.current.error as Error).message).toContain("404");
  });

  it("Network fail (fetch throw) → error", async () => {
    fetchMock.mockRejectedValue(new Error("Network failed"));
    const { result } = renderHook(
      () => usePyramidTier({ positionValue: 2000, portfolioValue: 100_000 }),
      { wrapper: withQueryClient() }
    );
    await waitFor(() => expect(result.current.isError).toBe(true));
    expect((result.current.error as Error).message).toContain("Network failed");
  });
});
