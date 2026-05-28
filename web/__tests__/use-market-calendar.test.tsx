/**
 * useMarketCalendar — ABD Borsa Takvim Durumu (Sprint 4-bis.7).
 *
 * 4 session: regular / pre_market / post_market / closed.
 * Sn. Ferit Türkiye'de yaşıyor — ET + TR saatleri paralel gösterilir.
 */
import { describe, it, expect, vi, beforeEach, afterEach } from "vitest";
import { renderHook, waitFor } from "@testing-library/react";
import {
  useMarketCalendar,
  type MarketSession,
  type MarketCalendarStatus,
} from "@/hooks/use-market-calendar";
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

const REGULAR_OPEN: MarketCalendarStatus = {
  is_open: true,
  session: "regular",
  reason: null,
  is_early_close: false,
  now_et: "2026-05-28 10:30:00",
  now_tr: "2026-05-28 17:30:00",
  next_open_et: "2026-05-29 09:30:00",
  next_open_tr: "2026-05-29 16:30:00",
  last_trading_day: "2026-05-27",
};

describe("useMarketCalendar — fetch + URL", () => {
  it("Fetch URL: /api/market/calendar/status", async () => {
    fetchMock.mockResolvedValue(jsonResponse(REGULAR_OPEN));
    renderHook(() => useMarketCalendar(), { wrapper: withQueryClient() });
    await waitFor(() => expect(fetchMock).toHaveBeenCalled());
    expect(fetchMock.mock.calls[0][0]).toBe("/api/market/calendar/status");
  });
});

describe("useMarketCalendar — 4 session tipi", () => {
  it.each<MarketSession>([
    "regular",
    "pre_market",
    "post_market",
    "closed",
  ])("session='%s' → data döner", async (session) => {
    fetchMock.mockResolvedValue(
      jsonResponse({
        ...REGULAR_OPEN,
        session,
        is_open: session === "regular",
      })
    );
    const { result } = renderHook(() => useMarketCalendar(), {
      wrapper: withQueryClient(),
    });
    await waitFor(() => expect(result.current.isSuccess).toBe(true));
    expect(result.current.data?.session).toBe(session);
    expect(result.current.data?.is_open).toBe(session === "regular");
  });
});

describe("useMarketCalendar — ET + TR çift saat (Sn. Ferit Türkiye'de)", () => {
  it("now_et + now_tr ikisi de döner (paralel saat)", async () => {
    fetchMock.mockResolvedValue(jsonResponse(REGULAR_OPEN));
    const { result } = renderHook(() => useMarketCalendar(), {
      wrapper: withQueryClient(),
    });
    await waitFor(() => expect(result.current.isSuccess).toBe(true));
    expect(result.current.data?.now_et).toBe("2026-05-28 10:30:00");
    expect(result.current.data?.now_tr).toBe("2026-05-28 17:30:00");
  });

  it("next_open_et + next_open_tr (gelecek seans)", async () => {
    fetchMock.mockResolvedValue(jsonResponse(REGULAR_OPEN));
    const { result } = renderHook(() => useMarketCalendar(), {
      wrapper: withQueryClient(),
    });
    await waitFor(() => expect(result.current.isSuccess).toBe(true));
    expect(result.current.data?.next_open_et).toBe("2026-05-29 09:30:00");
    expect(result.current.data?.next_open_tr).toBe("2026-05-29 16:30:00");
  });
});

describe("useMarketCalendar — tatil/erken kapanış", () => {
  it("ABD tatili (Memorial Day vb.) → is_open=false + reason", async () => {
    fetchMock.mockResolvedValue(
      jsonResponse({
        ...REGULAR_OPEN,
        is_open: false,
        session: "closed",
        reason: "Memorial Day (ABD tatili)",
      })
    );
    const { result } = renderHook(() => useMarketCalendar(), {
      wrapper: withQueryClient(),
    });
    await waitFor(() => expect(result.current.isSuccess).toBe(true));
    expect(result.current.data?.is_open).toBe(false);
    expect(result.current.data?.reason).toContain("Memorial Day");
  });

  it("Erken kapanış (Thanksgiving sonrası 13:00 ET) → is_early_close=true", async () => {
    fetchMock.mockResolvedValue(
      jsonResponse({ ...REGULAR_OPEN, is_early_close: true })
    );
    const { result } = renderHook(() => useMarketCalendar(), {
      wrapper: withQueryClient(),
    });
    await waitFor(() => expect(result.current.isSuccess).toBe(true));
    expect(result.current.data?.is_early_close).toBe(true);
  });
});

describe("useMarketCalendar — hata yolu", () => {
  it("HTTP 500 → error 'HTTP 500'", async () => {
    fetchMock.mockResolvedValue(new Response("", { status: 500 }));
    const { result } = renderHook(() => useMarketCalendar(), {
      wrapper: withQueryClient(),
    });
    await waitFor(() => expect(result.current.isError).toBe(true), {
      timeout: 3000,
    });
    expect((result.current.error as Error).message).toBe("HTTP 500");
  });
});
