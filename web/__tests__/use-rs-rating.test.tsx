/**
 * useRsRating (KARAR #733 alt P94 — Mark TLSMW Leaders First).
 *
 * GET /api/stock/{symbol}/rs, 4 kategori (LEADER/STRONG/AVERAGE/LAGGARD), 30dk cache.
 */
import { describe, it, expect, vi, beforeEach, afterEach } from "vitest";
import { renderHook, waitFor } from "@testing-library/react";
import {
  useRsRating,
  type RsRatingCategory,
  type RsRatingInfo,
} from "@/hooks/use-rs-rating";
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

const LEADER: RsRatingInfo = {
  rs_rating: 92,
  category: "LEADER",
  stock_return_pct: 45,
  benchmark_return_pct: 12,
  outperform_pct: 33,
  mark_says: "Mark Leaders First — RS 92 superperformance kümesi.",
};

describe("useRsRating — enabled koşulları", () => {
  it("symbol=undefined → fetch tetiklenmez", async () => {
    fetchMock.mockResolvedValue(jsonResponse(LEADER));
    renderHook(() => useRsRating(undefined), { wrapper: withQueryClient() });
    await new Promise((r) => setTimeout(r, 50));
    expect(fetchMock).not.toHaveBeenCalled();
  });

  it("symbol='' (boş) → fetch tetiklenmez", async () => {
    fetchMock.mockResolvedValue(jsonResponse(LEADER));
    renderHook(() => useRsRating(""), { wrapper: withQueryClient() });
    await new Promise((r) => setTimeout(r, 50));
    expect(fetchMock).not.toHaveBeenCalled();
  });

  it("symbol='NVDA' → /api/stock/NVDA/rs fetch", async () => {
    fetchMock.mockResolvedValue(jsonResponse(LEADER));
    renderHook(() => useRsRating("NVDA"), { wrapper: withQueryClient() });
    await waitFor(() => expect(fetchMock).toHaveBeenCalled());
    expect(fetchMock.mock.calls[0][0]).toBe("/api/stock/NVDA/rs");
  });
});

describe("useRsRating — 4 RS kategorisi", () => {
  it.each<[RsRatingCategory, number]>([
    ["LEADER", 92],     // ≥ 80 typically
    ["STRONG", 75],
    ["AVERAGE", 50],
    ["LAGGARD", 25],
  ])("category='%s' (RS=%i) → data parsed", async (category, rs_rating) => {
    fetchMock.mockResolvedValue(
      jsonResponse({ ...LEADER, category, rs_rating })
    );
    const { result } = renderHook(() => useRsRating("AAPL"), {
      wrapper: withQueryClient(),
    });
    await waitFor(() => expect(result.current.isSuccess).toBe(true));
    expect(result.current.data?.category).toBe(category);
    expect(result.current.data?.rs_rating).toBe(rs_rating);
  });

  it("RS=null (yetersiz veri) + category=null → fallback", async () => {
    fetchMock.mockResolvedValue(
      jsonResponse({
        ...LEADER,
        rs_rating: null,
        category: null,
        outperform_pct: null,
      })
    );
    const { result } = renderHook(() => useRsRating("UNKNOWN"), {
      wrapper: withQueryClient(),
    });
    await waitFor(() => expect(result.current.isSuccess).toBe(true));
    expect(result.current.data?.rs_rating).toBeNull();
    expect(result.current.data?.category).toBeNull();
  });
});

describe("useRsRating — outperform_pct hesabı", () => {
  it("stock=45, benchmark=12 → outperform=33 (mark superperformance)", async () => {
    fetchMock.mockResolvedValue(jsonResponse(LEADER));
    const { result } = renderHook(() => useRsRating("NVDA"), {
      wrapper: withQueryClient(),
    });
    await waitFor(() => expect(result.current.isSuccess).toBe(true));
    expect(result.current.data?.stock_return_pct).toBe(45);
    expect(result.current.data?.benchmark_return_pct).toBe(12);
    expect(result.current.data?.outperform_pct).toBe(33);
  });

  it("LAGGARD: stock=-10, benchmark=12 → outperform=-22 (Mark uzak dur)", async () => {
    fetchMock.mockResolvedValue(
      jsonResponse({
        ...LEADER,
        category: "LAGGARD",
        rs_rating: 20,
        stock_return_pct: -10,
        benchmark_return_pct: 12,
        outperform_pct: -22,
      })
    );
    const { result } = renderHook(() => useRsRating("BADCO"), {
      wrapper: withQueryClient(),
    });
    await waitFor(() => expect(result.current.isSuccess).toBe(true));
    expect(result.current.data?.outperform_pct).toBe(-22);
  });
});

describe("useRsRating — hata yolu", () => {
  it("HTTP 404 → error mesajında sembol + status", async () => {
    fetchMock.mockResolvedValue(new Response("not found", { status: 404 }));
    const { result } = renderHook(() => useRsRating("FOO"), {
      wrapper: withQueryClient(),
    });
    await waitFor(() => expect(result.current.isError).toBe(true));
    expect((result.current.error as Error).message).toContain("FOO");
    expect((result.current.error as Error).message).toContain("404");
  });

  it("Network fail → error", async () => {
    fetchMock.mockRejectedValue(new Error("network"));
    const { result } = renderHook(() => useRsRating("AAPL"), {
      wrapper: withQueryClient(),
    });
    await waitFor(() => expect(result.current.isError).toBe(true));
  });
});
