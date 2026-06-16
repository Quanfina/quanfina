/**
 * useTradesExpectancy (KARAR #733 alt P90 — Mark Brandon expectancy hesabı).
 *
 * GET /api/trades/expectancy[?strategy=X], 5dk cache, BrandonExpectancyCategory.
 */
import { describe, it, expect, vi, beforeEach, afterEach } from "vitest";
import { renderHook, waitFor } from "@testing-library/react";
import {
  useTradesExpectancy,
  type BrandonExpectancyInfo,
} from "@/hooks/use-trades-expectancy";
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

const EXCELLENT: BrandonExpectancyInfo = {
  expectancy_pct: 12.5,
  category: "EXCELLENT",
  meets_brandon_targets: true,
  mark_says: "Brandon EXCELLENT: %12.5 expectancy.",
  total_closed: 20,
  winners: 13,
  losers: 7,
  avg_gain_pct: 25,
  avg_loss_pct: 8,
  // #17 (denetim): backend win_rate KESİR (0-1): len(winners)/len(closed)=13/20=0.65.
  // Fixture %65 (100×) yanlıştı — UI yüzde formatlayınca %6500 gösterirdi.
  win_rate: 0.65,
};

describe("useTradesExpectancy — URL building", () => {
  it("strategy undefined → /api/trades/expectancy (no query)", async () => {
    fetchMock.mockResolvedValue(jsonResponse(EXCELLENT));
    renderHook(() => useTradesExpectancy(), {
      wrapper: withQueryClient(),
    });
    await waitFor(() => expect(fetchMock).toHaveBeenCalled());
    expect(fetchMock.mock.calls[0][0]).toBe("/api/trades/expectancy");
  });

  it("strategy='all' → /api/trades/expectancy (no query, special)", async () => {
    fetchMock.mockResolvedValue(jsonResponse(EXCELLENT));
    renderHook(() => useTradesExpectancy("all"), {
      wrapper: withQueryClient(),
    });
    await waitFor(() => expect(fetchMock).toHaveBeenCalled());
    expect(fetchMock.mock.calls[0][0]).toBe("/api/trades/expectancy");
  });

  it("strategy='minervini' → ?strategy=minervini", async () => {
    fetchMock.mockResolvedValue(jsonResponse(EXCELLENT));
    renderHook(() => useTradesExpectancy("minervini"), {
      wrapper: withQueryClient(),
    });
    await waitFor(() => expect(fetchMock).toHaveBeenCalled());
    expect(fetchMock.mock.calls[0][0]).toBe(
      "/api/trades/expectancy?strategy=minervini"
    );
  });

  it("strategy URL encode (özel karakter)", async () => {
    fetchMock.mockResolvedValue(jsonResponse(EXCELLENT));
    renderHook(() => useTradesExpectancy("my strategy"), {
      wrapper: withQueryClient(),
    });
    await waitFor(() => expect(fetchMock).toHaveBeenCalled());
    expect(fetchMock.mock.calls[0][0]).toBe(
      "/api/trades/expectancy?strategy=my%20strategy"
    );
  });
});

describe("useTradesExpectancy — Brandon kategori (EXCELLENT/GOOD/ACCEPTABLE/POOR)", () => {
  it.each<BrandonExpectancyInfo["category"]>([
    "EXCELLENT",
    "GOOD",
    "ACCEPTABLE",
    "POOR",
  ])("category='%s' → data döner", async (category) => {
    fetchMock.mockResolvedValue(
      jsonResponse({ ...EXCELLENT, category })
    );
    const { result } = renderHook(() => useTradesExpectancy(), {
      wrapper: withQueryClient(),
    });
    await waitFor(() => expect(result.current.isSuccess).toBe(true));
    expect(result.current.data?.category).toBe(category);
  });

  it("category=null (boş trade) → meets_brandon_targets=false", async () => {
    fetchMock.mockResolvedValue(
      jsonResponse({
        ...EXCELLENT,
        category: null,
        total_closed: 0,
        meets_brandon_targets: false,
      })
    );
    const { result } = renderHook(() => useTradesExpectancy(), {
      wrapper: withQueryClient(),
    });
    await waitFor(() => expect(result.current.isSuccess).toBe(true));
    expect(result.current.data?.category).toBeNull();
    expect(result.current.data?.total_closed).toBe(0);
  });
});

describe("useTradesExpectancy — istatistikler döner", () => {
  it("win_rate + winners + losers + avg_gain/loss tutarlı", async () => {
    fetchMock.mockResolvedValue(jsonResponse(EXCELLENT));
    const { result } = renderHook(() => useTradesExpectancy(), {
      wrapper: withQueryClient(),
    });
    await waitFor(() => expect(result.current.isSuccess).toBe(true));
    const d = result.current.data!;
    expect(d.total_closed).toBe(20);
    expect(d.winners + d.losers).toBe(d.total_closed);
    expect(d.win_rate).toBe(0.65);  // #17: kesir (0-1), %65 değil
    expect(d.avg_gain_pct).toBe(25);
    expect(d.avg_loss_pct).toBe(8);
  });
});

describe("useTradesExpectancy — hata yolu", () => {
  it("HTTP 500 → error", async () => {
    fetchMock.mockResolvedValue(new Response("err", { status: 500 }));
    const { result } = renderHook(() => useTradesExpectancy(), {
      wrapper: withQueryClient(),
    });
    await waitFor(() => expect(result.current.isError).toBe(true));
    expect((result.current.error as Error).message).toContain("500");
  });

  it("Network fail → error", async () => {
    fetchMock.mockRejectedValue(new Error("network"));
    const { result } = renderHook(() => useTradesExpectancy(), {
      wrapper: withQueryClient(),
    });
    await waitFor(() => expect(result.current.isError).toBe(true));
    expect((result.current.error as Error).message).toBe("network");
  });
});
