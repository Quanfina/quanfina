/**
 * use-watchlist-mutations hook 422 Pydantic parse — Paket 390.
 *
 * P387 backend WatchlistRowCreate `pivot_price gt=0` ekledim. Sn. Ferit
 * pivot_price=0 / negatif girerse 422 dönecek. Önceki hook naive
 * `err.detail ?? "HTTP 422"` -> "[object Object]" gosterirdi. P390 ortak
 * lib/api-error.ts'a delege ile field-bazli kullanıcı dostu mesaj.
 */
import { describe, it, expect, vi, beforeEach, afterEach } from "vitest";
import { renderHook, waitFor } from "@testing-library/react";
import {
  useAddWatchlistRow,
  useUpdateWatchlistRow,
  useDeleteWatchlistRow,
  usePromoteWatchlistRow,
} from "@/hooks/use-watchlist-mutations";
import { withQueryClient } from "./_test-utils";

const fetchMock = vi.fn<(input: RequestInfo | URL, init?: RequestInit) => Promise<Response>>();

beforeEach(() => {
  fetchMock.mockReset();
  vi.stubGlobal("fetch", fetchMock);
});

afterEach(() => {
  vi.unstubAllGlobals();
});

function pydanticErrorResponse(detail: unknown[], status = 422): Response {
  return new Response(JSON.stringify({ detail }), {
    status,
    headers: { "Content-Type": "application/json" },
  });
}

const VALID_WATCHLIST = {
  symbol: "AAPL",
  strategy: "minervini" as const,
  status: "watch" as const,
  pivot_price: 200.0,
};


describe("useAddWatchlistRow — 422 Pydantic parse (P390)", () => {
  it("422 pivot_price hata -> 'pivot_price: Input should be...'", async () => {
    fetchMock.mockResolvedValue(
      pydanticErrorResponse([
        { loc: ["body", "pivot_price"], msg: "Input should be greater than 0" },
      ])
    );
    const { result } = renderHook(() => useAddWatchlistRow(), {
      wrapper: withQueryClient(),
    });
    result.current.mutate(VALID_WATCHLIST);
    await waitFor(() => expect(result.current.isError).toBe(true));
    const msg = (result.current.error as Error).message;
    expect(msg).not.toContain("[object Object]");
    expect(msg).toContain("pivot_price");
    expect(msg).toContain("greater than 0");
  });

  it("409 string detail (duplicate symbol-strategy) -> dogrudan mesaj", async () => {
    fetchMock.mockResolvedValue(
      new Response(
        JSON.stringify({ detail: "AAPL-minervini zaten watchlist'te" }),
        { status: 409, headers: { "Content-Type": "application/json" } }
      )
    );
    const { result } = renderHook(() => useAddWatchlistRow(), {
      wrapper: withQueryClient(),
    });
    result.current.mutate(VALID_WATCHLIST);
    await waitFor(() => expect(result.current.isError).toBe(true));
    const msg = (result.current.error as Error).message;
    expect(msg).toContain("zaten watchlist");
  });
});


describe("useUpdateWatchlistRow — 422 (P390 paralel)", () => {
  it("PATCH 422 -> field-bazli mesaj", async () => {
    fetchMock.mockResolvedValue(
      pydanticErrorResponse([
        { loc: ["body", "status"], msg: "Input should be 'watch','on_deck','focus','buy'" },
      ])
    );
    const { result } = renderHook(() => useUpdateWatchlistRow(), {
      wrapper: withQueryClient(),
    });
    result.current.mutate({
      symbol: "AAPL",
      strategy: "minervini",
      update: { status: "watch" },  // valid type ama mock 422 simule eder
    });
    await waitFor(() => expect(result.current.isError).toBe(true));
    const msg = (result.current.error as Error).message;
    expect(msg).toContain("status");
    expect(msg).not.toContain("[object Object]");
  });
});


describe("useDeleteWatchlistRow + usePromoteWatchlistRow — error (P390)", () => {
  it("DELETE 404 -> dogrudan mesaj", async () => {
    fetchMock.mockResolvedValue(
      new Response(
        JSON.stringify({ detail: "Watchlist satiri bulunamadi" }),
        { status: 404, headers: { "Content-Type": "application/json" } }
      )
    );
    const { result } = renderHook(() => useDeleteWatchlistRow(), {
      wrapper: withQueryClient(),
    });
    result.current.mutate({ symbol: "ZZZ", strategy: "minervini" });
    await waitFor(() => expect(result.current.isError).toBe(true));
    expect((result.current.error as Error).message).toContain("bulunamadi");
  });

  it("PROMOTE 500 string -> dogrudan", async () => {
    fetchMock.mockResolvedValue(
      new Response(JSON.stringify({ detail: "Sunucu hatasi" }), { status: 500 })
    );
    const { result } = renderHook(() => usePromoteWatchlistRow(), {
      wrapper: withQueryClient(),
    });
    result.current.mutate({ symbol: "AAPL", strategy: "minervini" });
    await waitFor(() => expect(result.current.isError).toBe(true));
    expect((result.current.error as Error).message).toContain("Sunucu");
  });
});
