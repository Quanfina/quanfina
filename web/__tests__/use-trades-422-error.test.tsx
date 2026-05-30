/**
 * use-trades hook 422 Pydantic ValidationError parse — Paket 388 UX bug fix.
 *
 * P385-P387 backend Pydantic gt=0 sıkılaştırmasından sonra: kullanıcı
 * entry_price=0 yazıp gönderirse FastAPI 422 detail LİSTE döndürür
 * [{loc, msg, type}]. Eski hook `err.detail` string varsayıyordu ->
 * "[object Object]" göstererek kullanıcıyı yanıltabilirdi.
 *
 * Bu test: 422 mock response -> hook field-bazli kullanıcı dostu mesaj atar.
 * Mark "Objective Mirror Language" disiplini (Vizyon İLKE #11).
 */
import { describe, it, expect, vi, beforeEach, afterEach } from "vitest";
import { renderHook, waitFor } from "@testing-library/react";
import { useAddTrade, useUpdateTrade, useDeleteTrade } from "@/hooks/use-trades";
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

const VALID_TRADE = {
  symbol: "AAPL",
  strategy: "minervini",
  setup_type: "vcp",
  signal_source: "strategy" as const,
  entry_date: "2026-05-30",
  entry_price: 100.0,
  shares: 100,
  status: "open" as const,
  plan_entry_trigger: "VCP pivot",
  plan_stop: 95.0,
  plan_target: 115.0,
  plan_size_pct: 5.0,
  plan_exit_strategy: "Target / stop",
  plan_time_horizon: "swing" as const,
};


describe("useAddTrade 422 — Pydantic detail liste parse (P388)", () => {
  it("422 single field error -> 'entry_price: Input should be greater than 0'", async () => {
    fetchMock.mockResolvedValue(
      pydanticErrorResponse([
        {
          type: "greater_than",
          loc: ["body", "entry_price"],
          msg: "Input should be greater than 0",
          ctx: { gt: 0 },
        },
      ])
    );
    const { result } = renderHook(() => useAddTrade(), {
      wrapper: withQueryClient(),
    });
    result.current.mutate(VALID_TRADE);
    await waitFor(() => expect(result.current.isError).toBe(true));
    const msg = (result.current.error as Error).message;
    // "[object Object]" GÖSTERMEMELİ — kullanıcı dostu field + msg
    expect(msg).not.toContain("[object Object]");
    expect(msg).toContain("entry_price");
    expect(msg).toContain("greater than 0");
  });

  it("422 multi-field error -> joined with semicolon", async () => {
    fetchMock.mockResolvedValue(
      pydanticErrorResponse([
        { loc: ["body", "entry_price"], msg: "Input should be greater than 0" },
        { loc: ["body", "plan_size_pct"], msg: "Input should be less than or equal to 100" },
      ])
    );
    const { result } = renderHook(() => useAddTrade(), {
      wrapper: withQueryClient(),
    });
    result.current.mutate(VALID_TRADE);
    await waitFor(() => expect(result.current.isError).toBe(true));
    const msg = (result.current.error as Error).message;
    expect(msg).toContain("entry_price");
    expect(msg).toContain("plan_size_pct");
    expect(msg).toContain(";");  // multi-field separator
  });

  it("503 string detail (Cloud SQL down) -> direkt mesaj", async () => {
    fetchMock.mockResolvedValue(
      new Response(
        JSON.stringify({ detail: "Veritabanına ulaşılamıyor (Cloud SQL)." }),
        { status: 503, headers: { "Content-Type": "application/json" } }
      )
    );
    const { result } = renderHook(() => useAddTrade(), {
      wrapper: withQueryClient(),
    });
    result.current.mutate(VALID_TRADE);
    await waitFor(() => expect(result.current.isError).toBe(true));
    const msg = (result.current.error as Error).message;
    expect(msg).toContain("Cloud SQL");  // backend mesajı korundu
  });

  it("Bos body (malformed) -> 'HTTP <status>' fallback", async () => {
    fetchMock.mockResolvedValue(new Response("not json", { status: 500 }));
    const { result } = renderHook(() => useAddTrade(), {
      wrapper: withQueryClient(),
    });
    result.current.mutate(VALID_TRADE);
    await waitFor(() => expect(result.current.isError).toBe(true));
    const msg = (result.current.error as Error).message;
    expect(msg).toBe("HTTP 500");
  });
});


describe("useUpdateTrade 422 (P388 paralel)", () => {
  it("PATCH plan_stop negatif -> field-bazli mesaj", async () => {
    fetchMock.mockResolvedValue(
      pydanticErrorResponse([
        { loc: ["body", "plan_stop"], msg: "Input should be greater than 0" },
      ])
    );
    const { result } = renderHook(() => useUpdateTrade(), {
      wrapper: withQueryClient(),
    });
    result.current.mutate({ id: 1, update: { plan_stop: -5 } });
    await waitFor(() => expect(result.current.isError).toBe(true));
    const msg = (result.current.error as Error).message;
    expect(msg).toContain("plan_stop");
    expect(msg).not.toContain("[object Object]");
  });
});


describe("useDeleteTrade error (P388 paralel)", () => {
  it("404 string detail -> direkt mesaj", async () => {
    fetchMock.mockResolvedValue(
      new Response(
        JSON.stringify({ detail: "Trade 999 bulunamadı" }),
        { status: 404, headers: { "Content-Type": "application/json" } }
      )
    );
    const { result } = renderHook(() => useDeleteTrade(), {
      wrapper: withQueryClient(),
    });
    result.current.mutate(999);
    await waitFor(() => expect(result.current.isError).toBe(true));
    const msg = (result.current.error as Error).message;
    expect(msg).toContain("bulunamadı");
  });
});
