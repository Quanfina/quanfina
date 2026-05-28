/**
 * useSectorRotation hook — /api/sector-rotation (11 SPDR ETF RS rank).
 */
import { describe, it, expect, vi, beforeEach, afterEach } from "vitest";
import { renderHook, waitFor } from "@testing-library/react";
import {
  useSectorRotation,
  type SectorRotationEntry,
} from "@/hooks/use-sector-rotation";
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

const SECTORS: SectorRotationEntry[] = [
  {
    ticker: "XLK", sector_name: "Technology", perf_1w: 6.4, perf_1m: 19.8,
    perf_3m: 25.1, perf_6m: 15.5, perf_1y: 57.3, rs_score: 27.5, rs_rank: 1,
    scan_date: "2026-05-07",
  },
  {
    ticker: "XLE", sector_name: "Energy", perf_1w: -6.2, perf_1m: -3.6,
    perf_3m: 7.2, perf_6m: 28.3, perf_1y: 38.9, rs_score: 13.4, rs_rank: 2,
    scan_date: "2026-05-07",
  },
];

describe("useSectorRotation — fetch + parsing", () => {
  it("Fetch URL: /api/sector-rotation", async () => {
    fetchMock.mockResolvedValue(jsonResponse(SECTORS));
    renderHook(() => useSectorRotation(), { wrapper: withQueryClient() });
    await waitFor(() => expect(fetchMock).toHaveBeenCalled());
    expect(fetchMock.mock.calls[0][0]).toBe("/api/sector-rotation");
  });

  it("Happy → 11 sektör array (rank sıralı)", async () => {
    fetchMock.mockResolvedValue(jsonResponse(SECTORS));
    const { result } = renderHook(() => useSectorRotation(), {
      wrapper: withQueryClient(),
    });
    await waitFor(() => expect(result.current.isSuccess).toBe(true));
    expect(result.current.data).toHaveLength(2);
    expect(result.current.data?.[0].rs_rank).toBe(1);
  });

  it("XLK Technology rank 1 (lider sektör)", async () => {
    fetchMock.mockResolvedValue(jsonResponse(SECTORS));
    const { result } = renderHook(() => useSectorRotation(), {
      wrapper: withQueryClient(),
    });
    await waitFor(() => expect(result.current.isSuccess).toBe(true));
    expect(result.current.data?.[0].ticker).toBe("XLK");
    expect(result.current.data?.[0].sector_name).toBe("Technology");
    expect(result.current.data?.[0].perf_1y).toBe(57.3);
  });

  it("Boş array (scanner doldurmadı) → []", async () => {
    fetchMock.mockResolvedValue(jsonResponse([]));
    const { result } = renderHook(() => useSectorRotation(), {
      wrapper: withQueryClient(),
    });
    await waitFor(() => expect(result.current.isSuccess).toBe(true));
    expect(result.current.data).toEqual([]);
  });

  it("HTTP 500 → error", async () => {
    fetchMock.mockResolvedValue(new Response("err", { status: 500 }));
    const { result } = renderHook(() => useSectorRotation(), {
      wrapper: withQueryClient(),
    });
    await waitFor(() => expect(result.current.isError).toBe(true));
    expect((result.current.error as Error).message).toContain("500");
  });
});
