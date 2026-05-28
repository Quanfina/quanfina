/**
 * usePatterns hook (KARAR ADAY #714 — Pattern Library /api/patterns).
 *
 * 7 Mark/O'Neil canon pattern. Infinity cache (canon nadiren degisir).
 */
import { describe, it, expect, vi, beforeEach, afterEach } from "vitest";
import { renderHook, waitFor } from "@testing-library/react";
import { usePatterns, type PatternLibraryEntry } from "@/hooks/use-patterns";
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

const PATTERNS: PatternLibraryEntry[] = [
  {
    id: 1, pattern_name: "Standard VCP", mark_book_ref: "TLSMW Ch 10 s.195",
    contraction_count_min: 2, contraction_count_max: 4,
    base_weeks_min: 7, base_weeks_max: 65, notes: "Mark canon",
  },
  {
    id: 5, pattern_name: "Power Play (HTF)", mark_book_ref: "TLSMW Ch 10",
    contraction_count_min: 2, contraction_count_max: 3,
    base_weeks_min: 3, base_weeks_max: 6, notes: "POLE 100% + FLAG 10-25%",
  },
];

describe("usePatterns — fetch + parsing", () => {
  it("Fetch URL: /api/patterns", async () => {
    fetchMock.mockResolvedValue(jsonResponse(PATTERNS));
    renderHook(() => usePatterns(), { wrapper: withQueryClient() });
    await waitFor(() => expect(fetchMock).toHaveBeenCalled());
    expect(fetchMock.mock.calls[0][0]).toBe("/api/patterns");
  });

  it("Happy → pattern array döner", async () => {
    fetchMock.mockResolvedValue(jsonResponse(PATTERNS));
    const { result } = renderHook(() => usePatterns(), {
      wrapper: withQueryClient(),
    });
    await waitFor(() => expect(result.current.isSuccess).toBe(true));
    expect(result.current.data).toHaveLength(2);
    expect(result.current.data?.[0].pattern_name).toBe("Standard VCP");
  });

  it("Power Play HTF base_weeks 3-6 (Mark canon parse)", async () => {
    fetchMock.mockResolvedValue(jsonResponse(PATTERNS));
    const { result } = renderHook(() => usePatterns(), {
      wrapper: withQueryClient(),
    });
    await waitFor(() => expect(result.current.isSuccess).toBe(true));
    const pp = result.current.data?.find((p) => p.pattern_name === "Power Play (HTF)");
    expect(pp?.base_weeks_min).toBe(3);
    expect(pp?.base_weeks_max).toBe(6);
  });

  it("Her pattern mark_book_ref içerir (KALICI İLKE #4)", async () => {
    fetchMock.mockResolvedValue(jsonResponse(PATTERNS));
    const { result } = renderHook(() => usePatterns(), {
      wrapper: withQueryClient(),
    });
    await waitFor(() => expect(result.current.isSuccess).toBe(true));
    for (const p of result.current.data ?? []) {
      expect(p.mark_book_ref).toBeTruthy();
    }
  });

  it("Boş array (DB erişilemez fallback) → []", async () => {
    fetchMock.mockResolvedValue(jsonResponse([]));
    const { result } = renderHook(() => usePatterns(), {
      wrapper: withQueryClient(),
    });
    await waitFor(() => expect(result.current.isSuccess).toBe(true));
    expect(result.current.data).toEqual([]);
  });
});

describe("usePatterns — hata yolu", () => {
  it("HTTP 500 → error mesajında status", async () => {
    fetchMock.mockResolvedValue(new Response("err", { status: 500 }));
    const { result } = renderHook(() => usePatterns(), {
      wrapper: withQueryClient(),
    });
    await waitFor(() => expect(result.current.isError).toBe(true));
    expect((result.current.error as Error).message).toContain("500");
  });

  it("Network fail → error", async () => {
    fetchMock.mockRejectedValue(new Error("network"));
    const { result } = renderHook(() => usePatterns(), {
      wrapper: withQueryClient(),
    });
    await waitFor(() => expect(result.current.isError).toBe(true));
    expect((result.current.error as Error).message).toBe("network");
  });
});
