/**
 * useRbaMetrics (KARAR ADAY #722 — Mark TTLC Sec 4 RBA).
 *
 * GET /api/rba/metrics[?strategy=X&setup_type=Y]
 * URLSearchParams ile query string + filter cevap + severity.
 */
import { describe, it, expect, vi, beforeEach, afterEach } from "vitest";
import { renderHook, waitFor } from "@testing-library/react";
import {
  useRbaMetrics,
  type RbaResponse,
} from "@/hooks/use-rba-metrics";
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

const HAPPY: RbaResponse = {
  metrics: {
    num_trades: 30,
    win_rate: 0.6,
    avg_gain_pct: 15,
    avg_loss_pct: -7,
    largest_gain_pct: 35,
    largest_loss_pct: -12,
    adjusted_ratio: 2.5,
    expectancy_pct: 6.4,
    is_statistically_significant: true,
  },
  recommendation: {
    severity: "OK",
    message: "Mark TTLC Sec 4: pozitif expectancy, devam.",
  },
  filter_strategy: null,
  filter_setup_type: null,
};

describe("useRbaMetrics — URL building (URLSearchParams)", () => {
  it("Filtre yok → /api/rba/metrics (query yok)", async () => {
    fetchMock.mockResolvedValue(jsonResponse(HAPPY));
    renderHook(() => useRbaMetrics(), { wrapper: withQueryClient() });
    await waitFor(() => expect(fetchMock).toHaveBeenCalled());
    expect(fetchMock.mock.calls[0][0]).toBe("/api/rba/metrics");
  });

  it("strategy='minervini' → ?strategy=minervini", async () => {
    fetchMock.mockResolvedValue(jsonResponse(HAPPY));
    renderHook(() => useRbaMetrics({ strategy: "minervini" }), {
      wrapper: withQueryClient(),
    });
    await waitFor(() => expect(fetchMock).toHaveBeenCalled());
    expect(fetchMock.mock.calls[0][0]).toBe(
      "/api/rba/metrics?strategy=minervini"
    );
  });

  it("setup_type='vcp' → ?setup_type=vcp", async () => {
    fetchMock.mockResolvedValue(jsonResponse(HAPPY));
    renderHook(() => useRbaMetrics({ setup_type: "vcp" }), {
      wrapper: withQueryClient(),
    });
    await waitFor(() => expect(fetchMock).toHaveBeenCalled());
    expect(fetchMock.mock.calls[0][0]).toBe(
      "/api/rba/metrics?setup_type=vcp"
    );
  });

  it("Hem strategy hem setup_type → URLSearchParams sırasına göre birleşir", async () => {
    fetchMock.mockResolvedValue(jsonResponse(HAPPY));
    renderHook(
      () => useRbaMetrics({ strategy: "carr", setup_type: "pivot" }),
      { wrapper: withQueryClient() }
    );
    await waitFor(() => expect(fetchMock).toHaveBeenCalled());
    const url = fetchMock.mock.calls[0][0] as string;
    expect(url).toContain("strategy=carr");
    expect(url).toContain("setup_type=pivot");
    expect(url.startsWith("/api/rba/metrics?")).toBe(true);
  });

  it("Boş string strategy → falsy, query'e eklenmez", async () => {
    fetchMock.mockResolvedValue(jsonResponse(HAPPY));
    renderHook(() => useRbaMetrics({ strategy: "" }), {
      wrapper: withQueryClient(),
    });
    await waitFor(() => expect(fetchMock).toHaveBeenCalled());
    expect(fetchMock.mock.calls[0][0]).toBe("/api/rba/metrics");
  });
});

describe("useRbaMetrics — enabled flag", () => {
  it("enabled=false → fetch tetiklenmez", async () => {
    fetchMock.mockResolvedValue(jsonResponse(HAPPY));
    renderHook(() => useRbaMetrics({ enabled: false }), {
      wrapper: withQueryClient(),
    });
    await new Promise((r) => setTimeout(r, 50));
    expect(fetchMock).not.toHaveBeenCalled();
  });

  it("enabled default=true → fetch tetiklenir", async () => {
    fetchMock.mockResolvedValue(jsonResponse(HAPPY));
    renderHook(() => useRbaMetrics(), { wrapper: withQueryClient() });
    await waitFor(() => expect(fetchMock).toHaveBeenCalled());
  });
});

describe("useRbaMetrics — RBA metrikleri response parsing", () => {
  it("happy → metrics + recommendation döner", async () => {
    fetchMock.mockResolvedValue(jsonResponse(HAPPY));
    const { result } = renderHook(() => useRbaMetrics(), {
      wrapper: withQueryClient(),
    });
    await waitFor(() => expect(result.current.isSuccess).toBe(true));
    expect(result.current.data?.metrics.num_trades).toBe(30);
    expect(result.current.data?.metrics.win_rate).toBe(0.6);
    expect(result.current.data?.metrics.adjusted_ratio).toBe(2.5);
    expect(result.current.data?.metrics.is_statistically_significant).toBe(true);
  });

  it.each<RbaResponse["recommendation"]["severity"]>([
    "OK",
    "INFO",
    "WARNING",
    "CRITICAL",
  ])("severity='%s' → recommendation döner", async (severity) => {
    fetchMock.mockResolvedValue(
      jsonResponse({ ...HAPPY, recommendation: { severity, message: `${severity} test` } })
    );
    const { result } = renderHook(() => useRbaMetrics(), {
      wrapper: withQueryClient(),
    });
    await waitFor(() => expect(result.current.isSuccess).toBe(true));
    expect(result.current.data?.recommendation.severity).toBe(severity);
  });

  it("num_trades=5 + is_statistically_significant=false (<30 eşik)", async () => {
    fetchMock.mockResolvedValue(
      jsonResponse({
        ...HAPPY,
        metrics: { ...HAPPY.metrics, num_trades: 5, is_statistically_significant: false },
      })
    );
    const { result } = renderHook(() => useRbaMetrics(), {
      wrapper: withQueryClient(),
    });
    await waitFor(() => expect(result.current.isSuccess).toBe(true));
    expect(result.current.data?.metrics.is_statistically_significant).toBe(false);
  });
});

describe("useRbaMetrics — hata yolu", () => {
  it("HTTP 500 → error", async () => {
    fetchMock.mockResolvedValue(new Response("err", { status: 500 }));
    const { result } = renderHook(() => useRbaMetrics(), {
      wrapper: withQueryClient(),
    });
    await waitFor(() => expect(result.current.isError).toBe(true));
    expect((result.current.error as Error).message).toContain("500");
  });
});
