/**
 * useRiskAdvisor (KARAR ADAY #914 + #969 + #970 — Sprint 4-bis.7 Faz 1 B).
 *
 * useMutation pattern: POST /api/risk/advisor, mutate çağrısı + body + 6-rule cevap.
 */
import { describe, it, expect, vi, beforeEach, afterEach } from "vitest";
import { renderHook, waitFor, act } from "@testing-library/react";
import {
  useRiskAdvisor,
  type RiskAdvisorRequest,
  type RiskAdvisorResponse,
} from "@/hooks/use-risk-advisor";
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

const HAPPY_RESPONSE: RiskAdvisorResponse = {
  position_dollars: 5000,
  position_pct: 5.0,
  risk_dollars: 1000,
  risk_pct: 1.0,
  tier: "optimal",
  sizing_warnings: [],
  sizing_says: "Mark optimal R sizing.",
  recommended_stop_pct: 6.5,
  stop_method: "rba_based",
  stop_absolute_cap_applied: false,
  stop_says: "RBA bazlı 6.5% (Mark mutlak 7% altı).",
  six_rule_all_pass: true,
  six_rule_pass_count: 6,
  six_rule_critical_violations: [],
  six_rules: [],
  mark_constants: {
    stop_absolute_cap_pct: 7,
    equity_risk_min_pct: 0.5,
    equity_risk_max_pct: 2,
    position_max_pct: 50,
    position_optimal_range: [10, 25],
    portfolio_optimal_stocks: [4, 8],
    portfolio_max_stocks: 12,
  },
};

const BASIC_REQUEST: RiskAdvisorRequest = {
  portfolio_value: 100_000,
  target_risk_pct: 1.0,
};

describe("useRiskAdvisor — useMutation pattern", () => {
  it("İlk render → isPending=false, mutate function var", () => {
    const { result } = renderHook(() => useRiskAdvisor(), {
      wrapper: withQueryClient(),
    });
    expect(result.current.isPending).toBe(false);
    expect(result.current.isSuccess).toBe(false);
    expect(typeof result.current.mutate).toBe("function");
  });

  it("mutate → fetch tetiklenir, isSuccess true + data döner", async () => {
    fetchMock.mockResolvedValue(jsonResponse(HAPPY_RESPONSE));
    const { result } = renderHook(() => useRiskAdvisor(), {
      wrapper: withQueryClient(),
    });
    act(() => {
      result.current.mutate(BASIC_REQUEST);
    });
    await waitFor(() => expect(result.current.isSuccess).toBe(true));
    expect(result.current.data).toEqual(HAPPY_RESPONSE);
  });

  it("POST body: portfolio_value + target_risk_pct gönderilir", async () => {
    fetchMock.mockResolvedValue(jsonResponse(HAPPY_RESPONSE));
    const { result } = renderHook(() => useRiskAdvisor(), {
      wrapper: withQueryClient(),
    });
    act(() => {
      result.current.mutate({
        portfolio_value: 50_000,
        target_risk_pct: 1.5,
        is_best_name: true,
      });
    });
    await waitFor(() => expect(fetchMock).toHaveBeenCalled());
    const [url, init] = fetchMock.mock.calls[0];
    expect(url).toBe("/api/risk/advisor");
    expect(init?.method).toBe("POST");
    const body = JSON.parse(init!.body as string);
    expect(body).toEqual({
      portfolio_value: 50_000,
      target_risk_pct: 1.5,
      is_best_name: true,
    });
  });

  it("Content-Type: application/json header", async () => {
    fetchMock.mockResolvedValue(jsonResponse(HAPPY_RESPONSE));
    const { result } = renderHook(() => useRiskAdvisor(), {
      wrapper: withQueryClient(),
    });
    act(() => result.current.mutate(BASIC_REQUEST));
    await waitFor(() => expect(fetchMock).toHaveBeenCalled());
    const headers = fetchMock.mock.calls[0][1]?.headers as Record<string, string>;
    expect(headers["Content-Type"]).toBe("application/json");
  });
});

describe("useRiskAdvisor — 6 Rule cevap parsing", () => {
  it("six_rule_all_pass=true → data'da true", async () => {
    fetchMock.mockResolvedValue(jsonResponse(HAPPY_RESPONSE));
    const { result } = renderHook(() => useRiskAdvisor(), {
      wrapper: withQueryClient(),
    });
    act(() => result.current.mutate(BASIC_REQUEST));
    await waitFor(() => expect(result.current.isSuccess).toBe(true));
    expect(result.current.data?.six_rule_all_pass).toBe(true);
    expect(result.current.data?.six_rule_pass_count).toBe(6);
  });

  it("tier='optimal' + Mark sabitleri (max_pct, optimal_range)", async () => {
    fetchMock.mockResolvedValue(jsonResponse(HAPPY_RESPONSE));
    const { result } = renderHook(() => useRiskAdvisor(), {
      wrapper: withQueryClient(),
    });
    act(() => result.current.mutate(BASIC_REQUEST));
    await waitFor(() => expect(result.current.isSuccess).toBe(true));
    expect(result.current.data?.tier).toBe("optimal");
    expect(result.current.data?.mark_constants.stop_absolute_cap_pct).toBe(7);
    expect(result.current.data?.mark_constants.position_optimal_range).toEqual([10, 25]);
  });

  it("stop_method='rba_based' + stop_absolute_cap_applied=false", async () => {
    fetchMock.mockResolvedValue(jsonResponse(HAPPY_RESPONSE));
    const { result } = renderHook(() => useRiskAdvisor(), {
      wrapper: withQueryClient(),
    });
    act(() => result.current.mutate(BASIC_REQUEST));
    await waitFor(() => expect(result.current.isSuccess).toBe(true));
    expect(result.current.data?.stop_method).toBe("rba_based");
    expect(result.current.data?.stop_absolute_cap_applied).toBe(false);
  });
});

describe("useRiskAdvisor — hata yolu", () => {
  it("HTTP 422 → error", async () => {
    fetchMock.mockResolvedValue(new Response("validation", { status: 422 }));
    const { result } = renderHook(() => useRiskAdvisor(), {
      wrapper: withQueryClient(),
    });
    act(() => result.current.mutate(BASIC_REQUEST));
    await waitFor(() => expect(result.current.isError).toBe(true));
    expect((result.current.error as Error).message).toContain("422");
  });

  it("Network fail → error state", async () => {
    fetchMock.mockRejectedValue(new Error("offline"));
    const { result } = renderHook(() => useRiskAdvisor(), {
      wrapper: withQueryClient(),
    });
    act(() => result.current.mutate(BASIC_REQUEST));
    await waitFor(() => expect(result.current.isError).toBe(true));
    expect((result.current.error as Error).message).toBe("offline");
  });
});
