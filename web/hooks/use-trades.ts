"use client";

import { useMutation, useQuery, useQueryClient } from "@tanstack/react-query";
import type { Trade, TradeCreate, TradeUpdate, SetupType } from "@/types/trade";

async function fetchTrades(): Promise<Trade[]> {
  // 8sn timeout + 503 detail parse (Sinyaller/Watchlist pateni — Kural #20 UX)
  const res = await fetch("/api/trades", { signal: AbortSignal.timeout(8000) });
  if (!res.ok) {
    let detail = "";
    try {
      const body = await res.text();
      try {
        const json = JSON.parse(body) as { detail?: string };
        detail = json.detail ?? body.slice(0, 160);
      } catch {
        detail = body.slice(0, 160);
      }
    } catch {
      /* ignore */
    }
    throw new Error(`HTTP ${res.status}${detail ? ` — ${detail}` : ""}`);
  }
  return res.json() as Promise<Trade[]>;
}

async function fetchSetupTypes(): Promise<SetupType[]> {
  const res = await fetch("/api/setup-types");
  if (!res.ok) throw new Error(`HTTP ${res.status}`);
  return res.json() as Promise<SetupType[]>;
}

async function addTrade(body: TradeCreate): Promise<Trade> {
  const res = await fetch("/api/trades", {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify(body),
  });
  if (!res.ok) {
    const err = await res.json().catch(() => ({ detail: `HTTP ${res.status}` }));
    throw new Error((err as { detail?: string }).detail ?? `HTTP ${res.status}`);
  }
  return res.json() as Promise<Trade>;
}

async function updateTrade({
  id,
  update,
}: {
  id: number;
  update: TradeUpdate;
}): Promise<Trade> {
  const res = await fetch(`/api/trades/${id}`, {
    method: "PATCH",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify(update),
  });
  if (!res.ok) {
    const err = await res.json().catch(() => ({ detail: `HTTP ${res.status}` }));
    throw new Error((err as { detail?: string }).detail ?? `HTTP ${res.status}`);
  }
  return res.json() as Promise<Trade>;
}

async function deleteTrade(id: number): Promise<void> {
  const res = await fetch(`/api/trades/${id}`, { method: "DELETE" });
  if (!res.ok) {
    const err = await res.json().catch(() => ({ detail: `HTTP ${res.status}` }));
    throw new Error((err as { detail?: string }).detail ?? `HTTP ${res.status}`);
  }
}

export function useTrades() {
  return useQuery({
    queryKey: ["trades"],
    queryFn: fetchTrades,
    staleTime: 60_000,
    retry: 1,
  });
}

export function useSetupTypes() {
  return useQuery({
    queryKey: ["setup-types"],
    queryFn: fetchSetupTypes,
    staleTime: Infinity,
    gcTime: Infinity, // setup-types sabit liste — cache GC edilmesin
  });
}

export function useAddTrade() {
  const qc = useQueryClient();
  return useMutation({
    mutationFn: addTrade,
    onSuccess: () => qc.invalidateQueries({ queryKey: ["trades"] }),
  });
}

export function useUpdateTrade() {
  const qc = useQueryClient();
  return useMutation({
    mutationFn: updateTrade,
    onSuccess: () => qc.invalidateQueries({ queryKey: ["trades"] }),
  });
}

export function useDeleteTrade() {
  const qc = useQueryClient();
  return useMutation({
    mutationFn: deleteTrade,
    onSuccess: () => qc.invalidateQueries({ queryKey: ["trades"] }),
  });
}
