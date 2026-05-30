"use client";

import { useMutation, useQuery, useQueryClient } from "@tanstack/react-query";
import type { Trade, TradeCreate, TradeUpdate, SetupType } from "@/types/trade";

/**
 * P388: Pydantic 422 ValidationError detail parse (kullanıcı dostu mesaj).
 *
 * FastAPI 422 response yapısı: { detail: [{loc, msg, type, ctx}] } — detail
 * STRING DEĞİL LİSTE. Eski `err.detail ?? "HTTP 422"` doğrudan render edilirse
 * "[object Object]" gösterirdi. Bu helper:
 * - 422 liste -> "entry_price: Input should be greater than 0; shares: ..."
 * - 503/diğer string -> dogrudan dondur
 * - Bos/error -> jenerik fallback
 *
 * Mark "Objective Mirror Language" disiplini: kullanıcıya somut alan + sebep
 * göster (yağcılık değil, ham gerçek).
 */
function parsePydanticError(detail: unknown, status: number): string {
  if (typeof detail === "string") return detail;
  if (Array.isArray(detail)) {
    return detail
      .map((d) => {
        const err = d as { loc?: unknown[]; msg?: string };
        const field = Array.isArray(err.loc) ? err.loc[err.loc.length - 1] : "?";
        return `${field}: ${err.msg ?? "geçersiz"}`;
      })
      .join("; ");
  }
  return `HTTP ${status}`;
}

async function _parseErrorBody(res: Response): Promise<string> {
  try {
    const body = await res.json();
    return parsePydanticError(
      (body as { detail?: unknown }).detail,
      res.status,
    );
  } catch {
    return `HTTP ${res.status}`;
  }
}

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
    // P388: Pydantic 422 detail liste -> kullanici dostu field-bazli mesaj
    throw new Error(await _parseErrorBody(res));
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
    throw new Error(await _parseErrorBody(res));
  }
  return res.json() as Promise<Trade>;
}

async function deleteTrade(id: number): Promise<void> {
  const res = await fetch(`/api/trades/${id}`, { method: "DELETE" });
  if (!res.ok) {
    throw new Error(await _parseErrorBody(res));
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
