"use client";

import { useMutation, useQueryClient } from "@tanstack/react-query";
import type { WatchlistRow, WatchlistRowCreate, WatchlistRowUpdate } from "@/types/watchlist";

async function addRow(body: WatchlistRowCreate): Promise<WatchlistRow> {
  const res = await fetch("/api/watchlist", {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify(body),
  });
  if (!res.ok) {
    const err = await res.json().catch(() => ({ detail: `HTTP ${res.status}` }));
    throw new Error((err as { detail?: string }).detail ?? `HTTP ${res.status}`);
  }
  return res.json() as Promise<WatchlistRow>;
}

async function updateRow({
  symbol,
  strategy,
  update,
}: {
  symbol: string;
  strategy: string;
  update: WatchlistRowUpdate;
}): Promise<WatchlistRow> {
  const res = await fetch(`/api/watchlist/${symbol}/${strategy}`, {
    method: "PATCH",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify(update),
  });
  if (!res.ok) {
    const err = await res.json().catch(() => ({ detail: `HTTP ${res.status}` }));
    throw new Error((err as { detail?: string }).detail ?? `HTTP ${res.status}`);
  }
  return res.json() as Promise<WatchlistRow>;
}

async function deleteRow({
  symbol,
  strategy,
}: {
  symbol: string;
  strategy: string;
}): Promise<void> {
  const res = await fetch(`/api/watchlist/${symbol}/${strategy}`, {
    method: "DELETE",
  });
  if (!res.ok) {
    const err = await res.json().catch(() => ({ detail: `HTTP ${res.status}` }));
    throw new Error((err as { detail?: string }).detail ?? `HTTP ${res.status}`);
  }
}

async function promoteRow({
  symbol,
  strategy,
}: {
  symbol: string;
  strategy: string;
}): Promise<WatchlistRow> {
  const res = await fetch(`/api/watchlist/${symbol}/${strategy}/promote`, {
    method: "POST",
  });
  if (!res.ok) {
    const err = await res.json().catch(() => ({ detail: `HTTP ${res.status}` }));
    throw new Error((err as { detail?: string }).detail ?? `HTTP ${res.status}`);
  }
  return res.json() as Promise<WatchlistRow>;
}

export function useAddWatchlistRow() {
  const qc = useQueryClient();
  return useMutation({
    mutationFn: addRow,
    onSuccess: () => qc.invalidateQueries({ queryKey: ["watchlist"] }),
  });
}

export function useUpdateWatchlistRow() {
  const qc = useQueryClient();
  return useMutation({
    mutationFn: updateRow,
    onSuccess: () => qc.invalidateQueries({ queryKey: ["watchlist"] }),
  });
}

export function useDeleteWatchlistRow() {
  const qc = useQueryClient();
  return useMutation({
    mutationFn: deleteRow,
    onSuccess: () => qc.invalidateQueries({ queryKey: ["watchlist"] }),
  });
}

export function usePromoteWatchlistRow() {
  const qc = useQueryClient();
  return useMutation({
    mutationFn: promoteRow,
    onSuccess: () => qc.invalidateQueries({ queryKey: ["watchlist"] }),
  });
}
