"use client";

import { useQuery } from "@tanstack/react-query";

/**
 * DB bağlantı durumu hook'u — /api/health endpoint'inden çekiyor.
 *
 * Kaynak: api/main.py `db_health_check()` → `db_connected: bool`
 * Tetik: KARAR ADAY (22 May 2026) — Sn. Ferit talimat "Mockdan gerçek veriye
 * geçmeyi ayarla" → Seçenek B "MOCK koru + frontend banner".
 *
 * Polling: her 30 saniye otomatik refresh (DB ileride kopabilir, kullanıcı bilmeli).
 * Stale time: 15 sn (cache hassas — DB durumu değişebilir).
 */

export interface DbStatus {
  status: string;
  service: string;
  timestamp: string;
  db_connected: boolean;
}

async function fetchDbStatus(): Promise<DbStatus> {
  const res = await fetch("/api/health");
  if (!res.ok) throw new Error(`HTTP ${res.status}`);
  return res.json();
}

export function useDbStatus() {
  return useQuery({
    queryKey: ["db", "status"],
    queryFn: fetchDbStatus,
    staleTime: 15_000,
    refetchInterval: 30_000,
    refetchIntervalInBackground: false,
    retry: 1,
  });
}
