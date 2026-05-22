"use client";

import { useQuery } from "@tanstack/react-query";

/**
 * ABD Borsa Takvim Durumu Hook — Sprint 4-bis.7 (22 May 2026).
 *
 * Sn. Ferit talimat: "veri çekme saati ABD borsa saatleri ABD tatiller veri
 * çekme sistemini geliştirelim Türkiye'de yaşadığımı unutma."
 *
 * Backend: /api/market/calendar/status (market_calendar.py utility).
 * Polling: 60 saniyede bir refresh (borsa açık/kapalı/session geçişi yakalama).
 */

export type MarketSession = "regular" | "pre_market" | "post_market" | "closed";

export interface MarketCalendarStatus {
  is_open: boolean;
  session: MarketSession;
  reason: string | null;
  is_early_close: boolean;
  now_et: string;
  now_tr: string;
  next_open_et: string;
  next_open_tr: string;
  last_trading_day: string;
}

async function fetchMarketCalendarStatus(): Promise<MarketCalendarStatus> {
  const res = await fetch("/api/market/calendar/status");
  if (!res.ok) throw new Error(`HTTP ${res.status}`);
  return res.json();
}

export function useMarketCalendar() {
  return useQuery({
    queryKey: ["market", "calendar", "status"],
    queryFn: fetchMarketCalendarStatus,
    staleTime: 30_000,
    refetchInterval: 60_000,
    refetchIntervalInBackground: false,
    retry: 1,
  });
}
