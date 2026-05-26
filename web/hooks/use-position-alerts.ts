"use client";

import { useEffect, useMemo, useRef } from "react";
import { toast } from "sonner";
import type { Trade } from "@/types/trade";
import type { StockQuote } from "@/hooks/use-stock-quote";

/**
 * Paket 160 (26 May 2026): Açık trade canlı stop loss uyarısı.
 *
 * Mark felsefe canon:
 * - TTLC s.131: "Always cut loss at ~%7 below buy point — always"
 * - TLSMW Ch 12: "Never let a winner turn into a loser, never sell below stop"
 * - TTLC Sec 1.6: "Without a written plan, you have only hope" → plan_stop ZORUNLU
 *
 * Alert 4 tip:
 * 1. STOP_HIT (critical) — current_price <= plan_stop → kırmızı toast, kapat
 * 2. STOP_NEAR (warning) — plan_stop'a +%1 yakın (hazır ol)
 * 3. MINERVINI_7PCT (warning) — entry'den -%7 düştü (Mark mutlak kural)
 * 4. TARGET_NEAR (info) — plan_target'a +%2 yakın (kar al hazırlığı)
 *
 * Dedup: localStorage "position-alerts-dismissed" + Set<alertId>.
 * Alert ID = `${tradeId}-${type}-${YYYY-MM-DD}` (günlük yeniden bildir).
 */

export type AlertSeverity = "critical" | "warning" | "info";
export type AlertType = "stop_hit" | "stop_near" | "minervini_7pct" | "target_near";

export interface PositionAlert {
  id: string;
  tradeId: number;
  symbol: string;
  severity: AlertSeverity;
  type: AlertType;
  title: string;
  message: string;
  current_price: number;
  ref_price: number;          // plan_stop veya plan_target veya entry × 0.93
}

const DISMISS_KEY = "position-alerts-dismissed";

function getDismissed(): Set<string> {
  if (typeof window === "undefined") return new Set();
  try {
    const raw = localStorage.getItem(DISMISS_KEY);
    if (!raw) return new Set();
    const today = new Date().toISOString().slice(0, 10);
    const parsed = JSON.parse(raw) as { date: string; ids: string[] };
    if (parsed.date !== today) {
      // Yeni gün — geçmiş dismiss'leri temizle
      localStorage.removeItem(DISMISS_KEY);
      return new Set();
    }
    return new Set(parsed.ids);
  } catch {
    return new Set();
  }
}

function setDismissed(ids: Set<string>) {
  if (typeof window === "undefined") return;
  const today = new Date().toISOString().slice(0, 10);
  localStorage.setItem(
    DISMISS_KEY,
    JSON.stringify({ date: today, ids: Array.from(ids) })
  );
}

/**
 * Açık trade'ler + canlı quote → alert listesi hesapla.
 * Toast'ları otomatik atar (useEffect ile). Component sadece çağırır.
 */
export function usePositionAlerts(
  trades: Trade[],
  quotes: Array<{ data?: StockQuote }>
): PositionAlert[] {
  // Trade'leri sembol-quote ile eşle
  const alerts = useMemo<PositionAlert[]>(() => {
    if (!trades.length) return [];
    const quoteMap = new Map<string, number>();
    quotes.forEach((r) => {
      if (r.data) quoteMap.set(r.data.symbol.toUpperCase(), r.data.price);
    });

    const today = new Date().toISOString().slice(0, 10);
    const result: PositionAlert[] = [];

    trades.forEach((t) => {
      if (t.status !== "open") return;
      const price = quoteMap.get(t.symbol.toUpperCase());
      if (price == null) return;

      // 1. STOP_HIT — current_price <= plan_stop (kritik)
      if (t.plan_stop != null && price <= t.plan_stop) {
        result.push({
          id: `${t.id}-stop_hit-${today}`,
          tradeId: t.id,
          symbol: t.symbol,
          severity: "critical",
          type: "stop_hit",
          title: `🔴 ${t.symbol} — STOP'A DEĞDİ`,
          message: `Canlı $${price.toFixed(2)} ≤ plan stop $${t.plan_stop.toFixed(
            2
          )}. Mark TLSMW Ch 12: pozisyonu kapat.`,
          current_price: price,
          ref_price: t.plan_stop,
        });
      }
      // 2. STOP_NEAR — plan_stop'a +%1 yakın (uyarı, henüz değmedi)
      else if (
        t.plan_stop != null &&
        price > t.plan_stop &&
        price <= t.plan_stop * 1.01
      ) {
        result.push({
          id: `${t.id}-stop_near-${today}`,
          tradeId: t.id,
          symbol: t.symbol,
          severity: "warning",
          type: "stop_near",
          title: `🟡 ${t.symbol} — STOP'A YAKIN`,
          message: `Canlı $${price.toFixed(2)} stop $${t.plan_stop.toFixed(
            2
          )}'a %1 mesafede. Hazırlıklı ol.`,
          current_price: price,
          ref_price: t.plan_stop,
        });
      }

      // 3. MINERVINI_7PCT — entry'den -%7 düştü (Mark mutlak kural)
      const sevenPctStop = t.entry_price * 0.93;
      const pctLoss = ((price / t.entry_price) - 1) * 100;
      if (pctLoss <= -7 && (t.plan_stop == null || t.plan_stop < sevenPctStop)) {
        result.push({
          id: `${t.id}-minervini_7pct-${today}`,
          tradeId: t.id,
          symbol: t.symbol,
          severity: "warning",
          type: "minervini_7pct",
          title: `⚠️ ${t.symbol} — %7 KURALI`,
          message: `Entry $${t.entry_price.toFixed(2)} → canlı $${price.toFixed(
            2
          )} = ${pctLoss.toFixed(1)}%. Mark TTLC s.131: %7+ asla.`,
          current_price: price,
          ref_price: sevenPctStop,
        });
      }

      // 4. TARGET_NEAR — plan_target'a +%2 yakın (kar al hazırlığı)
      if (
        t.plan_target != null &&
        price < t.plan_target &&
        price >= t.plan_target * 0.98
      ) {
        result.push({
          id: `${t.id}-target_near-${today}`,
          tradeId: t.id,
          symbol: t.symbol,
          severity: "info",
          type: "target_near",
          title: `🟢 ${t.symbol} — HEDEFE YAKIN`,
          message: `Canlı $${price.toFixed(2)} hedef $${t.plan_target.toFixed(
            2
          )}'a %2 mesafede. Kar al disiplini.`,
          current_price: price,
          ref_price: t.plan_target,
        });
      }
    });

    return result;
  }, [trades, quotes]);

  // Toast tetik — render-dışı side effect
  // Dismissed Set ref ile takip, double-fire önleme
  const dismissedRef = useRef<Set<string>>(new Set());
  useEffect(() => {
    if (dismissedRef.current.size === 0) {
      dismissedRef.current = getDismissed();
    }
  }, []);

  useEffect(() => {
    if (!alerts.length) return;
    const dismissed = dismissedRef.current;
    let updated = false;
    alerts.forEach((a) => {
      if (dismissed.has(a.id)) return;
      // Toast severity'e göre stil
      const toastFn =
        a.severity === "critical"
          ? toast.error
          : a.severity === "warning"
          ? toast.warning
          : toast.info;
      toastFn(a.title, {
        description: a.message,
        duration: a.severity === "critical" ? 15000 : 8000,
        id: a.id, // sonner dedup — aynı ID ile çağrı upsert
      });
      dismissed.add(a.id);
      updated = true;
    });
    if (updated) setDismissed(dismissed);
  }, [alerts]);

  return alerts;
}
