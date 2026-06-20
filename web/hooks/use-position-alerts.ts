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
 * Alert 5 tip:
 * 1. STOP_HIT (critical) — current_price <= plan_stop → kırmızı toast, kapat
 * 2. STOP_NEAR (warning) — plan_stop'a +%1 yakın (hazır ol)
 * 3. MINERVINI_7PCT (warning) — entry'den -%7 düştü (Mark mutlak kural)
 * 4. TARGET_NEAR (info) — plan_target'a +%2 yakın (kar al hazırlığı)
 * 5. FREE_TRADE (warning/info) — kar başlangıç stop mesafesinin 2x/3x'ine ulaştı →
 *    stop'u breakeven'e taşı, risksiz pozisyon (Mark TTLC Böl.2; P555 —
 *    quanfina_math.should_move_to_breakeven eşikleri birebir, Kural #26). Yön-farkında.
 * 6. SELL_HALF (warning/info) — kar +%20'ye (≥%30 geç kaldı) ulaştı → yarı sat,
 *    "kazan-kazan" moduna gir (Mark TLSMW Böl.13; P557 — quanfina_math.should_sell_half
 *    statik eşiği birebir, Kural #26). Yön-farkında. (RBA-dinamik eşik kullanıcı
 *    geçmişi gerektirir — canlı alert'te statik %20 canon kullanılır.)
 *
 * Dedup: localStorage "position-alerts-dismissed" + Set<alertId>.
 * Alert ID = `${tradeId}-${type}-${YYYY-MM-DD}` (günlük yeniden bildir).
 */

export type AlertSeverity = "critical" | "warning" | "info";
export type AlertType =
  | "stop_hit"
  | "stop_near"
  | "minervini_7pct"
  | "target_near"
  | "free_trade"
  | "sell_half";

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
// Paket 168 (26 May 2026): Alert history log — son 24h timeline.
const HISTORY_KEY = "position-alerts-history";
const HISTORY_MAX = 50;        // En fazla 50 alert sakla (FIFO trim)
const HISTORY_TTL_HOURS = 24;  // 24h içindeki alert'leri göster

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

// Paket 168 (26 May 2026): Alert history persistence — Dashboard timeline card için
export interface AlertHistoryEntry extends PositionAlert {
  timestamp: number;  // Date.now()
}

export function getAlertHistory(): AlertHistoryEntry[] {
  if (typeof window === "undefined") return [];
  try {
    const raw = localStorage.getItem(HISTORY_KEY);
    if (!raw) return [];
    const parsed = JSON.parse(raw) as AlertHistoryEntry[];
    if (!Array.isArray(parsed)) return [];
    // 24h dışındaki entry'leri filtrele
    const cutoff = Date.now() - HISTORY_TTL_HOURS * 3600 * 1000;
    return parsed.filter((e) => e.timestamp >= cutoff);
  } catch {
    return [];
  }
}

function appendAlertHistory(alert: PositionAlert) {
  if (typeof window === "undefined") return;
  const history = getAlertHistory();
  // Aynı ID varsa skip (alert.id zaten günlük UUID)
  if (history.some((h) => h.id === alert.id)) return;
  history.unshift({ ...alert, timestamp: Date.now() });
  // FIFO trim
  const trimmed = history.slice(0, HISTORY_MAX);
  localStorage.setItem(HISTORY_KEY, JSON.stringify(trimmed));
}

/**
 * Açık trade'ler + canlı quote → alert listesi hesapla.
 * Toast'ları otomatik atar (useEffect ile). Component sadece çağırır.
 */
export function usePositionAlerts(
  trades: Trade[],
  quotes: Array<{ data?: StockQuote }>
): PositionAlert[] {
  // #30 (perf): quotes dizisi her render YENİ identity -> useMemo boşa recompute
  // ediyordu. Stabil imza (symbol:price:source) ile sadece veri değişince recompute.
  const quotesKey = quotes
    .map((r) => (r.data ? `${r.data.symbol}:${r.data.price}:${r.data.source}` : "·"))
    .join("|");

  // Trade'leri sembol-quote ile eşle
  const alerts = useMemo<PositionAlert[]>(() => {
    if (!trades.length) return [];
    const quoteMap = new Map<string, number>();
    quotes.forEach((r) => {
      // #9 (Kural #28): SADECE canlı yfinance quote'tan alarm üret. MOCK fiyat
      // sahte "STOP'A DEĞDİ" (kritik — pozisyonu kapat!) uyarısı tetikleyebilir;
      // paper trading kararı için yanıltıcı. Kaynak teyit edilmeden alarm yok.
      if (r.data && r.data.source === "yfinance") {
        quoteMap.set(r.data.symbol.toUpperCase(), r.data.price);
      }
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

      // 5. FREE_TRADE — kar baslangic stop mesafesinin 2x/3x'ine ulasti -> breakeven'e tasi
      //    Mark "Think and Trade Like a Champion" Bol 2. Esikler quanfina_math.
      //    should_move_to_breakeven BIREBIR (>=3 WARNING tasi / >=2 INFO yaklasiyor; Kural #26).
      //    Yon-farkinda. Stop zaten breakeven'da/ustundeyse uyarma.
      if (t.plan_stop != null) {
        const isLong = t.invest_type !== 2;
        const stopBelowEntry = isLong
          ? t.plan_stop < t.entry_price
          : t.plan_stop > t.entry_price;
        if (stopBelowEntry) {
          const initialStopPct =
            (Math.abs(t.entry_price - t.plan_stop) / t.entry_price) * 100;
          const gainPct = isLong
            ? (price / t.entry_price - 1) * 100
            : (1 - price / t.entry_price) * 100;
          if (initialStopPct > 0 && gainPct > 0) {
            const mult = gainPct / initialStopPct;
            if (mult >= 2) {
              const hot = mult >= 3;
              result.push({
                id: `${t.id}-free_trade-${today}`,
                tradeId: t.id,
                symbol: t.symbol,
                severity: hot ? "warning" : "info",
                type: "free_trade",
                title: hot
                  ? `🟢 ${t.symbol} — FREE TRADE: STOP'U BREAKEVEN'E TAŞI`
                  : `🟢 ${t.symbol} — BREAKEVEN YAKLAŞIYOR`,
                message: hot
                  ? `Kar %${gainPct.toFixed(1)} (${mult.toFixed(
                      1
                    )}x stop mesafesi). Stop'u entry $${t.entry_price.toFixed(
                      2
                    )}'a taşı — risksiz pozisyon (Mark TTLC Böl.2 Free Trade).`
                  : `Kar %${gainPct.toFixed(1)} (${mult.toFixed(
                      1
                    )}x stop mesafesi) — 3x'te breakeven taşıma zamanı (Mark TTLC Böl.2).`,
                current_price: price,
                ref_price: t.entry_price,
              });
            }
          }
        }
      }

      // 6. SELL_HALF — kar +%20 (>=%30 gec kaldi) -> yari sat, kazan-kazan moduna gir.
      //    Mark TLSMW Bol 13. Esik quanfina_math.should_sell_half STATIK dal birebir
      //    (gain>=20 INFO / >=30=20*1.5 WARNING gec; Kural #26). RBA-dinamik esik kullanici
      //    gecmisi gerektirir -> canli alert'te statik %20 canon. Yon-farkinda.
      {
        const isLongSh = t.invest_type !== 2;
        const gainPctSh = isLongSh
          ? (price / t.entry_price - 1) * 100
          : (1 - price / t.entry_price) * 100;
        if (gainPctSh >= 20) {
          const late = gainPctSh >= 30; // 20 * 1.5 (should_sell_half "gec kaldi" dali)
          result.push({
            id: `${t.id}-sell_half-${today}`,
            tradeId: t.id,
            symbol: t.symbol,
            severity: late ? "warning" : "info",
            type: "sell_half",
            title: late
              ? `🟢 ${t.symbol} — YARI SAT (gecikti)`
              : `🟢 ${t.symbol} — YARI SAT ZAMANI`,
            message: late
              ? `Kar %${gainPctSh.toFixed(1)} — Sell Half büyük ölçüde gecikti (eşik %20). ` +
                `Mark TLSMW Böl.13: yarı sat, kalanı bedavaya sür.`
              : `Kar %${gainPctSh.toFixed(1)} — Sell Half zamanı (eşik %20). ` +
                `Mark TLSMW Böl.13: yarı sat, "kazan-kazan" moduna gir.`,
            current_price: price,
            ref_price: t.entry_price,
          });
        }
      }
    });

    return result;
    // eslint-disable-next-line react-hooks/exhaustive-deps -- quotesKey tüm quote verisini kapsar (#30)
  }, [trades, quotesKey]);

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
      // Paket 168: history log'a yaz (Dashboard timeline)
      appendAlertHistory(a);
      dismissed.add(a.id);
      updated = true;
    });
    if (updated) setDismissed(dismissed);
  }, [alerts]);

  return alerts;
}
