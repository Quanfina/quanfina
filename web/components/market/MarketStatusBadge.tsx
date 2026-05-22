"use client";

import { useMarketCalendar, type MarketSession } from "@/hooks/use-market-calendar";
import { CircleDot, AlertCircle, Clock } from "lucide-react";

/**
 * ABD Borsa Durum Rozeti — Sprint 4-bis.7 (22 May 2026).
 *
 * Görsel durum:
 *   🟢 AÇIK    — regular session (9:30-16:00 ET)
 *   🟡 PRE/POST — pre-market (4:00-9:30) veya after-hours (16:00-20:00)
 *   🔴 KAPALI   — hafta sonu / tatil / gece (20:00-04:00 ET)
 *
 * Bilgi: TR + ET saatleri, sonraki açılış (TR formatı), tatil sebebi.
 * Sn. Ferit talimat: "Türkiye'de yaşadığımı unutma" — TR saati prioritised.
 */

function SessionDot({ session, is_open }: { session: MarketSession; is_open: boolean }) {
  // Renk haritası
  let color = "var(--mtp-danger)"; // closed
  if (is_open) color = "var(--mtp-excellent)"; // regular green
  else if (session === "pre_market" || session === "post_market")
    color = "var(--mtp-neutral)"; // yellow neutral

  return (
    <span
      className="inline-flex items-center justify-center w-2.5 h-2.5 rounded-full"
      style={{ backgroundColor: color, boxShadow: `0 0 6px ${color}` }}
      aria-hidden
    />
  );
}

function sessionLabel(session: MarketSession): string {
  switch (session) {
    case "regular":
      return "Açık";
    case "pre_market":
      return "Pre-Market";
    case "post_market":
      return "After-Hours";
    case "closed":
    default:
      return "Kapalı";
  }
}

/**
 * Sadece tarih kısmını TR formatına çevir ("2026-05-22 16:32 +03" → "22.05.2026 16:32")
 */
function formatTime(raw: string): string {
  const m = raw.match(/^(\d{4})-(\d{2})-(\d{2})\s+(\d{2}:\d{2})/);
  if (!m) return raw;
  return `${m[3]}.${m[2]}.${m[1]} ${m[4]}`;
}

export function MarketStatusBadge() {
  const { data, isLoading, isError } = useMarketCalendar();

  if (isLoading) {
    return (
      <div className="inline-flex items-center gap-2 px-3 py-1.5 rounded-md border bg-muted/30 text-sm text-muted-foreground">
        <Clock size={14} className="animate-pulse" />
        Yükleniyor...
      </div>
    );
  }

  if (isError || !data) {
    return (
      <div className="inline-flex items-center gap-2 px-3 py-1.5 rounded-md border text-sm"
        style={{
          borderColor: "var(--mtp-danger)",
          color: "var(--mtp-danger)",
          background: "color-mix(in srgb, var(--mtp-danger) 8%, transparent)",
        }}
      >
        <AlertCircle size={14} />
        Takvim verisi alınamadı
      </div>
    );
  }

  const label = sessionLabel(data.session);
  const isOpen = data.is_open;

  return (
    <div
      className="inline-flex flex-col gap-1 px-4 py-2.5 rounded-md border text-sm min-w-[280px]"
      style={{
        borderColor: isOpen
          ? "var(--mtp-excellent)"
          : data.session === "closed"
            ? "var(--mtp-danger)"
            : "var(--mtp-neutral)",
        background: isOpen
          ? "color-mix(in srgb, var(--mtp-excellent) 8%, transparent)"
          : data.session === "closed"
            ? "color-mix(in srgb, var(--mtp-danger) 6%, transparent)"
            : "color-mix(in srgb, var(--mtp-neutral) 8%, transparent)",
      }}
      role="status"
      aria-live="polite"
    >
      <div className="flex items-center gap-2">
        <SessionDot session={data.session} is_open={isOpen} />
        <span className="font-semibold tracking-tight">
          ABD Borsa: {label}
        </span>
        {data.is_early_close && (
          <span className="text-xs px-1.5 py-0.5 rounded bg-amber-500/20 text-amber-700 dark:text-amber-300">
            Yarı Gün
          </span>
        )}
      </div>

      {data.reason && !isOpen && (
        <div className="text-xs opacity-80 italic">
          {data.reason}
        </div>
      )}

      <div className="grid grid-cols-2 gap-x-3 gap-y-0.5 text-xs mt-1">
        <span className="text-muted-foreground">Şu an TR:</span>
        <span className="font-mono">{formatTime(data.now_tr)}</span>

        <span className="text-muted-foreground">Şu an ET:</span>
        <span className="font-mono">{formatTime(data.now_et)}</span>

        <span className="text-muted-foreground">
          {isOpen ? "Sonraki açılış:" : "Sonraki açılış (TR):"}
        </span>
        <span className="font-mono">{formatTime(data.next_open_tr)}</span>

        <span className="text-muted-foreground">Son tarama günü:</span>
        <span className="font-mono">
          {data.last_trading_day.split("-").reverse().join(".")}
        </span>
      </div>
    </div>
  );
}
