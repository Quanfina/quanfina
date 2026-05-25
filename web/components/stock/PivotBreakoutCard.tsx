"use client";

import { Target, CheckCircle2, AlertTriangle, Eye, XCircle } from "lucide-react";
import { usePivotBreakout, type PivotBreakoutStatus } from "@/hooks/use-pivot-breakout";
import { fmtUsd } from "@/lib/format-currency";

/**
 * KARAR #733 alt-paket (Paket 72): Pivot Breakout detay paneli /hisse/[symbol].
 * Mark TLSMW Ch 10 + O'Neil CANSLIM canon UI tezahürü.
 *
 * 4 durum:
 * - CONFIRMED: 🟢 AL Sinyali (yeşil + hacim teyitli)
 * - WEAK: 🟡 Zayıf Kırılım (sarı + hacim eksik)
 * - NEAR_PIVOT: 🔵 Yakın İzleme (mavi)
 * - BELOW_PIVOT: 🔴 Pivot Altı (kırmızı)
 */

interface StatusMeta {
  emoji: string;
  label: string;
  color: string;
  bg: string;
  icon: React.ReactNode;
}

const STATUS_META: Record<PivotBreakoutStatus, StatusMeta> = {
  CONFIRMED: {
    emoji: "✓",
    label: "AL Sinyali",
    color: "var(--mtp-excellent)",
    bg: "rgba(40,167,69,0.10)",
    icon: <CheckCircle2 size={16} />,
  },
  WEAK: {
    emoji: "⚠️",
    label: "Zayıf Kırılım",
    color: "#F59E0B",
    bg: "rgba(245,158,11,0.10)",
    icon: <AlertTriangle size={16} />,
  },
  NEAR_PIVOT: {
    emoji: "⏳",
    label: "Yakın İzleme",
    color: "var(--mtp-good, #4B9CD3)",
    bg: "rgba(75,156,211,0.10)",
    icon: <Eye size={16} />,
  },
  BELOW_PIVOT: {
    emoji: "○",
    label: "Pivot Altı",
    color: "var(--mtp-danger)",
    bg: "rgba(220,53,69,0.06)",
    icon: <XCircle size={16} />,
  },
};

export function PivotBreakoutCard({ symbol }: { symbol: string }) {
  const { data, isLoading, isError } = usePivotBreakout(symbol);

  if (isLoading) {
    return (
      <div className="rounded-lg border p-3 text-xs text-muted-foreground">
        Pivot Breakout yükleniyor...
      </div>
    );
  }

  if (isError || !data || !data.status) return null;

  const meta = STATUS_META[data.status];

  return (
    <div
      className="rounded-lg border p-3 flex flex-col gap-2"
      style={{ background: meta.bg, borderColor: `${meta.color}55` }}
    >
      {/* Başlık */}
      <div className="flex items-center gap-2">
        <Target size={16} style={{ color: meta.color }} />
        <h3 className="text-xs font-semibold flex-1">
          Pivot Breakout
          <span className="ml-1.5 text-[10px] font-normal text-muted-foreground italic">
            (Mark TLSMW Ch 10)
          </span>
        </h3>
        <span
          className="inline-flex items-center gap-1 text-[10px] font-bold px-1.5 py-0.5 rounded uppercase tracking-wider"
          style={{ background: meta.color, color: "#fff" }}
        >
          {meta.icon}
          {meta.label}
        </span>
      </div>

      {/* Mark felsefe */}
      <p
        className="text-xs italic leading-relaxed px-2 py-1.5 rounded bg-background/40 border-l-2"
        style={{ borderLeftColor: meta.color, color: meta.color }}
      >
        {data.mark_says}
      </p>

      {/* Metrikler */}
      <div className="grid grid-cols-3 gap-2 text-xs pt-1 border-t border-muted-foreground/15">
        <div className="flex flex-col">
          <span className="text-[10px] text-muted-foreground uppercase tracking-wider">
            Pivot
          </span>
          <span
            className="font-mono font-semibold tabular-nums"
            style={{ fontFamily: "var(--font-jetbrains-mono, monospace)" }}
          >
            {fmtUsd(data.pivot_price)}
          </span>
        </div>
        <div className="flex flex-col">
          <span className="text-[10px] text-muted-foreground uppercase tracking-wider">
            Sapma
          </span>
          <span
            className="font-mono font-semibold tabular-nums"
            style={{
              color:
                data.breakout_pct == null
                  ? "var(--muted-foreground)"
                  : data.breakout_pct > 0
                  ? "var(--mtp-excellent)"
                  : "var(--mtp-danger)",
            }}
          >
            {data.breakout_pct != null
              ? `${data.breakout_pct > 0 ? "+" : ""}${data.breakout_pct.toFixed(2)}%`
              : "—"}
          </span>
        </div>
        <div className="flex flex-col">
          <span className="text-[10px] text-muted-foreground uppercase tracking-wider">
            Hacim
          </span>
          <span
            className="font-mono font-semibold tabular-nums"
            style={{
              color: data.volume_confirmed
                ? "var(--mtp-excellent)"
                : "var(--mtp-danger)",
            }}
            title={
              data.volume_confirmed
                ? "Hacim teyit ✓ (50g ort ≥1.5x)"
                : "Hacim eksik ⚠️ (50g ort <1.5x)"
            }
          >
            {data.volume_multiplier != null
              ? `${data.volume_multiplier.toFixed(1)}x`
              : "—"}
          </span>
        </div>
      </div>
    </div>
  );
}
