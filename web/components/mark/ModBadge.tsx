"use client";

import Link from "next/link";
import { Shield, AlertTriangle, Rocket, Circle, ArrowRight } from "lucide-react";
import { useTradingMode, type TradingMode } from "@/hooks/use-trading-mode";

/**
 * Paket 193 (26 May 2026): Trading Mode rozet (Vizyon KALICI İLKE #10).
 *
 * 4 mod görsel kart — Sn. Ferit sabah Dashboard açtığında hangi modda
 * çalıştığını ANINDA görür. Mark canon: disiplin içsel, sistem dışsal —
 * sistem öneri verir, kullanıcı kuralla tartışmaz.
 *
 * variant="full" — Dashboard kart (gerekçe + UX davranışı + istatistik)
 * variant="compact" — Sidebar/Header rozet (sadece ikon + label)
 */

const MODE_ICONS: Record<TradingMode, typeof Shield> = {
  defansif: Shield,
  rehab: AlertTriangle,
  agresif: Rocket,
  normal: Circle,
};

const MODE_LABELS: Record<TradingMode, string> = {
  defansif: "DEFANSİF",
  rehab: "REHAB",
  agresif: "AGRESİF",
  normal: "NORMAL",
};

export interface ModBadgeProps {
  variant?: "full" | "compact";
}

export function ModBadge({ variant = "full" }: ModBadgeProps) {
  const info = useTradingMode();
  const Icon = MODE_ICONS[info.mode];
  const label = MODE_LABELS[info.mode];

  if (variant === "compact") {
    return (
      <span
        className="inline-flex items-center gap-1.5 px-2 py-1 rounded-md text-xs font-semibold border"
        style={{
          borderColor: info.color,
          color: info.color,
          background: `color-mix(in srgb, ${info.color} 10%, transparent)`,
        }}
        title={info.reason}
      >
        <Icon size={12} />
        MOD: {label}
      </span>
    );
  }

  // Full variant
  return (
    <Link
      href="/risk-yonetimi"
      className="rounded-lg border bg-card p-4 flex flex-col gap-3 hover:bg-accent hover:border-accent transition-colors group"
      style={{
        borderColor: info.mode !== "normal" ? `${info.color}55` : "var(--border)",
      }}
    >
      <div className="flex items-center justify-between">
        <div className="flex items-center gap-2">
          <Icon size={14} style={{ color: info.color }} />
          <h3 className="text-xs font-semibold tracking-wider uppercase text-muted-foreground">
            Trade Modu
          </h3>
        </div>
        <ArrowRight size={11} className="opacity-0 group-hover:opacity-60 transition-opacity" />
      </div>

      {/* Ana mod rozeti */}
      <div className="flex items-baseline gap-3">
        <span aria-hidden="true" className="text-2xl leading-none">
          {info.emoji}
        </span>
        <span
          className="text-xl font-bold tracking-tight"
          style={{ color: info.color }}
        >
          {label}
        </span>
        <span className="text-xs text-muted-foreground tabular-nums">
          %{(info.recommendedSizingPct * 100).toFixed(0) === "100" ? "1" : (info.recommendedSizingPct).toFixed(1).replace(".0", "")} R
        </span>
      </div>

      {/* Gerekçe */}
      <p className="text-xs italic leading-relaxed" style={{ color: info.color }}>
        {info.reason}
      </p>

      {/* UX davranış kuralı */}
      <p className="text-xs text-muted-foreground border-l-2 pl-2"
        style={{ borderLeftColor: `${info.color}88` }}
      >
        {info.uiBehavior}
      </p>

      {/* İstatistik footer */}
      <div className="flex items-center gap-3 text-[10px] text-muted-foreground pt-1 border-t border-muted-foreground/10">
        {info.consecutiveWins > 0 && (
          <span style={{ color: "var(--mtp-excellent)" }}>
            🟢 {info.consecutiveWins} ardışık kazanç
          </span>
        )}
        {info.consecutiveLosses > 0 && (
          <span style={{ color: "var(--mtp-danger)" }}>
            🔴 {info.consecutiveLosses} ardışık kayıp
          </span>
        )}
        {info.consecutiveWins === 0 && info.consecutiveLosses === 0 && (
          <span>0 streak (ilk trade veya nötr)</span>
        )}
        <span className="ml-auto">{info.totalClosedTrades} toplam kapalı</span>
      </div>
    </Link>
  );
}
