"use client";

import { BarChart3, Wind, Minus, ArrowUp, AlertOctagon } from "lucide-react";
import { useRelativeVolume, type RelativeVolumeCategory } from "@/hooks/use-relative-volume";

/**
 * KARAR #733 alt-paket (Paket 133, 26 May 2026): RelativeVolumeCard /hisse detay.
 * Mark TLSMW Ch 6 "Volume tells the story" canon UI tezahürü.
 *
 * 5 kategori:
 * - DRYING (<0.7x): Mark VCP final daralma adayı
 * - QUIET (0.7-1.0): Sıradan
 * - NORMAL (1.0-1.5): Sağlıklı katılım
 * - HIGH (1.5-2.5): Mark pivot kırılım eşiği
 * - SURGE (>2.5x): Climax/distribution uyarı
 */

interface CategoryMeta {
  label: string;
  color: string;
  bg: string;
  icon: React.ReactNode;
}

const CATEGORY_META: Record<RelativeVolumeCategory, CategoryMeta> = {
  DRYING: {
    label: "Kuruyor",
    color: "var(--mtp-good, #4B9CD3)",
    bg: "rgba(75,156,211,0.10)",
    icon: <Wind size={16} />,
  },
  QUIET: {
    label: "Sessiz",
    color: "var(--muted-foreground)",
    bg: "rgba(128,128,128,0.06)",
    icon: <Minus size={16} />,
  },
  NORMAL: {
    label: "Normal",
    color: "var(--mtp-excellent)",
    bg: "rgba(40,167,69,0.08)",
    icon: <Minus size={16} />,
  },
  HIGH: {
    label: "Yüksek",
    color: "#F59E0B",
    bg: "rgba(245,158,11,0.12)",
    icon: <ArrowUp size={16} />,
  },
  SURGE: {
    label: "Patlama",
    color: "var(--mtp-danger)",
    bg: "rgba(220,53,69,0.12)",
    icon: <AlertOctagon size={16} />,
  },
};

function fmtVol(v: number | null): string {
  if (v == null) return "—";
  if (v >= 1_000_000_000) return `${(v / 1_000_000_000).toFixed(2)}B`;
  if (v >= 1_000_000) return `${(v / 1_000_000).toFixed(1)}M`;
  if (v >= 1_000) return `${(v / 1_000).toFixed(0)}K`;
  return String(v);
}

export function RelativeVolumeCard({ symbol }: { symbol: string }) {
  const { data, isLoading, isError } = useRelativeVolume(symbol);

  if (isLoading) {
    return (
      <div className="rounded-lg border p-3 text-xs text-muted-foreground">
        Hacim oranı yükleniyor...
      </div>
    );
  }

  if (isError || !data || !data.category) return null;

  const meta = CATEGORY_META[data.category];

  return (
    <div
      className="rounded-lg border p-3 flex flex-col gap-2"
      style={{ background: meta.bg, borderColor: `${meta.color}55` }}
    >
      {/* Başlık */}
      <div className="flex items-center gap-2">
        <BarChart3 size={16} style={{ color: meta.color }} />
        <h3 className="text-xs font-semibold flex-1">
          Hacim Oranı
          <span className="ml-1.5 text-[10px] font-normal text-muted-foreground italic">
            (Mark TLSMW Ch 6)
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

      {/* Büyük rel_vol */}
      <div
        className="flex items-baseline gap-2 px-2 py-1.5 rounded bg-background/40 border-l-2"
        style={{ borderLeftColor: meta.color }}
      >
        <span className="text-[10px] text-muted-foreground uppercase tracking-wider">
          rel_vol
        </span>
        <span
          className="text-2xl font-mono font-bold tabular-nums"
          style={{ color: meta.color, fontFamily: "var(--font-jetbrains-mono, monospace)" }}
        >
          {data.rel_vol != null ? `${data.rel_vol.toFixed(2)}x` : "—"}
        </span>
        <span className="text-[10px] text-muted-foreground italic ml-auto">
          50g ortalama
        </span>
      </div>

      {/* Mark felsefe */}
      <p
        className="text-xs italic leading-relaxed px-2 py-1"
        style={{ color: meta.color }}
      >
        {data.mark_says}
      </p>

      {/* Bugün vs ortalama */}
      <div className="grid grid-cols-2 gap-2 text-xs pt-1 border-t border-muted-foreground/15">
        <div className="flex flex-col">
          <span className="text-[10px] text-muted-foreground uppercase tracking-wider">
            Bugün
          </span>
          <span
            className="font-mono font-semibold tabular-nums"
            style={{ fontFamily: "var(--font-jetbrains-mono, monospace)" }}
          >
            {fmtVol(data.today_volume)}
          </span>
        </div>
        <div className="flex flex-col">
          <span className="text-[10px] text-muted-foreground uppercase tracking-wider">
            50g Ort
          </span>
          <span
            className="font-mono font-semibold tabular-nums"
            style={{ fontFamily: "var(--font-jetbrains-mono, monospace)" }}
          >
            {fmtVol(data.avg_volume)}
          </span>
        </div>
      </div>
    </div>
  );
}
