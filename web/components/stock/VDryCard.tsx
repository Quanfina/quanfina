"use client";

import { Wind, Droplet, Droplets } from "lucide-react";
import { useVDry, type VDryClass } from "@/hooks/use-v-dry";

/**
 * P576 (21 Haz 2026): VDryCard /hisse detay — V-Dry hacim kuruması sınıflandırması.
 * Minervini TLSMW s.150 "Quiet Action" UI tezahürü. compute_v_dry_class:
 * son 5g ort. hacim / 50g ort. → STRONG (<0.50) / IDEAL (0.50-0.70) / WEAK (≥0.70).
 * Eşikler VCP kanonu (Kural #26). RelativeVolumeCard'a komşu ama farklı boyut
 * (o tek-gün spot; bu sürdürülen pencere + base-olgunluk).
 */

interface ClassMeta {
  color: string;
  bg: string;
  icon: React.ReactNode;
}

const CLASS_META: Record<VDryClass, ClassMeta> = {
  STRONG: {
    color: "var(--mtp-excellent)",
    bg: "rgba(40,167,69,0.10)",
    icon: <Droplet size={16} />,
  },
  IDEAL: {
    color: "#F59E0B",
    bg: "rgba(245,158,11,0.10)",
    icon: <Droplets size={16} />,
  },
  WEAK: {
    color: "var(--muted-foreground)",
    bg: "rgba(128,128,128,0.06)",
    icon: <Wind size={16} />,
  },
};

export function VDryCard({ symbol }: { symbol: string }) {
  const { data, isLoading, isError } = useVDry(symbol);

  if (isLoading) {
    return (
      <div className="rounded-lg border p-3 text-xs text-muted-foreground">
        Hacim kuruması yükleniyor...
      </div>
    );
  }

  if (isError || !data || !data.v_dry_class) return null;

  const meta = CLASS_META[data.v_dry_class];

  return (
    <div
      className="rounded-lg border p-3 flex flex-col gap-2"
      style={{ background: meta.bg, borderColor: `${meta.color}55` }}
    >
      {/* Başlık */}
      <div className="flex items-center gap-2">
        <Droplet size={16} style={{ color: meta.color }} />
        <h3 className="text-xs font-semibold flex-1">
          Hacim Kuruması
          <span className="ml-1.5 text-[10px] font-normal text-muted-foreground italic">
            (Minervini s.150 &quot;Quiet Action&quot;)
          </span>
        </h3>
        <span
          className="inline-flex items-center gap-1 text-[10px] font-bold px-1.5 py-0.5 rounded uppercase tracking-wider"
          style={{ background: meta.color, color: "#fff" }}
        >
          {meta.icon}
          {data.label}
        </span>
      </div>

      {/* Büyük oran */}
      <div
        className="flex items-baseline gap-2 px-2 py-1.5 rounded bg-background/40 border-l-2"
        style={{ borderLeftColor: meta.color }}
      >
        <span className="text-[10px] text-muted-foreground uppercase tracking-wider">
          5g / 50g
        </span>
        <span
          className="text-2xl font-mono font-bold tabular-nums"
          style={{ color: meta.color, fontFamily: "var(--font-jetbrains-mono, monospace)" }}
        >
          {data.vol_ratio != null ? `${(data.vol_ratio * 100).toFixed(0)}%` : "—"}
        </span>
        <span className="text-[10px] text-muted-foreground italic ml-auto">
          eşik %50 / %70
        </span>
      </div>

      {/* Açıklama */}
      <p
        className="text-xs italic leading-relaxed px-2 py-1"
        style={{ color: meta.color }}
      >
        {data.says}
      </p>
    </div>
  );
}
