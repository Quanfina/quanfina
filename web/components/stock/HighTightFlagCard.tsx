"use client";

import { Flag, CheckCircle2, ThumbsUp, AlertTriangle } from "lucide-react";
import { useHighTightFlag, type HighTightFlagQuality } from "@/hooks/use-high-tight-flag";

/**
 * Paket 462 (11 Haz 2026): HighTightFlagCard /hisse detay.
 * O'Neil High Tight Flag (= Minervini Power Play) — flagpole >=%100 (<=8hf) + tight flag.
 *
 * Derin internet arastirma (IBD/MarketSmith/Minervini s.255) ile esikler kaynak-atifli:
 *   flagpole >=%100 (tipik 100-120) <=8 hf | flag %10-25 (max %25) 3-6 hf | pivot = flag zirvesi
 *   En guclu + EN NADIR + EN RISKLI patern (O'Neil "strongest but very risky").
 *
 * NONE / detected yoksa kart gizlenir (HTF cok nadir — cogu hissede gizli kalir).
 */

interface QualityMeta {
  label: string;
  color: string;
  bg: string;
  icon: React.ReactNode;
}

const QUALITY_META: Record<Exclude<HighTightFlagQuality, "NONE">, QualityMeta> = {
  EXCELLENT: {
    label: "Tam Canon",
    color: "var(--mtp-excellent)",
    bg: "rgba(40,167,69,0.12)",
    icon: <CheckCircle2 size={16} />,
  },
  GOOD: {
    label: "Geçerli",
    color: "var(--mtp-neutral)",
    bg: "rgba(75,156,211,0.10)",
    icon: <ThumbsUp size={16} />,
  },
  MARGINAL: {
    label: "Kusurlu",
    color: "#F59E0B",
    bg: "rgba(245,158,11,0.10)",
    icon: <AlertTriangle size={16} />,
  },
};

function Metric({ label, value, ok }: { label: string; value: string; ok?: boolean }) {
  return (
    <div className="flex flex-col items-center gap-0.5 rounded px-1 py-1 bg-background/40">
      <span className="text-[9px] text-muted-foreground uppercase tracking-wider">{label}</span>
      <span
        className="font-mono font-bold tabular-nums text-[11px]"
        style={{
          color:
            ok === undefined
              ? "var(--foreground)"
              : ok
              ? "var(--mtp-excellent)"
              : "var(--mtp-danger)",
        }}
      >
        {value}
      </span>
    </div>
  );
}

export function HighTightFlagCard({ symbol }: { symbol: string }) {
  const { data, isLoading, isError } = useHighTightFlag(symbol);

  if (isLoading) {
    return (
      <div className="rounded-lg border p-3 text-xs text-muted-foreground">
        High Tight Flag taranıyor...
      </div>
    );
  }

  // Yapı yoksa (NONE) / hata -> kart gizle (HTF cok nadir, pole <%100 ise zaten NONE)
  if (isError || !data || !data.quality || data.quality === "NONE") return null;

  const meta = QUALITY_META[data.quality];
  const fmtPct = (v: number | null) => (v == null ? "—" : `%${v.toFixed(0)}`);

  return (
    <div
      className="rounded-lg border p-3 flex flex-col gap-2"
      style={{ background: meta.bg, borderColor: `${meta.color}55` }}
    >
      {/* Başlık */}
      <div className="flex items-center gap-2">
        <Flag size={16} style={{ color: meta.color }} />
        <h3 className="text-xs font-semibold flex-1">
          High Tight Flag
          <span className="ml-1.5 text-[10px] font-normal text-muted-foreground italic">
            (O&apos;Neil — nadir/güçlü)
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

      {/* Pivot (flag zirvesi = buy point) */}
      {data.pivot_price != null && (
        <div
          className="flex items-baseline gap-2 px-2 py-1.5 rounded bg-background/40 border-l-2"
          style={{ borderLeftColor: meta.color }}
        >
          <span className="text-[10px] text-muted-foreground uppercase tracking-wider">
            Pivot (Bayrak Zirvesi)
          </span>
          <span
            className="text-2xl font-mono font-bold tabular-nums"
            style={{ color: meta.color, fontFamily: "var(--font-jetbrains-mono, monospace)" }}
          >
            ${data.pivot_price.toFixed(2)}
          </span>
          <span className="text-[10px] text-muted-foreground italic">
            +%40 hacim kırılımda AL
          </span>
        </div>
      )}

      {/* O'Neil/Minervini felsefe */}
      <p className="text-xs italic leading-relaxed px-2 py-1" style={{ color: meta.color }}>
        {data.mark_says}
      </p>

      {/* Ölçülen parametreler: flagpole + flag */}
      <div className="grid grid-cols-3 gap-1 pt-1 border-t border-muted-foreground/15">
        <Metric
          label="Flagpole"
          value={
            data.flagpole_pct != null
              ? `+${data.flagpole_pct.toFixed(0)}%${data.flagpole_weeks != null ? ` / ${data.flagpole_weeks}hf` : ""}`
              : "—"
          }
          ok={data.flagpole_pct != null && data.flagpole_pct >= 100}
        />
        <Metric
          label="Bayrak"
          value={fmtPct(data.flag_depth_pct)}
          ok={data.flag_depth_pct != null && data.flag_depth_pct <= 25}
        />
        <Metric
          label="Bayrak Süre"
          value={data.flag_duration_days != null ? `${(data.flag_duration_days / 5).toFixed(1)}hf` : "—"}
          ok={data.flag_duration_days != null && data.flag_duration_days <= 30}
        />
      </div>

      {/* Risk uyarisi — O'Neil "strongest but very risky" */}
      <div className="text-[10px] px-2 text-muted-foreground italic">
        ⚠️ Nadir + yüksek riskli patern (O&apos;Neil) — pozisyon disiplini şart.
      </div>

      {/* Kusurlar (MARGINAL) */}
      {data.faults.length > 0 && (
        <div
          className="text-[10px] px-2 py-1 rounded flex flex-col gap-0.5"
          style={{ background: "rgba(245,158,11,0.10)", color: "#F59E0B" }}
        >
          <span className="font-semibold">⚠️ Kusur(lar):</span>
          {data.faults.map((f, i) => (
            <span key={i}>• {f}</span>
          ))}
        </div>
      )}
    </div>
  );
}
