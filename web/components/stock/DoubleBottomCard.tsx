"use client";

import { Waves, CheckCircle2, ThumbsUp, AlertTriangle } from "lucide-react";
import { useDoubleBottom, type DoubleBottomQuality } from "@/hooks/use-double-bottom";

/**
 * Paket 460 (11 Haz 2026): DoubleBottomCard /hisse detay.
 * O'Neil/IBD Double Bottom (W) — iki dip + orta tepe; 2. dip 1. dibi UNDERCUT eder.
 *
 * Derin internet arastirma (MarketSmith/IBD/TraderLion HIGH guven) ile esikler kaynak-atifli:
 *   min 7 hafta | undercut zorunlu (yoksa double bottom degil) | pivot = orta tepe (+10 cent)
 *   onceki advance >=%30 | derinlik <=%35 (ayida %40-50) | kirilim +%40 hacim.
 *
 * NONE / detected yoksa kart gizlenir (her hisse double bottom olusturmaz — selektif).
 */

interface QualityMeta {
  label: string;
  color: string;
  bg: string;
  icon: React.ReactNode;
}

const QUALITY_META: Record<Exclude<DoubleBottomQuality, "NONE">, QualityMeta> = {
  EXCELLENT: {
    label: "Tam Canon",
    color: "var(--mtp-excellent)",
    bg: "rgba(40,167,69,0.12)",
    icon: <CheckCircle2 size={16} />,
  },
  GOOD: {
    label: "Geçerli Baz",
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

export function DoubleBottomCard({ symbol }: { symbol: string }) {
  const { data, isLoading, isError } = useDoubleBottom(symbol);

  if (isLoading) {
    return (
      <div className="rounded-lg border p-3 text-xs text-muted-foreground">
        Double Bottom (W) taranıyor...
      </div>
    );
  }

  // Yapı yoksa (NONE) / hata -> kart gizle (undercut yoksa zaten NONE)
  if (isError || !data || !data.quality || data.quality === "NONE") return null;

  const meta = QUALITY_META[data.quality];
  const fmtPct = (v: number | null) => (v == null ? "—" : `%${v.toFixed(1)}`);
  const weeks = data.base_duration_days != null ? (data.base_duration_days / 5).toFixed(1) : null;

  return (
    <div
      className="rounded-lg border p-3 flex flex-col gap-2"
      style={{ background: meta.bg, borderColor: `${meta.color}55` }}
    >
      {/* Başlık */}
      <div className="flex items-center gap-2">
        <Waves size={16} style={{ color: meta.color }} />
        <h3 className="text-xs font-semibold flex-1">
          Double Bottom (W)
          <span className="ml-1.5 text-[10px] font-normal text-muted-foreground italic">
            (O&apos;Neil/IBD)
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

      {/* Pivot (orta tepe = buy point) */}
      {data.pivot_price != null && (
        <div
          className="flex items-baseline gap-2 px-2 py-1.5 rounded bg-background/40 border-l-2"
          style={{ borderLeftColor: meta.color }}
        >
          <span className="text-[10px] text-muted-foreground uppercase tracking-wider">
            Pivot (Orta Tepe)
          </span>
          <span
            className="text-2xl font-mono font-bold tabular-nums"
            style={{ color: meta.color, fontFamily: "var(--font-jetbrains-mono, monospace)" }}
          >
            ${data.pivot_price.toFixed(2)}
          </span>
          <span className="text-[10px] text-muted-foreground italic">
            W ortası · +%40 hacim kırılımda AL
          </span>
        </div>
      )}

      {/* Undercut görseli (1. dip -> 2. dip shakeout) */}
      {data.first_low != null && data.second_low != null && (
        <div className="flex items-center gap-2 px-2 text-[11px] font-mono tabular-nums">
          <span className="text-muted-foreground">1. dip</span>
          <span>${data.first_low.toFixed(2)}</span>
          <span className="text-muted-foreground">→</span>
          <span className="text-muted-foreground">2. dip</span>
          <span style={{ color: data.undercut ? "var(--mtp-excellent)" : "var(--mtp-danger)" }}>
            ${data.second_low.toFixed(2)} {data.undercut ? "✓ undercut" : "✗"}
          </span>
        </div>
      )}

      {/* O'Neil felsefe */}
      <p className="text-xs italic leading-relaxed px-2 py-1" style={{ color: meta.color }}>
        {data.mark_says}
      </p>

      {/* Ölçülen parametreler */}
      <div className="grid grid-cols-3 gap-1 pt-1 border-t border-muted-foreground/15">
        <Metric label="Süre" value={weeks != null ? `${weeks}hf` : "—"} ok={
          data.base_duration_days != null && data.base_duration_days >= 35
        } />
        <Metric label="Derinlik" value={fmtPct(data.base_depth_pct)} ok={
          data.base_depth_pct != null && data.base_depth_pct <= 35
        } />
        <Metric label="Ön Trend" value={fmtPct(data.prior_advance_pct)} ok={
          data.prior_advance_pct != null && data.prior_advance_pct >= 30
        } />
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
