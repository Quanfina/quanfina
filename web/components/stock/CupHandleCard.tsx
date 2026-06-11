"use client";

import { Coffee, CheckCircle2, ThumbsUp, AlertTriangle } from "lucide-react";
import { useCupHandle, type CupHandleQuality } from "@/hooks/use-cup-handle";

/**
 * Paket 456 (11 Haz 2026): CupHandleCard /hisse detay.
 * O'Neil CAN SLIM "How to Make Money in Stocks" Bol.15 cup-with-handle base.
 *
 * Uc-kaynak danisma (NotebookLM O'Neil + Minervini x2 + IBD) ile esikler kitap-birebir:
 *   prior +%30 (s.165) | kupa %12-33 (s.162-163) | kupa 7-65 hafta (s.162)
 *   kupa U sekil, V red (s.163) | kulp <=%15 (s.164,178) | kulp ust yari + 200MA (s.163-164)
 *   shakeout sart (s.163-164) | upward-wedging = flaw (s.164,178) | pivot = kulp zirvesi.
 *
 * NONE / detected yoksa kart gizlenir (her hisse cup-with-handle olusturmaz).
 */

interface QualityMeta {
  label: string;
  color: string;
  bg: string;
  icon: React.ReactNode;
}

const QUALITY_META: Record<Exclude<CupHandleQuality, "NONE">, QualityMeta> = {
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

export function CupHandleCard({ symbol }: { symbol: string }) {
  const { data, isLoading, isError } = useCupHandle(symbol);

  if (isLoading) {
    return (
      <div className="rounded-lg border p-3 text-xs text-muted-foreground">
        Cup-with-Handle taranıyor...
      </div>
    );
  }

  // Yapı yoksa (NONE) veya hata -> kart gizle (her hisse cup-handle olusturmaz)
  if (isError || !data || !data.quality || data.quality === "NONE") return null;

  const meta = QUALITY_META[data.quality];
  const fmtPct = (v: number | null) => (v == null ? "—" : `%${v.toFixed(1)}`);

  return (
    <div
      className="rounded-lg border p-3 flex flex-col gap-2"
      style={{ background: meta.bg, borderColor: `${meta.color}55` }}
    >
      {/* Başlık */}
      <div className="flex items-center gap-2">
        <Coffee size={16} style={{ color: meta.color }} />
        <h3 className="text-xs font-semibold flex-1">
          Cup-with-Handle
          <span className="ml-1.5 text-[10px] font-normal text-muted-foreground italic">
            (O&apos;Neil CAN SLIM, kitap s.162-178)
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

      {/* Pivot (buy point) */}
      {data.pivot_price != null && (
        <div
          className="flex items-baseline gap-2 px-2 py-1.5 rounded bg-background/40 border-l-2"
          style={{ borderLeftColor: meta.color }}
        >
          <span className="text-[10px] text-muted-foreground uppercase tracking-wider">
            Pivot (Buy Point)
          </span>
          <span
            className="text-2xl font-mono font-bold tabular-nums"
            style={{ color: meta.color, fontFamily: "var(--font-jetbrains-mono, monospace)" }}
          >
            ${data.pivot_price.toFixed(2)}
          </span>
          <span className="text-[10px] text-muted-foreground italic">
            kulp zirvesi · +%50 hacim kırılımda AL
          </span>
        </div>
      )}

      {/* Mark/O'Neil felsefe */}
      <p className="text-xs italic leading-relaxed px-2 py-1" style={{ color: meta.color }}>
        {data.mark_says}
      </p>

      {/* Ölçülen parametreler */}
      <div className="grid grid-cols-3 sm:grid-cols-5 gap-1 pt-1 border-t border-muted-foreground/15">
        <Metric label="Kupa" value={fmtPct(data.cup_depth_pct)} ok={
          data.cup_depth_pct != null && data.cup_depth_pct >= 12 && data.cup_depth_pct <= 33
        } />
        <Metric label="Süre" value={data.cup_duration_days != null ? `${data.cup_duration_days}g` : "—"} />
        <Metric label="Kulp" value={fmtPct(data.handle_depth_pct)} ok={
          data.handle_depth_pct != null && data.handle_depth_pct <= 15
        } />
        <Metric label="Üst Yarı" value={data.handle_in_upper_half ? "✓" : "✗"} ok={data.handle_in_upper_half} />
        <Metric label="Shakeout" value={data.shakeout ? "✓" : "✗"} ok={data.shakeout} />
      </div>

      {/* Önceki trend */}
      {data.prior_uptrend_pct != null && (
        <div className="text-[10px] text-muted-foreground px-2">
          Önceki trend: <span className="font-mono tabular-nums">{fmtPct(data.prior_uptrend_pct)}</span>
          <span className="italic"> (O&apos;Neil s.165: min %30)</span>
        </div>
      )}

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
