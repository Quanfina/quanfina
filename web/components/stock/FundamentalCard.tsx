"use client";

import { BarChart3, Minus } from "lucide-react";
import { useFundamental } from "@/hooks/use-fundamental";

/**
 * Paket 542 (20 Haz 2026): Fundamental paneli (/hisse/[symbol]) — #834/#855 EPS wire.
 *
 * Minervini CANSLIM "C/A" — çeyreklik EPS + satış büyümesi. Kanon: Minervini.md s.26
 * "Kâr ve Satış İvmesi %25-%30+ Q/Q" → eşik %25. SOFT score (s.1152: fundamental TEYİT,
 * teknik trend asıl gate — "EPS %200 olsa bile fiyat 200-MA altıysa listeye giremez").
 * Veri scan'den (minervini_scans EOD; çeyreklik veri gün-içi değişmez). Veri yoksa
 * "veri yok" (Kural #28: zayıf-vs-yok ayrımı — eksik veri eleme sebebi değil).
 */
export function FundamentalCard({ symbol }: { symbol: string }) {
  const { data, isLoading, isError } = useFundamental(symbol);

  if (isLoading) {
    return (
      <div className="rounded-lg border p-3 text-xs text-muted-foreground">
        Temel veri yükleniyor...
      </div>
    );
  }
  if (isError || !data) return null;

  const bothStrong = data.has_data && !!data.eps_pass && !!data.sales_pass;
  const color = !data.has_data
    ? "var(--mtp-neutral)"
    : bothStrong
      ? "var(--mtp-excellent)"
      : "var(--mtp-warning, #d97706)";
  const bg = !data.has_data
    ? "rgba(75, 156, 211, 0.08)"
    : bothStrong
      ? "rgba(40, 167, 69, 0.10)"
      : "rgba(217,119,6,0.10)";

  return (
    <div
      className="rounded-lg border p-3 flex flex-col gap-2"
      style={{ background: bg, borderColor: `${color}55` }}
      data-testid="hisse-fundamental-card"
    >
      <h3 className="text-xs font-semibold flex items-center gap-1.5">
        <span style={{ color }}>
          {data.has_data ? <BarChart3 size={16} /> : <Minus size={16} />}
        </span>
        Temel (CANSLIM C/A)
        <span className="text-[10px] font-normal text-muted-foreground italic">
          (Minervini s.26 — ≥%{data.threshold_pct.toFixed(0)} Q/Q, soft teyit)
        </span>
      </h3>

      {data.has_data ? (
        <>
          <div className="grid grid-cols-2 gap-2 text-xs pt-1 border-t border-muted-foreground/15">
            <Metric
              label="EPS Q/Q"
              value={data.eps_qoq != null ? `%${data.eps_qoq.toFixed(0)}` : "—"}
              pass={data.eps_pass}
            />
            <Metric
              label="Satış Q/Q"
              value={data.sales_qoq != null ? `%${data.sales_qoq.toFixed(0)}` : "—"}
              pass={data.sales_pass}
            />
          </div>
          <p className="text-[10px] text-muted-foreground leading-relaxed">
            {data.mark_says}
          </p>
        </>
      ) : (
        <p className="text-[10px] text-muted-foreground leading-relaxed">
          {data.mark_says}
        </p>
      )}
    </div>
  );
}

function Metric({ label, value, pass }: { label: string; value: string; pass: boolean | null }) {
  const color =
    pass == null
      ? "inherit"
      : pass
        ? "var(--mtp-excellent)"
        : "var(--mtp-danger)";
  return (
    <div className="flex flex-col">
      <span className="text-[10px] text-muted-foreground uppercase tracking-wider">{label}</span>
      <span className="font-mono font-semibold tabular-nums" style={{ color }}>
        {value} {pass == null ? "" : pass ? "✓" : "✗"}
      </span>
    </div>
  );
}
