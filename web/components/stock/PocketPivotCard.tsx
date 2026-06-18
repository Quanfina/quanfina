"use client";

import { Volume2, Minus } from "lucide-react";
import { usePocketPivot } from "@/hooks/use-pocket-pivot";
import { fmtUsd } from "@/lib/format-currency";

/**
 * Paket 533 (18 Haz 2026): Pocket Pivot paneli (/hisse/[symbol]).
 *
 * Minervini.md s.167 + Kacher/Morales "Trade Like an O'Neil Disciple" — bazın İÇİNDE
 * kurumsal güç dönüşü. KANON: bugün yukarı gün + hacmi son 10g en yüksek aşağı-gün
 * hacminden büyük (s.167) + Stage 2 ön-şart (s.186) + 10-DMA dönüşü (Kacher/Morales).
 * Minervini birincil AL DEĞİL — on-deck/ekleme sinyali olarak izler (s.165, 218). Bu
 * yüzden entry/stop/target + paper-trade butonu YOK (strateji setup'ı değil, güç teyidi).
 * quality GOOD (10-DMA dönüşü) / CANDIDATE (extended, göz kararı).
 */
export function PocketPivotCard({ symbol }: { symbol: string }) {
  const { data, isLoading, isError } = usePocketPivot(symbol);

  if (isLoading) {
    return (
      <div className="rounded-lg border p-3 text-xs text-muted-foreground">
        Pocket Pivot yükleniyor...
      </div>
    );
  }
  if (isError || !data) return null;

  const isGood = data.detected && data.quality === "GOOD";
  const color = data.detected
    ? isGood
      ? "var(--mtp-excellent)"
      : "var(--mtp-warning, #d97706)"
    : "var(--mtp-neutral)";
  const bg = data.detected
    ? isGood
      ? "rgba(40, 167, 69, 0.10)"
      : "rgba(217,119,6,0.10)"
    : "rgba(75, 156, 211, 0.08)";

  return (
    <div
      className="rounded-lg border p-3 flex flex-col gap-2"
      style={{ background: bg, borderColor: `${color}55` }}
      data-testid="hisse-pocket-pivot-card"
    >
      <h3 className="text-xs font-semibold flex items-center gap-1.5">
        <span style={{ color }}>
          {data.detected ? <Volume2 size={16} /> : <Minus size={16} />}
        </span>
        Pocket Pivot
        <span className="text-[10px] font-normal text-muted-foreground italic">
          (Minervini s.167 — Kacher/Morales, on-deck)
        </span>
      </h3>

      {data.is_mock && (
        <div
          className="text-[10px] px-2 py-1 rounded flex items-center gap-1"
          style={{ background: "rgba(245,158,11,0.10)", color: "#92400E" }}
          role="status"
          data-testid="hisse-pocket-pivot-mock"
        >
          <span aria-hidden="true">🟡</span> Sentetik / &lt;70 bar veri — hacim sinyali
          güvenilmez (50-DMA + 10g aşağı-hacim gerek).
        </div>
      )}

      <p className="text-sm font-bold" style={{ color }}>
        {data.detected
          ? isGood
            ? "🟢 Kurumsal güç (on-deck)"
            : "🟡 Aday (extended — göz kararı)"
          : "Sinyal yok"}
      </p>

      {data.detected ? (
        <>
          <p
            className="text-xs italic leading-relaxed px-2 py-1.5 rounded bg-background/40 border-l-2"
            style={{ borderLeftColor: color, color }}
          >
            {data.mark_says}
          </p>
          <div className="grid grid-cols-3 gap-2 text-xs pt-1 border-t border-muted-foreground/15">
            <Metric
              label="Hacim Oranı"
              value={data.volume_ratio != null ? `${data.volume_ratio}x` : "—"}
            />
            <Metric label="10-DMA" value={fmtUsd(data.sma10)} />
            <Metric label="50-DMA" value={fmtUsd(data.sma50)} />
          </div>
          <p className="text-[10px] text-muted-foreground">
            {data.off_ten_dma ? "10-DMA dönüşü ✓" : "extended (10-DMA temassız)"} · Stage 2 ✓ ·
            Minervini: birincil AL değil — ekleme/izleme sinyali (s.165, 218)
          </p>
        </>
      ) : (
        <div className="grid grid-cols-2 gap-2 text-xs pt-1 border-t border-muted-foreground/15">
          <Metric label="10-DMA" value={fmtUsd(data.sma10)} />
          <Metric label="50-DMA" value={fmtUsd(data.sma50)} />
        </div>
      )}
    </div>
  );
}

function Metric({ label, value }: { label: string; value: string }) {
  return (
    <div className="flex flex-col">
      <span className="text-[10px] text-muted-foreground uppercase tracking-wider">
        {label}
      </span>
      <span className="font-mono font-semibold tabular-nums">{value}</span>
    </div>
  );
}
