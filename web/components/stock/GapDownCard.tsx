"use client";

import { TrendingDown, Minus } from "lucide-react";
import { useGapDown } from "@/hooks/use-gap-down";
import { fmtUsd } from "@/lib/format-currency";
import { CarrPaperTradeButton } from "@/components/stock/CarrPaperTradeButton";
import type { InitialData } from "@/components/journal/AddTradeDialog";

/**
 * Paket 520 (18 Haz 2026): Carr Gap Down paneli (/hisse/[symbol]).
 *
 * Carr 2.baskı "Gap Down" (s.273-274) — uzun-ralli-sonu reversal SHORT ADAYI. GÖSTERGE YOK
 * (s.275 pure price, sadece 50MA). high[bugün]<low[dün]×0.99 unfilled gap + kırmızı + 50MA
 * 3 ay uptrend. ENTRY=close (market emri, s.270). HABER TEYİDİ şart (s.272) → quality=CANDIDATE.
 * SHORT → kırmızı/danger ton (Quanfina long-biased; bilgi amaçlı).
 */
export function GapDownCard({ symbol, onPaperTrade }: { symbol: string; onPaperTrade?: (d: InitialData) => void }) {
  const { data, isLoading, isError } = useGapDown(symbol);

  if (isLoading) {
    return (
      <div className="rounded-lg border p-3 text-xs text-muted-foreground">
        Gap Down yükleniyor...
      </div>
    );
  }
  if (isError || !data) return null;

  const color = data.detected ? "var(--mtp-danger)" : "var(--mtp-neutral)";
  const bg = data.detected ? "rgba(220, 53, 69, 0.10)" : "rgba(75, 156, 211, 0.08)";

  return (
    <div
      className="rounded-lg border p-3 flex flex-col gap-2"
      style={{ background: bg, borderColor: `${color}55` }}
      data-testid="hisse-gap-down-card"
    >
      <h3 className="text-xs font-semibold flex items-center gap-1.5">
        <span style={{ color }}>
          {data.detected ? <TrendingDown size={16} /> : <Minus size={16} />}
        </span>
        Gap Down
        <span className="text-[10px] font-normal text-muted-foreground italic">
          (Carr 2.baskı — SHORT reversal)
        </span>
      </h3>

      {data.is_mock && (
        <div
          className="text-[10px] px-2 py-1 rounded flex items-center gap-1"
          style={{ background: "rgba(245,158,11,0.10)", color: "#92400E" }}
          role="status"
          data-testid="hisse-gap-down-mock"
        >
          <span aria-hidden="true">🟡</span> Sentetik / &lt;110 bar veri — paper trade için
          güvenilmez.
        </div>
      )}

      <p className="text-sm font-bold" style={{ color }}>
        {data.detected ? "🔴 SHORT ADAYI (haber teyidi şart)" : "Aday yok"}
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
            <Metric label="Giriş (close)" value={fmtUsd(data.entry)} />
            <Metric label="Stop (%6 üstte)" value={fmtUsd(data.stop)} />
            <Metric label="Hedef (2R aşağı)" value={fmtUsd(data.target)} />
          </div>
          {data.eyeball_checks.length > 0 && (
            <ul className="text-[10px] text-muted-foreground list-disc pl-4 space-y-0.5 pt-1">
              {data.eyeball_checks.map((c, i) => (
                <li key={i}>{c}</li>
              ))}
            </ul>
          )}
          <p className="text-[10px] text-muted-foreground">
            Risk %{data.risk_pct} · R:R 1:{data.rr} · %{data.gap_pct} gap · Bullish/Range piyasa (s.266)
          </p>
          <CarrPaperTradeButton
            data={{ symbol, strategy: "carr", setup_type: "gap_down", invest_type: 2,
              entry_price: data.entry ?? undefined, plan_stop: data.stop ?? undefined,
              plan_target: data.target ?? undefined,
              plan_entry_trigger: `Carr Gap Down SHORT (s.273) — ralli-sonu unfilled gap %${data.gap_pct}; HABER TEYİDİ şart (s.272)` }}
            onPaperTrade={onPaperTrade}
            isMock={data.is_mock}
          />
        </>
      ) : (
        <div className="grid grid-cols-2 gap-2 text-xs pt-1 border-t border-muted-foreground/15">
          <Metric label="SMA50" value={fmtUsd(data.sma50)} />
          <Metric label="Gap %" value={data.gap_pct != null ? `${data.gap_pct}` : "—"} />
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
