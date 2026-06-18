"use client";

import { Zap, Minus } from "lucide-react";
import { useCoiledSpring } from "@/hooks/use-coiled-spring";
import { fmtUsd } from "@/lib/format-currency";
import { CarrPaperTradeButton } from "@/components/stock/CarrPaperTradeButton";
import type { InitialData } from "@/components/journal/AddTradeDialog";

/**
 * Paket 510 (18 Haz 2026): Carr Coiled Spring paneli (/hisse/[symbol]).
 *
 * Carr 2.baskı "Coiled Spring" (s.250 entry + s.252-324 exit) — STRONG+WEAK_BULL+RANGE
 * trend-takip LONG ADAYI. GÖSTERGE YOK (s.248 pure pattern, sadece 20MA+50MA+trendline).
 * TIER-2 EYEBALL: tarama aday bulur, "göz kararı" şart → quality=CANDIDATE. Aday varsa
 * giriş(signal high)/stop(50MA-%2)/hedef(2R) + eyeball checklist; yoksa SMA20/SMA50 bağlamı.
 */
export function CoiledSpringCard({ symbol, onPaperTrade }: { symbol: string; onPaperTrade?: (d: InitialData) => void }) {
  const { data, isLoading, isError } = useCoiledSpring(symbol);

  if (isLoading) {
    return (
      <div className="rounded-lg border p-3 text-xs text-muted-foreground">
        Coiled Spring yükleniyor...
      </div>
    );
  }
  if (isError || !data) return null;

  // CANDIDATE (eyeball) → amber/uyarı tonu (GOOD değil — onaylı sinyal değil)
  const color = data.detected ? "var(--mtp-warning, #d97706)" : "var(--mtp-neutral)";
  const bg = data.detected ? "rgba(217,119,6,0.10)" : "rgba(75, 156, 211, 0.08)";

  return (
    <div
      className="rounded-lg border p-3 flex flex-col gap-2"
      style={{ background: bg, borderColor: `${color}55` }}
      data-testid="hisse-coiled-spring-card"
    >
      <h3 className="text-xs font-semibold flex items-center gap-1.5">
        <span style={{ color }}>
          {data.detected ? <Zap size={16} /> : <Minus size={16} />}
        </span>
        Coiled Spring
        <span className="text-[10px] font-normal text-muted-foreground italic">
          (Carr 2.baskı — Daralan Yay, TIER-2)
        </span>
      </h3>

      {data.is_mock && (
        <div
          className="text-[10px] px-2 py-1 rounded flex items-center gap-1"
          style={{ background: "rgba(245,158,11,0.10)", color: "#92400E" }}
          role="status"
          data-testid="hisse-coiled-spring-mock"
        >
          <span aria-hidden="true">🟡</span> Sentetik / &lt;60 bar veri — paper trade için
          güvenilmez.
        </div>
      )}

      <p className="text-sm font-bold" style={{ color }}>
        {data.detected ? "🟡 LONG ADAYI (göz kararı şart)" : "Aday yok"}
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
            <Metric label="Giriş (signal high)" value={fmtUsd(data.entry)} />
            <Metric label="Stop (50MA)" value={fmtUsd(data.stop)} />
            <Metric label="Hedef (2R)" value={fmtUsd(data.target)} />
          </div>
          {data.eyeball_checks.length > 0 && (
            <ul className="text-[10px] text-muted-foreground list-disc pl-4 space-y-0.5 pt-1">
              {data.eyeball_checks.map((c, i) => (
                <li key={i}>{c}</li>
              ))}
            </ul>
          )}
          <p className="text-[10px] text-muted-foreground">
            Risk %{data.risk_pct} · R:R 1:{data.rr} · Time stop YOK (trailing — Carr s.324)
          </p>
          <CarrPaperTradeButton
            data={{ symbol, strategy: "carr", setup_type: "coiled_spring",
              entry_price: data.entry ?? undefined, plan_stop: data.stop ?? undefined,
              plan_target: data.target ?? undefined,
              plan_entry_trigger: "Carr Coiled Spring (s.250) — daralan yay ADAY; göz kararı: trendline kırılımı + 50MA temassız" }}
            onPaperTrade={onPaperTrade}
            isMock={data.is_mock}
          />
        </>
      ) : (
        <div className="grid grid-cols-2 gap-2 text-xs pt-1 border-t border-muted-foreground/15">
          <Metric label="SMA20" value={fmtUsd(data.sma20)} />
          <Metric label="SMA50" value={fmtUsd(data.sma50)} />
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
