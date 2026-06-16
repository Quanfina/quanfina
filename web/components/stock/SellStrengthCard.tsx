"use client";

import { TrendingDown, AlertTriangle, ShieldCheck, ArrowDownCircle } from "lucide-react";
import { useSellStrength, type SellCategory } from "@/hooks/use-sell-strength";

/**
 * Paket 475 (16 Haz 2026): SellStrengthCard — /hisse Mark satış sinyalleri (KARAR ADAY #976).
 *
 * Açık pozisyonda "ne zaman SAT/azalt" — skorlu kategori (HOLD/WATCH/REDUCE/SELL) + sinyal
 * listesi (defansif ▾ kırmızı / offensive ▴ yeşil). Canon: Mark TLSMW/TTLC/Mindset (NotebookLM
 * Quanfina Minervini, Kural #26). Pozisyon-spesifik sinyaller (Hard Stop/Sell Half) entry/avg_gain
 * gerektirir — /hisse'de market-state sinyaller (MA kırılım, Outside Day, Climax) görünür.
 */

const META: Record<SellCategory, { label: string; color: string; icon: React.ReactNode }> = {
  SELL:   { label: "SAT",   color: "var(--mtp-danger)",    icon: <ArrowDownCircle size={15} /> },
  REDUCE: { label: "AZALT", color: "#F59E0B",              icon: <TrendingDown size={15} /> },
  WATCH:  { label: "İZLE",  color: "var(--mtp-neutral)",   icon: <AlertTriangle size={15} /> },
  HOLD:   { label: "TUT",   color: "var(--mtp-excellent)", icon: <ShieldCheck size={15} /> },
};

export function SellStrengthCard({ symbol }: { symbol: string }) {
  const { data, isLoading, isError } = useSellStrength(symbol);

  if (isLoading) {
    return (
      <div className="rounded-lg border p-2.5 text-xs text-muted-foreground">
        Satış sinyalleri taranıyor...
      </div>
    );
  }
  if (isError || !data || !data.category) return null;

  const meta = META[data.category];

  return (
    <div
      className="rounded-lg border p-3 flex flex-col gap-2"
      style={{ background: `${meta.color}14`, borderColor: `${meta.color}55` }}
    >
      <div className="flex items-center gap-2">
        <span style={{ color: meta.color }}>{meta.icon}</span>
        <h3 className="text-xs font-semibold flex-1">
          Satış Gücü
          <span className="ml-1.5 text-[10px] font-normal text-muted-foreground italic">
            (Mark TLSMW/TTLC)
          </span>
        </h3>
        <span
          className="inline-flex items-center gap-1 text-[10px] font-bold px-1.5 py-0.5 rounded uppercase tracking-wider"
          style={{ background: meta.color, color: "#fff" }}
        >
          {meta.label} · {data.sell_strength}/10
        </span>
      </div>

      <p
        className="text-xs italic leading-relaxed px-2 py-1.5 rounded bg-background/40 border-l-2"
        style={{ borderLeftColor: meta.color, color: meta.color }}
      >
        {data.mark_says}
      </p>

      {data.signals.length > 0 && (
        <ul className="flex flex-col gap-1 text-[11px]">
          {data.defensive.map((s, i) => (
            <li key={`d${i}`} className="flex gap-1.5">
              <span style={{ color: "var(--mtp-danger)" }} aria-hidden="true">▾</span>
              <span className="text-muted-foreground">{s}</span>
            </li>
          ))}
          {data.offensive.map((s, i) => (
            <li key={`o${i}`} className="flex gap-1.5">
              <span style={{ color: "var(--mtp-excellent)" }} aria-hidden="true">▴</span>
              <span className="text-muted-foreground">{s}</span>
            </li>
          ))}
        </ul>
      )}
    </div>
  );
}
