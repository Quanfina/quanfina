import { Layers } from "lucide-react";
import type { StockInfo } from "@/types/stock";

function rsBackground(rs: number): string {
  if (rs >= 90) return "color-mix(in srgb, var(--mtp-excellent) 35%, transparent)";
  if (rs >= 70) return "color-mix(in srgb, var(--mtp-excellent) 18%, transparent)";
  if (rs >= 50) return "color-mix(in srgb, var(--mtp-neutral)   18%, transparent)";
  return              "color-mix(in srgb, var(--mtp-danger)    18%, transparent)";
}

function rsColor(rs: number): string {
  if (rs >= 70) return "var(--mtp-excellent)";
  if (rs >= 50) return "var(--mtp-neutral)";
  return "var(--mtp-danger)";
}

export function StockHeader({ info }: { info: StockInfo }) {
  const isPositive = info.change_pct >= 0;
  const consensus = info.active_strategies.length;

  return (
    <div className="flex items-start justify-between gap-4 flex-wrap">
      <div className="flex flex-col gap-1">
        <div className="flex items-baseline gap-3">
          <h1
            className="text-2xl font-bold tracking-tight"
            style={{ fontFamily: "var(--font-jetbrains-mono, monospace)" }}
          >
            {info.symbol}
          </h1>
          <span className="text-lg text-muted-foreground">{info.name}</span>
        </div>
        <div className="flex items-center gap-2 text-sm text-muted-foreground flex-wrap">
          <span>{info.sector}</span>
          <span>·</span>
          <span>{info.industry}</span>
          <span>·</span>
          <span>Piyasa Değeri: {info.market_cap}</span>
        </div>
      </div>

      <div className="flex items-center gap-3 shrink-0">
        {/* Price + change */}
        <div className="text-right">
          <div
            className="text-2xl font-bold"
            style={{ fontFamily: "var(--font-jetbrains-mono, monospace)" }}
          >
            ${info.price.toFixed(2)}
          </div>
          <div
            className="text-sm"
            style={{
              fontFamily: "var(--font-jetbrains-mono, monospace)",
              color: isPositive ? "var(--mtp-excellent)" : "var(--mtp-danger)",
            }}
          >
            {isPositive ? "+" : ""}
            {info.change_pct.toFixed(2)}%
          </div>
        </div>

        {/* RS badge */}
        <div
          className="flex flex-col items-center px-3 py-2 rounded-md min-w-[52px]"
          style={{
            background: rsBackground(info.rs_rating),
            color: rsColor(info.rs_rating),
          }}
        >
          <span className="text-xs text-muted-foreground leading-tight">RS</span>
          <span
            className="text-lg font-bold leading-tight"
            style={{ fontFamily: "var(--font-jetbrains-mono, monospace)" }}
          >
            {info.rs_rating}
          </span>
        </div>

        {/* Consensus badge (only when >= 2) */}
        {consensus >= 2 && (
          <div
            className="flex flex-col items-center px-3 py-2 rounded-md min-w-[52px]"
            style={{
              background: "color-mix(in srgb, var(--mtp-excellent) 18%, transparent)",
              color: "var(--mtp-excellent)",
            }}
          >
            <span className="text-xs text-muted-foreground leading-tight">Konsensus</span>
            <span
              className="text-lg font-bold leading-tight inline-flex items-center gap-1"
              style={{ fontFamily: "var(--font-jetbrains-mono, monospace)" }}
            >
              {consensus}
              <Layers size={13} strokeWidth={2} />
            </span>
          </div>
        )}
      </div>
    </div>
  );
}
