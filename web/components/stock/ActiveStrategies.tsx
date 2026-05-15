"use client";

import type { WatchlistRow } from "@/types/watchlist";
import { StatusBadge } from "@/components/watchlist/StatusBadge";
import { TermTooltip } from "@/components/terminology/TermTooltip";

const STRATEGY_LABELS: Record<string, string> = {
  minervini: "Minervini",
  carr: "Carr",
};

const STRATEGY_TERM_KEYS: Record<string, string> = {
  minervini: "sepa",
  carr: "carr_method",
};

const SETUP_TERM_KEYS: Record<string, string> = {
  VCP: "vcp",
  Pullback: "pullback",
  "Coiled Spring": "coiled_spring",
};

export function ActiveStrategies({
  strategies,
  symbol,
}: {
  strategies: WatchlistRow[];
  symbol: string;
}) {
  return (
    <div className="flex flex-col gap-2 h-full">
      <h3 className="text-xs font-semibold uppercase tracking-wider text-muted-foreground">
        Aktif Stratejiler
      </h3>

      {strategies.length === 0 ? (
        <div className="rounded-lg border px-4 py-3 text-sm text-muted-foreground">
          {symbol} watchlist'te yok
        </div>
      ) : (
        <div className="flex flex-col gap-2">
          {strategies.map((row, idx) => (
            <div key={idx} className="rounded-lg border px-3 py-3 flex flex-col gap-2">
              <div className="flex items-center justify-between">
                <TermTooltip termKey={STRATEGY_TERM_KEYS[row.strategy] ?? row.strategy}>
                  <span className="text-sm font-semibold">
                    {STRATEGY_LABELS[row.strategy] ?? row.strategy}
                  </span>
                </TermTooltip>
                <StatusBadge value={row.status} />
              </div>

              {row.setup_type && (
                <div className="text-xs text-muted-foreground">
                  Setup:{" "}
                  {SETUP_TERM_KEYS[row.setup_type] ? (
                    <TermTooltip termKey={SETUP_TERM_KEYS[row.setup_type]}>
                      <span>{row.setup_type}</span>
                    </TermTooltip>
                  ) : (
                    <span>{row.setup_type}</span>
                  )}
                </div>
              )}

              {row.pivot_price != null && (
                <div
                  className="text-xs text-muted-foreground"
                  style={{ fontFamily: "var(--font-jetbrains-mono, monospace)" }}
                >
                  Pivot: ${row.pivot_price.toFixed(2)}
                </div>
              )}

              {row.added_date && (
                <div className="text-xs text-muted-foreground">
                  Eklendi: {row.added_date}
                </div>
              )}
            </div>
          ))}
        </div>
      )}
    </div>
  );
}
