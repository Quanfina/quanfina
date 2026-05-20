"use client";

import Link from "next/link";
import { ArrowRight, Plus } from "lucide-react";
import type { Signal } from "@/types/signal";

const STATUS_LABELS: Record<string, string> = {
  buy: "Buy",
  focus: "Focus",
  on_deck: "On Deck",
  watch: "Watch",
};

const STATUS_COLORS: Record<string, string> = {
  buy: "#28A745",
  focus: "#4B9CD3",
  on_deck: "#F59E0B",
  watch: "var(--muted-foreground)",
};

const STRATEGY_LABELS: Record<string, string> = {
  minervini: "Minervini",
  carr: "Carr",
};

interface Props {
  signal: Signal;
  onTradeClick: (signal: Signal) => void;
}

// KARAR #469 (20 May 2026): Konsensus yapısı kaldırıldı.
// Her watchlist satırı = 1 kart (NVDA-Minervini ayrı, NVDA-Carr ayrı).
// ConsensusHighlight komponenti artık kullanılmıyor (Kural #18 pasif aday).
export function SignalCard({ signal, onTradeClick }: Props) {
  return (
    <div className="rounded-lg border bg-card text-card-foreground shadow-sm p-5 flex flex-col gap-4 hover:shadow-md transition-shadow">
      {/* Header: symbol + YENİ BUGÜN badge */}
      <div className="flex items-start justify-between gap-2">
        <span className="text-xl font-bold tracking-tight">{signal.symbol}</span>
        {signal.is_new_today && (
          <span
            className="text-xs font-semibold px-2 py-0.5 rounded-full shrink-0"
            style={{ background: "#28A745", color: "#fff" }}
          >
            YENİ BUGÜN
          </span>
        )}
      </div>

      {/* Body: tek strateji + statü + setup */}
      <div className="flex items-center gap-2 flex-wrap">
        <span className="text-sm font-medium text-foreground">
          {STRATEGY_LABELS[signal.strategy] ?? signal.strategy}
        </span>
        <span className="text-xs text-muted-foreground">·</span>
        <span
          className="text-sm font-semibold"
          style={{ color: STATUS_COLORS[signal.status] ?? "inherit" }}
        >
          {STATUS_LABELS[signal.status] ?? signal.status}
        </span>
        {signal.setup_type && (
          <>
            <span className="text-xs text-muted-foreground">·</span>
            <span className="text-sm text-muted-foreground">{signal.setup_type}</span>
          </>
        )}
      </div>

      {/* Price + RS */}
      <div className="flex items-center gap-3 text-sm">
        <span
          className="font-semibold tabular-nums"
          style={{ fontFamily: "var(--font-jetbrains-mono, monospace)" }}
        >
          ${signal.price.toFixed(2)}
        </span>
        <span className="text-muted-foreground">·</span>
        <span className="text-xs text-muted-foreground">
          RS{" "}
          <span className="font-semibold text-foreground">
            {Math.round(signal.rs_rating)}
          </span>
        </span>
      </div>

      {/* Actions */}
      <div className="flex items-center gap-2 pt-1">
        <Link
          href={`/hisse/${signal.symbol}`}
          className="flex-1 inline-flex items-center justify-center gap-1 h-8 rounded-md border border-input bg-background px-3 text-xs font-medium hover:bg-accent hover:text-accent-foreground transition-colors"
        >
          Hisse Detayı
          <ArrowRight size={12} />
        </Link>
        <button
          type="button"
          onClick={() => onTradeClick(signal)}
          className="flex-1 inline-flex items-center justify-center gap-1 h-8 rounded-md bg-primary text-primary-foreground px-3 text-xs font-medium hover:bg-primary/90 transition-colors"
        >
          <Plus size={12} />
          Trade Aç
        </button>
      </div>
    </div>
  );
}
