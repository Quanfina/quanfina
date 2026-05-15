"use client";

import { useMemo, useState } from "react";
import { Activity } from "lucide-react";
import { useSignals } from "@/hooks/use-signals";
import { SignalCard } from "@/components/signals/SignalCard";
import { AddTradeDialog } from "@/components/journal/AddTradeDialog";
import type { Signal } from "@/types/signal";

const SELECT =
  "h-8 rounded-md border border-input bg-background px-2 text-xs focus:outline-none focus:ring-1 focus:ring-ring";

function StatCard({ label, value }: { label: string; value: string | number }) {
  return (
    <div className="rounded-lg border bg-card px-4 py-3 flex flex-col gap-0.5">
      <span className="text-xs text-muted-foreground">{label}</span>
      <span className="text-lg font-semibold">{value}</span>
    </div>
  );
}

export default function SignalsPage() {
  const { data, isLoading, isError, error } = useSignals();

  const [minConsensus, setMinConsensus] = useState<"all" | "2" | "3">("all");
  const [statusFilter, setStatusFilter] = useState<"all" | "buy" | "focus_buy">("all");
  const [newTodayOnly, setNewTodayOnly] = useState(false);

  const [tradeOpen, setTradeOpen] = useState(false);
  const [tradeSignal, setTradeSignal] = useState<Signal | null>(null);

  function handleTradeClick(signal: Signal) {
    setTradeSignal(signal);
    setTradeOpen(true);
  }

  const filtered = useMemo(() => {
    let rows = data ?? [];
    if (minConsensus === "2") rows = rows.filter((s) => s.consensus_count >= 2);
    if (minConsensus === "3") rows = rows.filter((s) => s.consensus_count >= 3);
    if (statusFilter === "buy") rows = rows.filter((s) => s.max_status === "buy");
    if (statusFilter === "focus_buy")
      rows = rows.filter((s) => s.max_status === "buy" || s.max_status === "focus");
    if (newTodayOnly) rows = rows.filter((s) => s.is_new_today);
    return rows;
  }, [data, minConsensus, statusFilter, newTodayOnly]);

  const totalSignals = data?.length ?? 0;
  const newTodayCount = data?.filter((s) => s.is_new_today).length ?? 0;
  const strongest = data?.[0];

  const tradeInitial = tradeSignal
    ? {
        symbol: tradeSignal.symbol,
        strategy: tradeSignal.strategies[0]?.strategy,
        setup_type: tradeSignal.strategies[0]?.setup_type ?? undefined,
        entry_date: new Date().toISOString().split("T")[0],
        entry_price: tradeSignal.price,
      }
    : undefined;

  return (
    <div className="flex flex-col h-full">
      {/* Header */}
      <div className="px-6 py-3 border-b">
        <h1 className="text-xl font-semibold tracking-tight">Sinyaller</h1>
        <p className="text-sm text-muted-foreground">
          Konsensus sinyalleri — bugün ne var?
        </p>
      </div>

      {/* Stats */}
      {!isLoading && !isError && (
        <div className="px-6 py-3 border-b grid grid-cols-3 gap-3">
          <StatCard label="Yeni Bugün" value={newTodayCount > 0 ? `${newTodayCount} sinyal` : "—"} />
          <StatCard label="Toplam Sinyal" value={`${totalSignals} sinyal`} />
          <StatCard
            label="En Güçlü"
            value={
              strongest
                ? `${strongest.symbol} (${strongest.consensus_count} strateji)`
                : "—"
            }
          />
        </div>
      )}

      {/* Filters */}
      <div className="px-6 py-2 border-b flex flex-wrap items-center gap-2">
        <select
          value={minConsensus}
          onChange={(e) => setMinConsensus(e.target.value as "all" | "2" | "3")}
          className={SELECT}
        >
          <option value="all">Min Konsensus: Tümü</option>
          <option value="2">2+</option>
          <option value="3">3+</option>
        </select>

        <select
          value={statusFilter}
          onChange={(e) =>
            setStatusFilter(e.target.value as "all" | "buy" | "focus_buy")
          }
          className={SELECT}
        >
          <option value="all">Statü: Tümü</option>
          <option value="buy">Sadece Buy</option>
          <option value="focus_buy">Buy ve Focus</option>
        </select>

        <label className="flex items-center gap-1.5 text-xs cursor-pointer select-none">
          <input
            type="checkbox"
            checked={newTodayOnly}
            onChange={(e) => setNewTodayOnly(e.target.checked)}
            className="h-3.5 w-3.5 accent-primary"
          />
          Yeni Bugün
        </label>

        <span className="text-xs text-muted-foreground ml-1">
          {filtered.length} / {totalSignals} sinyal
        </span>
      </div>

      {/* Content */}
      <div className="flex-1 px-6 py-4 overflow-auto">
        {isLoading && (
          <div className="flex items-center justify-center h-64 text-sm text-muted-foreground">
            Yükleniyor...
          </div>
        )}
        {isError && (
          <div
            className="flex items-center justify-center h-64 text-sm"
            style={{ color: "var(--mtp-danger)" }}
          >
            Hata: {(error as Error)?.message ?? "Sinyal verisi alınamadı"}
          </div>
        )}
        {!isLoading && !isError && filtered.length === 0 && (
          <div className="flex flex-col items-center justify-center h-64 gap-3 text-center text-muted-foreground">
            <Activity size={32} strokeWidth={1.5} />
            <p className="text-sm">Bu filtreyle sinyal yok.</p>
            <p className="text-xs">
              Filtre seçimini değiştirin veya Watchlist üzerinden yeni sinyal ekleyin.
            </p>
          </div>
        )}
        {!isLoading && !isError && filtered.length > 0 && (
          <div className="grid grid-cols-1 md:grid-cols-2 gap-4">
            {filtered.map((signal) => (
              <SignalCard
                key={signal.symbol}
                signal={signal}
                onTradeClick={handleTradeClick}
              />
            ))}
          </div>
        )}
      </div>

      <AddTradeDialog
        open={tradeOpen}
        onOpenChange={(v) => {
          setTradeOpen(v);
          if (!v) setTradeSignal(null);
        }}
        initialData={tradeInitial}
      />
    </div>
  );
}
