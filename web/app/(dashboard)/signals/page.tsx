"use client";

import { useMemo, useState } from "react";
import { Activity } from "lucide-react";
import { useSignals } from "@/hooks/use-signals";
import { SignalCard } from "@/components/signals/SignalCard";
import { AddTradeDialog } from "@/components/journal/AddTradeDialog";
import { Button } from "@/components/ui/button";
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

// KARAR #469 (20 May 2026): Konsensus filtresi kaldırıldı.
// Her watchlist satırı = 1 sinyal kartı. Sn. Ferit talimat:
// "konsensus olmasın, tüm sinyaller görünsün".
export default function SignalsPage() {
  const { data, isLoading, isError, error, refetch, isFetching } = useSignals();

  const [statusFilter, setStatusFilter] = useState<"all" | "buy" | "focus_buy">("all");
  const [strategyFilter, setStrategyFilter] = useState<"all" | "minervini" | "carr">("all");
  const [newTodayOnly, setNewTodayOnly] = useState(false);

  const [tradeOpen, setTradeOpen] = useState(false);
  const [tradeSignal, setTradeSignal] = useState<Signal | null>(null);

  function handleTradeClick(signal: Signal) {
    setTradeSignal(signal);
    setTradeOpen(true);
  }

  const filtered = useMemo(() => {
    let rows = data ?? [];
    if (statusFilter === "buy") rows = rows.filter((s) => s.status === "buy");
    if (statusFilter === "focus_buy")
      rows = rows.filter((s) => s.status === "buy" || s.status === "focus");
    if (strategyFilter !== "all") rows = rows.filter((s) => s.strategy === strategyFilter);
    if (newTodayOnly) rows = rows.filter((s) => s.is_new_today);
    return rows;
  }, [data, statusFilter, strategyFilter, newTodayOnly]);

  const totalSignals = data?.length ?? 0;
  const newTodayCount = data?.filter((s) => s.is_new_today).length ?? 0;
  // En güçlü: en yüksek RS rating (KARAR #469 konsensus yok)
  const strongest = data?.[0];

  const tradeInitial = tradeSignal
    ? {
        symbol: tradeSignal.symbol,
        strategy: tradeSignal.strategy,
        setup_type: tradeSignal.setup_type ?? undefined,
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
          Tüm strateji sinyalleri — bugün ne var?
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
                ? `${strongest.symbol} (RS ${Math.round(strongest.rs_rating)})`
                : "—"
            }
          />
        </div>
      )}

      {/* Filters */}
      <div className="px-6 py-2 border-b flex flex-wrap items-center gap-2">
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

        <select
          value={strategyFilter}
          onChange={(e) =>
            setStrategyFilter(e.target.value as "all" | "minervini" | "carr")
          }
          className={SELECT}
        >
          <option value="all">Strateji: Tümü</option>
          <option value="minervini">Minervini</option>
          <option value="carr">Carr</option>
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
          <div className="flex items-center justify-center h-64 px-6">
            <div
              className="max-w-xl w-full p-4 border rounded-md"
              style={{ borderColor: "var(--mtp-danger)", background: "rgba(255, 80, 80, 0.06)" }}
              role="alert"
            >
              <div className="font-semibold mb-1" style={{ color: "var(--mtp-danger)" }}>
                ⚠️ Sinyal verisi alınamadı
              </div>
              <div className="text-xs mb-3 opacity-80">
                {(error as Error)?.message ?? "Bilinmeyen hata"}
              </div>
              <div className="text-xs mb-3 opacity-70">
                Olası sebep: Cloud SQL erişilemez (instance durmuş veya IP whitelist eski).
                GCP Console → SQL → instance durum kontrol et.
              </div>
              <Button size="sm" variant="outline" onClick={() => refetch()} disabled={isFetching}>
                {isFetching ? "Tekrar deneniyor..." : "Tekrar Dene"}
              </Button>
            </div>
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
                key={`${signal.symbol}-${signal.strategy}`}
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
