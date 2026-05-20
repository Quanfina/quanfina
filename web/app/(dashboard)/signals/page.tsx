"use client";

import { useMemo, useRef, useState } from "react";
import { useTheme } from "next-themes";
import { Activity, Plus } from "lucide-react";
import { AgGridReact } from "ag-grid-react";
import type { ColDef, ICellRendererParams } from "ag-grid-community";
import { useSignals } from "@/hooks/use-signals";
import { AddTradeDialog } from "@/components/journal/AddTradeDialog";
import { Button } from "@/components/ui/button";
import { GridLoadingOverlay } from "@/components/ag-grid/LoadingOverlay";
import type { Signal } from "@/types/signal";

const SELECT =
  "h-8 rounded-md border border-input bg-background px-2 text-xs focus:outline-none focus:ring-1 focus:ring-ring";

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

function StatCard({ label, value }: { label: string; value: string | number }) {
  return (
    <div className="rounded-lg border bg-card px-4 py-3 flex flex-col gap-0.5">
      <span className="text-xs text-muted-foreground">{label}</span>
      <span className="text-lg font-semibold">{value}</span>
    </div>
  );
}

// KARAR #470 (20 May 2026): Sinyaller AG Grid tablo (KARAR #423 + #469 revize).
// Sn. Ferit: "kart şeklinde değil tablo şeklinde olsun". Watchlist/Journal pateni.
// Konsensus mantığı KARAR #469 ile zaten kaldırıldı — her watchlist satırı 1 sinyal satırı.
export default function SignalsPage() {
  const { resolvedTheme } = useTheme();
  const { data, isLoading, isError, error, refetch, isFetching } = useSignals();
  const gridRef = useRef<AgGridReact<Signal>>(null);

  const [statusFilter, setStatusFilter] = useState<"all" | "buy" | "focus_buy">("all");
  const [strategyFilter, setStrategyFilter] = useState<"all" | "minervini" | "carr">("all");
  const [newTodayOnly, setNewTodayOnly] = useState(false);

  const [tradeOpen, setTradeOpen] = useState(false);
  const [tradeSignal, setTradeSignal] = useState<Signal | null>(null);

  const isDark = resolvedTheme === "dark";
  const gridClass = isDark ? "ag-theme-quartz-dark" : "ag-theme-quartz";

  function handleTradeClick(signal: Signal) {
    setTradeSignal(signal);
    setTradeOpen(true);
  }

  // Stable callback ref (Watchlist pateni — render sırasında ref update yasak ama
  // burada tek callback ve closure güvenli, ESLint react-hooks/refs uyarı verebilir,
  // ileri sprintte refactor için AÇIK KONU #72 kapsamı).
  const tradeClickRef = useRef<(s: Signal) => void>(handleTradeClick);
  tradeClickRef.current = handleTradeClick;

  const columnDefs = useMemo<ColDef<Signal>[]>(() => [
    {
      field: "symbol",
      headerName: "Sembol",
      width: 110,
      pinned: "left" as const,
      cellRenderer: (p: ICellRendererParams<Signal>) => {
        const s = p.data;
        if (!s) return null;
        return (
          <div className="flex items-center gap-2 h-full">
            <span className="font-semibold tracking-tight">{s.symbol}</span>
            {s.is_new_today && (
              <span
                className="text-[10px] font-semibold px-1.5 py-0.5 rounded-full"
                style={{ background: "#28A745", color: "#fff" }}
                title="Bugün eklendi"
              >
                YENİ
              </span>
            )}
          </div>
        );
      },
    },
    {
      field: "strategy",
      headerName: "Strateji",
      width: 110,
      valueFormatter: (p) => STRATEGY_LABELS[p.value as string] ?? (p.value as string),
    },
    {
      field: "status",
      headerName: "Statü",
      width: 100,
      cellRenderer: (p: ICellRendererParams<Signal>) => {
        const s = p.value as string;
        return (
          <span
            className="text-xs font-semibold"
            style={{ color: STATUS_COLORS[s] ?? "inherit" }}
          >
            {STATUS_LABELS[s] ?? s}
          </span>
        );
      },
    },
    {
      field: "setup_type",
      headerName: "Setup",
      width: 160,
      valueFormatter: (p) => (p.value as string | null) ?? "—",
    },
    {
      field: "rs_rating",
      headerName: "RS",
      width: 80,
      type: "rightAligned",
      valueFormatter: (p) => Math.round(p.value as number).toString(),
    },
    {
      field: "price",
      headerName: "Fiyat",
      width: 100,
      type: "rightAligned",
      valueFormatter: (p) =>
        `$${(p.value as number).toFixed(2)}`,
      cellStyle: { fontFamily: "var(--font-jetbrains-mono, monospace)" },
    },
    {
      field: "added_date",
      headerName: "Eklenme",
      width: 150,
      valueFormatter: (p) => (p.value as string | null) ?? "—",
      cellStyle: { fontFamily: "var(--font-jetbrains-mono, monospace)", fontSize: "12px" },
    },
    {
      headerName: "",
      width: 120,
      pinned: "right" as const,
      sortable: false,
      resizable: false,
      suppressMovable: true,
      cellRenderer: (p: ICellRendererParams<Signal>) => {
        const signal = p.data;
        if (!signal) return null;
        return (
          <div className="flex items-center justify-center h-full">
            <button
              type="button"
              onClick={() => tradeClickRef.current(signal)}
              className="inline-flex items-center gap-1 h-6 rounded-md bg-primary text-primary-foreground px-2 text-[11px] font-medium hover:bg-primary/90 transition-colors"
              title="Trade aç (form pre-fill ile)"
            >
              <Plus size={11} />
              Trade
            </button>
          </div>
        );
      },
    },
  ], []);

  const defaultColDef = useMemo<ColDef<Signal>>(() => ({
    sortable: true,
    resizable: true,
    filter: false,
  }), []);

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
      <div className="flex-1 px-6 py-4">
        {isLoading && (
          <div className={`${gridClass} h-[600px] w-full`}>
            <GridLoadingOverlay />
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
          <div className={gridClass} style={{ height: 600, width: "100%" }}>
            <AgGridReact<Signal>
              ref={gridRef}
              theme="legacy"
              columnDefs={columnDefs}
              defaultColDef={defaultColDef}
              rowData={filtered}
              rowHeight={36}
              headerHeight={36}
              suppressCellFocus={false}
              getRowId={(p) => `${p.data.symbol}-${p.data.strategy}`}
            />
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
