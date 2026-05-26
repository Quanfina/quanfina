"use client";

import { useEffect, useMemo, useRef, useState } from "react";
import { useGridTheme } from "@/hooks/use-grid-theme";
import { Activity, Plus, X, RotateCcw } from "lucide-react";
import { AgGridReact } from "ag-grid-react";
import type { ColDef, ICellRendererParams } from "ag-grid-community";

// KARAR #479 (20 May 2026): DRY MONO style — Watchlist/Journal pateni.
// KARAR #484 (20 May 2026): lib/grid-styles ortak helper'a tasindi (Hisse Tarama
// ile birlikte DRY birlestirme — sayfa-ici kopyalama yasak).
import { MONO, MONO_RIGHT } from "@/lib/grid-styles";
import { useSignals } from "@/hooks/use-signals";
import { AddTradeDialog } from "@/components/journal/AddTradeDialog";
import { AddRowDialog } from "@/components/watchlist/AddRowDialog";
import { Button } from "@/components/ui/button";
import { GridLoadingOverlay } from "@/components/ag-grid/LoadingOverlay";
import { formatDateTR } from "@/lib/format-date";
import { getPassedSignals, setPassedSignals, signalKey } from "@/lib/passed-signals";
import { toast } from "sonner";
import type { Signal } from "@/types/signal";
import { MarkBadgeStrip } from "@/components/mark/MarkBadgeStrip";
import { MarkRegimeBanner } from "@/components/mark/MarkRegimeBanner";
import { fmtUsd } from "@/lib/format-currency";

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
  const { gridClass } = useGridTheme();
  const { data, isLoading, isError, error, refetch, isFetching } = useSignals();
  const gridRef = useRef<AgGridReact<Signal>>(null);

  const [statusFilter, setStatusFilter] = useState<"all" | "buy" | "focus_buy">("all");
  const [strategyFilter, setStrategyFilter] = useState<"all" | "minervini" | "carr">("all");
  const [newTodayOnly, setNewTodayOnly] = useState(false);
  const [showPassed, setShowPassed] = useState(false);

  const [tradeOpen, setTradeOpen] = useState(false);
  const [tradeSignal, setTradeSignal] = useState<Signal | null>(null);
  // KARAR #478 (UX Bölüm 5): Manuel sinyal ekleme — Watchlist'e satır ekler,
  // Sinyaller listesi otomatik yenilenir (consensus_count + status hesaplanır).
  const [manualOpen, setManualOpen] = useState(false);

  // KARAR #475 (20 May 2026): localStorage-backed passed signals (UX Bölüm 6 "AL/GEÇ").
  // Initial load: SSR hydration uyumu için useEffect (window guard).
  const [passedKeys, setPassedKeys] = useState<Set<string>>(new Set());
  useEffect(() => {
    setPassedKeys(getPassedSignals());
  }, []);

  // KARAR #476: gridClass useGridTheme'den (SSR uyumu)

  function handleTradeClick(signal: Signal) {
    setTradeSignal(signal);
    setTradeOpen(true);
  }

  function handlePassClick(signal: Signal) {
    const key = signalKey(signal.symbol, signal.strategy);
    const next = new Set(passedKeys);
    next.add(key);
    setPassedKeys(next);
    setPassedSignals(next);
    toast.success(`${signal.symbol} (${signal.strategy}) geçildi`, {
      action: {
        label: "Geri Al",
        onClick: () => {
          const undo = new Set(next);
          undo.delete(key);
          setPassedKeys(undo);
          setPassedSignals(undo);
        },
      },
      duration: 4000,
    });
  }

  // Stable callback refs (Watchlist pateni)
  const tradeClickRef = useRef<(s: Signal) => void>(handleTradeClick);
  tradeClickRef.current = handleTradeClick;
  const passClickRef = useRef<(s: Signal) => void>(handlePassClick);
  passClickRef.current = handlePassClick;

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
      valueFormatter: (p) => fmtUsd(p.value as number),
      cellStyle: MONO_RIGHT,
    },
    {
      field: "stop_loss",
      headerName: "Stop",
      width: 90,
      type: "rightAligned",
      valueFormatter: (p) => fmtUsd(p.value as number | null),
      cellStyle: { ...MONO_RIGHT, color: "var(--mtp-danger)" },
    },
    {
      field: "target_price",
      headerName: "Hedef",
      width: 90,
      type: "rightAligned",
      valueFormatter: (p) => fmtUsd(p.value as number | null),
      cellStyle: { ...MONO_RIGHT, color: "var(--mtp-excellent)" },
    },
    {
      field: "risk_reward",
      headerName: "R/R",
      width: 80,
      type: "rightAligned",
      // KARAR #473: R/R = (hedef - fiyat) / (fiyat - stop). Backend hesaplar.
      // 1.0+ kabul edilebilir, 2.0+ iyi, 3.0+ mükemmel (mekanik karar referansı).
      cellRenderer: (p: ICellRendererParams<Signal>) => {
        const v = p.value as number | null;
        if (v == null) return <span className="text-muted-foreground">—</span>;
        const color =
          v >= 3 ? "var(--mtp-excellent)" :
          v >= 2 ? "var(--mtp-good)" :
          v >= 1 ? "var(--mtp-neutral)" :
                   "var(--mtp-danger)";
        return (
          <span
            className="font-semibold tabular-nums"
            style={{ color, fontFamily: "var(--font-jetbrains-mono, monospace)", fontSize: "12px" }}
            title={`Risk/Reward = (hedef-fiyat) / (fiyat-stop) = ${v.toFixed(2)}`}
          >
            {v.toFixed(2)}
          </span>
        );
      },
    },
    {
      field: "added_date",
      headerName: "Eklenme",
      width: 150,
      // KARAR #471 (20 May 2026): TR tarih formatı (DD.MM.YYYY HH:MM).
      // Backend ISO "YYYY-MM-DD[ HH:MM]" tutar, frontend formatDateTR helper kullanır
      // (web/lib/format-date.ts — DRY, Bilgi Mimarisi İlke #4 Tekrarsızlık).
      valueFormatter: (p) => formatDateTR(p.value as string | null),
      cellStyle: MONO,
    },
    // KARAR #726 (24 May 2026): Mark Profili kolonu (DRY MarkBadgeStrip 4. sayfa)
    {
      headerName: "Mark Profili",
      minWidth: 220,
      flex: 1,
      sortable: false,
      filter: false,
      cellRenderer: (p: ICellRendererParams<Signal>) => {
        const sig = p.data;
        if (!sig?.mark_signals) return <span className="text-xs text-muted-foreground">—</span>;
        return <MarkBadgeStrip signals={sig.mark_signals} density="compact" showEmpty={false} />;
      },
    },
    // KARAR #733 alt-paket (Paket 81): Pivot kolonu — Mark TLSMW Ch 10
    // AL/Zayıf/Yakın/Altı kompakt rozet (DRY MarkBadgeStrip pateni paralel).
    {
      headerName: "Pivot",
      field: "pivot_status",
      width: 110,
      sortable: false,
      filter: false,
      cellRenderer: (p: ICellRendererParams<Signal>) => {
        const status = p.data?.pivot_status;
        if (!status) return <span className="text-xs text-muted-foreground">—</span>;
        const meta =
          status === "CONFIRMED"
            ? { label: "AL ✓", color: "var(--mtp-excellent)", bg: "rgba(40,167,69,0.15)" }
            : status === "WEAK"
            ? { label: "Zayıf ⚠️", color: "#F59E0B", bg: "rgba(245,158,11,0.15)" }
            : status === "NEAR_PIVOT"
            ? { label: "Yakın ⏳", color: "var(--mtp-good, #4B9CD3)", bg: "rgba(75,156,211,0.15)" }
            : { label: "Altı ○", color: "var(--mtp-danger)", bg: "rgba(220,53,69,0.10)" };
        return (
          <span
            className="text-[10px] font-bold px-1.5 py-0.5 rounded uppercase tracking-wider"
            style={{
              background: meta.bg,
              color: meta.color,
              border:
                status === "CONFIRMED"
                  ? "1px solid var(--mtp-excellent)"
                  : "1px solid currentColor",
            }}
            title={`Mark TLSMW Ch 10 — Pivot ${status}`}
          >
            {meta.label}
          </span>
        );
      },
    },
    {
      headerName: "",
      width: 130,
      pinned: "right" as const,
      sortable: false,
      resizable: false,
      suppressMovable: true,
      // KARAR #475 (UX Bölüm 6): AL + GEÇ ikili buton — mekanik karar
      cellRenderer: (p: ICellRendererParams<Signal>) => {
        const signal = p.data;
        if (!signal) return null;
        return (
          <div className="flex items-center justify-center gap-1 h-full">
            <button
              type="button"
              onClick={() => tradeClickRef.current(signal)}
              className="inline-flex items-center gap-1 h-6 rounded-md px-2 text-[11px] font-semibold transition-colors"
              style={{ background: "#28A745", color: "#fff" }}
              title="Trade aç (form pre-fill ile)"
            >
              <Plus size={11} />
              AL
            </button>
            <button
              type="button"
              onClick={() => passClickRef.current(signal)}
              className="inline-flex items-center gap-1 h-6 rounded-md border px-2 text-[11px] font-medium hover:bg-accent transition-colors text-muted-foreground"
              title="Bu sinyali geç (gizle)"
            >
              <X size={11} />
              GEÇ
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
    // KARAR #475 — Geçilen sinyaller default gizli (showPassed true ise göster)
    if (!showPassed) {
      rows = rows.filter((s) => !passedKeys.has(signalKey(s.symbol, s.strategy)));
    }
    if (statusFilter === "buy") rows = rows.filter((s) => s.status === "buy");
    if (statusFilter === "focus_buy")
      rows = rows.filter((s) => s.status === "buy" || s.status === "focus");
    if (strategyFilter !== "all") rows = rows.filter((s) => s.strategy === strategyFilter);
    if (newTodayOnly) rows = rows.filter((s) => s.is_new_today);
    return rows;
  }, [data, statusFilter, strategyFilter, newTodayOnly, passedKeys, showPassed]);

  const totalSignals = data?.length ?? 0;
  const newTodayCount = data?.filter((s) => s.is_new_today).length ?? 0;
  const passedCount = passedKeys.size;
  const strongest = data?.[0];
  // KARAR #733 alt-paket (Paket 36): Stage 4 sayisi banner icin
  const stage4Count = data?.filter((s) => s.mark_signals?.carr_stage === 4).length ?? 0;

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
      <div className="px-6 py-3 border-b flex items-center justify-between">
        <div>
          <h1 className="text-xl font-semibold tracking-tight">Sinyaller</h1>
          <p className="text-sm text-muted-foreground">
            Tüm strateji sinyalleri — bugün ne var?
          </p>
        </div>
        <Button size="sm" onClick={() => setManualOpen(true)} title="Manuel sinyal ekle (İzleme Listesi üzerinden)">
          <Plus size={14} className="mr-1.5" />
          Manuel Sinyal
        </Button>
      </div>

      {/* KARAR #733 alt-paket (Paket 36): Mark Regime üst-uyarı banner */}
      <MarkRegimeBanner stage4Count={stage4Count} totalCount={totalSignals} />

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

        {passedCount > 0 && (
          <label className="flex items-center gap-1.5 text-xs cursor-pointer select-none">
            <input
              type="checkbox"
              checked={showPassed}
              onChange={(e) => setShowPassed(e.target.checked)}
              className="h-3.5 w-3.5 accent-primary"
            />
            Geçilenleri göster ({passedCount})
          </label>
        )}

        {passedCount > 0 && (
          <button
            type="button"
            onClick={() => {
              setPassedKeys(new Set());
              setPassedSignals(new Set());
              toast.success("Tüm geçilen sinyaller geri alındı");
            }}
            className="inline-flex items-center gap-1 text-xs text-muted-foreground hover:text-foreground transition-colors"
            title="Tüm geçilen sinyalleri geri al"
          >
            <RotateCcw size={11} />
            Geçişleri sıfırla
          </button>
        )}

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
              Filtre seçimini değiştirin veya İzleme Listesi üzerinden yeni sinyal ekleyin.
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

      {/* KARAR #478: Manuel Sinyal Ekleme (UX Bölüm 5) — AddRowDialog yeniden kullanımı */}
      <AddRowDialog open={manualOpen} onOpenChange={setManualOpen} />
    </div>
  );
}
