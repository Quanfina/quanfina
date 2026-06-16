"use client";

import { useMemo, useRef, useState } from "react";
import { useGridTheme } from "@/hooks/use-grid-theme";
import { useGridColumnState } from "@/hooks/use-grid-column-state";
import { Plus } from "lucide-react";
import "@/lib/ag-grid-setup"; // AG Grid modül kaydı (Paket 355 — bundle split)
import { AgGridReact } from "ag-grid-react";
import type { ColDef } from "ag-grid-community";
import { useTrades, useDeleteTrade } from "@/hooks/use-trades";
import { useTradesInfo } from "@/hooks/use-trades-info";
import { MockDataBanner } from "@/components/shared/MockDataBanner";
import { useStockQuotes } from "@/hooks/use-stock-quote";
import { usePositionAlerts } from "@/hooks/use-position-alerts";
import { TRADE_COL_DEFS, TRADE_DEFAULT_COL_DEF } from "@/components/journal/columns";
import { TradeRowActions } from "@/components/journal/TradeRowActions";
import { AddTradeDialog } from "@/components/journal/AddTradeDialog";
import { CloseTradeDialog } from "@/components/journal/CloseTradeDialog";
import { EditOpenTradeDialog } from "@/components/journal/EditOpenTradeDialog";
import { RbaSummaryCard } from "@/components/journal/RbaSummaryCard";
import { MarkRegimeBanner } from "@/components/mark/MarkRegimeBanner";
import { ModBadge } from "@/components/mark/ModBadge";
import { BrandonExpectancyCard } from "@/components/journal/BrandonExpectancyCard";
import { Button } from "@/components/ui/button";
import {
  AlertDialog,
  AlertDialogAction,
  AlertDialogCancel,
  AlertDialogContent,
  AlertDialogDescription,
  AlertDialogFooter,
  AlertDialogHeader,
  AlertDialogTitle,
} from "@/components/ui/alert-dialog";
import type { Trade } from "@/types/trade";

const SELECT = "h-8 rounded-md border border-input bg-background px-2 text-xs focus:outline-none focus:ring-1 focus:ring-ring";

export default function JournalPage() {
  const { gridClass } = useGridTheme();
  const gridRef = useRef<AgGridReact<Trade>>(null);
  const gridColumnState = useGridColumnState("quanfina-journal-cols");

  // Filters
  const [statusFilter, setStatusFilter]   = useState("all");
  const [strategyFilter, setStratFilter]  = useState("all");
  const [gradeFilter, setGradeFilter]     = useState("all");
  const [search, setSearch]               = useState("");

  // Data
  const { data, isLoading, isError, error, refetch, isFetching } = useTrades();
  // P417 (31 May 2026 — Kural #28 audit): Trade veri kaynağı meta (DB vs MOCK)
  const { data: tradesInfo } = useTradesInfo();
  const deleteMutation = useDeleteTrade();

  // Dialog state
  const [addOpen, setAddOpen]           = useState(false);
  const [closingTrade, setClosingTrade] = useState<Trade | null>(null);
  const [closeOpen, setCloseOpen]       = useState(false);
  const [deletingTrade, setDeletingTrade] = useState<Trade | null>(null);
  const [deleteOpen, setDeleteOpen]     = useState(false);
  // Migration 011 / #960: aktif stop/hedef düzenleme dialog state
  const [editingTrade, setEditingTrade] = useState<Trade | null>(null);
  const [editOpen, setEditOpen]         = useState(false);

  // Stable callback ref (callbacksRef pattern — KARAR #391)
  const cbRef = useRef({
    onEdit:     (_t: Trade) => {},
    onClose:    (_t: Trade) => {},
    onDelete:   (_t: Trade) => {},
    onEditStop: (_t: Trade) => {},
  });
  cbRef.current = {
    onEdit: (t) => { setClosingTrade(t); setCloseOpen(true); },
    onClose: (t) => { setClosingTrade(t); setCloseOpen(true); },
    onDelete: (t) => { setDeletingTrade(t); setDeleteOpen(true); },
    onEditStop: (t) => { setEditingTrade(t); setEditOpen(true); },
  };

  const columnDefs = useMemo<ColDef<Trade>[]>(
    () => [
      ...TRADE_COL_DEFS,
      {
        headerName: "",
        width: 48,
        minWidth: 48,
        pinned: "right" as const,
        sortable: false,
        resizable: false,
        suppressMovable: true,
        cellStyle: { display: "flex", alignItems: "center", justifyContent: "center" },
        cellRenderer: TradeRowActions,
        cellRendererParams: {
          onEdit:     (t: Trade) => cbRef.current.onEdit(t),
          onClose:    (t: Trade) => cbRef.current.onClose(t),
          onDelete:   (t: Trade) => cbRef.current.onDelete(t),
          onEditStop: (t: Trade) => cbRef.current.onEditStop(t),
        },
      },
    ],
    []
  );

  // Paket 151 (26 May 2026): Açık trade'ler için canlı fiyat fetch (paralel)
  // useStockQuotes 60s refetchInterval — yfinance + 5dk backend cache.
  const openSymbols = useMemo(
    () => Array.from(new Set((data ?? []).filter((t) => t.status === "open").map((t) => t.symbol))),
    [data]
  );
  const quoteResults = useStockQuotes(openSymbols);

  // Paket 160 (26 May 2026): Açık trade canlı stop loss uyarısı.
  // Mark TTLC s.131 %7 + TLSMW Ch 12 plan_stop disiplini → toast notification.
  // Hook quote değişiminde otomatik tetik (Toaster top-right, sonner dedup).
  usePositionAlerts(data ?? [], quoteResults);
  const quoteMap = useMemo(() => {
    const map = new Map<string, { price: number; source: string }>();
    quoteResults.forEach((r) => {
      if (r.data) map.set(r.data.symbol.toUpperCase(), { price: r.data.price, source: r.data.source });
    });
    return map;
  }, [quoteResults]);

  const rowData = useMemo(() => {
    let rows: Trade[] = data ?? [];
    if (statusFilter !== "all")   rows = rows.filter((r) => r.status === statusFilter);
    if (strategyFilter !== "all") rows = rows.filter((r) => r.strategy === strategyFilter);
    if (gradeFilter !== "all") {
      if (gradeFilter === "AB") rows = rows.filter((r) => r.grade === "A+" || r.grade === "A");
      else rows = rows.filter((r) => r.grade === gradeFilter);
    }
    if (search) {
      const q = search.toUpperCase();
      rows = rows.filter((r) => r.symbol.includes(q) || r.setup_type.toUpperCase().includes(q));
    }
    // Paket 151: client-side enrich — quote price + unrealized P&L (sadece açık trade)
    const enriched: Trade[] = rows.map((t) => {
      if (t.status !== "open") return t;
      const q = quoteMap.get(t.symbol.toUpperCase());
      // #10 (Kural #28): SADECE canlı yfinance quote ile enrich. MOCK fiyat "CANLI $"
      // + unrealized P/L'yi sahte "canlı" gibi gösteriyordu (quote_source tüketilmiyordu).
      // source mock/yok → enrich etme; kolon "—" gösterir (yanıltıcı sayı yok).
      if (!q || q.source !== "yfinance") return t;
      const unrealized_pl_dollar = (q.price - t.entry_price) * t.shares;
      const unrealized_pl_pct = ((q.price / t.entry_price) - 1) * 100;
      return {
        ...t,
        current_price: q.price,
        unrealized_pl_dollar,
        unrealized_pl_pct,
        quote_source: q.source as "yfinance" | "mock",
      };
    });
    // P402: Açık trade'ler önce (status='open'), Kapalı sonra — her grup
    // içinde tarih DESC. Markets360 "FULL POSITIONS / HALF POSITIONS" grouping
    // pattern'inin Quanfina uyarlaması (status bazlı sıralama, Mark "Risk
    // first" disiplini — açık pozisyonlar her zaman göz önünde).
    return enriched.sort((a, b) => {
      if (a.status === b.status) return b.entry_date.localeCompare(a.entry_date);
      return a.status === "open" ? -1 : 1;  // open önce
    });
  }, [data, statusFilter, strategyFilter, gradeFilter, search, quoteMap]);

  // P402: Status bazlı count badge'leri (Markets360 "(N) section header" pattern)
  const statusCounts = useMemo(() => {
    const all = data ?? [];
    return {
      open: all.filter((t) => t.status === "open").length,
      closed: all.filter((t) => t.status === "closed").length,
      topGrade: all.filter((t) => t.grade === "A+" || t.grade === "A").length,
    };
  }, [data]);

  // P402: AG Grid row vurgusu — açık trade'ler hafif arkaplan
  // (Markets360 "FULL POSITIONS" mavi-tint Quanfina uyarlaması, Mark "Risk first")
  const rowClassRules = useMemo(
    () => ({
      "qf-row-open": (params: { data?: Trade }) => params.data?.status === "open",
    }),
    [],
  );

  // KARAR #476: gridClass useGridTheme'den (SSR uyumu)

  return (
    <div className="flex flex-col h-full">
      {/* Header */}
      <div className="px-6 py-3 border-b flex items-center justify-between">
        <div>
          <h1 className="text-xl font-semibold tracking-tight">İşlem Günlüğü</h1>
          <p className="text-sm text-muted-foreground">
            Tüm trade kayıtları — grade, P/L, dersler
          </p>
        </div>
        <div className="flex items-center gap-3">
          {/* Paket 221 (27 May 2026): Vizyon İLKE #10 — Journal'da mod farkındalığı */}
          <ModBadge variant="compact" />
          <Button size="sm" onClick={() => setAddOpen(true)}>
            <Plus size={14} className="mr-1.5" />
            Yeni Trade
          </Button>
        </div>
      </div>

      {/* P418 (31 May 2026 DRY component): MockDataBanner — 4 sayfada paylaşılan
          tek doğruluk kaynağı (P417 patterni inline -> shared component refactor). */}
      <MockDataBanner isMock={tradesInfo?.is_mock} context="trade kayıt listesi" testId="journal-mock-banner" />

      {/* KARAR #733 alt-paket (Paket 39+41): Mark Regime banner — trade aç/kapa
          yaparken piyasa rejimi sürekli görünür. Stage 4 sayım Trade.mark_signals
          enrichment ile gerçek (P41 backend enrich) — açık trade'lerde Stage 4
          olanlar UZAK DUR adayı. */}
      <MarkRegimeBanner
        stage4Count={(data ?? []).filter((t) => t.status === "open" && t.mark_signals?.carr_stage === 4).length}
        totalCount={(data ?? []).filter((t) => t.status === "open").length}
        climaxTopCount={(data ?? []).filter((t) => t.status === "open" && t.mark_signals?.climax_category === "CLIMAX_TOP").length}
      />

      {/* KARAR ADAY #722 (24 May 2026): Mark RBA Summary Card (TTLC Sec 4) */}
      <div className="px-6 py-3 border-b">
        <RbaSummaryCard
          strategy={strategyFilter !== "all" ? strategyFilter : undefined}
          variant="full"
        />
      </div>

      {/* KARAR #733 alt-paket (Paket 90+97): Brandon Expectancy /journal canon
          Mark Brandon Video — kapali trade'ler agregasyon expectancy hesabi.
          RBA Summary Card felsefe simetrisi: RBA setup-bazli, Brandon trade-bazli.
          P97: strategyFilter prop -> canli filter (Mark vs Carr ayri E hesabi). */}
      <div className="px-6 py-3 border-b">
        <BrandonExpectancyCard strategy={strategyFilter !== "all" ? strategyFilter : undefined} />
      </div>

      {/* Filters */}
      <div className="px-6 py-2 border-b flex flex-wrap items-center gap-2">
        <select value={statusFilter} onChange={(e) => setStatusFilter(e.target.value)} className={SELECT}>
          <option value="all">Tüm Statü</option>
          <option value="open">Açık</option>
          <option value="closed">Kapalı</option>
        </select>
        <select value={strategyFilter} onChange={(e) => setStratFilter(e.target.value)} className={SELECT}>
          <option value="all">Tüm Strateji</option>
          <option value="minervini">Minervini</option>
          <option value="carr">Carr</option>
        </select>
        <select value={gradeFilter} onChange={(e) => setGradeFilter(e.target.value)} className={SELECT}>
          <option value="all">Tüm Grade</option>
          <option value="AB">A+ / A</option>
          <option value="B">B</option>
          <option value="C">C</option>
          <option value="D">D</option>
          <option value="F">F</option>
        </select>
        <input
          type="text"
          value={search}
          onChange={(e) => setSearch(e.target.value.toUpperCase())}
          placeholder="Hisse ara..."
          className="h-8 rounded-md border border-input bg-background px-2 text-xs w-28 focus:outline-none focus:ring-1 focus:ring-ring"
        />
        <span className="text-xs text-muted-foreground ml-1">
          {rowData.length} / {data?.length ?? 0} trade
        </span>
        {/* P402: Status count badge'leri (Markets360 section header pattern uyarlaması).
            Markets "Full/Half Positions (N)" görsel grouping → Quanfina Açık/Kapalı count. */}
        {(data?.length ?? 0) > 0 && (
          <div
            className="flex items-center gap-2 ml-auto text-[11px]"
            data-testid="journal-status-counts"
          >
            <span
              className="px-2 py-0.5 rounded-full font-medium tabular-nums"
              style={{ background: "rgba(40,167,69,0.12)", color: "var(--mtp-excellent)" }}
              title="Açık trade'ler — Mark Risk first öncelik"
            >
              Açık: {statusCounts.open}
            </span>
            <span
              className="px-2 py-0.5 rounded-full font-medium tabular-nums"
              style={{ background: "rgba(150,150,150,0.12)", color: "var(--muted-foreground)" }}
              title="Kapalı trade'ler — Mark RBA (TTLC Sec 4)"
            >
              Kapalı: {statusCounts.closed}
            </span>
            {statusCounts.topGrade > 0 && (
              <span
                className="px-2 py-0.5 rounded-full font-medium tabular-nums"
                style={{ background: "rgba(75,156,211,0.12)", color: "#4B9CD3" }}
                title="A+/A grade — Mark TradeGrader BP/SP (KARAR #445)"
              >
                A+/A: {statusCounts.topGrade}
              </span>
            )}
          </div>
        )}
      </div>

      {/* Grid */}
      <div className="flex-1 px-6 py-4">
        {isLoading && (
          <div className="flex items-center justify-center h-64 text-sm text-muted-foreground">Yükleniyor...</div>
        )}
        {isError && (
          <div className="flex items-center justify-center h-64 px-6">
            <div
              className="max-w-xl w-full p-4 border rounded-md"
              style={{ borderColor: "var(--mtp-danger)", background: "rgba(255, 80, 80, 0.06)" }}
              role="alert"
            >
              <div className="font-semibold mb-1" style={{ color: "var(--mtp-danger)" }}>
                ⚠️ Trade verisi alınamadı
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
        {!isLoading && !isError && (
          <div className={gridClass} style={{ height: 560, width: "100%" }}>
            <AgGridReact
              ref={gridRef}
              {...gridColumnState}
              theme="legacy"
              columnDefs={columnDefs}
              defaultColDef={TRADE_DEFAULT_COL_DEF}
              rowData={rowData}
              rowHeight={36}
              headerHeight={36}
              suppressCellFocus={false}
              rowClassRules={rowClassRules}
            />
          </div>
        )}
      </div>

      {/* Dialogs */}
      <AddTradeDialog open={addOpen} onOpenChange={setAddOpen} />
      <CloseTradeDialog
        trade={closingTrade}
        open={closeOpen}
        onOpenChange={(v) => { setCloseOpen(v); if (!v) setClosingTrade(null); }}
      />
      <EditOpenTradeDialog
        trade={editingTrade}
        open={editOpen}
        onOpenChange={(v) => { setEditOpen(v); if (!v) setEditingTrade(null); }}
      />
      <AlertDialog open={deleteOpen} onOpenChange={setDeleteOpen}>
        <AlertDialogContent>
          <AlertDialogHeader>
            <AlertDialogTitle>Trade silinsin mi?</AlertDialogTitle>
            <AlertDialogDescription>
              <strong>{deletingTrade?.symbol}</strong> — {deletingTrade?.entry_date} trade kaydı kalıcı olarak silinecek.
            </AlertDialogDescription>
          </AlertDialogHeader>
          <AlertDialogFooter>
            <AlertDialogCancel>İptal</AlertDialogCancel>
            <AlertDialogAction
              className="bg-destructive hover:bg-destructive/90 text-destructive-foreground"
              onClick={() => {
                if (deletingTrade) {
                  deleteMutation.mutate(deletingTrade.id, {
                    onSuccess: () => { setDeleteOpen(false); setDeletingTrade(null); },
                  });
                }
              }}
            >
              Sil
            </AlertDialogAction>
          </AlertDialogFooter>
        </AlertDialogContent>
      </AlertDialog>
    </div>
  );
}
