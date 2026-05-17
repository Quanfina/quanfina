"use client";

import { useMemo, useRef, useState } from "react";
import { useTheme } from "next-themes";
import { Plus } from "lucide-react";
import { AgGridReact } from "ag-grid-react";
import type { ColDef } from "ag-grid-community";
import { useTrades, useDeleteTrade } from "@/hooks/use-trades";
import { TRADE_COL_DEFS, TRADE_DEFAULT_COL_DEF } from "@/components/journal/columns";
import { TradeRowActions } from "@/components/journal/TradeRowActions";
import { AddTradeDialog } from "@/components/journal/AddTradeDialog";
import { CloseTradeDialog } from "@/components/journal/CloseTradeDialog";
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
  const { resolvedTheme } = useTheme();
  const gridRef = useRef<AgGridReact<Trade>>(null);

  // Filters
  const [statusFilter, setStatusFilter]   = useState("all");
  const [strategyFilter, setStratFilter]  = useState("all");
  const [gradeFilter, setGradeFilter]     = useState("all");
  const [search, setSearch]               = useState("");

  // Data
  const { data, isLoading, isError, error } = useTrades();
  const deleteMutation = useDeleteTrade();

  // Dialog state
  const [addOpen, setAddOpen]           = useState(false);
  const [closingTrade, setClosingTrade] = useState<Trade | null>(null);
  const [closeOpen, setCloseOpen]       = useState(false);
  const [deletingTrade, setDeletingTrade] = useState<Trade | null>(null);
  const [deleteOpen, setDeleteOpen]     = useState(false);

  // Stable callback ref (callbacksRef pattern — KARAR #391)
  const cbRef = useRef({
    onEdit:   (_t: Trade) => {},
    onClose:  (_t: Trade) => {},
    onDelete: (_t: Trade) => {},
  });
  cbRef.current = {
    onEdit: (t) => { setClosingTrade(t); setCloseOpen(true); },
    onClose: (t) => { setClosingTrade(t); setCloseOpen(true); },
    onDelete: (t) => { setDeletingTrade(t); setDeleteOpen(true); },
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
          onEdit:   (t: Trade) => cbRef.current.onEdit(t),
          onClose:  (t: Trade) => cbRef.current.onClose(t),
          onDelete: (t: Trade) => cbRef.current.onDelete(t),
        },
      },
    ],
    []
  );

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
    return [...rows].sort((a, b) => b.entry_date.localeCompare(a.entry_date));
  }, [data, statusFilter, strategyFilter, gradeFilter, search]);

  const isDark = resolvedTheme === "dark";
  const gridClass = isDark ? "ag-theme-quartz-dark" : "ag-theme-quartz";

  return (
    <div className="flex flex-col h-full">
      {/* Header */}
      <div className="px-6 py-3 border-b flex items-center justify-between">
        <div>
          <h1 className="text-xl font-semibold tracking-tight">Trade Journal</h1>
          <p className="text-sm text-muted-foreground">
            Tüm trade kayıtları — grade, P/L, dersler
          </p>
        </div>
        <Button size="sm" onClick={() => setAddOpen(true)}>
          <Plus size={14} className="mr-1.5" />
          Yeni Trade
        </Button>
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
      </div>

      {/* Grid */}
      <div className="flex-1 px-6 py-4">
        {isLoading && (
          <div className="flex items-center justify-center h-64 text-sm text-muted-foreground">Yükleniyor...</div>
        )}
        {isError && (
          <div className="flex items-center justify-center h-64 text-sm" style={{ color: "var(--mtp-danger)" }}>
            Hata: {(error as Error)?.message ?? "Trade verisi alınamadı"}
          </div>
        )}
        {!isLoading && !isError && (
          <div className={gridClass} style={{ height: 560, width: "100%" }}>
            <AgGridReact
              ref={gridRef}
              theme="legacy"
              columnDefs={columnDefs}
              defaultColDef={TRADE_DEFAULT_COL_DEF}
              rowData={rowData}
              rowHeight={36}
              headerHeight={36}
              suppressCellFocus={false}
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
