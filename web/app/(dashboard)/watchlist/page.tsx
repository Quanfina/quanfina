"use client";

import { useMemo, useRef, useState } from "react";
import { useGridTheme } from "@/hooks/use-grid-theme";
import { Plus } from "lucide-react";
import { AgGridReact } from "ag-grid-react";
import type { ColDef } from "ag-grid-community";
import { useWatchlist } from "@/hooks/use-watchlist";
import {
  useUpdateWatchlistRow,
  useDeleteWatchlistRow,
  usePromoteWatchlistRow,
} from "@/hooks/use-watchlist-mutations";
import { WatchlistFilters } from "@/components/watchlist/WatchlistFilters";
import { COL_DEFS, DEFAULT_COL_DEF } from "@/components/watchlist/columns";
import { RowActionsRenderer } from "@/components/watchlist/RowActionsRenderer";
import { AddRowDialog } from "@/components/watchlist/AddRowDialog";
import { EditNoteDialog } from "@/components/watchlist/EditNoteDialog";
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
import type { WatchlistRow } from "@/types/watchlist";
import { demoteStatus } from "@/lib/watchlist-status";
import { GridLoadingOverlay } from "@/components/ag-grid/LoadingOverlay";
import { MarkRegimeBanner } from "@/components/mark/MarkRegimeBanner";

export default function WatchlistPage() {
  const { gridClass } = useGridTheme();
  const [strategy, setStrategy] = useState("all");
  const [status, setStatus] = useState("all");
  const [search, setSearch] = useState("");
  // P114 (26 May 2026): LEADER only + Climax UYARI filter chip'leri
  const [leaderOnly, setLeaderOnly] = useState(false);
  const [climaxOnly, setClimaxOnly] = useState(false);
  // P129 (26 May 2026): Stage Onaylı chip
  const [stageConfirmedOnly, setStageConfirmedOnly] = useState(false);
  const { data, isLoading, isError, error, refetch, isFetching } = useWatchlist();
  const gridRef = useRef<AgGridReact<WatchlistRow>>(null);

  // Dialog state
  const [addOpen, setAddOpen] = useState(false);
  const [editingRow, setEditingRow] = useState<WatchlistRow | null>(null);
  const [editNoteOpen, setEditNoteOpen] = useState(false);
  const [deletingRow, setDeletingRow] = useState<WatchlistRow | null>(null);
  const [deleteConfirmOpen, setDeleteConfirmOpen] = useState(false);

  // Mutations
  const promoteMutation = usePromoteWatchlistRow();
  const updateMutation = useUpdateWatchlistRow();
  const deleteMutation = useDeleteWatchlistRow();

  // Stable callback refs — avoids useMemo dep churn while keeping fresh closures
  const callbacksRef = useRef({
    onPromote: (_row: WatchlistRow) => {},
    onDemote: (_row: WatchlistRow) => {},
    onEditNote: (_row: WatchlistRow) => {},
    onDelete: (_row: WatchlistRow) => {},
  });
  callbacksRef.current = {
    onPromote: (row) =>
      promoteMutation.mutate({ symbol: row.symbol, strategy: row.strategy }),
    onDemote: (row) =>
      updateMutation.mutate({
        symbol: row.symbol,
        strategy: row.strategy,
        update: { status: demoteStatus(row.status) as WatchlistRow["status"] },
      }),
    onEditNote: (row) => {
      setEditingRow(row);
      setEditNoteOpen(true);
    },
    onDelete: (row) => {
      setDeletingRow(row);
      setDeleteConfirmOpen(true);
    },
  };

  const columnDefs = useMemo<ColDef<WatchlistRow>[]>(
    () => [
      ...COL_DEFS,
      {
        headerName: "",
        width: 48,
        minWidth: 48,
        pinned: "right" as const,
        sortable: false,
        resizable: false,
        suppressMovable: true,
        cellStyle: { display: "flex", alignItems: "center", justifyContent: "center" },
        cellRenderer: RowActionsRenderer,
        cellRendererParams: {
          onPromote: (row: WatchlistRow) => callbacksRef.current.onPromote(row),
          onDemote: (row: WatchlistRow) => callbacksRef.current.onDemote(row),
          onEditNote: (row: WatchlistRow) => callbacksRef.current.onEditNote(row),
          onDelete: (row: WatchlistRow) => callbacksRef.current.onDelete(row),
        },
      },
    ],
    [] // stable: callbacksRef is a ref, wrapper lambdas don't change
  );

  // KARAR ADAY (21 May 2026): Konsensus filter + siralama kaldirildi. Her strateji
  // ayri satir kanon. Siralama: strategy > symbol alfabetik (deterministik).
  const rowData = useMemo(() => {
    let rows: WatchlistRow[] = data ?? [];
    if (strategy !== "all") rows = rows.filter((r) => r.strategy === strategy);
    if (status !== "all") rows = rows.filter((r) => r.status === status);
    if (search) {
      const q = search.toUpperCase();
      rows = rows.filter(
        (r) => r.symbol.includes(q) || (r.setup_type ?? "").toUpperCase().includes(q)
      );
    }
    // P114: LEADER only (rs_rating >= 80) ve Climax UYARI chip filtreleri
    if (leaderOnly) rows = rows.filter((r) => Math.round(r.rs_rating) >= 80);
    if (climaxOnly) rows = rows.filter((r) => r.mark_signals?.climax_category === "CLIMAX_TOP");
    if (stageConfirmedOnly) rows = rows.filter((r) => r.mark_signals?.stage_category === "CONFIRMED_STAGE_2");
    return [...rows].sort(
      (a, b) => a.strategy.localeCompare(b.strategy) || a.symbol.localeCompare(b.symbol)
    );
  }, [data, strategy, status, search, leaderOnly, climaxOnly, stageConfirmedOnly]);

  // KARAR #476: gridClass useGridTheme'den (SSR uyumu)
  // KARAR #733 alt-paket (Paket 36): Mark Regime banner Stage 4 sayim
  const stage4Count = data?.filter((r) => r.mark_signals?.carr_stage === 4).length ?? 0;

  return (
    <div className="flex flex-col h-full">
      <div className="px-6 py-3 border-b flex items-center justify-between">
        <div>
          <h1 className="text-xl font-semibold tracking-tight">İzleme Listesi</h1>
          <p className="text-sm text-muted-foreground">
            Tüm stratejiler — çapraz görünüm (İLKE #44 / KARAR #350)
          </p>
        </div>
        <Button size="sm" onClick={() => setAddOpen(true)}>
          <Plus size={14} className="mr-1.5" />
          Hisse Ekle
        </Button>
      </div>

      {/* KARAR #733 alt-paket (Paket 36): Mark Regime üst-uyarı banner */}
      <MarkRegimeBanner
        stage4Count={stage4Count}
        totalCount={data?.length ?? 0}
        climaxTopCount={(data ?? []).filter((r) => r.mark_signals?.climax_category === "CLIMAX_TOP").length}
      />

      <div className="px-6 py-3 border-b">
        <WatchlistFilters
          strategy={strategy}
          onStrategyChange={setStrategy}
          status={status}
          onStatusChange={setStatus}
          search={search}
          onSearchChange={setSearch}
          leaderOnly={leaderOnly}
          onLeaderOnlyChange={setLeaderOnly}
          climaxOnly={climaxOnly}
          onClimaxOnlyChange={setClimaxOnly}
          stageConfirmedOnly={stageConfirmedOnly}
          onStageConfirmedOnlyChange={setStageConfirmedOnly}
          totalRows={data?.length ?? 0}
          filteredRows={rowData.length}
        />
      </div>

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
                ⚠️ İzleme Listesi verisi alınamadı
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
        {!isLoading && !isError && rowData.length === 0 && (data?.length ?? 0) === 0 && (
          <div className="flex flex-col items-center justify-center gap-3 p-10 border bg-muted/30 rounded-md">
            <span className="text-3xl">📋</span>
            <div className="font-medium">Henüz hisse eklemediniz</div>
            <div className="text-xs text-muted-foreground max-w-md text-center">
              Sinyaller veya Tarama sayfasından aday hisseleri tek tıkla Watch listesine ekleyebilirsiniz.
            </div>
            <Button size="sm" onClick={() => setAddOpen(true)} className="mt-1">
              <Plus size={14} className="mr-1.5" />
              Hisse Ekle
            </Button>
          </div>
        )}
        {!isLoading && !isError && rowData.length === 0 && (data?.length ?? 0) > 0 && (
          <div className="flex flex-col items-center justify-center gap-2 p-8 border bg-muted/30 rounded-md text-sm text-muted-foreground">
            <span className="text-2xl">🔍</span>
            <div className="font-medium text-foreground">Filtreyle eşleşen hisse yok</div>
            <div className="text-xs opacity-80">
              Strateji / Statü / Konsensus filtrelerini gevşet veya arama metnini temizle.
            </div>
          </div>
        )}
        {!isLoading && !isError && rowData.length > 0 && (
          <div className={gridClass} style={{ height: 600, width: "100%" }}>
            <AgGridReact
              ref={gridRef}
              theme="legacy"
              columnDefs={columnDefs}
              defaultColDef={DEFAULT_COL_DEF}
              rowData={rowData}
              rowHeight={36}
              headerHeight={36}
              suppressCellFocus={false}
            />
          </div>
        )}
      </div>

      {/* Dialogs */}
      <AddRowDialog open={addOpen} onOpenChange={setAddOpen} />
      <EditNoteDialog
        row={editingRow}
        open={editNoteOpen}
        onOpenChange={(v) => {
          setEditNoteOpen(v);
          if (!v) setEditingRow(null);
        }}
      />
      <AlertDialog open={deleteConfirmOpen} onOpenChange={setDeleteConfirmOpen}>
        <AlertDialogContent>
          <AlertDialogHeader>
            <AlertDialogTitle>Silmek istediğinizden emin misiniz?</AlertDialogTitle>
            <AlertDialogDescription>
              <strong>{deletingRow?.symbol}</strong> —{" "}
              {deletingRow?.strategy === "minervini" ? "Minervini" : "Carr"}{" "}
              watchlist&apos;ten kaldırılacak. Bu işlem geri alınamaz.
            </AlertDialogDescription>
          </AlertDialogHeader>
          <AlertDialogFooter>
            <AlertDialogCancel>İptal</AlertDialogCancel>
            <AlertDialogAction
              className="bg-destructive hover:bg-destructive/90 text-destructive-foreground"
              onClick={() => {
                if (deletingRow) {
                  deleteMutation.mutate(
                    { symbol: deletingRow.symbol, strategy: deletingRow.strategy },
                    {
                      onSuccess: () => {
                        setDeleteConfirmOpen(false);
                        setDeletingRow(null);
                      },
                    }
                  );
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
