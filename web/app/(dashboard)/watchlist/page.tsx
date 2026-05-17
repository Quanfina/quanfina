"use client";

import { useMemo, useRef, useState } from "react";
import { useTheme } from "next-themes";
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

export default function WatchlistPage() {
  const { resolvedTheme } = useTheme();
  const [strategy, setStrategy] = useState("all");
  const [status, setStatus] = useState("all");
  const [minConsensus, setMinConsensus] = useState(0);
  const [search, setSearch] = useState("");
  const { data, isLoading, isError, error } = useWatchlist();
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

  const rowData = useMemo(() => {
    let rows: WatchlistRow[] = data ?? [];
    if (strategy !== "all") rows = rows.filter((r) => r.strategy === strategy);
    if (status !== "all") rows = rows.filter((r) => r.status === status);
    if (minConsensus > 0) rows = rows.filter((r) => r.consensus_count >= minConsensus);
    if (search) {
      const q = search.toUpperCase();
      rows = rows.filter(
        (r) => r.symbol.includes(q) || (r.setup_type ?? "").toUpperCase().includes(q)
      );
    }
    return [...rows].sort(
      (a, b) => b.consensus_count - a.consensus_count || a.symbol.localeCompare(b.symbol)
    );
  }, [data, strategy, status, minConsensus, search]);

  const isDark = resolvedTheme === "dark";
  const gridClass = isDark ? "ag-theme-quartz-dark" : "ag-theme-quartz";

  return (
    <div className="flex flex-col h-full">
      <div className="px-6 py-3 border-b flex items-center justify-between">
        <div>
          <h1 className="text-xl font-semibold tracking-tight">Watchlist</h1>
          <p className="text-sm text-muted-foreground">
            Tüm stratejiler — çapraz görünüm (İLKE #44 / KARAR #350)
          </p>
        </div>
        <Button size="sm" onClick={() => setAddOpen(true)}>
          <Plus size={14} className="mr-1.5" />
          Hisse Ekle
        </Button>
      </div>

      <div className="px-6 py-3 border-b">
        <WatchlistFilters
          strategy={strategy}
          onStrategyChange={setStrategy}
          status={status}
          onStatusChange={setStatus}
          minConsensus={minConsensus}
          onMinConsensusChange={setMinConsensus}
          search={search}
          onSearchChange={setSearch}
          totalRows={data?.length ?? 0}
          filteredRows={rowData.length}
        />
      </div>

      <div className="flex-1 px-6 py-4">
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
            Hata: {(error as Error)?.message ?? "Watchlist verisi alınamadı"}
          </div>
        )}
        {!isLoading && !isError && (
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
              watchlist'ten kaldırılacak. Bu işlem geri alınamaz.
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
