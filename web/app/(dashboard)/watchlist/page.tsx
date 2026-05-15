"use client";

import { useMemo, useRef, useState } from "react";
import { useTheme } from "next-themes";
import { AgGridReact } from "ag-grid-react";
import { useWatchlist } from "@/hooks/use-watchlist";
import { WatchlistFilters } from "@/components/watchlist/WatchlistFilters";
import { COL_DEFS, DEFAULT_COL_DEF } from "@/components/watchlist/columns";
import type { WatchlistRow } from "@/types/watchlist";

export default function WatchlistPage() {
  const { resolvedTheme } = useTheme();
  const [strategy, setStrategy] = useState("all");
  const [status, setStatus] = useState("all");
  const [minConsensus, setMinConsensus] = useState(0);
  const [search, setSearch] = useState("");
  const { data, isLoading, isError, error } = useWatchlist();
  const gridRef = useRef<AgGridReact<WatchlistRow>>(null);

  const rowData = useMemo(() => {
    let rows: WatchlistRow[] = data ?? [];
    if (strategy !== "all") rows = rows.filter((r) => r.strategy === strategy);
    if (status !== "all")   rows = rows.filter((r) => r.status === status);
    if (minConsensus > 0)   rows = rows.filter((r) => r.consensus_count >= minConsensus);
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
      <div className="px-6 py-3 border-b flex flex-col gap-0.5">
        <h1 className="text-xl font-semibold tracking-tight">Watchlist</h1>
        <p className="text-sm text-muted-foreground">
          Tüm stratejiler — çapraz görünüm (İLKE #44 / KARAR #350)
        </p>
      </div>

      <div className="px-6 py-3 border-b">
        <WatchlistFilters
          strategy={strategy}         onStrategyChange={setStrategy}
          status={status}             onStatusChange={setStatus}
          minConsensus={minConsensus} onMinConsensusChange={setMinConsensus}
          search={search}             onSearchChange={setSearch}
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
              columnDefs={COL_DEFS}
              defaultColDef={DEFAULT_COL_DEF}
              rowData={rowData}
              rowHeight={36}
              headerHeight={36}
              suppressCellFocus={false}
            />
          </div>
        )}
      </div>
    </div>
  );
}
