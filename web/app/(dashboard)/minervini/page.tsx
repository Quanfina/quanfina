"use client";

import { useCallback, useEffect, useMemo, useRef, useState } from "react";
import { useTheme } from "next-themes";
import { AgGridReact } from "ag-grid-react";
import type { CellValueChangedEvent } from "ag-grid-community";
import { useMinerviniStocks } from "@/hooks/use-minervini-stocks";
import { COL_DEFS, DEFAULT_COL_DEF } from "@/components/minervini/columns";
import { LIST_LABELS } from "@/types/minervini";
import type { MinerviniStock, ListType } from "@/types/minervini";

type FilterKey = ListType | "all";

const FILTERS: FilterKey[] = ["all", "buy", "focus", "on_deck", "watch"];

export default function MinerviniPage() {
  const { resolvedTheme } = useTheme();
  const [filter, setFilter] = useState<FilterKey>("all");
  const { data, isLoading, isError, error } = useMinerviniStocks();
  const [localRows, setLocalRows] = useState<MinerviniStock[]>([]);
  const gridRef = useRef<AgGridReact<MinerviniStock>>(null);

  useEffect(() => {
    if (data) setLocalRows(data);
  }, [data]);

  const rowData = useMemo(() => {
    if (filter === "all") return localRows;
    return localRows.filter((s) => s.list_type === filter);
  }, [localRows, filter]);

  const handleCellValueChanged = useCallback(
    (params: CellValueChangedEvent<MinerviniStock>) => {
      if (params.colDef.field === "list_type") {
        setLocalRows((prev) =>
          prev.map((r) =>
            r.symbol === params.data?.symbol
              ? { ...r, list_type: params.newValue as ListType }
              : r
          )
        );
        gridRef.current?.api.flashCells({
          rowNodes: [params.node!],
          flashDuration: 600,
          fadeDuration: 200,
        });
      }
    },
    []
  );

  const isDark = resolvedTheme === "dark";
  const gridClass = isDark ? "ag-theme-quartz-dark" : "ag-theme-quartz";

  return (
    <div className="flex flex-col h-full">
      <div className="px-6 py-3 border-b flex items-center gap-4">
        <h1 className="text-xl font-semibold tracking-tight">
          Minervini Tarama
        </h1>
        <div className="flex items-center gap-3 ml-4">
          <label className="text-sm font-medium text-muted-foreground">
            Liste:
          </label>
          <select
            value={filter}
            onChange={(e) => setFilter(e.target.value as FilterKey)}
            className="text-sm rounded border border-input bg-background px-3 py-1.5 focus:outline-none focus:ring-2 focus:ring-ring"
          >
            {FILTERS.map((f) => (
              <option key={f} value={f}>
                {LIST_LABELS[f]}
              </option>
            ))}
          </select>
          {localRows.length > 0 && (
            <span className="text-xs font-mono text-muted-foreground">
              {rowData.length} hisse
            </span>
          )}
        </div>
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
            Hata: {(error as Error)?.message ?? "Bilinmeyen hata"}
          </div>
        )}
        {!isLoading && !isError && (
          <div className={gridClass} style={{ height: 560, width: "100%" }}>
            <AgGridReact
              ref={gridRef}
              theme="legacy"
              columnDefs={COL_DEFS}
              defaultColDef={DEFAULT_COL_DEF}
              rowData={rowData}
              rowHeight={32}
              headerHeight={36}
              suppressCellFocus={false}
              onCellValueChanged={handleCellValueChanged}
            />
          </div>
        )}
      </div>
    </div>
  );
}
