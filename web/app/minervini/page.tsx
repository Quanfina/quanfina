"use client";

import { useMemo, useState } from "react";
import { useTheme } from "next-themes";
import { AgGridReact } from "ag-grid-react";
import Link from "next/link";
import { ThemeToggle } from "@/components/layout/theme-toggle";
import { useMinerviniStocks } from "@/hooks/use-minervini-stocks";
import { COL_DEFS, DEFAULT_COL_DEF } from "@/components/minervini/columns";
import { LIST_LABELS } from "@/types/minervini";
import type { ListType } from "@/types/minervini";

type FilterKey = ListType | "all";

const FILTERS: FilterKey[] = ["all", "buy", "focus", "on_deck", "watch"];

export default function MinerviniPage() {
  const { resolvedTheme } = useTheme();
  const [filter, setFilter] = useState<FilterKey>("all");
  const { data, isLoading, isError, error } = useMinerviniStocks();

  const rowData = useMemo(() => {
    if (!data) return [];
    if (filter === "all") return data;
    return data.filter((s) => s.list_type === filter);
  }, [data, filter]);

  const isDark = resolvedTheme === "dark";
  const gridClass = isDark ? "ag-theme-quartz-dark" : "ag-theme-quartz";

  return (
    <div className="min-h-screen flex flex-col">
      <header className="px-6 py-4 border-b flex items-center justify-between">
        <div className="flex items-center gap-4">
          <Link
            href="/"
            className="text-sm text-muted-foreground hover:text-foreground transition-colors"
          >
            ← Ana Sayfa
          </Link>
          <h1 className="text-xl font-semibold tracking-tight">
            Minervini Tarama
          </h1>
        </div>
        <ThemeToggle />
      </header>

      <div className="px-6 py-3 border-b flex items-center gap-4">
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
        {data && (
          <span className="text-xs font-mono text-muted-foreground">
            {rowData.length} hisse
          </span>
        )}
      </div>

      <main className="flex-1 px-6 py-4">
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
              theme="legacy"
              columnDefs={COL_DEFS}
              defaultColDef={DEFAULT_COL_DEF}
              rowData={rowData}
              rowHeight={32}
              headerHeight={36}
              suppressCellFocus
            />
          </div>
        )}
      </main>
    </div>
  );
}
