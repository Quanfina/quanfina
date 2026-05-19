"use client";

/**
 * Sprint 4-bis.1b — Hisse Tarama (Screens) Sayfası
 *
 * Kaynak: notebook/Notebook_C1_Sprint_QuickStart.md
 *         notebook/Notebook_B6_AdimlarKarar.md (KARAR #340 + #349 + #351)
 *
 * Felsefe: Screen (eleme filtresi) != Recipe (strateji siralamasi)
 *          Bu sayfa: "Piyasadaki firsatlar neler?" sorusunu cevaplar.
 *          Strateji-bagimsiz keşif.
 */

import { useMemo, useState } from "react";
import { useTheme } from "next-themes";
import { AgGridReact } from "ag-grid-react";
import type { ColDef } from "ag-grid-community";
import { useScreenMeta } from "@/hooks/use-screen-meta";
import { useScreenResults } from "@/hooks/use-screen-results";
import type { ScreenSlug, ScreenResultRow } from "@/types/screens";
import { SCREEN_CATEGORIES } from "@/types/screens";

const COL_DEFS: ColDef<ScreenResultRow>[] = [
  { field: "symbol", headerName: "Sembol", pinned: "left", width: 110 },
  { field: "grade", headerName: "Grade", width: 90 },
  { field: "rs_ibd", headerName: "RS IBD", width: 100, type: "numericColumn" },
  {
    field: "price",
    headerName: "Fiyat ($)",
    width: 110,
    type: "numericColumn",
    valueFormatter: (p) =>
      p.value !== null && p.value !== undefined
        ? `$${Number(p.value).toFixed(2)}`
        : "—",
  },
  { field: "passed", headerName: "Passed", width: 90, type: "numericColumn" },
  { field: "scan_date", headerName: "Scan Tarihi", width: 130 },
];

const DEFAULT_COL_DEF: ColDef = {
  sortable: true,
  filter: true,
  resizable: true,
  floatingFilter: false,
};

export default function ScreensPage() {
  const { resolvedTheme } = useTheme();
  const [selectedSlug, setSelectedSlug] = useState<ScreenSlug | null>(
    "stage2_10p" // default: en kalabalik ekran (727 satir, gercek veriyle PASS)
  );

  const metaQ = useScreenMeta();
  const resultsQ = useScreenResults(selectedSlug, 500);

  const selectedMeta = useMemo(
    () => metaQ.data?.find((m) => m.slug === selectedSlug),
    [metaQ.data, selectedSlug]
  );

  const rowCount = resultsQ.data?.length ?? 0;
  const themeClass =
    resolvedTheme === "dark" ? "ag-theme-quartz-dark" : "ag-theme-quartz";

  return (
    <div className="p-6 space-y-4">
      <div className="space-y-2">
        <h1 className="text-2xl font-bold">Hisse Tarama (Screens)</h1>
        <p className="text-sm text-muted-foreground">
          8 hazır tarama (Sprint 4-bis.1b). Strateji-bağımsız fırsat keşfi.
        </p>
      </div>

      {/* Dropdown + Meta info */}
      <div className="flex flex-col gap-2 sm:flex-row sm:items-center">
        <label htmlFor="screen-select" className="text-sm font-medium">
          Tarama seç:
        </label>
        <select
          id="screen-select"
          value={selectedSlug ?? ""}
          onChange={(e) => setSelectedSlug(e.target.value as ScreenSlug)}
          className="px-3 py-2 border rounded-md bg-background min-w-[280px]"
          disabled={metaQ.isLoading}
        >
          {metaQ.data?.map((meta) => (
            <option key={meta.slug} value={meta.slug}>
              {SCREEN_CATEGORIES[meta.slug]} — {meta.label}
            </option>
          ))}
        </select>

        {selectedMeta && (
          <span
            className="text-xs text-muted-foreground font-mono"
            title="SQL filtresi"
          >
            {selectedMeta.filter_summary}
          </span>
        )}

        <span className="ml-auto text-sm font-medium">
          {resultsQ.isLoading
            ? "Yükleniyor…"
            : resultsQ.isError
            ? "❌ Hata"
            : `${rowCount} sonuç`}
        </span>
      </div>

      {/* Hata mesajı */}
      {resultsQ.isError && (
        <div className="p-4 border border-destructive bg-destructive/10 rounded-md text-sm">
          API hata: {(resultsQ.error as Error).message}
        </div>
      )}

      {/* Empty state */}
      {!resultsQ.isLoading && !resultsQ.isError && rowCount === 0 && (
        <div className="p-4 border bg-muted/50 rounded-md text-sm text-muted-foreground">
          Bu taramaya uygun hisse bulunamadı. (Son scan_date için filtre eşleşmedi.)
        </div>
      )}

      {/* AG Grid */}
      {rowCount > 0 && (
        <div className={`${themeClass} h-[600px] w-full`}>
          <AgGridReact<ScreenResultRow>
            rowData={resultsQ.data ?? []}
            columnDefs={COL_DEFS}
            defaultColDef={DEFAULT_COL_DEF}
            animateRows
            rowHeight={36}
          />
        </div>
      )}
    </div>
  );
}
