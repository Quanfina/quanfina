"use client";

/**
 * Sprint 4-bis.1b — Hisse Tarama (Screens) Sayfası
 *
 * Mimari: notebook/Sprint_4_bis_Mimari_Kararlar.md
 *   KARAR ADAY #453 (Theme), #455 (AG Grid ColDef Pattern)
 *
 * 3 Dalga Markets360 ham tarama sentezi:
 *   - chip-A/B/C/D renk bandı (CSS hex doğrudan kanıt)
 *   - RS IBD renk bandı (TPR grade → RPR eşik kanıt)
 *   - pinned-left symbol (pinnedColumn 299 frequency)
 *   - headerTooltip (21 frequency)
 *   - Sayaç motifi (Gösterilen X / Toplam Y)
 *
 * Felsefe: Screen (eleme filtresi) != Recipe (strateji siralamasi)
 *          Bu sayfa: "Piyasadaki firsatlar neler?" sorusunu cevaplar.
 *          Strateji-bagimsiz keşif.
 */

import { useMemo, useRef, useState } from "react";
import { useTheme } from "next-themes";
import { AgGridReact } from "ag-grid-react";
import type { ColDef, ICellRendererParams } from "ag-grid-community";
import { toast } from "sonner";
import { useScreenMeta } from "@/hooks/use-screen-meta";
import { useScreenResults } from "@/hooks/use-screen-results";
import { useAddWatchlistRow } from "@/hooks/use-watchlist-mutations";
import type { ScreenSlug, ScreenResultRow } from "@/types/screens";
import { SCREEN_CATEGORIES } from "@/types/screens";

// === Grade chip renkleri (CSS hex kanıt: chip-long #def2e5, chip-short #ffc7c7) ===
// KARAR ADAY #453 — Quanfina Theme Sistemi
const GRADE_STYLE: Record<string, { bg: string; color: string; label: string }> = {
  A: { bg: "#def2e5", color: "#0f5132", label: "A" },  // koyu yeşil (RS 90+ destekli)
  B: { bg: "#d1e7dd", color: "#155724", label: "B" },  // açık yeşil
  C: { bg: "#fff3cd", color: "#664d03", label: "C" },  // sarı (warning tonu)
  D: { bg: "#ffc7c7", color: "#842029", label: "D" },  // kırmızı
};

// === RS IBD renk bandı (TPR → RPR eşik kanıt: A=90+, B=80-90, vs.) ===
function getRsBandStyle(rs: number | null): { bg: string; color: string } {
  if (rs === null || rs === undefined) return { bg: "transparent", color: "inherit" };
  if (rs >= 95) return { bg: "#58bd7d", color: "#fff" };   // koyu yeşil — elit
  if (rs >= 89) return { bg: "#78ca96", color: "#fff" };   // yeşil — güçlü
  if (rs >= 70) return { bg: "#eab308", color: "#1a1a1a" }; // sarı — orta
  return { bg: "#ff6a6a", color: "#fff" };                 // kırmızı — zayıf
}

// === cellRenderer'lar ===

function GradeCell(p: ICellRendererParams<ScreenResultRow>) {
  if (!p.value) return <span style={{ color: "#888" }}>—</span>;
  const style = GRADE_STYLE[p.value as string];
  if (!style) return <span>{p.value}</span>;
  return (
    <span
      style={{
        display: "inline-block",
        padding: "2px 10px",
        borderRadius: "9999px",
        background: style.bg,
        color: style.color,
        fontWeight: 600,
        fontSize: "12px",
        minWidth: "32px",
        textAlign: "center",
      }}
    >
      {style.label}
    </span>
  );
}

function RsIbdCell(p: ICellRendererParams<ScreenResultRow>) {
  if (p.value === null || p.value === undefined) return <span style={{ color: "#888" }}>—</span>;
  const style = getRsBandStyle(p.value as number);
  return (
    <span
      style={{
        display: "inline-block",
        padding: "2px 8px",
        borderRadius: "4px",
        background: style.bg,
        color: style.color,
        fontWeight: 500,
        fontSize: "12px",
        minWidth: "36px",
        textAlign: "center",
      }}
    >
      {p.value}
    </span>
  );
}

function PassedCell(p: ICellRendererParams<ScreenResultRow>) {
  if (p.value === 1) return <span style={{ color: "#58bd7d", fontWeight: 600 }}>✓</span>;
  if (p.value === 0) return <span style={{ color: "#ff6a6a" }}>✗</span>;
  return <span style={{ color: "#888" }}>—</span>;
}

// === AG Grid ColDef (KARAR ADAY #455 standart pattern) ===

const COL_DEFS: ColDef<ScreenResultRow>[] = [
  {
    field: "symbol",
    headerName: "Sembol",
    headerTooltip: "Hisse sembolü (NYSE/NASDAQ/ARCA)",
    pinned: "left",
    width: 110,
    cellStyle: { fontWeight: 600 },
  },
  {
    field: "grade",
    headerName: "Grade",
    headerTooltip: "TPR letter grade (A/B/C/D) — Minervini kavramı",
    width: 100,
    cellRenderer: GradeCell,
    cellStyle: { display: "flex", alignItems: "center", padding: "0 8px" },
  },
  {
    field: "rs_ibd",
    headerName: "RS IBD",
    headerTooltip: "IBD Relative Strength (0-100). 95+ elit, 89+ güçlü, 70-89 orta",
    width: 110,
    type: "numericColumn",
    cellRenderer: RsIbdCell,
    cellStyle: { display: "flex", alignItems: "center", padding: "0 8px" },
  },
  {
    field: "price",
    headerName: "Fiyat",
    headerTooltip: "Son scan_date fiyatı (USD)",
    width: 110,
    type: "numericColumn",
    valueFormatter: (p) =>
      p.value !== null && p.value !== undefined
        ? `$${Number(p.value).toFixed(2)}`
        : "—",
  },
  {
    field: "passed",
    headerName: "Passed",
    headerTooltip: "Trend Template 8/8 (1=PASS, 0=FAIL)",
    width: 90,
    type: "numericColumn",
    cellRenderer: PassedCell,
    cellStyle: { display: "flex", alignItems: "center", justifyContent: "center" },
  },
  {
    field: "scan_date",
    headerName: "Scan Tarihi",
    headerTooltip: "scanner.py son çalıştırma tarihi (YYYY-MM-DD)",
    width: 130,
  },
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
    "stage2_10p" // default: en kalabalik ekran (727 satir gercek veri)
  );
  const gridRef = useRef<AgGridReact<ScreenResultRow>>(null);
  const [bulkAddInProgress, setBulkAddInProgress] = useState(false);

  const metaQ = useScreenMeta();
  const resultsQ = useScreenResults(selectedSlug, 500);
  const refetchResults = resultsQ.refetch;
  const isRefetchingResults = resultsQ.isFetching && !resultsQ.isLoading;
  const addWatchlistRow = useAddWatchlistRow();

  // Sprint 4-bis.1c — Multi-select Watch'a ekleme (KARAR ADAY #454 sonner toast)
  const handleAddSelectedToWatch = async () => {
    const selectedRows = gridRef.current?.api?.getSelectedRows() ?? [];
    if (selectedRows.length === 0) {
      toast.warning("Önce hisse seçin", { description: "Tablodan en az 1 hisse seçmelisin (Ctrl/Shift+tıkla)." });
      return;
    }

    setBulkAddInProgress(true);
    const tId = toast.loading(`${selectedRows.length} hisse Watch'a ekleniyor...`);

    let success = 0;
    let already = 0;
    let failed = 0;
    const failedSymbols: string[] = [];

    for (const row of selectedRows) {
      if (!row.symbol) continue;
      try {
        await addWatchlistRow.mutateAsync({
          symbol: row.symbol,
          strategy: "minervini",  // Screens sayfası minervini ekol — KARAR #351 (Screen/Recipe ayrımı)
          status: "watch",         // 4 liste hiyerarşisi en alttan başla (Minervini Konu 5)
          note: `screens/${selectedSlug} (RS=${row.rs_ibd ?? "—"})`,
        });
        success++;
      } catch (e) {
        const msg = (e as Error).message ?? "";
        if (msg.toLowerCase().includes("already") || msg.includes("409")) {
          already++;
        } else {
          failed++;
          failedSymbols.push(row.symbol);
        }
      }
    }

    setBulkAddInProgress(false);
    toast.dismiss(tId);

    if (success > 0 && failed === 0) {
      toast.success(`${success} hisse Watch'a eklendi`, {
        description: already > 0 ? `(${already} zaten Watch'taydı)` : undefined,
      });
    } else if (success > 0 && failed > 0) {
      toast.warning(`${success} eklendi, ${failed} başarısız`, {
        description: `Hata: ${failedSymbols.slice(0, 5).join(", ")}${failedSymbols.length > 5 ? "…" : ""}`,
      });
    } else if (failed > 0) {
      toast.error(`${failed} hisse eklenemedi`, {
        description: failedSymbols.slice(0, 5).join(", "),
      });
    } else if (already > 0) {
      toast.info(`${already} hisse zaten Watch'taydı`, { description: "Yeni ekleme yapılmadı." });
    }

    gridRef.current?.api?.deselectAll();
  };

  const selectedMeta = useMemo(
    () => metaQ.data?.find((m) => m.slug === selectedSlug),
    [metaQ.data, selectedSlug]
  );

  const totalCount = resultsQ.data?.length ?? 0;
  // Sayaç motifi — gelecek: filter sonrası gösterilen satır sayısı (AG Grid api)
  const shownCount = totalCount;

  const themeClass =
    resolvedTheme === "dark" ? "ag-theme-quartz-dark" : "ag-theme-quartz";

  return (
    <div className="p-6 space-y-4">
      <div className="space-y-2">
        <h1 className="text-2xl font-bold">Hisse Tarama (Screens)</h1>
        <p className="text-sm text-muted-foreground">
          22 tarama: 8 Ready (saf SQL) + 7 Parse (text-parse) + 6 Scan-Diff (Self-JOIN) + 1 Deferred.
          Strateji-bağımsız fırsat keşfi.
        </p>
      </div>

      {/* Dropdown + Meta info + Sayaç */}
      <div className="flex flex-col gap-2 sm:flex-row sm:items-center sm:flex-wrap">
        <label htmlFor="screen-select" className="text-sm font-medium">
          Tarama:
        </label>
        <select
          id="screen-select"
          value={selectedSlug ?? ""}
          onChange={(e) => setSelectedSlug(e.target.value as ScreenSlug)}
          className="px-3 py-2 border rounded-md bg-background min-w-[300px]"
          disabled={metaQ.isLoading}
        >
          {/* Sprint 4-bis.2 — 3 kategori (ready/parse/deferred) optgroup ile gruplu */}
          <optgroup label="✅ Ready (8) — Saf SQL filtre">
            {metaQ.data?.filter((m) => m.category === "ready").map((meta) => (
              <option key={meta.slug} value={meta.slug}>
                {SCREEN_CATEGORIES[meta.slug]} — {meta.label}
              </option>
            ))}
          </optgroup>
          <optgroup label="⚙️ Parse (7) — Confirmations/Violations text-parse">
            {metaQ.data?.filter((m) => m.category === "parse").map((meta) => (
              <option key={meta.slug} value={meta.slug}>
                {SCREEN_CATEGORIES[meta.slug]} — {meta.label}
              </option>
            ))}
          </optgroup>
          <optgroup label="📊 Scan-Diff (6) — Önceki scan karşılaştırma (Self-JOIN)">
            {metaQ.data?.filter((m) => m.category === "diff").map((meta) => (
              <option key={meta.slug} value={meta.slug}>
                {SCREEN_CATEGORIES[meta.slug]} — {meta.label}
              </option>
            ))}
          </optgroup>
          <optgroup label="⏳ Deferred (1) — Sprint 4-bis.4">
            {metaQ.data?.filter((m) => m.category === "deferred").map((meta) => (
              <option key={meta.slug} value={meta.slug} disabled>
                {SCREEN_CATEGORIES[meta.slug]} — {meta.label} (henüz hazır değil)
              </option>
            ))}
          </optgroup>
        </select>

        {selectedMeta && (
          <code
            className="text-xs text-muted-foreground bg-muted/50 px-2 py-1 rounded"
            title="SQL filtresi (Notebook_C1 SCREENS tuple)"
          >
            {selectedMeta.filter_summary}
          </code>
        )}

        {/* Sayaç motifi (3 dalga JS kanıt: RowsPerPageDropdown 7) */}
        <span className="ml-auto text-sm font-medium">
          {resultsQ.isLoading ? (
            <span className="text-muted-foreground">Yükleniyor…</span>
          ) : resultsQ.isError ? (
            <span className="text-destructive">❌ Hata</span>
          ) : (
            <span>
              Gösterilen: <strong>{shownCount}</strong>{" "}
              <span className="text-muted-foreground">/ Toplam: {totalCount}</span>
            </span>
          )}
        </span>

        {/* Add to Watch (Sprint 4-bis.1c) */}
        <button
          type="button"
          onClick={handleAddSelectedToWatch}
          disabled={bulkAddInProgress || totalCount === 0}
          className="px-3 py-2 rounded-md bg-primary text-primary-foreground hover:opacity-90 disabled:opacity-50 disabled:cursor-not-allowed text-sm font-medium"
          title="Seçili hisseleri Minervini Watch listesine ekle"
        >
          {bulkAddInProgress ? "Ekleniyor..." : "+ Watch'a Ekle"}
        </button>
      </div>

      {/* Hata mesajı — DB unreachable senaryosu için actionable banner */}
      {resultsQ.isError && (
        <div className="p-4 border border-destructive bg-destructive/10 rounded-md" role="alert">
          <div className="font-semibold text-destructive mb-1">
            ⚠️ Tarama sonucu alınamadı
          </div>
          <div className="text-xs mb-2 opacity-80">
            {(resultsQ.error as Error).message}
          </div>
          <div className="text-xs mb-3 opacity-70">
            Olası sebep: Cloud SQL erişilemez (instance durmuş veya IP whitelist eski).
            GCP Console → SQL → instance durum kontrol et. Meta liste yine çalışır
            (dropdown&apos;da kategorileri görebilirsin).
          </div>
          <button
            type="button"
            onClick={() => refetchResults()}
            disabled={isRefetchingResults}
            className="px-3 py-1.5 rounded-md border bg-background text-sm hover:bg-muted disabled:opacity-50 disabled:cursor-not-allowed"
          >
            {isRefetchingResults ? "Tekrar deneniyor..." : "Tekrar Dene"}
          </button>
        </div>
      )}

      {/* Empty state */}
      {!resultsQ.isLoading && !resultsQ.isError && totalCount === 0 && (
        <div className="p-4 border bg-muted/50 rounded-md text-sm text-muted-foreground">
          Bu taramaya uygun hisse bulunamadı. (Son scan_date için filtre eşleşmedi.
          Olası sebep: grade kolonu NULL — AÇIK KONU #69, scanner.py grade güncellemesi
          gerekli.)
        </div>
      )}

      {/* AG Grid */}
      {totalCount > 0 && (
        <div className={`${themeClass} h-[600px] w-full`}>
          <AgGridReact<ScreenResultRow>
            ref={gridRef}
            rowData={resultsQ.data ?? []}
            columnDefs={COL_DEFS}
            defaultColDef={DEFAULT_COL_DEF}
            animateRows
            rowHeight={40}
            rowSelection="multiple"
            suppressRowClickSelection={false}
            // KARAR ADAY #454: Add to Watch multi-select (1c canlı)
          />
        </div>
      )}
    </div>
  );
}
