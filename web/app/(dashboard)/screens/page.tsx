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
import { GridLoadingOverlay } from "@/components/ag-grid/LoadingOverlay";

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

// KARAR #465 — VCP Ready Score 0-100 sayisal renk bandı
function VcpReadyCell(p: ICellRendererParams<ScreenResultRow>) {
  const score = p.value as number | null | undefined;
  if (score === null || score === undefined) return <span style={{ color: "#888" }}>—</span>;
  // Renk bandı: 70+ koyu yeşil, 50-69 sarı, <50 gri
  let style: { bg: string; color: string };
  if (score >= 70) style = { bg: "#0f5132", color: "#fff" };
  else if (score >= 50) style = { bg: "#eab308", color: "#1a1a1a" };
  else style = { bg: "#6b7280", color: "#fff" };
  return (
    <span
      style={{
        display: "inline-block",
        padding: "2px 10px",
        borderRadius: "4px",
        background: style.bg,
        color: style.color,
        fontWeight: 600,
        fontSize: "12px",
        minWidth: "40px",
        textAlign: "center",
      }}
      title="VCP Ready Score (KARAR #465) — 50 puan Inside Day + 30 puan V-Dry + 20 puan tight closes. 70+ = Ready filtre."
    >
      {score}
    </span>
  );
}

// KARAR #466 — VCP Kalite Skoru rozeti (EXCELLENT/PASS/None)
function VcpQualityCell(p: ICellRendererParams<ScreenResultRow>) {
  const score = p.value as "EXCELLENT" | "PASS" | null | undefined;
  if (!score) return <span style={{ color: "#888" }}>—</span>;
  const style = score === "EXCELLENT"
    ? { bg: "#0f5132", color: "#fff", label: "A+ Kalite" }    // koyu yeşil — Mark canon "%50 alti"
    : { bg: "#d1e7dd", color: "#155724", label: "Olgun" };    // açık yeşil — Brandon PASS filtre
  return (
    <span
      style={{
        display: "inline-block",
        padding: "2px 10px",
        borderRadius: "9999px",
        background: style.bg,
        color: style.color,
        fontWeight: 600,
        fontSize: "11px",
        textAlign: "center",
      }}
      title={score === "EXCELLENT"
        ? "VCP A+ Kalite — Mark canon: hacim 50d MA × %50 altı (en sıkı pivot anı)"
        : "VCP Olgun — Brandon muhafazakar filtre: hacim 50d MA × %70 altı"}
    >
      {style.label}
    </span>
  );
}

// === AG Grid ColDef (KARAR ADAY #455 standart pattern) ===

// KARAR #466 + #465 + #467 — VCP slug'larinda Kalite + Ready + Power Play kolonlari
const VCP_SLUGS = new Set<string>([
  "tight_low_volume", "tight_low_vol_excellent",
  "vcp_ready_high", "power_play_ready"
]);

// KARAR #467 — Power Play (HTF) rozet
function PowerPlayCell(p: ICellRendererParams<ScreenResultRow>) {
  const pass = p.value as boolean | null | undefined;
  if (pass === null || pass === undefined) return <span style={{ color: "#888" }}>—</span>;
  if (pass) {
    return (
      <span
        style={{
          display: "inline-block",
          padding: "2px 10px",
          borderRadius: "9999px",
          background: "#f59e0b",  // amber/turuncu — momentum gold
          color: "#1a1a1a",
          fontWeight: 700,
          fontSize: "11px",
          textAlign: "center",
        }}
        title="Power Play / High Tight Flag (KARAR #467 Mark canon) — POLE 8 hafta %100+ yükseliş + FLAG 2-6 hafta %10-25 düzeltme. Trade Like a Stock Market Wizard Bölüm 10."
      >
        ⚡ Power Play
      </span>
    );
  }
  return <span style={{ color: "#888", fontSize: "11px" }}>—</span>;
}

function buildColDefs(slug: string | null): ColDef<ScreenResultRow>[] {
  const isVcpSlug = slug ? VCP_SLUGS.has(slug) : false;
  if (!isVcpSlug) return [];
  return [
    {
      field: "vcp_quality_score" as keyof ScreenResultRow,
      headerName: "Kalite",
      headerTooltip:
        "VCP Kalite Skoru (KARAR #466) — EXCELLENT: Mark canon %50 alti (A+); " +
        "PASS: Brandon muhafazakar filtre %70 alti",
      width: 110,
      cellRenderer: VcpQualityCell,
      cellStyle: { display: "flex", alignItems: "center", justifyContent: "center" },
    },
    {
      field: "vcp_ready_score" as keyof ScreenResultRow,
      headerName: "Ready",
      headerTooltip:
        "VCP Ready Score 0-100 (KARAR #465 Minervini Uzmani) — 50 puan Inside Day + " +
        "30 V-Dry + 20 tight. 70+ Ready, 50-69 Yaklasan, <50 Hazir degil.",
      width: 90,
      type: "numericColumn",
      cellRenderer: VcpReadyCell,
      cellStyle: { display: "flex", alignItems: "center", justifyContent: "center" },
    },
    {
      field: "power_play_pass" as keyof ScreenResultRow,
      headerName: "HTF",
      headerTooltip:
        "Power Play / High Tight Flag (KARAR #467) — Mark canon: POLE 8 hafta %100+ " +
        "yukselis + FLAG 2-6 hafta %10-25 duzeltme. Trade Like a Stock Market Wizard Bolum 10.",
      width: 120,
      cellRenderer: PowerPlayCell,
      cellStyle: { display: "flex", alignItems: "center", justifyContent: "center" },
    },
  ];
}

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

  // KARAR #466 — VCP slug'larinda Kalite kolonu acilir, base COL_DEFS sonuna eklenir
  const dynamicColDefs = useMemo(
    () => [...COL_DEFS, ...buildColDefs(selectedSlug)],
    [selectedSlug]
  );

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
          25 tarama: 12 Ready (saf SQL) + 7 Parse (text-parse) + 6 Scan-Diff (Self-JOIN).
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
          {/* Sprint 4-bis.5 KARAR #467 — Ready 11 → 12 (power_play_ready eklendi) */}
          <optgroup label="✅ Ready (12) — Saf SQL filtre">
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
          {/* KARAR #461 (Sprint 4-bis.4): Deferred kategorisi boşaldı (tight_low_volume Ready'ye taşındı).
              30 gün sonra Kural #18 pasif öğe çıkarma adayı — optgroup tamamen kaldırılabilir. */}
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

      {/* Sayfa-bağlamlı empty state (KARAR #463 — Markets360 4 mesaj varyantından ileri) */}
      {!resultsQ.isLoading && !resultsQ.isError && totalCount === 0 && (
        <div className="flex flex-col items-center justify-center gap-3 p-8 border bg-muted/30 rounded-md text-sm text-muted-foreground">
          <span className="text-2xl">🔍</span>
          <div className="font-medium text-foreground">
            {selectedSlug === "tight_low_volume"
              ? "Bu taramaya uygun VCP setup'ı yok"
              : "Bu tarama için son scan_date'te eşleşme yok"}
          </div>
          <div className="text-xs opacity-80 max-w-md text-center">
            {selectedSlug === "tight_low_volume"
              ? "scanner.py tight_low_vol_pass kolonu hesaplanmamış olabilir (KARAR #461 — Sn. Ferit DB ayağa kalkınca migration 002 + tarama tetik)."
              : "Olası sebep: grade kolonu NULL (AÇIK KONU #69 — scanner.py grade güncellemesi) veya bu eşik son güne uymadı."}
          </div>
          <button
            type="button"
            onClick={() => refetchResults()}
            disabled={isRefetchingResults}
            className="mt-1 px-3 py-1.5 rounded-md border bg-background text-xs hover:bg-muted disabled:opacity-50"
          >
            {isRefetchingResults ? "Tekrar deneniyor..." : "Tekrar Dene"}
          </button>
        </div>
      )}

      {/* AG Grid — Skeleton loading (KARAR #463 B2 uygulaması) + gerçek veri */}
      {(resultsQ.isLoading || totalCount > 0) && (
        <div className={`${themeClass} h-[600px] w-full`}>
          {resultsQ.isLoading ? (
            <GridLoadingOverlay />
          ) : (
            <AgGridReact<ScreenResultRow>
              ref={gridRef}
              rowData={resultsQ.data ?? []}
              columnDefs={dynamicColDefs}
              defaultColDef={DEFAULT_COL_DEF}
              animateRows
              rowHeight={40}
              rowSelection="multiple"
              suppressRowClickSelection={false}
              // KARAR ADAY #454: Add to Watch multi-select (1c canlı)
              // KARAR #466: VCP slug'larinda Kalite kolonu (dynamicColDefs)
            />
          )}
        </div>
      )}
    </div>
  );
}
