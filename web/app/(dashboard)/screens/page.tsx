"use client";

/**
 * Tarama Sayfası — Sprint 4-bis.7 Adım Adım Yeniden İnşa (22 May 2026)
 *
 * Sn. Ferit talimat (22 May 2026): "küçük küçük başlıcaz" + "Minervini'ye
 * başlıyoruz, evren küçültmeden son aşamaya kadar".
 *
 * Adım 1 ✅: stage2_10p (Trend Template) eklendi.
 *   - Backend: /api/screens/stage2_10p (canlı, db_helpers.SCREENS_READY_9)
 *   - quanfina_math: passed=1 = Trend Template PASS (Mark resmi kuralı 8 koşul)
 *
 * Sıradaki adımlar (sırayla):
 *   Adım 2: power_play_ready (KARAR #467 — HTF Mark resmi kuralı)
 *   Adım 3: vcp_ready_high (KARAR #465 — Ready Score 70+)
 *   Adım 4: tight_low_vol_excellent (KARAR #466 — A+ Kalite)
 *   Adım 5+: Cup-Handle (yeni screen, Sprint 4-bis.7+)
 *
 * NOT: Migration 004-006 Cloud SQL'e UYGULANMAMIŞ → backend SQL'de
 * vcp_quality_score, vcp_ready_score, power_play_pass kolonları geçici
 * çıkarıldı. AÇIK KONU #70 ile bağlı, sonra çözülecek.
 */

import { useMemo, useRef, useState } from "react";
import { useGridTheme } from "@/hooks/use-grid-theme";
import { AgGridReact } from "ag-grid-react";
import type { ColDef, CellClassParams } from "ag-grid-community";
import { useScreenResults } from "@/hooks/use-screen-results";
import type { ScreenSlug, ScreenResultRow } from "@/types/screens";
import { MarkBadgeStrip } from "@/components/mark/MarkBadgeStrip";
import { ModBadge } from "@/components/mark/ModBadge";
import { useTradingMode, getModUiTheme } from "@/hooks/use-trading-mode";
import { SCREEN_CATEGORIES, SCREEN_CONDITIONS, CONDITION_SOURCE_LABEL } from "@/types/screens";
import { ChevronDown, ChevronRight } from "lucide-react";
import { GridLoadingOverlay } from "@/components/ag-grid/LoadingOverlay";
import { SymbolCellRenderer } from "@/components/watchlist/SymbolCellRenderer";
import { PivotBadgeCell } from "@/components/shared/PivotBadgeCell";
import { MONO, MONO_RIGHT } from "@/lib/grid-styles";
import { formatDateTR } from "@/lib/format-date";
import { fmtUsd } from "@/lib/format-currency";

// Grade chip renkleri (Sprint 4-bis.1b paten — KARAR ADAY #453)
const GRADE_STYLE: Record<string, { bg: string; color: string }> = {
  A: { bg: "#def2e5", color: "#0f5132" },
  B: { bg: "#d1e7dd", color: "#155724" },
  C: { bg: "#fff3cd", color: "#664d03" },
  D: { bg: "#ffc7c7", color: "#842029" },
};

// RS IBD renk bandı (TPR/RPR eşik — Mark Resmi Kural)
function getRsBandStyle(rs: number | null): { bg: string; color: string } {
  if (rs === null || rs === undefined) return { bg: "transparent", color: "inherit" };
  if (rs >= 95) return { bg: "#58bd7d", color: "#fff" };
  if (rs >= 89) return { bg: "#78ca96", color: "#fff" };
  if (rs >= 70) return { bg: "#eab308", color: "#1a1a1a" };
  return { bg: "#ff6a6a", color: "#fff" };
}

const SCREEN_SLUGS = Object.keys(SCREEN_CATEGORIES) as ScreenSlug[];
const DEFAULT_SLUG: ScreenSlug | null = SCREEN_SLUGS[0] ?? null;

export default function ScreensPage() {
  const { gridClass: themeGridClass } = useGridTheme();
  const gridRef = useRef<AgGridReact<ScreenResultRow>>(null);
  const [selectedSlug, setSelectedSlug] = useState<ScreenSlug | null>(DEFAULT_SLUG);
  // Paket 253 (27 May 2026): Defansif mod farkındalığı (Mark TTLC s.187)
  const tradingMode = useTradingMode();

  const resultsQ = useScreenResults(selectedSlug, 500);
  // Default açık — Sn. Ferit talimat (22 May 2026): koşullar her sayfa açılışta görünsün,
  // toggle butonu var, isteyen kapatır.
  const [conditionsOpen, setConditionsOpen] = useState(true);

  const columnDefs = useMemo<ColDef<ScreenResultRow>[]>(
    () => [
      {
        field: "symbol",
        headerName: "HİSSE",
        pinned: "left" as const,
        width: 90,
        minWidth: 80,
        cellRenderer: SymbolCellRenderer,
        cellStyle: {
          fontWeight: 700,
          fontFamily: "var(--font-jetbrains-mono, monospace)",
        },
      },
      {
        field: "grade",
        headerName: "NOT",
        width: 70,
        minWidth: 60,
        cellRenderer: (p: { value: string | null }) => {
          if (!p.value) return <span style={{ color: "#888" }}>—</span>;
          const s = GRADE_STYLE[p.value];
          if (!s) return <span>{p.value}</span>;
          return (
            <span
              style={{
                display: "inline-block",
                padding: "2px 10px",
                borderRadius: 9999,
                background: s.bg,
                color: s.color,
                fontWeight: 600,
                fontSize: 12,
                minWidth: 32,
                textAlign: "center",
              }}
            >
              {p.value}
            </span>
          );
        },
      },
      {
        field: "rs_ibd",
        headerName: "RS IBD",
        width: 90,
        minWidth: 80,
        cellStyle: (p: CellClassParams<ScreenResultRow, number>) => {
          const band = getRsBandStyle(p.value ?? null);
          return {
            ...MONO,
            background: band.bg,
            color: band.color,
            fontWeight: 600,
          };
        },
        valueFormatter: (p) => (p.value != null ? String(p.value) : "—"),
      },
      {
        field: "price",
        headerName: "FİYAT",
        width: 100,
        minWidth: 90,
        cellStyle: MONO_RIGHT,
        valueFormatter: (p) => fmtUsd(p.value as number | null),
      },
      {
        field: "scan_date",
        headerName: "TARİH",
        width: 110,
        minWidth: 100,
        cellStyle: MONO,
        // KARAR #471 paten: ISO YYYY-MM-DD → DD.MM.YYYY (Sinyaller + Journal ile tutarlı)
        valueFormatter: (p) => formatDateTR(p.value as string | null | undefined),
      },
      // KARAR ADAY #721 (24 May 2026): Mark Profile Badge Strip kolonu (DRY component).
      // Backend Cloud SQL Migration 004-007 uygulandıkça otomatik canlanır (AÇIK KONU #70).
      // Mevcut field undefined ise rozet gösterilmez (cell boş kalır).
      {
        headerName: "MARK PROFİLİ",
        minWidth: 260,
        flex: 1,
        sortable: false,
        filter: false,
        cellRenderer: (p: { data?: ScreenResultRow }) => {
          if (!p.data) return null;
          return (
            <MarkBadgeStrip
              signals={{
                vcp_quality_score: p.data.vcp_quality_score,
                vcp_ready_score: p.data.vcp_ready_score,
                power_play_pass: p.data.power_play_pass,
                tennis_ball_pattern: p.data.tennis_ball_pattern,
                volume_asymmetry_tier: p.data.volume_asymmetry_tier,
                code_33_pattern: p.data.code_33_pattern,
              }}
              density="compact"
              showEmpty={true}
            />
          );
        },
      },
      // KARAR #733 alt-paket (Paket 83): Pivot kolonu — Mark TLSMW Ch 10
      // (P81 Sinyaller + P82 Watchlist paten — Tarama'da da AL/Zayıf/Yakın/Altı)
      // Paket 158 (26 May 2026): document.createElement -> PivotBadgeCell DRY (React 19 fix)
      {
        headerName: "PIVOT",
        field: "pivot_status",
        width: 100,
        minWidth: 90,
        sortable: false,
        filter: false,
        cellRenderer: PivotBadgeCell,
      },
    ],
    []
  );

  const totalCount = resultsQ.data?.length ?? 0;

  return (
    <div className="flex flex-col h-full">
      {/* Header — başlık + ModBadge (Vizyon İLKE #10 — 10. UI nokta P246) */}
      <div className="px-6 py-3 border-b flex items-center justify-between">
        <h1 className="text-xl font-semibold tracking-tight">Tarama</h1>
        <ModBadge variant="compact" />
      </div>

      {/* Paket 253 (P261 refactor — getModUiTheme DRY helper P259):
          Defansif modda Tarama'da uyarı banner (Mark TTLC s.187). */}
      {(() => {
        const theme = getModUiTheme(tradingMode.mode);
        if (!theme || tradingMode.mode !== "defansif") return null;
        return (
          <div
            className="px-6 py-2 border-b text-xs flex items-center gap-2"
            style={{
              background: theme.background,
              borderColor: theme.borderColor,
              color: theme.color,
            }}
          >
            <span aria-hidden="true">{theme.emoji}</span>
            <span>
              <b>{theme.shortMessage}</b> Tarama sonuçları görsel inceleme amaçlı.
              AddTradeDialog override gerektirir.
            </span>
          </div>
        );
      })()}

      {/* Filter bar — başlık altında ayrı satır (Watchlist + Journal pateni) */}
      {SCREEN_SLUGS.length > 0 && (
        <div className="px-6 py-2 border-b flex flex-col gap-1">
          <div className="flex items-center gap-2 flex-wrap">
            <label htmlFor="screen-select" className="text-sm font-medium text-muted-foreground">
              Tarama:
            </label>
            <select
              id="screen-select"
              value={selectedSlug ?? ""}
              onChange={(e) => setSelectedSlug(e.target.value as ScreenSlug)}
              className="h-8 rounded-md border border-input bg-background px-3 text-sm focus:outline-none focus:ring-2 focus:ring-ring min-w-[280px]"
            >
              {SCREEN_SLUGS.map((slug, idx) => {
                const label = SCREEN_CATEGORIES[slug];
                const suffix =
                  slug === selectedSlug && totalCount > 0 ? ` (${totalCount})` : "";
                const num = String(idx + 1).padStart(2, "0");
                return (
                  <option key={slug} value={slug}>
                    {num}. {label}{suffix}
                  </option>
                );
              })}
            </select>
            {resultsQ.isLoading && (
              <span className="text-xs font-mono text-muted-foreground ml-2">
                Yükleniyor…
              </span>
            )}
          </div>
          {selectedSlug && SCREEN_CONDITIONS[selectedSlug] && SCREEN_CONDITIONS[selectedSlug].length > 0 && (
            <div>
              <button
                type="button"
                onClick={() => setConditionsOpen((v) => !v)}
                className="inline-flex items-center gap-1 text-xs font-medium text-muted-foreground hover:text-foreground transition-colors"
                aria-expanded={conditionsOpen}
              >
                {conditionsOpen ? <ChevronDown size={12} /> : <ChevronRight size={12} />}
                Koşullar ({SCREEN_CONDITIONS[selectedSlug].length})
              </button>
              {conditionsOpen && (
                <ul className="mt-1.5 ml-1 text-xs text-muted-foreground space-y-0.5 list-none">
                  {SCREEN_CONDITIONS[selectedSlug].map((cond, i) => (
                    <li key={i} className="leading-snug flex gap-2 flex-wrap">
                      <span
                        className="font-mono shrink-0 opacity-70"
                        style={{ fontVariantNumeric: "tabular-nums" }}
                      >
                        {String(i + 1).padStart(3, "0")}.
                      </span>
                      <span className="font-medium shrink-0" style={{
                        // 22 May 2026 — 3 kategori renk:
                        // mark (yeşil — Mark Resmi Kural kitap birebir)
                        // mark_ekstra (turuncu — Mark kitap tavsiyesi)
                        // quanfina (nötr — Quanfina Ek Filtre)
                        color: cond.source === "mark"
                          ? "var(--mtp-excellent)"
                          : cond.source === "mark_ekstra"
                            ? "#f97316" // turuncu (Tailwind orange-500)
                            : "var(--mtp-neutral)",
                      }}>
                        {CONDITION_SOURCE_LABEL[cond.source]} —
                      </span>
                      <span>{cond.text}</span>
                    </li>
                  ))}
                </ul>
              )}
            </div>
          )}
        </div>
      )}

      <div className="flex-1 px-6 py-4">
        {!selectedSlug && (
          <div className="flex flex-col items-center justify-center h-64 gap-3 text-center text-muted-foreground">
            <span className="text-3xl">🔍</span>
            <p className="text-sm">Henüz Tarama eklenmedi.</p>
            <p className="text-xs">Sprint 4-bis.7 sıralı ekleme: Cup-Handle → Pocket Pivot → Flat Base.</p>
          </div>
        )}

        {selectedSlug && resultsQ.isLoading && (
          <div className={`${themeGridClass} h-[600px] w-full`}>
            <GridLoadingOverlay />
          </div>
        )}

        {selectedSlug && resultsQ.isError && (
          <div className="flex items-center justify-center h-64 px-6">
            <div
              className="max-w-xl w-full p-4 border rounded-md"
              style={{ borderColor: "var(--mtp-danger)", background: "rgba(255, 80, 80, 0.06)" }}
              role="alert"
            >
              <div className="font-semibold mb-1" style={{ color: "var(--mtp-danger)" }}>
                ⚠️ Tarama verisi alınamadı
              </div>
              <div className="text-xs opacity-80">
                {(resultsQ.error as Error)?.message ?? "Bilinmeyen hata"}
              </div>
            </div>
          </div>
        )}

        {selectedSlug && !resultsQ.isLoading && !resultsQ.isError && totalCount === 0 && (
          <div className="flex flex-col items-center justify-center h-64 gap-3 text-center text-muted-foreground">
            <span className="text-3xl">📭</span>
            <p className="text-sm">Bu taramada bugün hisse yok.</p>
            <p className="text-xs">Scanner gece yeniden çalışacak.</p>
          </div>
        )}

        {selectedSlug && !resultsQ.isLoading && !resultsQ.isError && totalCount > 0 && (
          <div className={`${themeGridClass} h-[600px] w-full`}>
            <AgGridReact
              ref={gridRef}
              theme="legacy"
              columnDefs={columnDefs}
              defaultColDef={{ sortable: true, resizable: true, suppressMovable: false }}
              rowData={resultsQ.data ?? []}
              rowHeight={32}
              headerHeight={36}
              suppressCellFocus={false}
            />
          </div>
        )}
      </div>
    </div>
  );
}
