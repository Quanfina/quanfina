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
import "@/lib/ag-grid-setup"; // AG Grid modül kaydı (Paket 355 — bundle split)
import { AgGridReact } from "ag-grid-react";
import type { ColDef, CellClassParams, ICellRendererParams } from "ag-grid-community";
import { useScreenResults } from "@/hooks/use-screen-results";
import { useGridColumnState } from "@/hooks/use-grid-column-state";
import type { ScreenSlug, ScreenResultRow } from "@/types/screens";
import { MarkBadgeStrip } from "@/components/mark/MarkBadgeStrip";
import { ModBadge } from "@/components/mark/ModBadge";
import { useTradingMode, getModUiTheme } from "@/hooks/use-trading-mode";
import { SCREEN_CATEGORIES, SCREEN_CONDITIONS, CONDITION_SOURCE_LABEL, SCREEN_GROUPS } from "@/types/screens";
import { ChevronDown, ChevronRight } from "lucide-react";
import { GridLoadingOverlay } from "@/components/ag-grid/LoadingOverlay";
import { SymbolCellRenderer } from "@/components/watchlist/SymbolCellRenderer";
import { PivotBadgeCell } from "@/components/shared/PivotBadgeCell";
import { RsRatingBadge } from "@/components/shared/RsRatingBadge";
import { TermHeaderComponent } from "@/components/terminology/TermHeaderComponent";
import { MONO_RIGHT, rsBackground } from "@/lib/grid-styles";
import { formatDateTR, formatDayLabel } from "@/lib/format-date";
import { fmtUsd } from "@/lib/format-currency";

// Grade chip renkleri (Sprint 4-bis.1b paten — KARAR ADAY #453)
const GRADE_STYLE: Record<string, { bg: string; color: string }> = {
  A: { bg: "#def2e5", color: "#0f5132" },
  B: { bg: "#d1e7dd", color: "#155724" },
  C: { bg: "#fff3cd", color: "#664d03" },
  D: { bg: "#ffc7c7", color: "#842029" },
};

// P420 (31 May 2026 — Sn. Ferit "izleme listesindeki tasarıma uydur"):
// RS arka plan bandı + RsRatingBadge İzleme Listesi ile BİREBİR aynı tasarım
// dili. rsBackground helper'ı çiftleme önlemek için lib/grid-styles.ts'e taşındı
// (workflow tasarım denetimi DRY çiftleme riskini yakaladı — iki sayfa tek kaynak).

const SCREEN_SLUGS = Object.keys(SCREEN_CATEGORIES) as ScreenSlug[];
const DEFAULT_SLUG: ScreenSlug | null = SCREEN_SLUGS[0] ?? null;

export default function ScreensPage() {
  const { gridClass: themeGridClass } = useGridTheme();
  const gridRef = useRef<AgGridReact<ScreenResultRow>>(null);
  // Sprint 4.8 — Column Preferences: sütun düzeni (sıra/görünür/genişlik/sort) persist
  const gridColumnState = useGridColumnState("quanfina-screens-cols");
  const [selectedSlug, setSelectedSlug] = useState<ScreenSlug | null>(DEFAULT_SLUG);
  // Paket 253 (27 May 2026): Defansif mod farkındalığı (Mark TTLC s.187)
  const tradingMode = useTradingMode();

  const resultsQ = useScreenResults(selectedSlug, 500);
  // Default açık — Sn. Ferit talimat (22 May 2026): koşullar her sayfa açılışta görünsün,
  // toggle butonu var, isteyen kapatır.
  const [conditionsOpen, setConditionsOpen] = useState(true);

  const columnDefs = useMemo<ColDef<ScreenResultRow>[]>(
    () => [
      // P419 (31 May 2026 — Sn. Ferit "bu listenin tasarımını sevmedim"):
      // KOMPAKT redesign. MARK PROFİLİ + TARİH gizlendi (Migration 004-007 boş +
      // hep aynı gün), CARR STAGE rozeti eklendi, kolon genişlikleri sıkıştırıldı.
      // Sn. Ferit görsel hiyerarşi + bilgi yoğunluğu istedi (Markets360 Focus
      // List patenli kompakt görünüm — clean-room disiplini ile soyutlama).
      {
        // P420: İzleme Listesi ile birebir — width 90/80, inline cellStyle
        // kaldırıldı (SymbolCellRenderer zaten font-bold + mono uyguluyor,
        // çiftlemeydi). Watchlist columns.ts symbol kolonu ile aynı.
        field: "symbol",
        headerName: "HİSSE",
        pinned: "left" as const,
        width: 90,
        minWidth: 80,
        cellRenderer: SymbolCellRenderer,
      },
      {
        // P422 (31 May 2026 — Sn. Ferit "NOT satırı ney"): "NOT" başlığı yanıltıcıydı
        // (İzleme Listesi'nde NOT = serbest metin notu; burada temel grade). Ayrıca eski
        // tooltip yanlış "Mark TradeGrader KARAR #445" diyordu — bu TradeGrader DEĞİL,
        // scanner_helpers._compute_grade (EPS Q/Q + Satış Q/Q). Başlık "TEMEL" + doğru
        // tooltip. "D" hem zayıf hem veri-yok demek (Kural #28 — AÇIK KONU backend ayrımı).
        field: "grade",
        headerName: "TEMEL",
        headerTooltip:
          "Temel Grade (Mark TLSMW EPS+Satış): A=EPS>%40&Satış>%25 / B=EPS>%25&Satış>%15 / " +
          "C=EPS>%20&Satış>%10 / D=zayıf VEYA veri yok. Teknik tarama PASS + temel kalite.",
        width: 75,
        minWidth: 60,
        cellRenderer: (p: ICellRendererParams<ScreenResultRow, string>) => {
          if (!p.value) return <span style={{ color: "#888" }}>—</span>;
          // P423 (Kural #28): grade='D' + grade_no_data → EPS/Satış verisi YOK
          // (gerçekten zayıf değil). Kırmızı "D" yerine gri nötr "D?" + tooltip —
          // paper trading kararı "zayıf hisse mi, veri mi eksik" karışmasın.
          if (p.value === "D" && p.data?.grade_no_data) {
            return (
              <span
                title="EPS/Satış Q/Q verisi yok — zayıf temel DEĞİL, sadece veri eksik (Kural #28)"
                style={{
                  display: "inline-block",
                  padding: "2px 8px",
                  borderRadius: 9999,
                  background: "color-mix(in srgb, var(--muted-foreground) 15%, transparent)",
                  color: "var(--muted-foreground)",
                  fontWeight: 600,
                  fontSize: 12,
                  minWidth: 32,
                  textAlign: "center",
                }}
              >
                D?
              </span>
            );
          }
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
        // P420: RS kolonu İzleme Listesi ile birebir aynı — RsRatingBadge
        // component (sayı + L/S/A/↓ kategori rozeti) + color-mix soft arka plan +
        // TermHeaderComponent tooltip (termKey "rs_ibd"). Watchlist columns.ts ile
        // tek tasarım dili (DRY shared component, raw hex kaldırıldı).
        field: "rs_ibd",
        headerName: "RS",
        headerComponent: TermHeaderComponent,
        headerComponentParams: { termKey: "rs_ibd" },
        width: 100,
        minWidth: 85,
        // P421: agNumberColumnFilter kaldırıldı (floatingFilter ile birlikte —
        // KARAR #456 negatif tescil). İzleme Listesi RS kolonu da filtresiz.
        cellStyle: (p: CellClassParams<ScreenResultRow, number>) => ({
          // P420: İzleme Listesi RS cellStyle ile birebir (MONO_RIGHT + color-mix
          // band + flex space-between — RsRatingBadge sağ hizalı rozeti yerleştirir).
          ...MONO_RIGHT,
          background: rsBackground(p.value ?? null),
          display: "flex",
          alignItems: "center",
          justifyContent: "space-between",
          paddingRight: "4px",
        }),
        cellRenderer: RsRatingBadge,
      },
      {
        field: "price",
        headerName: "FİYAT",
        width: 90,
        minWidth: 80,
        // P421: agNumberColumnFilter kaldırıldı (floatingFilter ile — KARAR #456 pasif)
        cellStyle: MONO_RIGHT,
        valueFormatter: (p) => fmtUsd(p.value as number | null),
        headerTooltip: "Son tarama fiyatı (snapshot — canlı değil)",
      },
      // P419: CARR STAGE rozeti yeni kolon (ScreenResultRow.carr_stage zaten var,
      // sadece gösterilmiyordu). Mark Stage 4 "Avoid" Sn. Ferit'in göreceği yer.
      {
        field: "carr_stage",
        headerName: "STAGE",
        headerTooltip: "Carr/Weinstein Stage (1=Base, 2=Advancing, 3=Distribution, 4=Decline)",
        width: 80,
        minWidth: 70,
        sortable: false,
        filter: false,
        cellRenderer: (p: { value: number | null | undefined }) => {
          if (p.value == null) return <span style={{ color: "#888" }}>—</span>;
          // P420: Rozet şekli İzleme Listesi MarkBadgeCell tasarım diliyle hizalandı
          // (borderRadius 9999→4 köşeli, opacity 0.18→0.10 yumuşak tint, 1px border
          // color+'33'). Carr Stage zaten watchlist MARK PROFİLİ içinde bu şekilde
          // görünüyor — Stage 4 "Avoid" iki sayfada aynı görsel dil.
          const meta: Record<number, { label: string; tint: string; color: string }> = {
            1: { label: "Stage 1", tint: "rgba(75,156,211,0.10)", color: "var(--mtp-neutral)" },
            2: { label: "Stage 2", tint: "rgba(40,167,69,0.10)", color: "var(--mtp-excellent)" },
            3: { label: "Stage 3", tint: "rgba(245,158,11,0.10)", color: "#92400E" },
            4: { label: "Stage 4", tint: "rgba(220,53,69,0.10)", color: "var(--mtp-danger)" },
          };
          const m = meta[p.value];
          if (!m) return <span style={{ color: "#888" }}>—</span>;
          return (
            <span
              style={{
                display: "inline-block",
                padding: "2px 8px",
                borderRadius: 4,
                background: m.tint,
                color: m.color,
                border: `1px solid color-mix(in srgb, ${m.color} 33%, transparent)`,
                fontWeight: 600,
                fontSize: 11,
                fontFamily: "var(--font-jetbrains-mono, monospace)",
              }}
              title="Carr/Weinstein Stage Analysis — 1=Base, 2=Advancing, 3=Distribution, 4=Decline"
            >
              {m.label}
            </span>
          );
        },
      },
      // KARAR #733 alt-paket (Paket 83): Pivot kolonu — Mark TLSMW Ch 10
      // (P81 Sinyaller + P82 Watchlist paten — Tarama'da da AL/Zayıf/Yakın/Altı)
      // P420: width 90→100 İzleme Listesi PIVOT footprint ile birebir.
      {
        headerName: "PIVOT",
        field: "pivot_status",
        width: 100,
        sortable: false,
        filter: false,
        cellRenderer: PivotBadgeCell,
      },
      // P556 (20 Haz 2026): GRUP kolonu — hissenin sektörü lider mi? (Minervini s.95 +
      // O'Neil 'L' "leading stock in leading group"). sector_rotation RS rank. LAGGING =
      // "yalnız kurt" riski (güçlü hisse, zayıf sektör). Veri yoksa "—" (Kural #28 dürüst).
      {
        headerName: "GRUP",
        field: "group_tier",
        width: 96,
        minWidth: 80,
        sortable: true,
        filter: false,
        headerTooltip: "Sektör RS (Minervini s.95 + O'Neil 'L'): Lider/Nötr/Zayıf. Zayıf = yalnız kurt riski.",
        cellRenderer: (p: { value: string | null | undefined; data?: ScreenResultRow }) => {
          if (!p.value) return <span style={{ color: "#888" }}>—</span>;
          const meta: Record<string, { label: string; tint: string; color: string }> = {
            LEADING: { label: "Lider", tint: "rgba(40,167,69,0.10)", color: "var(--mtp-excellent)" },
            NEUTRAL: { label: "Nötr", tint: "rgba(245,158,11,0.10)", color: "#92400E" },
            LAGGING: { label: "Zayıf", tint: "rgba(220,53,69,0.10)", color: "var(--mtp-danger)" },
          };
          const m = meta[p.value];
          if (!m) return <span style={{ color: "#888" }}>—</span>;
          const sektor = p.data?.sector ?? "";
          return (
            <span
              style={{
                display: "inline-block",
                padding: "2px 8px",
                borderRadius: 4,
                background: m.tint,
                color: m.color,
                border: `1px solid color-mix(in srgb, ${m.color} 33%, transparent)`,
                fontWeight: 600,
                fontSize: 11,
                fontFamily: "var(--font-jetbrains-mono, monospace)",
              }}
              title={`${sektor} — ${m.label} sektör (Minervini s.95 + O'Neil 'L'). ${
                p.value === "LAGGING" ? "Yalnız kurt riski: güçlü hisse, zayıf sektör." : ""
              }`}
            >
              {m.label}
            </span>
          );
        },
      },
      // P419 (31 May 2026): TARİH ve MARK PROFİLİ gizlendi.
      // - scan_date hep aynı gün (tek tarama, tüm satırlar aynı tarih) → sayfa
      //   üstüne tek bilgi olarak yazılıyor (DataFreshnessBanner P375 zaten var)
      // - MARK PROFİLİ Migration 004-007 uygulanmadığı için tüm satırlarda boş →
      //   yer israfı. Migration tamamlanınca hide=false yapılır
      {
        field: "scan_date",
        headerName: "TARİH",
        hide: true,
        suppressColumnsToolPanel: true,
        valueFormatter: (p) => formatDateTR(p.value as string | null | undefined),
      },
      {
        headerName: "MARK PROFİLİ",
        hide: true,
        suppressColumnsToolPanel: true,
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
    ],
    []
  );

  const totalCount = resultsQ.data?.length ?? 0;
  // P419: TARİH kolonu gizlendi (hep aynı gün) — son tarama tarihi başlığa
  const lastScanDate = resultsQ.data?.[0]?.scan_date ?? null;
  const lastScanLabel = lastScanDate ? formatDayLabel(lastScanDate) : null;

  return (
    <div className="flex flex-col h-full">
      {/* Header — başlık + ModBadge (Vizyon İLKE #10 — 10. UI nokta P246) */}
      <div className="px-6 py-3 border-b flex items-center justify-between gap-3 flex-wrap">
        <div className="flex items-baseline gap-3">
          <h1 className="text-xl font-semibold tracking-tight">Tarama</h1>
          {/* P419: TARİH kolonu kaldırıldı → tek özet bilgi başlığa.
              {sayım} satır • Son tarama: {Pzt/Salı/05.06.2026} */}
          {totalCount > 0 && lastScanLabel && (
            <span className="text-xs text-muted-foreground tabular-nums">
              {totalCount} satır • Son tarama: <b className="text-foreground">{lastScanLabel}</b>
            </span>
          )}
        </div>
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
              {(() => {
                // P524: strateji-bazlı optgroup. Global numara grup sırasını takip eder.
                let n = 0;
                return SCREEN_GROUPS.map((group) => (
                  <optgroup key={group.label} label={group.label}>
                    {group.slugs.map((slug) => {
                      n += 1;
                      const label = SCREEN_CATEGORIES[slug];
                      const suffix =
                        slug === selectedSlug && totalCount > 0 ? ` (${totalCount})` : "";
                      const num = String(n).padStart(2, "0");
                      return (
                        <option key={slug} value={slug}>
                          {num}. {label}{suffix}
                        </option>
                      );
                    })}
                  </optgroup>
                ));
              })()}
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
                            : cond.source === "carr"
                              ? "#8b5cf6" // mor (Carr — Mark'tan ayrı strateji, P502)
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
              {...gridColumnState}
              columnDefs={columnDefs}
              // P421 (31 May 2026 — Sn. Ferit "İzleme Listesi gibi temiz"):
              // floatingFilter satırı KALDIRILDI. Sn. Ferit "bu boşluklar ne işe
              // yarıyor" → her kolon altındaki boş filtre input'ları görsel kalabalık
              // yaratıyordu, İzleme Listesi'nde yok. KARAR #456 (Screener Filter
              // Operator) NEGATİF TESCİL (Kural #18) — pasif. Watchlist DEFAULT_COL_DEF
              // ile birebir: filter:false + autoHeight:false. Filtreleme üstteki
              // tarama dropdown'ı ile yapılır (her slug zaten ayrı filtrelenmiş liste).
              defaultColDef={{
                sortable: true,
                resizable: true,
                filter: false,
                suppressMovable: false,
                autoHeight: false,
              }}
              rowData={resultsQ.data ?? []}
              rowHeight={36}
              headerHeight={36}
              suppressCellFocus={false}
              getRowStyle={(params) => {
                // P420: İzleme Listesi paten — Defansif modda RS<70 satırlar solgun
                // (Mark TLSMW "Leaders First" — zayıf RS dikkat dağıtmasın).
                if (
                  tradingMode.mode === "defansif" &&
                  params.data &&
                  (params.data.rs_ibd ?? 0) < 70
                ) {
                  return { opacity: 0.45 };
                }
                return undefined;
              }}
            />
          </div>
        )}
      </div>
    </div>
  );
}
