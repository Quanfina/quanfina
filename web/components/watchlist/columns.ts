import type { ColDef, ValueFormatterParams, CellClassParams } from "ag-grid-community";
import type { WatchlistRow } from "@/types/watchlist";
import { StatusBadge } from "./StatusBadge";
// ConsensusBadge import kaldirildi (KARAR ADAY 21 May 2026): Konsensus kavrami kaldirildi,
// her strateji ayri satir kanon. Komponent dosyasi (ConsensusBadge.tsx) silinmedi -
// Kural #18 pasif oge mantigi.
import { StrategyCellRenderer } from "./StrategyCellRenderer";
import { SetupCellRenderer } from "./SetupCellRenderer";
import { SymbolCellRenderer } from "./SymbolCellRenderer";
import { MarkBadgeCell } from "./MarkBadgeCell";
import { PivotBadgeCell } from "@/components/shared/PivotBadgeCell";
import { RsRatingBadge } from "@/components/shared/RsRatingBadge";
import { TermHeaderComponent } from "@/components/terminology/TermHeaderComponent";
// KARAR #492 (20 May 2026 ~16:45): DRY hijyen - yerel MONO -> @/lib/grid-styles
// Sinyaller + Hisse Tarama paterniyle eşitleme. Davranış: fontSize 13px global ->
// 12px MONO_RIGHT (Sinyaller exact). Tüm tablolarda fiyat 12px tutarlı.
import { MONO_RIGHT as MONO, rsBackground } from "@/lib/grid-styles";
import { fmtUsd } from "@/lib/format-currency";
import { formatDayLabel } from "@/lib/format-date";

function fmtPrice(p: ValueFormatterParams<WatchlistRow>) {
  return fmtUsd(p.value as number | null);
}

// P416 (31 May 2026): relativeDate -> formatDayLabel DRY (lib/format-date)
// Eski "3 gün önce" yerine "Pzt/Salı..." kısa format — Sn. Ferit Focus List
// patenli hızlı okuma. lib/format-date.ts'de tek doğruluk kaynağı.
function relativeDate(iso: string): string {
  return formatDayLabel(iso);
}

// P420 (31 May 2026): rsBackground lib/grid-styles.ts'e taşındı (DRY tek kaynak)
// — Tarama (screens) sayfası da aynı helper'ı kullanıyordu, çiftleme riskini
// workflow tasarım denetimi yakaladı. Yerel kopya kaldırıldı, import edildi.

export const COL_DEFS: ColDef<WatchlistRow>[] = [
  {
    field: "symbol",
    headerName: "HİSSE",
    pinned: "left",
    width: 90,
    minWidth: 80,
    cellRenderer: SymbolCellRenderer,
  },
  {
    field: "strategy",
    headerName: "STRATEJİ",
    headerComponent: TermHeaderComponent,
    headerComponentParams: { termKey: "watchlist_status" },
    width: 120,
    minWidth: 110,
    cellRenderer: StrategyCellRenderer,
  },
  {
    field: "status",
    headerName: "STATÜ",
    width: 100,
    minWidth: 90,
    cellRenderer: StatusBadge,
  },
  // Paket 412 (30 May 2026 — Kural #28 Canlı Veri Önce uygulama):
  // FİYAT kolonu UI'dan kaldırıldı (stale snapshot — eklendiği anda kaydedilen
  // fiyat, paper trading kararı için yanıltıcı). Backend `watchlist.price` field
  // DB'de korundu (geriye uyum + denetim trail), sadece UI render'dan çıkarıldı.
  // Sn. Ferit FİYAT $875.40 vs CANLI $211.14 (NVDA 17 gün stale) kafa
  // karışıklığı yaşadı → tek doğru fiyat CANLI $ (aşağıda current_price).
  {
    field: "price",
    headerName: "FİYAT (kaldırıldı)",
    hide: true,  // P412: UI'dan gizli, DB'de saklı (geriye uyum)
    suppressColumnsToolPanel: true,
    valueFormatter: fmtPrice,
    cellStyle: MONO,
  },
  // Paket 154 (26 May 2026): Watchlist canlı fiyat — paper trading sabah bakışı
  // useStockQuotes 60s refetch, yfinance + 5dk backend cache. MOCK fiyat değil.
  {
    field: "current_price",
    headerName: "CANLI $",
    width: 95,
    minWidth: 85,
    valueFormatter: fmtPrice,
    cellStyle: (p: CellClassParams<WatchlistRow, number>) => ({
      ...MONO,
      color: p.value == null ? "var(--muted-foreground)" : "var(--mtp-neutral)",
      fontWeight: 600,
    }),
    headerTooltip: "Anlık piyasa fiyatı (yfinance, 60s yenileme)",
  },
  {
    field: "change_pct_today",
    headerName: "GÜN %",
    width: 85,
    minWidth: 75,
    valueFormatter: (p) => {
      const v = p.value as number | null | undefined;
      if (v == null) return "—";
      return (v >= 0 ? "+" : "") + v.toFixed(2) + "%";
    },
    cellStyle: (p: CellClassParams<WatchlistRow, number>) => {
      if (p.value == null) return { ...MONO, color: "var(--muted-foreground)" };
      return {
        ...MONO,
        color: p.value >= 0 ? "var(--mtp-excellent)" : "var(--mtp-danger)",
        fontWeight: 600,
      };
    },
    headerTooltip: "Bugünkü yüzde değişim (son kapanış vs dünkü)",
  },
  {
    field: "setup_type",
    headerName: "SETUP",
    width: 160,
    minWidth: 140,
    cellRenderer: SetupCellRenderer,
  },
  // KARAR #733 alt-paket (Paket 98, 26 May 2026): RS Rating kategori rozet
  // Mark TLSMW Ch 3-5 / IBD canon — sadece sayı değil, kategori rozet
  // (LEADER/STRONG/AVERAGE/LAGGARD) ile Sn. Ferit anında "Top 20%" tarar.
  // Paket 158 (26 May 2026): document.createElement -> RsRatingBadge DRY (React 19 fix)
  {
    field: "rs_rating",
    headerName: "RS",
    headerComponent: TermHeaderComponent,
    headerComponentParams: { termKey: "rs_ibd" },
    width: 100,
    minWidth: 85,
    cellStyle: (p: CellClassParams<WatchlistRow, number>) => ({
      ...MONO,
      background: rsBackground(p.value ?? null),
      display: "flex",
      alignItems: "center",
      justifyContent: "space-between",
      paddingRight: "4px",
    }),
    cellRenderer: RsRatingBadge,
  },
  // P416 (31 May 2026): Bağıl Hacim — Mark TLSMW Bol. 6 "Hacim teyit" canon.
  // Bugün / 50-gün ortalama hacim oranı. Eşikler kanon ⚠️ AÇIK KONU adayı —
  // şu an Quanfina iç kararı (≥1.5 patlama / 0.7-1.0 sönük / aksi nötr).
  // Sn. Ferit'in talimat referansı: Focus List "v/50" sütunu — yüksek ROI.
  // Veri kaynağı: backend /api/watchlist enrichment (yfinance + 5dk cache).
  {
    field: "relative_volume",
    headerName: "BAĞIL HCM",
    width: 100,
    minWidth: 85,
    valueFormatter: (p: ValueFormatterParams<WatchlistRow, number>) =>
      p.value == null ? "—" : `${p.value.toFixed(2)}×`,
    cellStyle: (p: CellClassParams<WatchlistRow, number>) => {
      const v = p.value;
      let color = "var(--muted-foreground)";
      if (v != null) {
        if (v >= 1.5) color = "var(--mtp-excellent)";  // patlama (yeşil)
        else if (v < 0.7) color = "var(--mtp-danger)";  // sönük (kırmızı)
        else if (v >= 1.0) color = "var(--mtp-neutral)";  // normal üstü (mavi)
      }
      return { ...MONO, color, fontWeight: v != null && v >= 1.5 ? 600 : 400 };
    },
    headerTooltip: "Bağıl Hacim: bugün / 50-gün ortalama. ≥1.5 patlama, <0.7 sönük (Mark TLSMW Bol. 6).",
  },
  // KONSENSUS kolonu kaldirildi (KARAR ADAY 21 May 2026): her strateji ayri satir
  // kanon, konsensus sayim noise.
  {
    field: "added_date",
    headerName: "EKLENME",
    width: 90,
    minWidth: 70,
    valueFormatter: (p) => p.value ? relativeDate(p.value as string) : "—",
    cellStyle: { fontSize: 12, color: "var(--muted-foreground)" },
    headerTooltip: "Eklenme zamanı — son 6 gün gün adı, eski tarihler DD.MM.YYYY",
  },
  // KARAR ADAY #724 (24 May 2026): Mark Profili rozetleri (DRY MarkBadgeStrip)
  // Backend Migration 004-007 sonrasi otomatik canli. MOCK feed (_STOCK_MARK_SIGNALS).
  {
    headerName: "MARK PROFİLİ",
    minWidth: 240,
    flex: 1,
    sortable: false,
    filter: false,
    cellRenderer: MarkBadgeCell,
  },
  // KARAR #733 alt-paket (Paket 82): Pivot kolonu — Mark TLSMW Ch 10 (P81 Sinyaller paten)
  // Paket 153 (26 May 2026): document.createElement -> PivotBadgeCell React component
  // (React 19 crash fix — "Objects are not valid as a React child" — DRY shared).
  {
    headerName: "PIVOT",
    field: "pivot_status",
    width: 100,
    sortable: false,
    filter: false,
    cellRenderer: PivotBadgeCell,
  },
  // P537 (19 Haz 2026): Pocket Pivot on-deck sinyali (Minervini s.167 + Kacher/Morales).
  // Bazın içinde kurumsal güç dönüşü — Minervini "on-deck kanıtı" (s.165, 218). Sadece
  // detected GOOD/CANDIDATE → rozet; yoksa "—". Detay /hisse PocketPivotCard.
  {
    headerName: "POCKET",
    field: "pocket_pivot",
    width: 95,
    minWidth: 80,
    sortable: false,
    filter: false,
    valueFormatter: (p: ValueFormatterParams<WatchlistRow, string>) =>
      p.value === "GOOD" ? "🟢 PP" : p.value === "CANDIDATE" ? "🟡 PP" : "—",
    cellStyle: (p: CellClassParams<WatchlistRow, string>) => {
      const v = p.value;
      let color = "var(--muted-foreground)";
      if (v === "GOOD") color = "var(--mtp-excellent)";
      else if (v === "CANDIDATE") color = "var(--mtp-warning, #d97706)";
      return { ...MONO, color, fontWeight: v ? 600 : 400 };
    },
    headerTooltip: "Pocket Pivot (Minervini s.167 + Kacher/Morales): bazın içinde kurumsal güç dönüşü, on-deck sinyali. 🟢 GOOD (10-DMA dönüşü) / 🟡 CANDIDATE (extended). Detay: hisse sayfası.",
  },
  {
    field: "note",
    headerName: "NOT",
    width: 140,
    minWidth: 100,
    valueFormatter: (p) => (p.value as string | null) ?? "",
    cellStyle: { fontSize: 12 },
  },
];

export const DEFAULT_COL_DEF: ColDef<WatchlistRow> = {
  sortable: true,
  resizable: true,
  filter: false,
  suppressMovable: false,
  autoHeight: false,
};
