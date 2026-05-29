"use client";

import { useScanFreshness } from "@/hooks/use-scan-freshness";
import { AlertTriangle } from "lucide-react";

/**
 * Tarama Veri Tazeliği Banner'ı (P375) — minervini_scans bayatsa kırmızı uyarı.
 *
 * Tetik: Sn. Ferit "14 gün eski veri" acısı. Scanner Cloud Run'da durursa günlük
 * tarama atlanır; kullanıcı farkında olmadan haftalarca eski screener verisiyle
 * trade edebilir. (Canlı fiyat/yfinance taze kalır ama tarama sonuçları bayatlar.)
 *
 * Davranış:
 *   - is_stale=false (taze)        → banner GİZLİ (sessiz, DbStatusBanner pateni)
 *   - is_stale=true (>4 gün eski)  → KIRMIZI banner: son tarama tarihi + gün
 *   - yükleniyor / data yok        → gizli (flicker önleme)
 *
 * Bağlı: api/main.py /api/scan/freshness, useScanFreshness hook.
 * Felsefe: Sn. Ferit gördüğü tarama verisinin ne kadar taze olduğunu bilmeli
 * (objektif ayna — somut tarih + gün, yağcılık değil).
 */
export function DataFreshnessBanner() {
  const { data } = useScanFreshness();

  // Taze veya henüz yüklenmedi → banner gizle (flicker önleme)
  if (!data || !data.is_stale) return null;

  return (
    <div
      className="flex items-center gap-2 px-4 py-2 border-b text-sm"
      style={{
        background: "color-mix(in srgb, var(--mtp-danger) 10%, transparent)",
        borderColor: "var(--mtp-danger)",
        color: "var(--mtp-danger)",
      }}
      role="alert"
    >
      <AlertTriangle size={14} className="shrink-0" />
      <span className="font-medium">Tarama verisi bayat</span>
      <span className="opacity-80">
        — Son tarama {data.latest_scan_date ?? "yok"}
        {data.calendar_days_old != null ? ` (${data.calendar_days_old} gün önce)` : ""}.
        Günlük tarama atlanmış; bayat screener verisiyle trade kararı vermeyin.
      </span>
    </div>
  );
}
