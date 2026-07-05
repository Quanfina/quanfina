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
 * Davranış (B1-03, 05 Tem 2026 — çok-tablo):
 *   - any_stale=false (hepsi taze) → banner GİZLİ (sessiz, DbStatusBanner pateni)
 *   - any_stale=true (herhangi biri bayat) → KIRMIZI banner: backend kaynak-adlı mesaj
 *     (yalnız Sektör rotasyonu bayatsa onu adlandırır — minervini'nin taze tarihini
 *     göstermez, kafa karışmaz)
 *   - yükleniyor / data yok        → gizli (flicker önleme)
 *
 * B1-03 öncesi API `any_stale` döndürmez → `is_stale` (minervini) fallback (eski api
 * deploy'u beklerken geriye-uyum). Kaynak-adlı mesaj api/main.py `_freshness_message`
 * TEK kaynaktan gelir (DRY); banner sadece render eder.
 *
 * Bağlı: api/main.py /api/scan/freshness, useScanFreshness hook.
 * Felsefe: Sn. Ferit gördüğü tarama verisinin ne kadar taze olduğunu bilmeli
 * (objektif ayna — somut tarih + gün, yağcılık değil).
 */
export function DataFreshnessBanner() {
  const { data } = useScanFreshness();

  // any_stale yeni alan (B1-03); eski api deploy'u beklerken undefined → is_stale fallback
  const stale = data ? (data.any_stale ?? data.is_stale) : false;

  // Taze veya henüz yüklenmedi → banner gizle (flicker önleme)
  if (!data || !stale) return null;

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
        — {data.message} Bayat screener verisiyle trade kararı vermeyin.
      </span>
    </div>
  );
}
