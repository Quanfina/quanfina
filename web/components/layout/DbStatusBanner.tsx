"use client";

import { useDbStatus } from "@/hooks/use-db-status";
import { AlertTriangle, RefreshCw } from "lucide-react";

/**
 * DB Durum Banner'ı — Cloud SQL erişilemez ise MOCK veri uyarısı gösterir.
 *
 * Tetik: KARAR ADAY (22 May 2026) — Sn. Ferit talimat "Mockdan gerçek veriye
 * geçmeyi ayarla" → Seçenek B "MOCK koru + frontend banner".
 *
 * Davranış:
 *   - DB bağlı (db_connected=true)        → banner GİZLİ (sessiz, gerçek veri)
 *   - DB erişilemez (db_connected=false)  → SARI banner: "MOCK MOD"
 *   - /api/health hata                    → KIRMIZI banner: "API erişilemez"
 *   - Polling: 30 sn otomatik refresh
 *
 * Bağlı: api/main.py /api/health endpoint, useDbStatus hook.
 * Felsefe: Sn. Ferit gördüğü verinin gerçek mi mock mu olduğunu HER ZAMAN bilmeli.
 */

export function DbStatusBanner() {
  const { data, isError, refetch, isFetching } = useDbStatus();

  // DB bağlı + veri var → banner gizle (varsayılan, sessiz)
  if (data?.db_connected === true) return null;

  // /api/health hata → KIRMIZI banner
  if (isError) {
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
        <AlertTriangle size={14} />
        <span className="font-medium">API erişilemez</span>
        <span className="opacity-80">— FastAPI sunucusu cevap vermiyor (port 8000).</span>
        <button
          type="button"
          onClick={() => refetch()}
          disabled={isFetching}
          className="ml-auto inline-flex items-center gap-1 px-2 py-0.5 rounded border border-current hover:bg-black/5 text-xs disabled:opacity-50"
        >
          <RefreshCw size={11} className={isFetching ? "animate-spin" : ""} />
          Tekrar Dene
        </button>
      </div>
    );
  }

  // DB erişilemez (db_connected=false) → SARI banner: MOCK MOD
  if (data && data.db_connected === false) {
    return (
      <div
        className="flex items-center gap-2 px-4 py-2 border-b text-sm"
        style={{
          background: "color-mix(in srgb, var(--mtp-neutral) 12%, transparent)",
          borderColor: "var(--mtp-neutral)",
          color: "var(--mtp-neutral)",
        }}
        role="status"
      >
        <AlertTriangle size={14} />
        <span className="font-medium">Geliştirme Modu — MOCK veri</span>
        <span className="opacity-80">
          — Cloud SQL erişilemez, sahte veri gösteriliyor. Gerçek trade kararı vermeyin.
        </span>
        <button
          type="button"
          onClick={() => refetch()}
          disabled={isFetching}
          className="ml-auto inline-flex items-center gap-1 px-2 py-0.5 rounded border border-current hover:bg-black/5 text-xs disabled:opacity-50"
        >
          <RefreshCw size={11} className={isFetching ? "animate-spin" : ""} />
          Tekrar Dene
        </button>
      </div>
    );
  }

  // İlk yüklenmede (data undefined) banner gösterme — yanıltıcı flicker önlenir
  return null;
}
