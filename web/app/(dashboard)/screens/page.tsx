"use client";

/**
 * Tarama Sayfası — ARŞİV MODU (22 May 2026)
 *
 * Sn. Ferit talimat (22 May 2026): "Adım adım gidicem artık. Notebook kaydet,
 * tarama sayfasındaki listeleri de kaydederek kaldır."
 *
 * 25 screen UI'dan kaldırıldı, kod arşivlendi:
 *   - Tam envanter: notebook/Tarama_Listeleri_Arsiv.md
 *   - 5-Kaynak sentez: notebook/Sprint_4_bis_5_Kaynak_Sentez.md
 *
 * Backend KORUNDU (scanner.py, quanfina_math.py, api/, DB tabloları).
 * Bu dosya: dropdown + tablo kaldırıldı, boş state gösterildi.
 *
 * Yeniden inşa planı (olay-tetikli sıralı):
 *   1. Cup with Handle (yeni screen) — Sprint 4-bis.7
 *   2. Pocket Pivot — Sprint 4-bis.7
 *   3. Flat Base alt-filtre — Sprint 4-bis.8
 *
 * Tetik: AÇIK KONU #70 (Cloud SQL erişimi) çözüldükten + Migration 003 PVH OHLC
 * canlandıktan sonra Sprint 4-bis.7 açılır.
 *
 * KARAR ref: KARAR ADAY #442, #468, Kural #4 (yıkıcı onay verildi), Kural #18
 * (pasif öğe çıkarma — kod silme yok, UI'dan gizle).
 */

import { ScanLine } from "lucide-react";
import Link from "next/link";

export default function ScreensPage() {
  return (
    <div className="flex flex-col h-full">
      <div className="px-6 py-3 border-b">
        <h1 className="text-xl font-semibold tracking-tight">Tarama</h1>
        <p className="text-sm text-muted-foreground">
          Mark Minervini canon eşikli çoklu mercek havuzu — sıfırdan yeniden inşa ediliyor.
        </p>
      </div>

      <div className="flex-1 flex items-center justify-center px-6 py-12">
        <div className="max-w-xl text-center space-y-6">
          <div
            className="inline-flex items-center justify-center w-16 h-16 rounded-full"
            style={{
              background: "color-mix(in srgb, var(--mtp-neutral) 12%, transparent)",
              color: "var(--mtp-neutral)",
            }}
          >
            <ScanLine size={28} strokeWidth={1.75} />
          </div>

          <div className="space-y-2">
            <h2 className="text-lg font-semibold">Tarama listesi yeniden inşa ediliyor</h2>
            <p className="text-sm text-muted-foreground leading-relaxed">
              Önceki 25 screen Mark canon eşikli çoklu mercek havuzu olarak çalıştı.
              22 May 2026 itibarıyla <strong>temiz tablo</strong> kararı alındı —
              gerçek değer üreten setup&apos;lar sırayla, olay-tetikli olarak eklenecek.
            </p>
          </div>

          <div className="text-left bg-muted/40 rounded-md p-4 space-y-2">
            <div className="text-xs font-semibold uppercase tracking-wider text-muted-foreground">
              Sıradaki Ekleme
            </div>
            <ul className="text-sm space-y-1.5">
              <li>
                <span className="inline-block w-5 text-muted-foreground">1.</span>
                <strong>Cup with Handle</strong>
                <span className="text-muted-foreground"> — Minervini Dream Pattern</span>
              </li>
              <li>
                <span className="inline-block w-5 text-muted-foreground">2.</span>
                <strong>Pocket Pivot</strong>
                <span className="text-muted-foreground"> — Kacher/Morales early entry</span>
              </li>
              <li>
                <span className="inline-block w-5 text-muted-foreground">3.</span>
                <strong>Flat Base alt-filtre</strong>
                <span className="text-muted-foreground"> — Stage 2 içine eklenecek</span>
              </li>
            </ul>
            <div className="text-xs text-muted-foreground pt-2 border-t mt-2">
              Tetik: AÇIK KONU #70 (Cloud SQL) çözüldükten + Migration 003 PVH OHLC
              canlandıktan sonra Sprint 4-bis.7 başlar.
            </div>
          </div>

          <div className="flex gap-2 justify-center text-sm">
            <Link
              href="/watchlist"
              className="px-3 py-1.5 rounded-md border hover:bg-accent transition-colors"
            >
              İzleme Listesi&apos;ne git
            </Link>
            <Link
              href="/signals"
              className="px-3 py-1.5 rounded-md border hover:bg-accent transition-colors"
            >
              Sinyaller&apos;e git
            </Link>
          </div>
        </div>
      </div>
    </div>
  );
}
