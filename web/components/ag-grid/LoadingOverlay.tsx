"use client";

import { Skeleton } from "@/components/ui/skeleton";

/**
 * AG Grid loadingOverlayComponent — Skeleton-row placeholder.
 *
 * KARAR #463 (Sprint 4-bis sonrası B2 uygulaması):
 *   Markets360 4. dalga (p-skeleton 16 kullanım) sentezi.
 *   "Yükleniyor..." metin yerine 6 placeholder satır + profesyonel görünüm.
 *
 * Kural #21 (Browser Test Hibrit) ile doğrulanır.
 *
 * Kullanım (AG Grid props):
 *   loadingOverlayComponent={GridLoadingOverlay}
 */
export function GridLoadingOverlay() {
  return (
    <div className="flex flex-col gap-2 w-full p-4" role="status" aria-live="polite">
      <span className="sr-only">Yükleniyor</span>
      {[0, 1, 2, 3, 4, 5].map((i) => (
        <div key={i} className="flex items-center gap-3">
          <Skeleton className="h-8 w-16" /> {/* symbol */}
          <Skeleton className="h-6 w-10 rounded-full" /> {/* grade chip */}
          <Skeleton className="h-6 w-14" /> {/* rs */}
          <Skeleton className="h-6 w-20" /> {/* price */}
          <Skeleton className="h-6 w-8" /> {/* passed */}
          <Skeleton className="h-6 w-24" /> {/* scan_date */}
        </div>
      ))}
    </div>
  );
}
