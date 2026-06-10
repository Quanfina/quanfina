"use client";

import { Layers } from "lucide-react";
import { usePatterns } from "@/hooks/use-patterns";

/**
 * KARAR ADAY #714 — Pattern Kütüphanesi referans sayfası.
 *
 * Migration 010 pattern_library (7 Mark/O'Neil canon) + /api/patterns (P333) +
 * usePatterns hook (P334) UI tüketicisi. Salt-okunur referans — detector'ların
 * kullandığı canon parametreleri (contraction count, base weeks) Mark kitap
 * referansıyla gösterir (KALICI İLKE #4 — kaynak şeffaflığı).
 */
export default function PatternKutuphanesiPage() {
  const { data, isLoading, isError } = usePatterns();

  return (
    <div className="flex flex-col h-full">
      {/* Header */}
      <div className="px-6 py-3 border-b">
        <div className="flex flex-col gap-1">
          <h1 className="text-xl font-semibold tracking-tight flex items-center gap-2">
            <Layers size={18} className="text-muted-foreground" />
            Pattern Kütüphanesi
          </h1>
          <p className="text-sm text-muted-foreground">
            Mark Minervini + O&apos;Neil canon kırılım pattern&apos;leri (TLSMW Ch 10) —
            detector parametre kaynağı, kitap referanslı (KARAR ADAY #714).
          </p>
        </div>
      </div>

      {/* İçerik */}
      <div className="flex-1 overflow-auto px-6 py-4">
        {isLoading ? (
          <p className="text-sm text-muted-foreground py-12 text-center">
            Pattern kütüphanesi yükleniyor...
          </p>
        ) : isError ? (
          <p className="text-sm py-12 text-center" style={{ color: "var(--mtp-danger)" }}>
            Pattern kütüphanesi alınamadı (Cloud SQL erişilemez olabilir).
          </p>
        ) : !data || data.length === 0 ? (
          <p className="text-sm text-muted-foreground py-12 text-center">
            Pattern kütüphanesi boş (Migration 010 uygulanmamış olabilir).
          </p>
        ) : (
          <div className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-3 gap-3">
            {data.map((p) => (
              <div
                key={p.id}
                className="rounded-lg border bg-card p-4 flex flex-col gap-2 hover:shadow-sm transition-shadow"
                style={{ borderLeftWidth: "3px", borderLeftColor: "var(--mtp-neutral)" }}
              >
                <div className="flex items-start justify-between gap-2">
                  <h3 className="text-sm font-semibold">{p.pattern_name}</h3>
                  {p.mark_book_ref && (
                    <span className="text-[10px] text-muted-foreground italic shrink-0">
                      {p.mark_book_ref}
                    </span>
                  )}
                </div>

                <div className="grid grid-cols-2 gap-2 text-xs">
                  <div className="flex flex-col">
                    <span className="text-[10px] text-muted-foreground uppercase tracking-wider">
                      Daralma (contraction)
                    </span>
                    <span className="font-mono font-semibold tabular-nums">
                      {p.contraction_count_min != null && p.contraction_count_max != null
                        ? `${p.contraction_count_min}-${p.contraction_count_max}`
                        : "—"}
                    </span>
                  </div>
                  <div className="flex flex-col">
                    <span className="text-[10px] text-muted-foreground uppercase tracking-wider">
                      Baz süre (hafta)
                    </span>
                    <span className="font-mono font-semibold tabular-nums">
                      {p.base_weeks_min != null && p.base_weeks_max != null
                        ? `${p.base_weeks_min}-${p.base_weeks_max}`
                        : "—"}
                    </span>
                  </div>
                </div>

                {p.notes && (
                  <p className="text-xs text-muted-foreground leading-relaxed border-t pt-2 mt-1">
                    {p.notes}
                  </p>
                )}
              </div>
            ))}
          </div>
        )}
      </div>
    </div>
  );
}
