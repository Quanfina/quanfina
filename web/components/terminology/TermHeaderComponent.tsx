"use client";

import { useEffect, useState } from "react";
import type { IHeaderParams } from "ag-grid-community";
import { TermTooltip } from "@/components/terminology/TermTooltip";

interface TermHeaderParams extends IHeaderParams {
  termKey: string;
}

/**
 * AG Grid custom header — terim (?) tooltip + tıklayarak SIRALAMA.
 *
 * P428 (31 May 2026 — BUG FIX): Önceki sürüm sadece <TermTooltip>{displayName}</TermTooltip>
 * render ediyordu; custom headerComponent AG Grid'in varsayılan sort-on-click
 * davranışını EZER, bu component progressSort çağırmadığı için RS/STRATEJİ/MA200/
 * 52W/CANSLIM kolonlarında tıklayarak sıralama çalışmıyordu. Düzeltme: başlık
 * etiketi tıklanınca progressSort + sıralama oku (▲/▼); (?) tooltip butonu ayrı
 * (stopPropagation — ? tıklayınca sıralama değil, tanım açılır).
 *
 * Tüm tablolarda DRY tek kaynak (watchlist + minervini + screens).
 */
export function TermHeaderComponent(props: TermHeaderParams) {
  const { displayName, termKey, enableSorting, progressSort, column } = props;
  const [sort, setSort] = useState<"asc" | "desc" | null>(null);

  useEffect(() => {
    const sync = () => setSort((column.getSort() as "asc" | "desc" | null) ?? null);
    sync(); // ilk durum
    column.addEventListener("sortChanged", sync);
    return () => column.removeEventListener("sortChanged", sync);
  }, [column]);

  const handleSortClick = (e: React.MouseEvent) => {
    // enableSorting=false ise (sortable:false kolon) tıklama no-op
    if (enableSorting) progressSort(e.shiftKey);
  };

  return (
    <div className="flex items-center gap-1 h-full w-full select-none">
      {/* Etiket + sıralama oku — tıklanınca sort (varsayılan AG Grid davranışı geri) */}
      <span
        onClick={handleSortClick}
        className={
          // P429: truncate KALDIRILDI — P428'de eklenen "truncate" RS IBD (90px) +
          // EPS Q/Q (100px) gibi dar kolonlarda başlığı "RS I..." kırpıyordu.
          // flex-1 min-w-0 KORUNDU (tüm header tıklanabilir = sort alanı), sadece
          // inner truncate gitti; kısa başlık tam görünür, uzun olanı cell clip eder.
          enableSorting
            ? "flex items-center gap-0.5 cursor-pointer flex-1 min-w-0"
            : "flex items-center gap-0.5 flex-1 min-w-0"
        }
        data-testid="term-header-sort"
      >
        <span className="whitespace-nowrap">{displayName}</span>
        {sort === "asc" && <span aria-hidden="true" className="text-[10px] opacity-70">▲</span>}
        {sort === "desc" && <span aria-hidden="true" className="text-[10px] opacity-70">▼</span>}
      </span>
      {/* (?) terim tanımı — TermTooltip butonu kendi stopPropagation'ı ile sort'u engeller */}
      <TermTooltip termKey={termKey}>{""}</TermTooltip>
    </div>
  );
}
