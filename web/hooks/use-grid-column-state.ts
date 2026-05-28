"use client";

import { useCallback } from "react";
import type {
  GridReadyEvent,
  ColumnState,
  ColumnMovedEvent,
  ColumnVisibleEvent,
  ColumnResizedEvent,
  ColumnPinnedEvent,
  SortChangedEvent,
} from "ag-grid-community";

/**
 * Sprint 4.8 — Column Preferences (AG Grid sütun tercihleri persist).
 *
 * Kullanıcının sütun düzenini (sıra, görünürlük, genişlik, pinned, sıralama)
 * localStorage'a kaydeder + sayfa açılışında geri yükler. Her AG Grid sayfası
 * kendi storageKey'i ile çağırır (DRY — screens/watchlist/journal/signals).
 *
 * Kullanım:
 *   const gridState = useGridColumnState("quanfina-screens-cols");
 *   <AgGridReact {...gridState} ... />
 *
 * Kapsam: getColumnState() — order + hide + width + pinned + sort. AG Grid v35
 * Community API. SSR-safe (typeof window guard).
 */
export function useGridColumnState(storageKey: string) {
  const onGridReady = useCallback(
    (e: GridReadyEvent) => {
      if (typeof window === "undefined") return;
      const saved = window.localStorage.getItem(storageKey);
      if (!saved) return;
      try {
        const state = JSON.parse(saved) as ColumnState[];
        if (Array.isArray(state) && state.length > 0) {
          e.api.applyColumnState({ state, applyOrder: true });
        }
      } catch {
        // Bozuk JSON — sessizce yoksay (default sütun düzeni)
      }
    },
    [storageKey]
  );

  const saveState = useCallback(
    (e: { api: { getColumnState: () => ColumnState[] } }) => {
      if (typeof window === "undefined") return;
      try {
        const state = e.api.getColumnState();
        window.localStorage.setItem(storageKey, JSON.stringify(state));
      } catch {
        // localStorage dolu / erişilemez — sessizce geç
      }
    },
    [storageKey]
  );

  return {
    onGridReady,
    onColumnMoved: saveState as (e: ColumnMovedEvent) => void,
    onColumnVisible: saveState as (e: ColumnVisibleEvent) => void,
    onColumnResized: saveState as (e: ColumnResizedEvent) => void,
    onColumnPinned: saveState as (e: ColumnPinnedEvent) => void,
    onSortChanged: saveState as (e: SortChangedEvent) => void,
  };
}

/** localStorage'daki sütun tercihini sıfırla (varsayılana dön). */
export function resetGridColumnState(storageKey: string): void {
  if (typeof window === "undefined") return;
  window.localStorage.removeItem(storageKey);
}
