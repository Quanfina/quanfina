/**
 * useGridColumnState (Sprint 4.8 — Column Preferences).
 *
 * AG Grid sütun düzeni localStorage persist: onGridReady restore + save callbacks.
 */
import { describe, it, expect, vi, beforeEach } from "vitest";
import { renderHook } from "@testing-library/react";
import {
  useGridColumnState,
  resetGridColumnState,
} from "@/hooks/use-grid-column-state";

const KEY = "test-grid-cols";

beforeEach(() => {
  localStorage.clear();
});

// Sahte ColumnState
const SAMPLE_STATE = [
  { colId: "ticker", width: 120, hide: false },
  { colId: "rs_ibd", width: 90, hide: true },
];

function makeApi(state = SAMPLE_STATE) {
  return {
    getColumnState: vi.fn(() => state),
    applyColumnState: vi.fn(),
  };
}

describe("useGridColumnState — save (onColumnMoved vb.)", () => {
  it("saveState → getColumnState localStorage'a yazar", () => {
    const { result } = renderHook(() => useGridColumnState(KEY));
    const api = makeApi();
    result.current.onColumnMoved({ api } as never);
    const saved = localStorage.getItem(KEY);
    expect(saved).not.toBeNull();
    expect(JSON.parse(saved!)).toEqual(SAMPLE_STATE);
  });

  it("onColumnVisible / onColumnResized / onSortChanged hepsi kaydeder", () => {
    const { result } = renderHook(() => useGridColumnState(KEY));
    for (const handler of [
      result.current.onColumnVisible,
      result.current.onColumnResized,
      result.current.onColumnPinned,
      result.current.onSortChanged,
    ]) {
      localStorage.clear();
      const api = makeApi();
      handler({ api } as never);
      expect(localStorage.getItem(KEY)).not.toBeNull();
    }
  });
});

describe("useGridColumnState — restore (onGridReady)", () => {
  it("localStorage'da state varsa applyColumnState çağrılır", () => {
    localStorage.setItem(KEY, JSON.stringify(SAMPLE_STATE));
    const { result } = renderHook(() => useGridColumnState(KEY));
    const api = makeApi();
    result.current.onGridReady({ api } as never);
    expect(api.applyColumnState).toHaveBeenCalledWith({
      state: SAMPLE_STATE,
      applyOrder: true,
    });
  });

  it("localStorage boş → applyColumnState çağrılmaz (default düzen)", () => {
    const { result } = renderHook(() => useGridColumnState(KEY));
    const api = makeApi();
    result.current.onGridReady({ api } as never);
    expect(api.applyColumnState).not.toHaveBeenCalled();
  });

  it("Bozuk JSON → crash yok, applyColumnState çağrılmaz", () => {
    localStorage.setItem(KEY, "{bozuk json");
    const { result } = renderHook(() => useGridColumnState(KEY));
    const api = makeApi();
    expect(() => result.current.onGridReady({ api } as never)).not.toThrow();
    expect(api.applyColumnState).not.toHaveBeenCalled();
  });

  it("Boş array state → applyColumnState çağrılmaz", () => {
    localStorage.setItem(KEY, JSON.stringify([]));
    const { result } = renderHook(() => useGridColumnState(KEY));
    const api = makeApi();
    result.current.onGridReady({ api } as never);
    expect(api.applyColumnState).not.toHaveBeenCalled();
  });
});

describe("useGridColumnState — round-trip (kaydet → geri yükle)", () => {
  it("save sonra restore → aynı state applyColumnState'e gider", () => {
    const { result } = renderHook(() => useGridColumnState(KEY));
    // Kaydet
    result.current.onColumnMoved({ api: makeApi() } as never);
    // Geri yükle
    const restoreApi = makeApi();
    result.current.onGridReady({ api: restoreApi } as never);
    expect(restoreApi.applyColumnState).toHaveBeenCalledWith({
      state: SAMPLE_STATE,
      applyOrder: true,
    });
  });

  it("Farklı storageKey'ler izole (screens vs watchlist)", () => {
    const { result: screens } = renderHook(() => useGridColumnState("screens-cols"));
    const { result: watchlist } = renderHook(() => useGridColumnState("watchlist-cols"));
    screens.current.onColumnMoved({ api: makeApi([{ colId: "a" }]) } as never);
    watchlist.current.onColumnMoved({ api: makeApi([{ colId: "b" }]) } as never);
    expect(JSON.parse(localStorage.getItem("screens-cols")!)).toEqual([{ colId: "a" }]);
    expect(JSON.parse(localStorage.getItem("watchlist-cols")!)).toEqual([{ colId: "b" }]);
  });
});

describe("resetGridColumnState", () => {
  it("localStorage anahtarını siler (varsayılana dön)", () => {
    localStorage.setItem(KEY, JSON.stringify(SAMPLE_STATE));
    resetGridColumnState(KEY);
    expect(localStorage.getItem(KEY)).toBeNull();
  });
});
