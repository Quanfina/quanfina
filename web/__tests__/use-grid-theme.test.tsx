/**
 * useGridTheme (KARAR #476 — 20 May 2026).
 *
 * AG Grid SSR + dark mode senkron — mounted guard + dark varsayım.
 * 4 AG Grid sayfası (Screens/Watchlist/Journal/Sinyaller) DRY tek kaynak.
 */
import { describe, it, expect, vi, beforeEach } from "vitest";
import { renderHook, waitFor } from "@testing-library/react";
import { useGridTheme } from "@/hooks/use-grid-theme";

// next-themes mock — kontrollü resolvedTheme
const mockResolvedTheme = vi.fn<() => string | undefined>();
vi.mock("next-themes", () => ({
  useTheme: () => ({ resolvedTheme: mockResolvedTheme() }),
}));

beforeEach(() => {
  mockResolvedTheme.mockReset();
});

describe("useGridTheme — SSR + mounted guard", () => {
  it("Mounted dark (resolvedTheme='dark') → 'ag-theme-quartz-dark'", async () => {
    mockResolvedTheme.mockReturnValue("dark");
    const { result } = renderHook(() => useGridTheme());
    // useEffect mounted=true tetiklenmesi için bir tick bekle
    await waitFor(() => expect(result.current.mounted).toBe(true));
    expect(result.current.gridClass).toBe("ag-theme-quartz-dark");
    expect(result.current.isDark).toBe(true);
  });

  it("Mounted light (resolvedTheme='light') → 'ag-theme-quartz'", async () => {
    mockResolvedTheme.mockReturnValue("light");
    const { result } = renderHook(() => useGridTheme());
    await waitFor(() => expect(result.current.mounted).toBe(true));
    expect(result.current.gridClass).toBe("ag-theme-quartz");
    expect(result.current.isDark).toBe(false);
  });

  it("resolvedTheme=undefined (SSR/system) + mounted=true → light fallback", async () => {
    mockResolvedTheme.mockReturnValue(undefined);
    const { result } = renderHook(() => useGridTheme());
    await waitFor(() => expect(result.current.mounted).toBe(true));
    // mounted + undefined → isDark = (undefined === "dark") = false → light
    expect(result.current.isDark).toBe(false);
    expect(result.current.gridClass).toBe("ag-theme-quartz");
  });

  it("Çıktı şekli: { gridClass, isDark, mounted } 3 alan", async () => {
    mockResolvedTheme.mockReturnValue("dark");
    const { result } = renderHook(() => useGridTheme());
    await waitFor(() => expect(result.current.mounted).toBe(true));
    expect(result.current).toHaveProperty("gridClass");
    expect(result.current).toHaveProperty("isDark");
    expect(result.current).toHaveProperty("mounted");
    expect(typeof result.current.gridClass).toBe("string");
    expect(typeof result.current.isDark).toBe("boolean");
    expect(typeof result.current.mounted).toBe("boolean");
  });
});
