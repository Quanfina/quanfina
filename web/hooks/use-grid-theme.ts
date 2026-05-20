"use client";

import { useEffect, useState } from "react";
import { useTheme } from "next-themes";

/**
 * AG Grid tema senkronizasyonu — SSR uyumu için mounted guard.
 *
 * KARAR #476 (20 May 2026): Screens sayfasında dark mode aktifken AG Grid
 * arka planı beyaz çıkıyordu — SSR'de `resolvedTheme === undefined` → false → light.
 *
 * Çözüm: mounted guard + dark varsayım. İlk SSR render'da `ag-theme-quartz-dark`
 * (Sn. Ferit'in default tercihi), client mounted olunca gerçek theme'e geçer.
 * Bu flicker'ı minimize eder (dark→dark veya dark→light geçişi).
 *
 * Bilgi Mimarisi İlke #4 (Tekrarsızlık): 4 AG Grid sayfası (Screens/Watchlist/
 * Journal/Sinyaller) aynı hook'u kullanır.
 */
export function useGridTheme(): { gridClass: string; isDark: boolean; mounted: boolean } {
  const [mounted, setMounted] = useState(false);
  const { resolvedTheme } = useTheme();

  useEffect(() => setMounted(true), []);

  // Default: dark (SSR + ilk paint için). Mounted olunca gerçek theme.
  const isDark = !mounted ? true : resolvedTheme === "dark";
  const gridClass = isDark ? "ag-theme-quartz-dark" : "ag-theme-quartz";

  return { gridClass, isDark, mounted };
}
