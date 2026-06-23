"use client";

import { useCallback, useEffect, useState } from "react";
import {
  DEFAULT_PORTFOLIO_VALUE,
  PORTFOLIO_VALUE_STORAGE_KEY,
  getPortfolioValue,
  setPortfolioValue as savePortfolioValue,
} from "@/lib/portfolio-settings";

/**
 * P400: Portföy büyüklüğü hook'u (localStorage persist).
 *
 * usePortfolioValue() döndürür:
 *   - value: number (mevcut kayıtlı değer veya $100K default)
 *   - setValue: (n: number) => void (kaydet + state senkron)
 *
 * SSR güvenli: initial state lazy callback ile (typeof window kontrolü
 * portfolio-settings.ts içinde). React 19 useEffect-in-effect anti-pattern
 * yok — sadece storage event subscribe (effect içinde setState YOK,
 * sadece listener kayıt).
 */
export function usePortfolioValue() {
  // P586 (22 Haz 2026) #418 FIX: initial state SABİT default (localStorage OKUNMAZ).
  // Eski lazy-init `() => getPortfolioValue()` SSR'da window-yok → default, client'ta →
  // localStorage değeri → SSR≠client hydration mismatch (#418). Kanonik SSR-safe pattern:
  // SSR + client-first-render ikisi de DEFAULT (eşleşir), gerçek değer mount sonrası gelir.
  const [value, setValueState] = useState<number>(DEFAULT_PORTFOLIO_VALUE);

  useEffect(() => {
    // Mount sonrası localStorage'dan gerçek değeri al (hydration-safe)
    setValueState(getPortfolioValue());
    function onStorage(e: StorageEvent) {
      if (e.key === PORTFOLIO_VALUE_STORAGE_KEY) {
        setValueState(getPortfolioValue());
      }
    }
    window.addEventListener("storage", onStorage);
    return () => window.removeEventListener("storage", onStorage);
  }, []);

  const setValue = useCallback((newValue: number) => {
    if (savePortfolioValue(newValue)) {
      setValueState(newValue);
    }
  }, []);

  return { value, setValue, defaultValue: DEFAULT_PORTFOLIO_VALUE };
}
