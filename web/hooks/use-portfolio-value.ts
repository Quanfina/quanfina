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
  // Lazy initial state — SSR güvenli, useEffect-de set yok (anti-pattern önlendi)
  const [value, setValueState] = useState<number>(() => getPortfolioValue());

  useEffect(() => {
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
