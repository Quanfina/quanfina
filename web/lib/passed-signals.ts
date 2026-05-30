"use client";

/**
 * Geçilen sinyaller (UX Bölüm 6 "Mekanik karar — AL/GEÇ").
 * KARAR #475 (20 May 2026): Sn. Ferit bir sinyali "GEÇ" yaparsa
 * localStorage'a kaydedilir, listeden filtrelenir.
 *
 * Backend persistence YOK (MVP) — DB ayağa kalkınca migration adayı:
 *   "web_signal_passes" tablosu + /api/signals/{symbol}/{strategy}/pass endpoint.
 *
 * Key format: "{symbol}-{strategy}" (Sinyal unique key, KARAR #469)
 */

const STORAGE_KEY = "quanfina_passed_signals";

export function getPassedSignals(): Set<string> {
  if (typeof window === "undefined") return new Set();
  try {
    const raw = localStorage.getItem(STORAGE_KEY);
    if (!raw) return new Set();
    const arr = JSON.parse(raw);
    return Array.isArray(arr) ? new Set(arr as string[]) : new Set();
  } catch {
    return new Set();
  }
}

export function setPassedSignals(set: Set<string>): void {
  if (typeof window === "undefined") return;
  try {
    localStorage.setItem(STORAGE_KEY, JSON.stringify(Array.from(set)));
  } catch {
    /* quota exceeded — sessizce yoksay */
  }
}

export function signalKey(symbol: string, strategy: string): string {
  return `${symbol}-${strategy}`;
}

/**
 * P411: Tüm "geçilmiş" sinyalleri sıfırla (toplu reset).
 *
 * Sn. Ferit önceki testte çok sinyali geçtiyse /signals sayfası "0/0" boş
 * gibi görünüyordu. Bu helper localStorage'ı temizler — yeni başlangıç.
 * Kural #4 yıkıcı eylem onayı: caller UI'da "Sıfırla" butonuna basarak
 * onay verir (programmatic auto-clear YAPILMAZ).
 */
export function clearPassedSignals(): void {
  if (typeof window === "undefined") return;
  try {
    localStorage.removeItem(STORAGE_KEY);
  } catch {
    /* ignore */
  }
}
