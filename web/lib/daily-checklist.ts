"use client";

/**
 * Pazar Günü Hazırlık Paneli — UX Bölüm 8 "Aksiyon Modu".
 * KARAR #480 (20 May 2026): Günlük rutin checklist'i.
 *
 * Mantık:
 *   - Tarih bazlı (YYYY-MM-DD key) — her gün ayrı liste
 *   - localStorage: { "2026-05-20": ["item-1", "item-3"], ... } (tamamlananlar)
 *   - Dashboard'a entegre, sabah açıldığında otomatik üretilen 5 madde
 *   - Sn. Ferit tıklayınca işaretler, sayfa yenilenince hatırlar
 *   - Yeni gün başlayınca otomatik sıfırlanır (yeni date key)
 *
 * Backend persistence YOK (MVP) — Sprint 4-bis.7+ migration adayı.
 */

const STORAGE_KEY = "quanfina_daily_checklist";

interface ChecklistStore {
  [date: string]: string[];  // YYYY-MM-DD → ["item-1", "item-2"]
}

function getStore(): ChecklistStore {
  if (typeof window === "undefined") return {};
  try {
    const raw = localStorage.getItem(STORAGE_KEY);
    return raw ? (JSON.parse(raw) as ChecklistStore) : {};
  } catch {
    return {};
  }
}

function setStore(store: ChecklistStore): void {
  if (typeof window === "undefined") return;
  try {
    localStorage.setItem(STORAGE_KEY, JSON.stringify(store));
  } catch {
    /* quota exceeded — sessizce yoksay */
  }
}

function todayKey(): string {
  return new Date().toISOString().split("T")[0];
}

export function getDoneItems(): Set<string> {
  const store = getStore();
  return new Set(store[todayKey()] ?? []);
}

export function toggleItem(itemId: string, done: boolean): void {
  const store = getStore();
  const key = todayKey();
  const current = new Set(store[key] ?? []);
  if (done) {
    current.add(itemId);
  } else {
    current.delete(itemId);
  }
  store[key] = Array.from(current);
  // 7+ gün eski entries temizle (saklama disiplini)
  const cutoff = new Date();
  cutoff.setDate(cutoff.getDate() - 7);
  const cutoffStr = cutoff.toISOString().split("T")[0];
  for (const k of Object.keys(store)) {
    if (k < cutoffStr) delete store[k];
  }
  setStore(store);
}
