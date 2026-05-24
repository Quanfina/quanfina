/**
 * KARAR ADAY #720 alt (Paket 28): Mindset Card "okundu" persistence.
 *
 * Sn. Ferit her sabah Dashboard'da Mark hatırlatması okur disiplini —
 * "Okudum" tıklayınca o günün kart ID'si localStorage'a yazılır.
 * Yarın yeni günün kartı tekrar "okunmamış" başlar.
 *
 * Format: { date: "YYYY-MM-DD", cardId: "risk-01" }
 */

const STORAGE_KEY = "quanfina-mindset-read";

interface MindsetReadState {
  date: string;     // YYYY-MM-DD (bugünün okunan kartı için)
  cardId: string;
}

function todayStr(date: Date = new Date()): string {
  return date.toISOString().slice(0, 10);
}

/** Bugünün kartı okunmuş mu? */
export function isCardReadToday(cardId: string): boolean {
  if (typeof window === "undefined") return false;
  try {
    const raw = window.localStorage.getItem(STORAGE_KEY);
    if (!raw) return false;
    const state = JSON.parse(raw) as MindsetReadState;
    return state.date === todayStr() && state.cardId === cardId;
  } catch {
    return false;
  }
}

/** "Okudum" işaretle */
export function markCardReadToday(cardId: string): void {
  if (typeof window === "undefined") return;
  try {
    const state: MindsetReadState = { date: todayStr(), cardId };
    window.localStorage.setItem(STORAGE_KEY, JSON.stringify(state));
  } catch {
    // localStorage quota / private mode → sessiz geç
  }
}

/** Streak hesabı — son N gün içinde kaç gün okuma yapıldı (UI motivasyon).
 *  Şu an basit: bugün okundu mu sadece. İlerleyen paket: streak history. */
export function isReadTodayAny(): boolean {
  if (typeof window === "undefined") return false;
  try {
    const raw = window.localStorage.getItem(STORAGE_KEY);
    if (!raw) return false;
    const state = JSON.parse(raw) as MindsetReadState;
    return state.date === todayStr();
  } catch {
    return false;
  }
}
