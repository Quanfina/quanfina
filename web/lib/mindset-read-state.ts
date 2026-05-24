/**
 * KARAR ADAY #720 alt (Paket 28 + 29): Mindset Card "okundu" persistence + streak.
 *
 * Sn. Ferit her sabah Dashboard'da Mark hatırlatması okur disiplini —
 * "Okudum" tıklayınca o günün kart ID'si localStorage'a yazılır.
 * Yarın yeni günün kartı tekrar "okunmamış" başlar.
 *
 * Paket 29: Streak history — son N gün üst üste okuma sayım (motivasyon).
 *
 * Format: {
 *   date: "YYYY-MM-DD",       (bugünün son okunan kartı)
 *   cardId: "risk-01",
 *   history: ["2026-05-22", "2026-05-23", "2026-05-24"]  (son 60 gün max)
 * }
 */

const STORAGE_KEY = "quanfina-mindset-read";
const HISTORY_MAX_DAYS = 60;

interface MindsetReadState {
  date: string;     // YYYY-MM-DD (bugünün okunan kartı için)
  cardId: string;
  history?: string[];  // P29: okunmuş günler (eskiden yeniye)
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

/** "Okudum" işaretle + streak history güncelle (P29) */
export function markCardReadToday(cardId: string): void {
  if (typeof window === "undefined") return;
  try {
    const today = todayStr();
    const raw = window.localStorage.getItem(STORAGE_KEY);
    let history: string[] = [];
    if (raw) {
      try {
        const prev = JSON.parse(raw) as MindsetReadState;
        history = Array.isArray(prev.history) ? prev.history.slice() : [];
        // Bugün zaten history'de ise tekrar ekleme (idempotent)
        if (prev.date && !history.includes(prev.date)) {
          history.push(prev.date);
        }
      } catch {
        history = [];
      }
    }
    // Bugünü history'e ekle (eğer henüz yoksa)
    if (!history.includes(today)) {
      history.push(today);
    }
    // Sıralı + son HISTORY_MAX_DAYS gün
    history = Array.from(new Set(history)).sort();
    if (history.length > HISTORY_MAX_DAYS) {
      history = history.slice(history.length - HISTORY_MAX_DAYS);
    }
    const state: MindsetReadState = { date: today, cardId, history };
    window.localStorage.setItem(STORAGE_KEY, JSON.stringify(state));
  } catch {
    // localStorage quota / private mode → sessiz geç
  }
}


/**
 * KARAR #720 alt (Paket 29) — Streak hesap: bugünden geriye kesintisiz okuma günleri.
 *
 * Bugün okunmamışsa dünden başlar (zincir kırılmasın diye günün ortasına kadar bekle).
 * Örnek: dün + bugünden önceki gün okundu, bugün okunmamış → streak = 2.
 *        bugün dahil tüm son N gün okundu → streak = N.
 *
 * @returns { streak: number, todayRead: boolean }
 */
export function getReadStreak(today: Date = new Date()): { streak: number; todayRead: boolean } {
  if (typeof window === "undefined") return { streak: 0, todayRead: false };
  try {
    const raw = window.localStorage.getItem(STORAGE_KEY);
    if (!raw) return { streak: 0, todayRead: false };
    const state = JSON.parse(raw) as MindsetReadState;
    const history = Array.isArray(state.history) ? state.history : [];
    if (history.length === 0) return { streak: 0, todayRead: false };

    const todayKey = todayStr(today);
    const set = new Set(history);
    const todayRead = set.has(todayKey);

    // Sayım: bugünden (veya dünden, bugün yoksa) geriye kesintisiz gün
    let streak = 0;
    const cursor = new Date(today);
    if (!todayRead) {
      // Henüz okunmadıysa, dünden başla (gün sonu hesabını koru)
      cursor.setDate(cursor.getDate() - 1);
    }
    for (let i = 0; i < HISTORY_MAX_DAYS; i++) {
      const key = todayStr(cursor);
      if (set.has(key)) {
        streak += 1;
        cursor.setDate(cursor.getDate() - 1);
      } else {
        break;
      }
    }
    return { streak, todayRead };
  } catch {
    return { streak: 0, todayRead: false };
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
