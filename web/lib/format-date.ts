/**
 * Tarih ve saati TR formatına çevirir.
 * KARAR #471 (20 May 2026): ISO format → DD.MM.YYYY HH:MM (Sn. Ferit talimat).
 * Bilgi Mimarisi İlke #4 (Tekrarsızlık): tek doğruluk kaynağı, 3+ sayfa kullanır.
 *
 * Girdi formatları:
 *   - "YYYY-MM-DD"         → "DD.MM.YYYY"
 *   - "YYYY-MM-DD HH:MM"   → "DD.MM.YYYY HH:MM"
 *   - "YYYY-MM-DDTHH:MM:SS" → "DD.MM.YYYY HH:MM:SS"  (ISO 8601 full)
 *   - null/undefined/""    → "—"
 *   - parse edilemeyen     → ham değer geri döner (defensive)
 */
export function formatDateTR(iso: string | null | undefined): string {
  if (!iso) return "—";
  // YYYY-MM-DD prefix + opsiyonel rest (saat veya boş)
  const m = iso.match(/^(\d{4})-(\d{2})-(\d{2})(.*)$/);
  if (!m) return iso;
  const [, year, month, day, rest] = m;
  // ISO 8601 "T" ayırıcı varsa boşlukla değiştir, "Z" timezone'u sadeleştir
  const tail = rest.replace(/^T/, " ").replace(/Z$/, "").trim();
  return tail ? `${day}.${month}.${year} ${tail}` : `${day}.${month}.${year}`;
}

/**
 * Bugünün tarihini YEREL saat dilimine göre "YYYY-MM-DD" döner (Paket 356).
 *
 * NEDEN: `new Date().toISOString().slice(0,10)` UTC tarih verir. Sn. Ferit
 * Türkiye'de (UTC+3) ABD piyasası trade ederken, yerel 00:00–03:00 arası AL
 * butonuna basarsa UTC hâlâ "dün" → entry_date yanlış ön-dolar (off-by-one).
 * getFullYear/getMonth/getDate yerel saat dilimini kullanır → kayma yok.
 *
 * `<input type="date">` değeri zaten yerel YYYY-MM-DD bekler; bu helper ön-dolum
 * için doğru kaynak.
 */
export function todayLocalISO(d: Date = new Date()): string {
  const y = d.getFullYear();
  const mo = String(d.getMonth() + 1).padStart(2, "0");
  const day = String(d.getDate()).padStart(2, "0");
  return `${y}-${mo}-${day}`;
}

/**
 * Eklenme zamani kisa etiket (P416, 31 May 2026 — Sn. Ferit talimat: Focus
 * List patenli "Monday/Tuesday" hizli okunur format).
 *
 * Mantık:
 *   - Bugün → "Bugün"
 *   - Dün → "Dün"
 *   - Son 6 gün → "Pzt", "Sal", "Çar", "Per", "Cum", "Cmt", "Paz"
 *   - 7+ gün eski → formatDateTR (DD.MM.YYYY)
 *
 * Türkçe gün adları (Sn. Ferit Türkiye yerleşimi).
 *
 * Bilgi Mimarisi İlke #4: tek doğruluk kaynağı (3+ yerde kullanım hedefi —
 * watchlist columns, signals, screens olası).
 */
const TR_DAY_NAMES = ["Paz", "Pzt", "Sal", "Çar", "Per", "Cum", "Cmt"];

export function formatDayLabel(iso: string | null | undefined, now: Date = new Date()): string {
  if (!iso) return "—";
  const m = iso.match(/^(\d{4})-(\d{2})-(\d{2})/);
  if (!m) return iso;
  const [, year, month, day] = m;
  const target = new Date(Number(year), Number(month) - 1, Number(day));

  // Sadece gün-bazlı karşılaştırma — saatten arındırılmış
  const todayY = now.getFullYear();
  const todayM = now.getMonth();
  const todayD = now.getDate();
  const todayStart = new Date(todayY, todayM, todayD);
  const targetStart = new Date(target.getFullYear(), target.getMonth(), target.getDate());

  const diffMs = todayStart.getTime() - targetStart.getTime();
  const diffDays = Math.round(diffMs / (24 * 60 * 60 * 1000));

  if (diffDays === 0) return "Bugün";
  if (diffDays === 1) return "Dün";
  if (diffDays > 1 && diffDays <= 6) {
    return TR_DAY_NAMES[targetStart.getDay()];
  }
  return formatDateTR(iso);
}
