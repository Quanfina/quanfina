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
