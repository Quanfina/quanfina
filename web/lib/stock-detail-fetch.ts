/**
 * #25 (denetim DRY, İlke #4): /api/stock/{symbol}/{path} detay endpoint'leri için
 * ortak fetch + ok-check + json iskeleti. ~13 stock-detay hook'u (rs/atr/cup-handle/
 * flat-base/double-bottom/pivot/climax/stage/...) bu 4 satırı birebir kopyalıyordu.
 *
 * Davranış-koruyan: hata mesajı formatı mevcut hook'larla AYNI
 * (`{label} alınamadı ({symbol}): {status}`). Yeni hook'lar bu helper'ı kullanır.
 */
export async function fetchStockDetail<T>(
  symbol: string,
  path: string,
  label: string,
): Promise<T> {
  const res = await fetch(`/api/stock/${encodeURIComponent(symbol)}/${path}`);
  if (!res.ok) {
    throw new Error(`${label} alınamadı (${symbol}): ${res.status}`);
  }
  return res.json() as Promise<T>;
}
