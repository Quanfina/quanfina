/**
 * Paket 400: Portföy büyüklüğü kullanıcı ayarı (localStorage helper).
 *
 * Sn. Ferit'in gerçek portföy büyüklüğü Quanfina'da kalıcı saklı değildi —
 * MarkPyramidCard + OpenPositionsRiskPanel hardcoded $100K varsayım yapıyordu.
 * Paper trading'de pozisyon yüzdesi hesabı yanıltıcı sonuç veriyordu (örn.
 * gerçek portföy $50K ise %1 risk hesabı 2× hatalı görünür).
 *
 * Bu helper localStorage'dan değer okur/yazar — paper trading uygun (kalıcı,
 * cross-session, ek altyapı gerektirmez). Production'da DB'ye geçiş ileride.
 *
 * SSR güvenli: typeof window kontrolü.
 */

const STORAGE_KEY = "quanfina:portfolio_value";
const DEFAULT_VALUE = 100000;

export const PORTFOLIO_VALUE_STORAGE_KEY = STORAGE_KEY;
export const DEFAULT_PORTFOLIO_VALUE = DEFAULT_VALUE;

/**
 * Portföy değeri oku. localStorage erişilemezse veya geçersiz değer ise
 * default $100K döner (UX kesintisiz, ilk kullanıcı için anlamlı baseline).
 */
export function getPortfolioValue(): number {
  if (typeof window === "undefined") return DEFAULT_VALUE;
  try {
    const raw = window.localStorage.getItem(STORAGE_KEY);
    if (!raw) return DEFAULT_VALUE;
    const n = parseFloat(raw);
    return Number.isFinite(n) && n > 0 ? n : DEFAULT_VALUE;
  } catch {
    return DEFAULT_VALUE;
  }
}

/**
 * Portföy değeri kaydet. Geçersiz girdi (NaN, <=0, Infinity) sessizce
 * reddedilir (UX kesintisiz, hatalı veri saklanmaz). storage event tetiklenir
 * → açık tab'larda usePortfolioValue hook'u senkronize olur.
 */
export function setPortfolioValue(value: number): boolean {
  if (typeof window === "undefined") return false;
  if (!Number.isFinite(value) || value <= 0) return false;
  try {
    window.localStorage.setItem(STORAGE_KEY, String(value));
    // Aynı tab'da storage event tetiklenmez (sadece diğer tab'larda) —
    // manuel dispatch ile hook'a haber ver
    window.dispatchEvent(
      new StorageEvent("storage", { key: STORAGE_KEY, newValue: String(value) }),
    );
    return true;
  } catch {
    return false;
  }
}

/**
 * Portföy değeri sıfırla (default'a dön). Test ve "ayarları temizle"
 * kullanım senaryosu için.
 */
export function clearPortfolioValue(): void {
  if (typeof window === "undefined") return;
  try {
    window.localStorage.removeItem(STORAGE_KEY);
    window.dispatchEvent(
      new StorageEvent("storage", { key: STORAGE_KEY, newValue: null }),
    );
  } catch {
    /* ignore */
  }
}
