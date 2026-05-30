/**
 * Paket 390: FastAPI hata response parse helper'i (DRY tek kaynak).
 *
 * P388 use-trades.ts'de doğdu, P390'da lib/'e taşındı (DRY İlke #4) — diger
 * mutation hook'lari (watchlist + future) da kullanabilsin.
 *
 * Pydantic 422 ValidationError response yapisi:
 *   { detail: [{loc, msg, type, ctx}] }  — detail STRING DEĞİL LİSTE
 *
 * Eski naive `err.detail` doğrudan render edildiğinde "[object Object]"
 * gösterirdi (UX kotu). Bu helper field-bazli kullanıcı dostu mesaj uretir.
 *
 * Mark "Objective Mirror Language" disiplini (Vizyon İLKE #11):
 * "Sayilar konusur, hisler degil" — kullaniciya somut field + sebep göster.
 */

/**
 * Pydantic detail icerigini insan-okur stringe cevirir.
 * - Array (422) -> "field: msg; field2: msg2"
 * - String (503/404/...) -> dogrudan dondur
 * - Diger -> "HTTP <status>" fallback
 */
export function parsePydanticError(detail: unknown, status: number): string {
  if (typeof detail === "string") return detail;
  if (Array.isArray(detail)) {
    return detail
      .map((d) => {
        const err = d as { loc?: unknown[]; msg?: string };
        const field = Array.isArray(err.loc) ? err.loc[err.loc.length - 1] : "?";
        return `${field}: ${err.msg ?? "geçersiz"}`;
      })
      .join("; ");
  }
  return `HTTP ${status}`;
}

/**
 * Response error body'sini parse eder. Kullanim:
 *   if (!res.ok) throw new Error(await parseErrorBody(res));
 */
export async function parseErrorBody(res: Response): Promise<string> {
  try {
    const body = await res.json();
    return parsePydanticError(
      (body as { detail?: unknown }).detail,
      res.status,
    );
  } catch {
    return `HTTP ${res.status}`;
  }
}
