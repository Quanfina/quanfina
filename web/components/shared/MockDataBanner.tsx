"use client";

/**
 * P418 (31 May 2026 — Kural #28 audit DRY component):
 * MOCK fallback şeffaflık banner — P408+P417 patterni DRY hijyen.
 *
 * Sn. Ferit Cloud SQL erişilemez durumda /journal /istatistikler /pazar-hazirligi
 * /Dashboard sayfalarında 8 hardcoded MOCK trade üzerinden hesap görür — paper
 * trading kararı için yanıltıcı. Bu component 4+ sayfada tekrarlanan banner'ın
 * tek doğruluk kaynağı (Bilgi Mimarisi İlke #4 DRY).
 *
 * Kullanım:
 *   const { data: tradesInfo } = useTradesInfo();
 *   return <>
 *     <MockDataBanner isMock={tradesInfo?.is_mock} context="istatistik" />
 *     ...
 *   </>;
 */
interface Props {
  /** Backend /api/trades/info'dan gelen is_mock flag (undefined ise banner gizli) */
  isMock: boolean | undefined;
  /** Sayfa bağlamı — banner metnine eklenir ("8 trade tasarım örneği üzerinden {context} hesabı"). */
  context?: string;
  /** Test/debug için ek selector */
  testId?: string;
}

export function MockDataBanner({ isMock, context, testId = "mock-data-banner" }: Props) {
  if (!isMock) return null;
  const contextSuffix = context ? ` (${context} hesabı yanıltıcı)` : "";
  return (
    <div
      className="px-6 py-2 border-b text-xs flex items-center gap-2"
      style={{
        background: "rgba(245,158,11,0.10)",
        borderColor: "rgba(245,158,11,0.40)",
        color: "#92400E",
      }}
      role="status"
      data-testid={testId}
    >
      <span aria-hidden="true">🟡</span>
      <span>
        <b>MOCK VERİ:</b> Cloud SQL erişilemez — <b>8 tasarım örneği trade</b>
        {contextSuffix}. Paper trading kararı için DB bağlantısı şart.
        (Kural #28 audit P418)
      </span>
    </div>
  );
}
