import type { MarketStatus } from "@/types/market";

/**
 * P483 (#1, Kural #28): Karar-kritik şerit MOCK şeffaflık banner'ı.
 *
 * /api/market/status canlı veri (yfinance index/VIX + sector_rotation DB) alınamazsa
 * sessizce MOCK'a düşüyordu — dd/stage/health/suggested_mode/mark_regime sentetik
 * veriden türüyor ama UI bunu "canlı" gibi gösteriyordu (P408/P409 breadth rozet
 * paterni bu şeritte yoktu). Bu banner sadece MOCK durumunda görünür; canlı veride
 * (3 flag de false) null döner. Paper trading kararı yanıltıcı sinyalle alınmasın.
 */
export function MarketDataMockBanner({
  status,
}: {
  status: Pick<MarketStatus, "index_is_mock" | "vix_is_mock" | "sectors_is_mock">;
}) {
  const parts: string[] = [];
  if (status.index_is_mock) parts.push("endeks/stage/sağlık/rejim");
  if (status.vix_is_mock) parts.push("VIX");
  if (status.sectors_is_mock) parts.push("sektörler");
  if (parts.length === 0) return null;

  return (
    <div
      role="alert"
      data-testid="market-mock-banner"
      className="rounded-lg border px-3 py-2 text-xs flex items-start gap-2"
      style={{
        borderColor: "rgba(245,158,11,0.45)",
        background: "rgba(245,158,11,0.08)",
        color: "#F59E0B",
      }}
    >
      <span aria-hidden>⚠</span>
      <span>
        <strong>Geçici sentetik veri:</strong> {parts.join(", ")} canlı kaynaktan
        alınamadı (yfinance/DB). Bu değerler MOCK — paper trading kararı için canlı
        veriyi bekleyin (Kural #28).
      </span>
    </div>
  );
}
