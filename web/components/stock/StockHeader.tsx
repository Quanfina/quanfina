import type { StockInfo } from "@/types/stock";
import { MarkBadgeStrip } from "@/components/mark/MarkBadgeStrip";

// P441: IBD canon kategori eşiği (LEADER>=80 / STRONG 70-80 / AVERAGE 50-70 /
// LAGGARD <50) — RsRatingBadge + MarkProfileBar + rs_rating_category birebir.
// Eski 90/70/50 (70-80 yeşil) tutarsızdı: header yeşil, kart mavi (STRONG) idi.
function rsBackground(rs: number): string {
  if (rs >= 90) return "color-mix(in srgb, var(--mtp-excellent) 35%, transparent)";
  if (rs >= 80) return "color-mix(in srgb, var(--mtp-excellent) 18%, transparent)";
  if (rs >= 70) return "color-mix(in srgb, var(--mtp-neutral)   18%, transparent)";
  if (rs >= 50) return "color-mix(in srgb, #F59E0B 18%, transparent)";
  return              "color-mix(in srgb, var(--mtp-danger)    18%, transparent)";
}

function rsColor(rs: number): string {
  if (rs >= 80) return "var(--mtp-excellent)";
  if (rs >= 70) return "var(--mtp-neutral)";
  if (rs >= 50) return "#F59E0B";
  return "var(--mtp-danger)";
}

// KARAR ADAY (21 May 2026): Konsensus rozeti kaldirildi. Sn. Ferit talimat:
// "konsesus kalksin nasil olsa her strateji tabloda farkli satirda gorukucek".
// Aktif strateji bilgisi alt sekmelerde gosterilebilir (ActiveStrategies komponent).
// P431 (31 May 2026 — görsel denetim BUG FIX): info.price scan/MOCK BAYAT fiyat
// (NVDA $875.40) — canlı quote ($206.10) varken yanıltıcıydı (P412 stale-price
// /hisse karşılığı). livePrice/liveChangePct verilirse onlar gösterilir (yfinance
// canlı), yoksa info.price fallback. Kural #28: paper trading kararı canlı fiyatla.
export function StockHeader({
  info,
  livePrice,
  liveChangePct,
}: {
  info: StockInfo;
  livePrice?: number | null;
  liveChangePct?: number | null;
}) {
  const price = livePrice ?? info.price;
  const changePct = liveChangePct ?? info.change_pct;
  const isLive = livePrice != null;
  const isPositive = changePct >= 0;

  return (
    <div className="flex items-start justify-between gap-4 flex-wrap">
      <div className="flex flex-col gap-1.5">
        <div className="flex items-baseline gap-3">
          <h1
            className="text-2xl font-bold tracking-tight"
            style={{ fontFamily: "var(--font-jetbrains-mono, monospace)" }}
          >
            {info.symbol}
          </h1>
          <span className="text-lg text-muted-foreground">{info.name}</span>
        </div>
        <div className="flex items-center gap-2 text-sm text-muted-foreground flex-wrap">
          <span>{info.sector}</span>
          <span>·</span>
          <span>{info.industry}</span>
          <span>·</span>
          <span>Piyasa Değeri: {info.market_cap}</span>
        </div>
        {/* KARAR ADAY #723 (24 May 2026): Mark Profili rozetleri (DRY MarkBadgeStrip) */}
        {info.mark_signals && (
          <div className="mt-1">
            <MarkBadgeStrip signals={info.mark_signals} density="full" showEmpty={false} />
          </div>
        )}
      </div>

      <div className="flex items-center gap-3 shrink-0">
        {/* Price + change (P431: canlı quote öncelikli) */}
        <div className="text-right">
          <div
            className="text-2xl font-bold flex items-baseline gap-1.5 justify-end"
            style={{ fontFamily: "var(--font-jetbrains-mono, monospace)" }}
          >
            ${price.toFixed(2)}
            {isLive && (
              <span
                className="text-[9px] font-semibold uppercase tracking-wider px-1 py-0.5 rounded"
                style={{ background: "color-mix(in srgb, var(--mtp-neutral) 18%, transparent)", color: "var(--mtp-neutral)" }}
                title="Anlık piyasa fiyatı (yfinance, 60s yenileme)"
              >
                CANLI
              </span>
            )}
          </div>
          <div
            className="text-sm"
            style={{
              fontFamily: "var(--font-jetbrains-mono, monospace)",
              color: isPositive ? "var(--mtp-excellent)" : "var(--mtp-danger)",
            }}
          >
            {isPositive ? "+" : ""}
            {changePct.toFixed(2)}%
          </div>
        </div>

        {/* RS badge */}
        <div
          className="flex flex-col items-center px-3 py-2 rounded-md min-w-[52px]"
          style={{
            background: rsBackground(info.rs_rating),
            color: rsColor(info.rs_rating),
          }}
        >
          <span className="text-xs text-muted-foreground leading-tight">RS</span>
          <span
            className="text-lg font-bold leading-tight"
            style={{ fontFamily: "var(--font-jetbrains-mono, monospace)" }}
          >
            {info.rs_rating}
          </span>
        </div>

        {/* Konsensus rozeti kaldirildi (KARAR ADAY 21 May 2026) */}
      </div>
    </div>
  );
}
