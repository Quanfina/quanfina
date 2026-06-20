"use client";

import { TrendingUp, Minus, AlertTriangle } from "lucide-react";
import { useGroupRS } from "@/hooks/use-group-rs";

/**
 * Paket 553 (20 Haz 2026): Grup/Endüstri RS onayı kartı (/hisse/[symbol]).
 *
 * 2 uzman convergence (Minervini TLSMW s.95 "leading stocks in leading groups" + O'Neil
 * CAN SLIM 'L': bir hissenin fiyat hareketinin ~yarısı endüstri grubundan gelir). Hisse RS
 * Rating'in yanında durur: güçlü hisse + güçlü grup = tam teyit; güçlü hisse + zayıf grup =
 * "yalnız kurt" riski. sector_rotation (11 SPDR, gerçek günlük tarama) RS rank.
 *
 * DÜRÜSTLÜK (Kural #26): SEKTÖR seviyesi proxy (11 SPDR), gerçek IBD 197 endüstri grubu
 * DEĞİL. Veri yoksa "veri yok" (Kural #28: MOCK YOK, dürüst eksiklik).
 */
export function GroupRSCard({ symbol }: { symbol: string }) {
  const { data, isLoading, isError } = useGroupRS(symbol);

  if (isLoading) {
    return (
      <div className="rounded-lg border p-3 text-xs text-muted-foreground">
        Sektör RS yükleniyor...
      </div>
    );
  }
  if (isError || !data) return null;

  const tier = data.tier;
  const color = !data.available
    ? "var(--mtp-neutral)"
    : tier === "LEADING"
      ? "var(--mtp-excellent)"
      : tier === "LAGGING"
        ? "var(--mtp-danger)"
        : "var(--mtp-warning, #d97706)";
  const bg = !data.available
    ? "rgba(75, 156, 211, 0.08)"
    : tier === "LEADING"
      ? "rgba(40, 167, 69, 0.10)"
      : tier === "LAGGING"
        ? "rgba(220, 53, 69, 0.10)"
        : "rgba(217,119,6,0.10)";

  const tierLabel =
    tier === "LEADING"
      ? "LİDER SEKTÖR"
      : tier === "LAGGING"
        ? "ZAYIF SEKTÖR — YALNIZ KURT"
        : tier === "NEUTRAL"
          ? "NÖTR SEKTÖR"
          : "VERİ YOK";

  const Icon = !data.available
    ? Minus
    : data.is_lone_wolf
      ? AlertTriangle
      : tier === "LEADING"
        ? TrendingUp
        : Minus;

  return (
    <div
      className="rounded-lg border p-3 flex flex-col gap-2"
      style={{ background: bg, borderColor: `${color}55` }}
      data-testid="hisse-group-rs-card"
    >
      <h3 className="text-xs font-semibold flex items-center gap-1.5">
        <span style={{ color }}>
          <Icon size={16} />
        </span>
        Grup RS (Sektör Teyidi)
        <span className="text-[10px] font-normal text-muted-foreground italic">
          (Minervini s.95 + O&apos;Neil &apos;L&apos;)
        </span>
      </h3>

      {data.available ? (
        <>
          <div className="flex items-baseline gap-2 pt-1 border-t border-muted-foreground/15">
            <span
              className="text-[11px] font-bold uppercase tracking-wide"
              style={{ color }}
            >
              {tierLabel}
            </span>
            <span className="font-mono font-semibold tabular-nums text-xs ml-auto" style={{ color }}>
              {`${data.sector} #${data.rs_rank}/${data.total}`}
            </span>
          </div>
          <p className="text-[10px] text-muted-foreground leading-relaxed">{data.mark_says}</p>
        </>
      ) : (
        <p className="text-[10px] text-muted-foreground leading-relaxed">{data.mark_says}</p>
      )}
    </div>
  );
}
