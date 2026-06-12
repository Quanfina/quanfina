"use client";

import { Layers, CheckCircle2, ThumbsUp, AlertTriangle, MinusCircle } from "lucide-react";
import { useCupHandle } from "@/hooks/use-cup-handle";
import { useFlatBase } from "@/hooks/use-flat-base";
import { useDoubleBottom } from "@/hooks/use-double-bottom";
import { useHighTightFlag } from "@/hooks/use-high-tight-flag";

/**
 * Paket 467 (12 Haz 2026): BaseSummaryCard — /hisse base detector ÖZET.
 *
 * 4 O'Neil base detector'ı (Cup-with-Handle, Flat Base, Double Bottom, High Tight Flag)
 * TEK bakışta konsolide eder. Çoğu hissede hepsi NONE -> 4 kart gizli -> kullanıcı baz
 * hakkında hiçbir şey görmez. Bu özet: "en güçlü baz + kalite + pivot" VEYA "geçerli baz
 * yok" (detector çalıştı, bir şey bulamadı — net). Yeni detector DEĞİL, mevcutları
 * konsolide eder (Yetenek Minimalizmi KARAR #442). React Query cache -> ek API yok.
 */

type Q = "EXCELLENT" | "GOOD" | "MARGINAL" | "NONE" | null | undefined;

const RANK: Record<string, number> = { EXCELLENT: 3, GOOD: 2, MARGINAL: 1 };

const QUALITY_META: Record<"EXCELLENT" | "GOOD" | "MARGINAL", { color: string; icon: React.ReactNode }> = {
  EXCELLENT: { color: "var(--mtp-excellent)", icon: <CheckCircle2 size={15} /> },
  GOOD: { color: "var(--mtp-neutral)", icon: <ThumbsUp size={15} /> },
  MARGINAL: { color: "#F59E0B", icon: <AlertTriangle size={15} /> },
};

export function BaseSummaryCard({ symbol }: { symbol: string }) {
  const cup = useCupHandle(symbol);
  const flat = useFlatBase(symbol);
  const db = useDoubleBottom(symbol);
  const htf = useHighTightFlag(symbol);

  const isLoading = cup.isLoading || flat.isLoading || db.isLoading || htf.isLoading;

  if (isLoading) {
    return (
      <div className="rounded-lg border p-2.5 text-xs text-muted-foreground flex items-center gap-2">
        <Layers size={15} /> Baz paternleri taranıyor...
      </div>
    );
  }

  type BaseItem = { name: string; q: "EXCELLENT" | "GOOD" | "MARGINAL"; pivot: number | null };
  const detected: BaseItem[] = [
    { name: "Cup-with-Handle", q: cup.data?.quality as Q, pivot: cup.data?.pivot_price ?? null },
    { name: "Flat Base", q: flat.data?.quality as Q, pivot: flat.data?.pivot_price ?? null },
    { name: "Double Bottom (W)", q: db.data?.quality as Q, pivot: db.data?.pivot_price ?? null },
    { name: "High Tight Flag", q: htf.data?.quality as Q, pivot: htf.data?.pivot_price ?? null },
  ]
    .filter((b): b is BaseItem => b.q != null && b.q !== "NONE")
    .sort((a, b) => RANK[b.q] - RANK[a.q]);

  // Geçerli baz yok — detector çalıştı, bir şey bulamadı (net bilgi)
  if (detected.length === 0) {
    return (
      <div className="rounded-lg border p-2.5 flex items-center gap-2 text-xs text-muted-foreground">
        <MinusCircle size={15} />
        <span>
          <span className="font-semibold text-foreground">Baz Durumu:</span> geçerli O&apos;Neil baz
          paterni yok (Cup/Flat/Double/HTF — hiçbiri tespit edilmedi)
        </span>
      </div>
    );
  }

  const best = detected[0];
  const meta = QUALITY_META[best.q];
  const others = detected.slice(1);

  return (
    <div
      className="rounded-lg border p-2.5 flex items-center gap-2 flex-wrap"
      style={{ background: `${meta.color}14`, borderColor: `${meta.color}55` }}
    >
      <Layers size={15} style={{ color: meta.color }} />
      <span className="text-xs font-semibold">Baz Durumu:</span>
      <span
        className="inline-flex items-center gap-1 text-[11px] font-bold px-1.5 py-0.5 rounded uppercase tracking-wide"
        style={{ background: meta.color, color: "#fff" }}
      >
        {meta.icon}
        {best.name} · {best.q}
      </span>
      {best.pivot != null && (
        <span className="text-[11px] font-mono tabular-nums text-muted-foreground">
          pivot ${best.pivot.toFixed(2)}
        </span>
      )}
      {others.length > 0 && (
        <span className="text-[10px] text-muted-foreground italic">
          +{others.length} diğer ({others.map((o) => o.name.split(" ")[0]).join(", ")})
        </span>
      )}
      <span className="text-[10px] text-muted-foreground italic ml-auto">detay ↓ kartlarda</span>
    </div>
  );
}
