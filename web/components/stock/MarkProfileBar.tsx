"use client";

import { usePivotBreakout } from "@/hooks/use-pivot-breakout";
import { useClimaxRun } from "@/hooks/use-climax-run";
import { useRsRating } from "@/hooks/use-rs-rating";
import { useStageTransition } from "@/hooks/use-stage-transition";

/**
 * KARAR #733 alt-paket (Paket 125, 26 May 2026): Mark Profili kompakt rozet bar.
 * /hisse detay sayfası üst kısmında grid kartlarının yatay özeti — Sn. Ferit
 * hisse açtığında Mark canon profilini 1 bakışta görür. TanStack Query cache
 * aşağıdaki grid kartlarıyla shared (yeniden network çağrı yok). P436: layout
 * sidebar'dan grafik-altı grid'e geçti.
 */

export function MarkProfileBar({ symbol }: { symbol: string }) {
  const { data: pivot } = usePivotBreakout(symbol);
  const { data: climax } = useClimaxRun(symbol);
  const { data: rs } = useRsRating(symbol);
  const { data: stage } = useStageTransition(symbol);

  const hasAnyData = pivot?.status || climax?.category || rs?.rs_rating != null || stage?.category;
  if (!hasAnyData) return null;

  return (
    <div className="flex flex-wrap items-center gap-1.5 px-6 py-2 border-b bg-muted/30">
      <span className="text-[10px] font-bold uppercase tracking-wider text-muted-foreground mr-1">
        Mark Profili:
      </span>

      {/* Pivot rozet */}
      {pivot?.status && (
        <span
          className="text-[10px] font-bold px-1.5 py-0.5 rounded uppercase tracking-wider"
          style={{
            background:
              pivot.status === "CONFIRMED"
                ? "rgba(40,167,69,0.15)"
                : pivot.status === "WEAK"
                ? "rgba(245,158,11,0.15)"
                : pivot.status === "NEAR_PIVOT"
                ? "rgba(75,156,211,0.15)"
                : "rgba(220,53,69,0.10)",
            color:
              pivot.status === "CONFIRMED"
                ? "var(--mtp-excellent)"
                : pivot.status === "WEAK"
                ? "#F59E0B"
                : pivot.status === "NEAR_PIVOT"
                ? "var(--mtp-neutral)"
                : "var(--mtp-danger)",
          }}
          title={`Pivot Breakout — ${pivot.mark_says}`}
        >
          Pivot:{" "}
          {pivot.status === "CONFIRMED"
            ? "AL ✓"
            : pivot.status === "WEAK"
            ? "Zayıf ⚠️"
            : pivot.status === "NEAR_PIVOT"
            ? "Yakın ⏳"
            : "Altı ○"}
        </span>
      )}

      {/* RS rozet */}
      {rs?.rs_rating != null && (
        <span
          className="text-[10px] font-bold px-1.5 py-0.5 rounded uppercase tracking-wider"
          style={{
            background:
              rs.rs_rating >= 80
                ? "rgba(40,167,69,0.15)"
                : rs.rs_rating >= 70
                ? "rgba(75,156,211,0.15)"
                : rs.rs_rating >= 50
                ? "rgba(245,158,11,0.10)"
                : "rgba(220,53,69,0.10)",
            color:
              rs.rs_rating >= 80
                ? "var(--mtp-excellent)"
                : rs.rs_rating >= 70
                ? "var(--mtp-neutral)"
                : rs.rs_rating >= 50
                ? "#F59E0B"
                : "var(--mtp-danger)",
          }}
          title={`RS Rating — ${rs.mark_says}`}
        >
          RS {rs.rs_rating}
          {rs.rs_rating >= 80 ? " 👑" : rs.rs_rating < 50 ? " ↓" : ""}
        </span>
      )}

      {/* Climax — sadece uyarı durumlarında */}
      {(climax?.category === "CLIMAX_TOP" || climax?.category === "POTENTIAL_CLIMAX") && (
        <span
          className="text-[10px] font-bold px-1.5 py-0.5 rounded uppercase tracking-wider"
          style={{
            background:
              climax.category === "CLIMAX_TOP"
                ? "rgba(220,53,69,0.15)"
                : "rgba(245,158,11,0.15)",
            color:
              climax.category === "CLIMAX_TOP" ? "var(--mtp-danger)" : "#F59E0B",
          }}
          title={`Climax Run — ${climax.mark_says}`}
        >
          Climax {climax.category === "CLIMAX_TOP" ? "🔴 SAT" : "⚠️"}
        </span>
      )}

      {/* Stage — NO_TRANSITION gizli */}
      {stage?.category && stage.category !== "NO_TRANSITION" && (
        <span
          className="text-[10px] font-bold px-1.5 py-0.5 rounded uppercase tracking-wider"
          style={{
            background:
              stage.category === "CONFIRMED_STAGE_2"
                ? "rgba(40,167,69,0.15)"
                : stage.category === "EARLY_STAGE_2"
                ? "rgba(245,158,11,0.15)"
                : "rgba(75,156,211,0.15)",
            color:
              stage.category === "CONFIRMED_STAGE_2"
                ? "var(--mtp-excellent)"
                : stage.category === "EARLY_STAGE_2"
                ? "#F59E0B"
                : "var(--mtp-neutral)",
          }}
          title={`Stage Transition — ${stage.mark_says}`}
        >
          Stage:{" "}
          {stage.category === "CONFIRMED_STAGE_2"
            ? "Onaylı ✓"
            : stage.category === "EARLY_STAGE_2"
            ? "Erken ⚡"
            : "Olgun ⏳"}
        </span>
      )}

      <span className="text-[9px] text-muted-foreground italic ml-auto hidden sm:inline">
        Detay: aşağıdaki kartlar
      </span>
    </div>
  );
}
