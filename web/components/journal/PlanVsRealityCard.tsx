"use client";

import { CheckCircle2, AlertCircle, XCircle, Target, TrendingUp } from "lucide-react";
import type { Trade } from "@/types/trade";
import { computeRMultiple, formatR } from "@/lib/r-multiple";

/**
 * KARAR ADAY #725 (24 May 2026) — Plan vs Reality Karti.
 *
 * Mark TTLC Sec 4 birebir: "Know the truth about your trading."
 * Trade kapatılırken Sn. Ferit'in planından sapması ölçülür.
 *
 * KARAR #717 (Plan ZORUNLU) + KARAR #722 (RBA) birleşimi:
 * - plan_stop vs gerçek exit (stop disiplini)
 * - plan_target vs gerçek exit (hedef disiplini)
 * - Plan'a uydu mu? indikatörü → uzun vadede RBA'ya beslenir
 */

interface Props {
  trade: Trade;
  /** Form'dan canlı exit fiyatı (henüz kaydedilmedi) */
  exitPrice: string;
}

interface Deviation {
  label: string;
  planValue: number | null;
  actualValue: number | null;
  /** "+5.0% (planın üstünde)" gibi göreceli sapma */
  deviationText: string;
  /** OK / WARN / VIOLATION semantik */
  severity: "ok" | "warn" | "violation" | "info";
  meaning: string;
}

const SEV_COLORS = {
  ok:        { bg: "rgba(40,167,69,0.10)",  text: "var(--mtp-excellent)",        icon: <CheckCircle2 size={14} /> },
  warn:      { bg: "rgba(245,158,11,0.10)", text: "#F59E0B",                     icon: <AlertCircle size={14} /> },
  violation: { bg: "rgba(220,53,69,0.10)",  text: "var(--mtp-danger)",           icon: <XCircle size={14} /> },
  info:      { bg: "rgba(75,156,211,0.08)", text: "var(--mtp-neutral)",    icon: <Target size={14} /> },
};

function fmtPct(value: number, decimals: number = 1): string {
  return `${value > 0 ? "+" : ""}${value.toFixed(decimals)}%`;
}

function calcDeviations(trade: Trade, exitPriceNum: number): Deviation[] {
  const out: Deviation[] = [];

  // Stop disiplini — exit > stop ise plan'a uydu (stop tetiklenmedi/altına düşmedi)
  if (trade.plan_stop != null) {
    const stopDeviation = ((exitPriceNum - trade.plan_stop) / trade.plan_stop) * 100;
    let severity: Deviation["severity"];
    let meaning: string;
    if (exitPriceNum >= trade.plan_stop) {
      severity = "ok";
      meaning = "Stop seviyesinin üstünde — plan korundu.";
    } else if (exitPriceNum >= trade.plan_stop * 0.97) {
      severity = "warn";
      meaning = "Stop seviyesinin hafif altında (3% tolerans). Slippage olmuş olabilir.";
    } else {
      severity = "violation";
      meaning = "Stop seviyesinin önemli ölçüde altında. Disiplin ihlali — kayıp planlanandan büyük.";
    }
    out.push({
      label: "Stop Disiplini",
      planValue: trade.plan_stop,
      actualValue: exitPriceNum,
      deviationText: fmtPct(stopDeviation),
      severity,
      meaning,
    });
  }

  // Hedef disiplini — exit hedefe ne kadar yakın
  if (trade.plan_target != null) {
    const targetDeviation = ((exitPriceNum - trade.plan_target) / trade.plan_target) * 100;
    let severity: Deviation["severity"];
    let meaning: string;
    if (exitPriceNum >= trade.plan_target) {
      severity = "ok";
      meaning = `Hedefe ulaşıldı veya geçildi. Plan tutmuş — disiplin başarısı.`;
    } else if (exitPriceNum >= trade.plan_target * 0.85) {
      severity = "info";
      meaning = "Hedef yaklaşıldı ama tam ulaşılmadı. Erken çıkış veya doğal düzeltme.";
    } else {
      severity = "warn";
      meaning = "Hedeften uzak çıkış. Stop tetik veya planlı strateji değişikliği olmuş olabilir.";
    }
    out.push({
      label: "Hedef Disiplini",
      planValue: trade.plan_target,
      actualValue: exitPriceNum,
      deviationText: fmtPct(targetDeviation),
      severity,
      meaning,
    });
  }

  return out;
}

export function PlanVsRealityCard({ trade, exitPrice }: Props) {
  // Plan alanları yoksa eski trade (Migration 008 öncesi) — kart gösterilmez
  if (trade.plan_stop == null && trade.plan_target == null) {
    return null;
  }

  const exitPriceNum = parseFloat(exitPrice);
  if (!exitPrice || isNaN(exitPriceNum) || exitPriceNum <= 0) {
    return (
      <div className="rounded-md border px-3 py-2 text-xs text-muted-foreground bg-muted/30">
        <span className="font-semibold">📋 Plan vs Reality: </span>
        Çıkış fiyatı girilince plan disiplini kontrolü gösterilir.
      </div>
    );
  }

  const deviations = calcDeviations(trade, exitPriceNum);
  if (deviations.length === 0) return null;

  // En kötü severity overall — özetin rengi için
  const worstSev = deviations.reduce<Deviation["severity"]>((acc, d) => {
    const order = { ok: 0, info: 1, warn: 2, violation: 3 };
    return order[d.severity] > order[acc] ? d.severity : acc;
  }, "ok");

  const colors = SEV_COLORS[worstSev];

  return (
    <div
      className="rounded-lg border p-3 flex flex-col gap-2"
      style={{
        background: colors.bg,
        borderColor: `${colors.text}55`,
      }}
    >
      <div className="flex items-center gap-2 text-sm font-semibold" style={{ color: colors.text }}>
        {colors.icon}
        <span>Plan vs Reality</span>
        <span className="text-xs font-normal text-muted-foreground italic ml-auto">
          Mark TTLC Sec 4: &ldquo;Know the truth&rdquo;
        </span>
      </div>

      <div className="flex flex-col gap-2">
        {deviations.map((d) => {
          const sevColors = SEV_COLORS[d.severity];
          return (
            <div
              key={d.label}
              className="flex flex-col gap-1 px-2 py-1.5 rounded border bg-background/40"
              style={{ borderColor: `${sevColors.text}33` }}
            >
              <div className="flex items-center gap-2 text-xs">
                <span style={{ color: sevColors.text }}>{sevColors.icon}</span>
                <span className="font-semibold">{d.label}</span>
                <span className="ml-auto tabular-nums" style={{ color: sevColors.text }}>
                  {d.deviationText}
                </span>
              </div>
              <div className="flex items-center gap-3 text-xs text-muted-foreground tabular-nums">
                <span>Plan: ${d.planValue?.toFixed(2)}</span>
                <span>·</span>
                <span>Gerçek: ${d.actualValue?.toFixed(2)}</span>
              </div>
              <span className="text-xs" style={{ color: sevColors.text }}>
                {d.meaning}
              </span>
            </div>
          );
        })}
      </div>

      {/* KARAR #734 sinerji (Paket 25): R-Multiple satırı — Mark RBA tek-trade ölçümü */}
      {trade.plan_stop != null && (() => {
        const rResult = computeRMultiple(trade.entry_price, trade.plan_stop, exitPriceNum, trade.shares);
        if (!rResult) return null;
        return (
          <div
            className="flex items-center gap-3 px-2 py-2 rounded border bg-background/40"
            style={{ borderColor: `${rResult.color}44` }}
          >
            <TrendingUp size={16} style={{ color: rResult.color }} />
            <div className="flex flex-col flex-1 min-w-0">
              <div className="flex items-center gap-2 text-xs">
                <span className="font-semibold">R-Multiple:</span>
                <span
                  className="font-mono font-bold tabular-nums"
                  style={{ color: rResult.color, fontSize: 14 }}
                >
                  {formatR(rResult.r)}
                </span>
                <span
                  className="text-[10px] px-1.5 py-0.5 rounded font-medium"
                  style={{ background: `${rResult.color}22`, color: rResult.color }}
                >
                  {rResult.label}
                </span>
              </div>
              <span className="text-[11px] mt-0.5" style={{ color: rResult.color }}>
                {rResult.markSays}
              </span>
            </div>
          </div>
        );
      })()}

      {trade.plan_exit_strategy && (
        <div className="text-xs pt-2 border-t border-muted-foreground/20">
          <span className="text-muted-foreground font-medium">Plandaki çıkış stratejisi: </span>
          <span className="italic">&ldquo;{trade.plan_exit_strategy}&rdquo;</span>
        </div>
      )}
    </div>
  );
}
