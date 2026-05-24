"use client";

import { useMemo, useState } from "react";
import { Layers, Lock, Unlock, CheckCircle2, AlertCircle, XCircle } from "lucide-react";
import {
  evaluatePyramidTier,
  suggestPositionDollars,
  TIER_LIMITS,
  TIER_ORDER,
  type TierEvaluation,
} from "@/lib/pyramid-calculator";
import { usePyramidTier } from "@/hooks/use-pyramid-tier";

/**
 * KARAR ADAY #732 (24 May 2026) — Mark Pyramid Calculator Karti.
 *
 * KARAR #487 Mark Pyramid 3-Tier (Pilot/Standart/Full) + "Trades Working" güvenlik kilidi.
 *
 * Sn. Ferit pozisyon ekleme öncesi tier hesabı görür:
 * - Mevcut pozisyon hangi tier?
 * - Sonraki tier alış noktası ne?
 * - Mark X kilit: önceki tier kâra geçti mi?
 */

interface Props {
  entryPrice: string;
  shares: string;
  /** Toplam portföy $ — Sn. Ferit ayarı (placeholder $100K) */
  portfolioValue?: number;
  /** Mark X kilit override: pilot/standart kâra geçti mi */
  initialPrevTierProfitable?: boolean;
}

const DEFAULT_PORTFOLIO_VALUE = 100000;

const SEVERITY_ICONS = {
  ok: <CheckCircle2 size={14} />,
  info: <Unlock size={14} />,
  warn: <AlertCircle size={14} />,
  violation: <XCircle size={14} />,
};

export function MarkPyramidCard({
  entryPrice,
  shares,
  portfolioValue = DEFAULT_PORTFOLIO_VALUE,
  initialPrevTierProfitable = false,
}: Props) {
  const [prevTierProfitable, setPrevTierProfitable] = useState(initialPrevTierProfitable);

  // Pozisyon değeri (entry × shares)
  const positionValue = useMemo(() => {
    const ep = parseFloat(entryPrice);
    const sh = parseInt(shares);
    return !isNaN(ep) && !isNaN(sh) && ep > 0 && sh > 0 ? ep * sh : 0;
  }, [entryPrice, shares]);

  // KARAR #732 alt-paket (Paket 19): backend /api/pyramid/tier tercih + client fallback
  const { data: backendResult } = usePyramidTier({
    positionValue,
    portfolioValue,
    prevTierProfitable,
  });

  // Backend cevap geldiyse onu kullan, gelmezse client-side hesap (DRY fallback)
  const evalResult: TierEvaluation = useMemo(() => {
    const clientFallback = evaluatePyramidTier(positionValue, portfolioValue, prevTierProfitable);
    if (!backendResult) {
      return clientFallback;
    }
    // Backend cevabını client TierEvaluation shape'ine çevir
    // (görsel meta — color/emoji/markSays vs. — client fallback'tan, severity/tier backend'den)
    const limits =
      backendResult.tier === "PILOT" ? TIER_LIMITS.PILOT :
      backendResult.tier === "STANDARD" ? TIER_LIMITS.STANDARD :
      backendResult.tier === "FULL" ? TIER_LIMITS.FULL : null;
    return {
      ...clientFallback,
      positionPct: backendResult.position_pct,
      currentTier:
        backendResult.tier === "BELOW_PILOT" ? "BELOW_PILOT" :
        backendResult.tier === "OVER_MAX" ? "OVER_MAX" :
        backendResult.tier,
      tierLabel: limits ? limits.label : clientFallback.tierLabel,
      severity: backendResult.severity,
      markSays: backendResult.mark_says,
      nextTier: backendResult.next_tier,
    };
  }, [backendResult, positionValue, portfolioValue, prevTierProfitable]);

  if (evalResult.positionValue <= 0) {
    return (
      <div className="rounded-md border border-dashed border-muted-foreground/30 p-3 text-xs text-muted-foreground">
        <span className="font-semibold">🔺 Mark Pyramid: </span>
        Entry fiyatı ve adet girilince tier hesabı aktif olacak.
      </div>
    );
  }

  return (
    <div
      className="rounded-md border p-3 flex flex-col gap-3"
      style={{
        background: "rgba(75,156,211,0.04)",
        borderColor: `${evalResult.tierColor}55`,
      }}
    >
      <div className="flex items-center justify-between gap-2">
        <div className="flex items-center gap-2">
          <Layers size={16} style={{ color: evalResult.tierColor }} />
          <h3 className="text-sm font-semibold">
            Mark Pyramid Tier
          </h3>
          <span className="text-[10px] font-normal text-muted-foreground italic">
            (KARAR #487 — Pilot/Standart/Full)
          </span>
        </div>
        <span
          className="text-xs px-2 py-0.5 rounded font-mono font-semibold inline-flex items-center gap-1"
          style={{ background: `${evalResult.tierColor}22`, color: evalResult.tierColor }}
        >
          {SEVERITY_ICONS[evalResult.severity]}
          %{evalResult.positionPct.toFixed(2)} — {evalResult.tierLabel}
        </span>
      </div>

      {/* Mark felsefe yorumu */}
      <p
        className="text-xs leading-relaxed px-2 py-1.5 rounded bg-background/50 border-l-2"
        style={{ borderLeftColor: evalResult.tierColor, color: evalResult.tierColor }}
      >
        {evalResult.markSays}
      </p>

      {/* 3 Tier görsel ölçek */}
      <div className="flex flex-col gap-1.5">
        {TIER_ORDER.map((tier) => {
          const limits = TIER_LIMITS[tier];
          const active = evalResult.currentTier === tier;
          const reached = evalResult.positionPct >= limits.minPct;
          const range = suggestPositionDollars(portfolioValue, tier);
          return (
            <div
              key={tier}
              className="flex items-center gap-3 px-2 py-1.5 rounded border text-xs"
              style={{
                background: active ? `${evalResult.tierColor}11` : "transparent",
                borderColor: active ? `${evalResult.tierColor}55` : "var(--border)",
                opacity: reached ? 1 : 0.6,
              }}
            >
              <span aria-hidden="true" className="text-lg leading-none">
                {limits.emoji}
              </span>
              <div className="flex-1 min-w-0">
                <div className="flex items-center gap-2">
                  <span className="font-semibold">{limits.label}</span>
                  <span className="text-muted-foreground tabular-nums">
                    %{limits.minPct}-{limits.maxPct}
                  </span>
                  <span className="text-muted-foreground tabular-nums ml-auto">
                    ${range.min.toFixed(0)} - ${range.max.toFixed(0)}
                  </span>
                </div>
                <p className="text-[11px] text-muted-foreground mt-0.5 italic">
                  {limits.description}
                </p>
              </div>
            </div>
          );
        })}
      </div>

      {/* Mark X kilit toggle */}
      <label className="flex items-center gap-2 text-xs cursor-pointer p-2 rounded border bg-background/40">
        {prevTierProfitable ? (
          <Unlock size={14} style={{ color: "var(--mtp-excellent)" }} />
        ) : (
          <Lock size={14} style={{ color: "#F59E0B" }} />
        )}
        <input
          type="checkbox"
          checked={prevTierProfitable}
          onChange={(e) => setPrevTierProfitable(e.target.checked)}
          className="sr-only"
        />
        <button
          type="button"
          onClick={() => setPrevTierProfitable((v) => !v)}
          className="flex-1 text-left"
        >
          <span className="font-semibold">
            {prevTierProfitable ? "Önceki tier KÂRA GEÇTİ ✓" : "Önceki tier henüz kârlı değil ✗"}
          </span>
          <span className="text-[10px] text-muted-foreground block mt-0.5">
            Mark X: &ldquo;Trades not working = no size increase&rdquo; — bir üst tier&apos;a geçiş izinli mi?
          </span>
        </button>
      </label>

      {/* Sonraki tier önerisi */}
      {evalResult.nextTier && evalResult.nextTierGap && (
        <div className="text-xs px-2 py-1.5 rounded bg-muted/40 flex items-center justify-between">
          <span className="text-muted-foreground">Sonraki Tier:</span>
          <span className="font-semibold">
            {TIER_LIMITS[evalResult.nextTier].label} (%{evalResult.nextTierGap.from}-{evalResult.nextTierGap.to})
          </span>
        </div>
      )}

      {/* Mark felsefesi atıf */}
      <p className="text-[10px] text-muted-foreground italic pt-1 border-t border-muted-foreground/15">
        Mark birebir kaynak zinciri: TraderLion Lesson 7 (Pilot) + Brandon Video (Standart) +
        Mark Video Kelly 2:1 (Full) + Mark X (Trades Working kilit).
      </p>
    </div>
  );
}
