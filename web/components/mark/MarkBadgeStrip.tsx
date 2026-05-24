"use client";

import type { ReactNode } from "react";

/**
 * KARAR ADAY #721 — Mark Profile Badge Strip (DRY component)
 *
 * Mark Minervini algoritma rozetleri için generic bileşen.
 * Tüm sayfalarda (screens, hisse detay, watchlist tooltip) aynı görsel disiplin.
 *
 * Backend field eşleme:
 * - vcp_quality_score = 'EXCELLENT' → 🏆 VCP A+
 * - vcp_quality_score = 'PASS'      → 📊 VCP Pass
 * - vcp_ready_score >= 70           → 🎯 VCP Ready (score)
 * - power_play_pass = true          → ⚡ Power Play
 * - tennis_ball_pattern = 'TENNIS_BALL' → 🎾 Tennis Ball
 * - volume_asymmetry_tier = 'healthy' → 📈 Healthy Accum
 *
 * Cloud SQL Migration 004-007 uygulandıkça otomatik canlanır
 * (AÇIK KONU #70 bağlı). Field undefined/null ise rozet gösterilmez.
 *
 * Mark birebir alıntı tooltip her rozette (KALICI İLKE #4).
 */

export interface MarkSignals {
  vcp_quality_score?: "EXCELLENT" | "PASS" | null;
  vcp_ready_score?: number | null;
  power_play_pass?: boolean | null;
  tennis_ball_pattern?: "TENNIS_BALL" | "partial" | "none" | null;
  volume_asymmetry_tier?: "healthy" | "neutral" | "distribution" | null;
  code_33_pattern?: "CODE_33" | "partial" | "none" | null;
  // KARAR ADAY #735 (24 May 2026): Carr Stage rozet (Mark+Carr birleşik)
  carr_stage?: 1 | 2 | 3 | 4 | null;
}

interface BadgeProps {
  emoji: string;
  label: string;
  color: string;     // background tint
  textColor: string;
  tooltip: string;
}

function Badge({ emoji, label, color, textColor, tooltip }: BadgeProps): ReactNode {
  return (
    <span
      title={tooltip}
      className="inline-flex items-center gap-1 px-1.5 py-0.5 rounded text-xs font-medium border whitespace-nowrap cursor-help"
      style={{
        background: color,
        color: textColor,
        borderColor: `${textColor}33`,
      }}
    >
      <span aria-hidden="true">{emoji}</span>
      <span>{label}</span>
    </span>
  );
}

interface Props {
  signals: MarkSignals;
  /** Az veya çok rozet için yoğunluk — "compact" sadece en kritik, "full" hepsini */
  density?: "compact" | "full";
  /** Hiç sinyal yoksa placeholder göster */
  showEmpty?: boolean;
}

export function MarkBadgeStrip({ signals, density = "compact", showEmpty = false }: Props) {
  const badges: ReactNode[] = [];

  // VCP Quality — En kritik (Mark TLSMW VCP Footprint TLSMW s.~210)
  if (signals.vcp_quality_score === "EXCELLENT") {
    badges.push(
      <Badge
        key="vcp-exc"
        emoji="🏆"
        label="VCP A+"
        color="rgba(40,167,69,0.10)"
        textColor="var(--mtp-excellent)"
        tooltip="VCP EXCELLENT — Mark canon 'en sığ daralma' (TLSMW VCP Footprint, KARAR #466)"
      />
    );
  } else if (signals.vcp_quality_score === "PASS" && density === "full") {
    badges.push(
      <Badge
        key="vcp-pass"
        emoji="📊"
        label="VCP Pass"
        color="rgba(75,156,211,0.08)"
        textColor="var(--mtp-good, #4B9CD3)"
        tooltip="VCP Pass — Brandon yarılanma kuralı (KARAR #461)"
      />
    );
  }

  // Power Play — Mark TLSMW Ch 9 (HTF)
  if (signals.power_play_pass === true) {
    badges.push(
      <Badge
        key="pp"
        emoji="⚡"
        label="Power Play"
        color="rgba(245,158,11,0.10)"
        textColor="#F59E0B"
        tooltip="Power Play (HTF) — Mark TLSMW Ch 9: explosive %90-130 / 8 hafta + tight base 3-6 hafta (KARAR #467)"
      />
    );
  }

  // VCP Ready Score 70+ (Inside Day + V-Dry + Tight Range)
  if (typeof signals.vcp_ready_score === "number" && signals.vcp_ready_score >= 70) {
    const score = signals.vcp_ready_score;
    badges.push(
      <Badge
        key="ready"
        emoji="🎯"
        label={`Ready ${score}`}
        color="rgba(75,156,211,0.10)"
        textColor="var(--mtp-good, #4B9CD3)"
        tooltip={`VCP Ready Score ${score}/100 — Minervini Uzmanı: Inside Day + V-Dry + Tight Range (KARAR #465)`}
      />
    );
  }

  // Tennis Ball — Mark William M. B. Berger (TLSMW s.253)
  if (signals.tennis_ball_pattern === "TENNIS_BALL") {
    badges.push(
      <Badge
        key="tb"
        emoji="🎾"
        label="Tennis Ball"
        color="rgba(40,167,69,0.10)"
        textColor="var(--mtp-excellent)"
        tooltip='Tennis Ball — Mark: "I want to own tennis balls, not eggs." Sağlıklı pullback + recovery (KARAR ADAY #893)'
      />
    );
  }

  // Volume Asymmetry — Mark TLSMW s.234 (Up vol >> Down vol)
  if (signals.volume_asymmetry_tier === "healthy") {
    badges.push(
      <Badge
        key="va"
        emoji="📈"
        label="Healthy Accum"
        color="rgba(40,167,69,0.10)"
        textColor="var(--mtp-excellent)"
        tooltip="Volume Asymmetry healthy — institutional accumulation (Mark TLSMW s.234, KARAR ADAY #882)"
      />
    );
  } else if (signals.volume_asymmetry_tier === "distribution" && density === "full") {
    badges.push(
      <Badge
        key="va-dist"
        emoji="📉"
        label="Distribution"
        color="rgba(220,53,69,0.10)"
        textColor="var(--mtp-danger)"
        tooltip="Volume Distribution — institutional satış sinyali (Mark TLSMW s.234)"
      />
    );
  }

  // KARAR ADAY #735 (24 May 2026): Carr Stage rozet (Stan Weinstein 4-Stage)
  // Stage 2 Advancing = Mark superperformance, Stage 4 Declining = uzak dur
  if (signals.carr_stage === 2) {
    badges.push(
      <Badge
        key="cs2"
        emoji="📈"
        label="Stage 2"
        color="rgba(40,167,69,0.10)"
        textColor="var(--mtp-excellent)"
        tooltip="Carr Stage 2 (Advancing) — Mark+Carr alım fazı, superperformance hisseleri burada (KARAR #733)"
      />
    );
  } else if (signals.carr_stage === 4) {
    badges.push(
      <Badge
        key="cs4"
        emoji="⛔"
        label="Stage 4"
        color="rgba(220,53,69,0.10)"
        textColor="var(--mtp-danger)"
        tooltip="Carr Stage 4 (Declining) — Mark felsefesi: UZAK DUR. 30W MA altı + slope negatif (KARAR #733)"
      />
    );
  } else if (signals.carr_stage === 1 && density === "full") {
    badges.push(
      <Badge
        key="cs1"
        emoji="⏳"
        label="Stage 1"
        color="rgba(75,156,211,0.08)"
        textColor="var(--mtp-good, #4B9CD3)"
        tooltip="Carr Stage 1 (Basing) — 30W MA flat, breakout izle (KARAR #733)"
      />
    );
  } else if (signals.carr_stage === 3 && density === "full") {
    badges.push(
      <Badge
        key="cs3"
        emoji="⚠️"
        label="Stage 3"
        color="rgba(245,158,11,0.10)"
        textColor="#F59E0B"
        tooltip="Carr Stage 3 (Topping) — Slope flattening, çıkış hazırlık, trail stop sıkı (KARAR #733)"
      />
    );
  }

  // Code 33 — Mark TLSMW s.173 (EPS+Sales+Margin triple accel) — gelecek
  if (signals.code_33_pattern === "CODE_33") {
    badges.push(
      <Badge
        key="c33"
        emoji="⭐"
        label="Code 33"
        color="rgba(255,215,0,0.15)"
        textColor="#B8860B"
        tooltip="Code 33 — Mark TLSMW s.173: 3-quarter EPS+Sales+Margin triple accel (potent recipe)"
      />
    );
  }

  if (badges.length === 0) {
    if (!showEmpty) return null;
    return (
      <span className="text-xs text-muted-foreground italic">—</span>
    );
  }

  return <div className="inline-flex flex-wrap gap-1 items-center">{badges}</div>;
}
