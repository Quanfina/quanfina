"use client";

/**
 * Paket 229 (27 May 2026, KARAR #480 paralel — Mark Canon Pre-flight)
 *
 * Pre-trade Mark Canon Checklist — Trade Aç akışı öncesi disiplin teyidi.
 *
 * Vizyon İLKE #10 + Mark TTLC Sec 1.6 (Plan ZORUNLU) + KARAR #475 (AL/GEÇ
 * Mekanik Karar): Sn. Ferit AddTradeDialog'a girmeden ÖNCE Mark canon
 * 7 koşulu görsel olarak teyit eder. Hatalı pozisyonu önle.
 *
 * 7 Mark Canon Koşulu (Trend Template + plan disiplini):
 *  1. Stage 2 piyasa (Carr/Weinstein — 30W MA üstü, slope pozitif)
 *  2. RS Rating ≥ 70 (Mark TLSMW Leaders First)
 *  3. VCP veya Pivot Setup mevcut (TTLC Sec 4)
 *  4. Plan: Giriş tetikleyicisi net
 *  5. Plan: Stop loss net (Mark TTLC s.131 %7 mutlak)
 *  6. Plan: Hedef net (R/R ≥ 2)
 *  7. Mod farkındalığı (Defansif değilse OK, Rehab uyarı, Agresif izin)
 *
 * Tüm 7 yeşil = AL düğmesi aktif. Eksik varsa Sn. Ferit "bilinçli AL"
 * butonunu kullanır (override — Kural #23 otonom karar yetkisi).
 *
 * Bu komponent AddTradeDialog'un en üstünde render edilebilir veya
 * Trade Aç akışında ön-sayfa olarak kullanılabilir.
 */

import { useMemo } from "react";
import { CheckCircle2, XCircle, AlertCircle } from "lucide-react";
import { useTradingMode } from "@/hooks/use-trading-mode";

export interface PreTradeChecklistProps {
  symbol?: string;
  stage?: number | null;        // Carr stage (1-4)
  rsRating?: number | null;     // 0-100
  vcpPass?: boolean | null;
  pivotPass?: boolean | null;
  planEntryTrigger?: string;
  planStop?: number | null;
  planTarget?: number | null;
  entryPrice?: number | null;
  /**
   * Paket 265 (27 May 2026): planSizePct — Pozisyon büyüklük yüzdesi (Mark
   * TTLC s.85 sektör konsantrasyon paralel — tek pozisyon ≤25% disiplini).
   * 0-100 arası. compact modda gizli.
   */
  planSizePct?: number | null;
  /**
   * Paket 257 (27 May 2026): compact prop — sembol seviyesi mini-versiyon.
   * compact=true ise plan alanları gösterilmez (sadece Stage + RS + Setup + Mod).
   * Default false (tam 8 koşul — P265 ile genişledi).
   */
  compact?: boolean;
}

interface CheckRow {
  label: string;
  status: "ok" | "warn" | "fail";
  detail: string;
}

export function PreTradeChecklist({
  symbol = "—",
  stage,
  rsRating,
  vcpPass,
  pivotPass,
  planEntryTrigger,
  planStop,
  planTarget,
  entryPrice,
  planSizePct,
  compact = false,
}: PreTradeChecklistProps) {
  const tradingMode = useTradingMode();

  const rows = useMemo<CheckRow[]>(() => {
    const result: CheckRow[] = [];

    // 1. Stage 2 piyasa (Carr/Weinstein)
    result.push(
      stage === 2
        ? { label: "Stage 2 piyasa", status: "ok", detail: "30W MA üstü, slope pozitif" }
        : stage === 1
        ? { label: "Stage 2 piyasa", status: "warn", detail: "Stage 1 — temel atmış ama henüz Stage 2 değil" }
        : stage === 3 || stage === 4
        ? { label: "Stage 2 piyasa", status: "fail", detail: `Stage ${stage} — Mark UZAK DUR` }
        : { label: "Stage 2 piyasa", status: "warn", detail: "Stage verisi yok" }
    );

    // 2. RS ≥ 70
    result.push(
      rsRating != null && rsRating >= 70
        ? { label: "RS Rating ≥ 70", status: "ok", detail: `RS ${rsRating} — Leader (TLSMW)` }
        : rsRating != null && rsRating >= 50
        ? { label: "RS Rating ≥ 70", status: "warn", detail: `RS ${rsRating} — Average (Leader değil)` }
        : rsRating != null
        ? { label: "RS Rating ≥ 70", status: "fail", detail: `RS ${rsRating} — Laggard (Mark canon dışı)` }
        : { label: "RS Rating ≥ 70", status: "warn", detail: "RS verisi yok" }
    );

    // 3. VCP veya Pivot setup
    const setupOK = vcpPass === true || pivotPass === true;
    result.push(
      setupOK
        ? { label: "VCP / Pivot Setup", status: "ok", detail: vcpPass ? "VCP onaylı" : "Pivot breakout onaylı" }
        : { label: "VCP / Pivot Setup", status: "warn", detail: "Belirli bir setup tanımlı değil (TTLC Sec 4)" }
    );

    // 4. Plan giriş tetikleyicisi (compact modda gizli — sembol seviyesi sayfa)
    if (!compact) {
      result.push(
        planEntryTrigger && planEntryTrigger.trim().length > 0
          ? { label: "Plan: Giriş tetikleyicisi", status: "ok", detail: planEntryTrigger.slice(0, 60) }
          : { label: "Plan: Giriş tetikleyicisi", status: "fail", detail: "Mark TTLC Sec 1.6 — ZORUNLU" }
      );
    }

    // 5. Plan stop (Mark TTLC s.131 %7 mutlak) — compact modda gizli
    if (compact) {
      // Compact: sadece Stage + RS + Setup + Mod (4 koşul)
    } else if (planStop != null && entryPrice != null && entryPrice > 0) {
      const stopPct = ((entryPrice - planStop) / entryPrice) * 100;
      if (stopPct > 0 && stopPct <= 7) {
        result.push({ label: "Plan: Stop loss", status: "ok", detail: `${stopPct.toFixed(1)}% (Mark %7 limit içinde)` });
      } else if (stopPct > 7) {
        result.push({ label: "Plan: Stop loss", status: "fail", detail: `${stopPct.toFixed(1)}% — Mark TTLC s.131 %7 mutlak limit AŞILDI` });
      } else {
        result.push({ label: "Plan: Stop loss", status: "fail", detail: "Stop entry üstünde (long için imkansız)" });
      }
    } else {
      result.push({ label: "Plan: Stop loss", status: "fail", detail: "Stop $ tanımlı değil" });
    }

    // 6. Plan hedef (R/R ≥ 2) — compact modda gizli
    if (compact) {
      // skip — sembol seviyesi sayfa hedef hesap yapmaz
    } else if (planTarget != null && planStop != null && entryPrice != null && entryPrice > 0) {
      const risk = entryPrice - planStop;
      const reward = planTarget - entryPrice;
      const rr = risk > 0 ? reward / risk : 0;
      if (rr >= 2) {
        result.push({ label: "Plan: Hedef R/R ≥ 2", status: "ok", detail: `R/R = ${rr.toFixed(2)} (Mark uyumlu)` });
      } else if (rr >= 1) {
        result.push({ label: "Plan: Hedef R/R ≥ 2", status: "warn", detail: `R/R = ${rr.toFixed(2)} (zayıf — Mark minimum 2)` });
      } else {
        result.push({ label: "Plan: Hedef R/R ≥ 2", status: "fail", detail: `R/R = ${rr.toFixed(2)} (kabul edilemez)` });
      }
    } else {
      result.push({ label: "Plan: Hedef R/R ≥ 2", status: "fail", detail: "Hedef $ tanımlı değil" });
    }

    // 7'. Pozisyon büyüklük disiplini (Mark TTLC s.85) — compact modda gizli
    if (!compact) {
      if (planSizePct != null && planSizePct > 0) {
        if (planSizePct <= 25) {
          result.push({ label: "Plan: Pozisyon ≤ 25%", status: "ok", detail: `${planSizePct.toFixed(0)}% (Mark TTLC s.85 sektör limiti içinde)` });
        } else if (planSizePct <= 30) {
          result.push({ label: "Plan: Pozisyon ≤ 25%", status: "warn", detail: `${planSizePct.toFixed(0)}% — Mark canon 25-30% sınır bölgesi` });
        } else {
          result.push({ label: "Plan: Pozisyon ≤ 25%", status: "fail", detail: `${planSizePct.toFixed(0)}% — Mark TTLC s.85 LİMİT AŞILDI` });
        }
      } else {
        result.push({ label: "Plan: Pozisyon ≤ 25%", status: "warn", detail: "Pozisyon % tanımlı değil" });
      }
    }

    // 7. Mod farkındalığı
    if (tradingMode.mode === "defansif") {
      result.push({ label: "Mod farkındalığı", status: "fail", detail: "DEFANSİF mod — Mark TTLC s.187 yeni AL BLOK" });
    } else if (tradingMode.mode === "rehab") {
      result.push({ label: "Mod farkındalığı", status: "warn", detail: "REHAB mod — %0.5 R sizing (yarım pozisyon)" });
    } else if (tradingMode.mode === "agresif") {
      result.push({ label: "Mod farkındalığı", status: "ok", detail: "AGRESİF mod — %1.5-2 R Conviction High" });
    } else {
      result.push({ label: "Mod farkındalığı", status: "ok", detail: "NORMAL mod — standart %1 R sizing" });
    }

    return result;
  }, [stage, rsRating, vcpPass, pivotPass, planEntryTrigger, planStop, planTarget, entryPrice, planSizePct, tradingMode.mode, compact]);

  const okCount = rows.filter((r) => r.status === "ok").length;
  const failCount = rows.filter((r) => r.status === "fail").length;
  const warnCount = rows.filter((r) => r.status === "warn").length;

  const overallColor =
    failCount > 0
      ? "var(--mtp-danger)"
      : warnCount > 0
      ? "#F59E0B"
      : "var(--mtp-excellent)";
  const overallLabel =
    failCount > 0
      ? `${failCount} kritik eksik`
      : warnCount > 0
      ? `${warnCount} uyarı`
      : "Tüm Mark canon koşulları sağlandı";

  return (
    <div
      className="rounded-md border p-3 text-xs"
      style={{
        background: `color-mix(in srgb, ${overallColor} 6%, transparent)`,
        borderColor: `${overallColor}55`,
      }}
    >
      <div className="flex items-center justify-between mb-2 pb-2 border-b" style={{ borderColor: `${overallColor}33` }}>
        <span className="font-semibold" style={{ color: overallColor }}>
          Mark Canon Pre-flight — {symbol}
        </span>
        <span className="text-[10px]" style={{ color: overallColor }}>
          {okCount} / {rows.length} ✓ — {overallLabel}
        </span>
      </div>
      <ul className="space-y-1">
        {rows.map((r) => (
          <li key={r.label} className="flex items-start gap-1.5">
            {r.status === "ok" ? (
              <CheckCircle2 size={13} className="mt-0.5 shrink-0" style={{ color: "var(--mtp-excellent)" }} />
            ) : r.status === "warn" ? (
              <AlertCircle size={13} className="mt-0.5 shrink-0" style={{ color: "#F59E0B" }} />
            ) : (
              <XCircle size={13} className="mt-0.5 shrink-0" style={{ color: "var(--mtp-danger)" }} />
            )}
            <span className="flex-1">
              <b>{r.label}</b>
              <span className="text-muted-foreground"> — {r.detail}</span>
            </span>
          </li>
        ))}
      </ul>
    </div>
  );
}
