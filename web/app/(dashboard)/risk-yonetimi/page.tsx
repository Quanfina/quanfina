"use client";

import { useMemo, useState } from "react";
import { Calculator, TrendingUp, ShieldCheck, ShieldAlert, Info } from "lucide-react";
import {
  evaluatePyramidTier,
  suggestPositionDollars,
  TIER_LIMITS,
  TIER_ORDER,
  type PyramidTier,
} from "@/lib/pyramid-calculator";
import { usePyramidTier } from "@/hooks/use-pyramid-tier";

/**
 * KARAR #733 alt-paket (Paket 38): Risk Yönetimi Pyramid Calculator sayfası.
 *
 * Mark KARAR #487 v20.98 — 3-Tier Pyramiding Scale + "Trades Working" Kilidi.
 * Sn. Ferit pozisyon büyüklüğü kararını disiplinli vermek için stand-alone tool.
 *
 * Mark felsefe kaynakları:
 * - Pilot (%1-3): TraderLion Lesson 7
 * - Standart (%6.25-12.5): Brandon Video
 * - Full (%15-25): Mark Video Kelly 2:1
 * - Kilit: Mark X "Trades not working = no size increase"
 *
 * Backend tercih + client-side fallback (DRY).
 */

const SEVERITY_BG: Record<string, string> = {
  ok: "rgba(40, 167, 69, 0.10)",
  info: "rgba(75, 156, 211, 0.10)",
  warn: "rgba(245, 158, 11, 0.10)",
  violation: "rgba(220, 53, 69, 0.10)",
};

const SEVERITY_LABEL: Record<string, string> = {
  ok: "DİSİPLİNLİ",
  info: "BİLGİ",
  warn: "DİKKAT",
  violation: "İHLAL",
};

function fmtUsd(v: number): string {
  return `$${v.toLocaleString("en-US", { minimumFractionDigits: 0, maximumFractionDigits: 2 })}`;
}

export default function RiskYonetimiPage() {
  // Form state
  const [portfolioStr, setPortfolioStr] = useState("100000");
  const [positionStr, setPositionStr] = useState("");
  const [prevProfitable, setPrevProfitable] = useState(false);

  const portfolioValue = parseFloat(portfolioStr) || 0;
  const positionValue = parseFloat(positionStr) || 0;

  // Backend hook (DRY) + client fallback
  const backend = usePyramidTier({
    portfolioValue,
    positionValue,
    prevTierProfitable: prevProfitable,
  });

  // Client fallback (backend down ise veya inputlar henüz girilmediyse)
  const clientEval = useMemo(() => {
    if (positionValue <= 0 || portfolioValue <= 0) return null;
    return evaluatePyramidTier(positionValue, portfolioValue, prevProfitable);
  }, [positionValue, portfolioValue, prevProfitable]);

  // Backend cevabı varsa onu göster, yoksa client
  const showBackend = backend.data != null;
  const tier = showBackend ? backend.data!.tier : clientEval?.currentTier ?? null;
  const positionPct = showBackend ? backend.data!.position_pct : clientEval?.positionPct ?? 0;
  const severity = showBackend ? backend.data!.severity : clientEval?.severity ?? "info";
  const markSays = showBackend ? backend.data!.mark_says : clientEval?.markSays ?? "Pozisyon ve portföy değerlerini gir.";
  const nextTier = showBackend ? backend.data!.next_tier : clientEval?.nextTier ?? null;

  // 3 tier $ aralıkları
  const tierDollarRanges = useMemo(() => {
    if (portfolioValue <= 0) return null;
    return TIER_ORDER.map((t) => ({
      tier: t,
      ...suggestPositionDollars(portfolioValue, t),
    }));
  }, [portfolioValue]);

  return (
    <div className="p-6 flex flex-col gap-6 max-w-4xl">
      {/* Header */}
      <div className="flex items-center gap-3">
        <Calculator size={24} className="text-muted-foreground" />
        <div>
          <h1 className="text-2xl font-semibold tracking-tight">Risk Yönetimi — Pyramid Calculator</h1>
          <p className="text-sm text-muted-foreground">
            Mark Minervini 3-Tier Pyramiding (KARAR #487 v20.98) — Pilot / Standart / Full
          </p>
        </div>
      </div>

      {/* Input Form */}
      <div className="rounded-lg border bg-card p-4 flex flex-col gap-4">
        <h2 className="text-sm font-semibold flex items-center gap-2">
          <TrendingUp size={16} className="text-muted-foreground" />
          Pozisyon Girişi
        </h2>

        <div className="grid grid-cols-1 md:grid-cols-2 gap-4">
          <div className="flex flex-col gap-1.5">
            <label htmlFor="portfolio" className="text-xs font-medium text-muted-foreground">
              Toplam Portföy ($)
            </label>
            <input
              id="portfolio"
              type="number"
              value={portfolioStr}
              onChange={(e) => setPortfolioStr(e.target.value)}
              placeholder="100000"
              className="h-10 rounded-md border border-input bg-background px-3 text-sm font-mono tabular-nums focus:outline-none focus:ring-2 focus:ring-ring"
              min={0}
              step={1000}
            />
            <span className="text-[10px] text-muted-foreground">
              Tüm pozisyonlar + nakit dahil toplam (broker bakiyesi)
            </span>
          </div>

          <div className="flex flex-col gap-1.5">
            <label htmlFor="position" className="text-xs font-medium text-muted-foreground">
              Pozisyon Değeri ($) — entry × adet
            </label>
            <input
              id="position"
              type="number"
              value={positionStr}
              onChange={(e) => setPositionStr(e.target.value)}
              placeholder="6250"
              className="h-10 rounded-md border border-input bg-background px-3 text-sm font-mono tabular-nums focus:outline-none focus:ring-2 focus:ring-ring"
              min={0}
              step={100}
            />
            <span className="text-[10px] text-muted-foreground">
              Yeni veya mevcut pozisyon dolar değeri (giriş × adet)
            </span>
          </div>
        </div>

        {/* Mark X Kilit toggle */}
        <label className="flex items-start gap-3 p-3 rounded-md border cursor-pointer hover:bg-accent/30 transition-colors">
          <input
            type="checkbox"
            checked={prevProfitable}
            onChange={(e) => setPrevProfitable(e.target.checked)}
            className="mt-0.5 h-4 w-4 accent-primary"
          />
          <div className="flex-1">
            <div className="text-sm font-medium flex items-center gap-1.5">
              <ShieldCheck size={14} className="text-muted-foreground" />
              Önceki tier kâra geçti (Mark X kilit)
            </div>
            <div className="text-xs text-muted-foreground mt-0.5">
              Mark: <em>&ldquo;Trades not working = no size increase.&rdquo;</em>{" "}
              Pilot tier kâra geçmeden Standart&apos;a, Standart kâra geçmeden Full&apos;a YASAK.
            </div>
          </div>
        </label>
      </div>

      {/* Sonuç paneli */}
      {positionValue > 0 && portfolioValue > 0 && tier && (
        <div
          className="rounded-lg border p-4 flex flex-col gap-3"
          style={{ background: SEVERITY_BG[severity] }}
        >
          <div className="flex items-center justify-between flex-wrap gap-3">
            <div className="flex items-center gap-3">
              {severity === "violation" || severity === "warn" ? (
                <ShieldAlert
                  size={28}
                  style={{
                    color:
                      severity === "violation" ? "var(--mtp-danger)" : "#F59E0B",
                  }}
                />
              ) : (
                <ShieldCheck
                  size={28}
                  style={{
                    color:
                      severity === "ok"
                        ? "var(--mtp-excellent)"
                        : "var(--mtp-good, #4B9CD3)",
                  }}
                />
              )}
              <div>
                <div className="text-xs uppercase tracking-wider text-muted-foreground">
                  Mevcut Tier
                </div>
                <div className="text-2xl font-bold">
                  {tier === "BELOW_PILOT"
                    ? "Pilot Altı"
                    : tier === "OVER_MAX"
                    ? "MAX AŞILDI"
                    : TIER_LIMITS[tier as PyramidTier]?.label ?? tier}
                </div>
              </div>
            </div>

            <div className="flex flex-col items-end">
              <span className="text-xs uppercase tracking-wider text-muted-foreground">
                Portföy %
              </span>
              <span
                className="text-3xl font-bold tabular-nums"
                style={{ fontFamily: "var(--font-jetbrains-mono, monospace)" }}
              >
                {positionPct.toFixed(2)}%
              </span>
            </div>

            <span
              className="text-xs font-bold px-3 py-1 rounded-full uppercase tracking-wider"
              style={{
                background: SEVERITY_BG[severity],
                color:
                  severity === "ok"
                    ? "var(--mtp-excellent)"
                    : severity === "info"
                    ? "var(--mtp-good, #4B9CD3)"
                    : severity === "warn"
                    ? "#F59E0B"
                    : "var(--mtp-danger)",
                border: "1px solid currentColor",
              }}
            >
              {SEVERITY_LABEL[severity] ?? severity}
            </span>
          </div>

          {/* Mark felsefe yorumu */}
          <div className="p-3 rounded-md bg-background/60 border-l-2 text-sm italic leading-relaxed">
            {markSays}
          </div>

          {/* Sonraki tier önerisi */}
          {nextTier && (
            <div className="flex items-center gap-2 text-xs text-muted-foreground">
              <Info size={12} />
              <span>
                Sonraki tier: <strong>{TIER_LIMITS[nextTier].label}</strong>{" "}
                (%{TIER_LIMITS[nextTier].minPct}–%{TIER_LIMITS[nextTier].maxPct}) —{" "}
                {fmtUsd(suggestPositionDollars(portfolioValue, nextTier).min)} –{" "}
                {fmtUsd(suggestPositionDollars(portfolioValue, nextTier).max)}
              </span>
            </div>
          )}

          {/* Backend / fallback göstergesi */}
          <div className="text-[10px] text-muted-foreground/70 flex items-center gap-1.5">
            <span className="inline-block w-1.5 h-1.5 rounded-full" style={{ background: showBackend ? "var(--mtp-excellent)" : "#F59E0B" }} />
            {showBackend ? "Backend hesap (DRY)" : "Client fallback (backend yanıt vermedi)"}
          </div>
        </div>
      )}

      {/* 3 Tier $ aralık tablosu */}
      {tierDollarRanges && (
        <div className="rounded-lg border bg-card p-4 flex flex-col gap-3">
          <h2 className="text-sm font-semibold">
            3 Tier Pozisyon $ Aralıkları (Portföy {fmtUsd(portfolioValue)})
          </h2>

          <div className="grid grid-cols-1 md:grid-cols-3 gap-3">
            {tierDollarRanges.map(({ tier: t, min, max }) => {
              const meta = TIER_LIMITS[t];
              const isCurrent = tier === t;
              return (
                <div
                  key={t}
                  className="rounded-md border p-3 flex flex-col gap-2"
                  style={{
                    borderColor: isCurrent ? "var(--mtp-excellent)" : undefined,
                    borderWidth: isCurrent ? 2 : 1,
                    background: isCurrent ? "rgba(40,167,69,0.05)" : undefined,
                  }}
                >
                  <div className="flex items-center justify-between">
                    <span className="text-xl" aria-hidden="true">{meta.emoji}</span>
                    {isCurrent && (
                      <span
                        className="text-[10px] font-bold px-1.5 py-0.5 rounded-full uppercase"
                        style={{ background: "var(--mtp-excellent)", color: "#fff" }}
                      >
                        Mevcut
                      </span>
                    )}
                  </div>
                  <div className="text-sm font-semibold">{meta.label}</div>
                  <div className="text-xs text-muted-foreground">
                    %{meta.minPct} – %{meta.maxPct}
                  </div>
                  <div
                    className="text-base font-bold tabular-nums"
                    style={{ fontFamily: "var(--font-jetbrains-mono, monospace)" }}
                  >
                    {fmtUsd(min)} – {fmtUsd(max)}
                  </div>
                  <div className="text-[11px] text-muted-foreground leading-snug">
                    {meta.description}
                  </div>
                  <div className="text-[10px] text-muted-foreground/70 italic">
                    Kaynak: {meta.markSource}
                  </div>
                </div>
              );
            })}
          </div>
        </div>
      )}

      {/* Mark felsefe açıklama paneli */}
      <details className="rounded-lg border bg-card p-4 group">
        <summary className="text-sm font-semibold cursor-pointer flex items-center gap-2">
          <Info size={14} className="text-muted-foreground" />
          Mark Pyramiding Felsefesi (KARAR #487 v20.98)
        </summary>
        <div className="text-xs leading-relaxed text-muted-foreground mt-3 flex flex-col gap-2">
          <p>
            <strong>Progressive Exposure (&ldquo;vites kutusu&rdquo;):</strong> Pilot
            tier düşük risk başlangıç. Trades working kanıtlandıktan sonra
            Standart&apos;a, ardından Full&apos;a vites büyüt. Pyramiding doğrusal
            değil — kademeli güven artışına bağlı.
          </p>
          <p>
            <strong>Mark X Kilit:</strong>{" "}
            <em>&ldquo;Trades not working = no size increase.&rdquo;</em> Pilot
            kâra geçmeden Standart&apos;a, Standart kâra geçmeden Full&apos;a YASAK.
            Bu kilit risk artışını disipline bağlar — duygu değil.
          </p>
          <p>
            <strong>Tier kaynakları:</strong>
          </p>
          <ul className="list-disc pl-5 space-y-1">
            <li>Pilot (%1-3): TraderLion Lesson 7 — nakitten ilk giriş</li>
            <li>Standart (%6.25-12.5): Brandon Video — normal piyasa average</li>
            <li>Full (%15-25): Mark Video Kelly 2:1 — peak market + 60% win rate</li>
          </ul>
          <p className="opacity-80">
            MAX_POSITION_MAX_PCT = %50 sert tavan; %25+ pozisyon Mark&apos;a göre
            zaten aşırı risktir.
          </p>
        </div>
      </details>
    </div>
  );
}
