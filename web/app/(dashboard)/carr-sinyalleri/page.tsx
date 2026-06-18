"use client";

/**
 * Carr Sinyalleri — Paket 523 (18 Haz 2026)
 *
 * 9 Carr setup tek dashboard (aggregate özet): bugün hangi hisse hangi Carr setup'ını
 * tetikliyor. /api/carr/summary (tek pass, son scan pvh). /screens'teki 9 ayrı Carr bulk
 * ekranını tek görünümde toplar (capstone). LONG/SHORT gruplu, count + top adaylar (RS'e
 * göre), sembol tıklanınca /hisse/{symbol}.
 *
 * Carr catalog: Pullback, Mean Reversion, Blue Sky, Coiled Spring, Bullish Base, Bullish
 * Divergence (LONG) + Blue Sea, Gap Down, Rising Wedge (SHORT). Hepsi çift danışma teyitli.
 */

import Link from "next/link";
import { useCarrSummary, type CarrSummarySetup } from "@/hooks/use-carr-summary";

export default function CarrSinyalleriPage() {
  const { data, isLoading, isError, error } = useCarrSummary();

  const longs = (data ?? []).filter((s) => s.direction === "LONG");
  const shorts = (data ?? []).filter((s) => s.direction === "SHORT");
  const totalLong = longs.reduce((a, s) => a + s.count, 0);
  const totalShort = shorts.reduce((a, s) => a + s.count, 0);

  return (
    <div className="p-4 md:p-6 flex flex-col gap-4 max-w-5xl">
      <header>
        <h1 className="text-xl font-bold tracking-tight">Carr Sinyalleri</h1>
        <p className="text-xs text-muted-foreground mt-0.5">
          9 Carr setup tek görünüm (P500-P522, çift danışma teyitli) — bugünün taraması.
          Sembole tıkla → hisse detay. Setup başlığına tıkla → ilgili tarama ekranı.
        </p>
      </header>

      {isLoading && (
        <div className="flex flex-col gap-3" aria-busy="true" data-testid="carr-summary-loading">
          <div className="text-sm text-muted-foreground">
            Carr sinyalleri hesaplanıyor (9 setup, tüm evren)…
          </div>
          <div className="grid gap-2 sm:grid-cols-2 lg:grid-cols-3">
            {Array.from({ length: 9 }).map((_, i) => (
              <div
                key={i}
                className="rounded-lg border p-3 flex flex-col gap-2 animate-pulse"
                style={{ borderColor: "var(--mtp-neutral)33" }}
              >
                <div className="h-3 w-2/3 rounded bg-muted" />
                <div className="flex flex-wrap gap-1.5 pt-1">
                  <div className="h-4 w-10 rounded bg-muted" />
                  <div className="h-4 w-12 rounded bg-muted" />
                  <div className="h-4 w-9 rounded bg-muted" />
                </div>
              </div>
            ))}
          </div>
        </div>
      )}

      {isError && (
        <div className="rounded-lg border border-red-300 bg-red-50 p-4 text-sm text-red-700">
          Sinyaller alınamadı: {error instanceof Error ? error.message : "bilinmeyen hata"}
        </div>
      )}

      {data && (
        <>
          <div className="flex gap-3 text-xs">
            <span className="px-2.5 py-1 rounded-md font-semibold" style={{ background: "rgba(40,167,69,0.12)", color: "var(--mtp-excellent)" }}>
              LONG: {totalLong} aday
            </span>
            <span className="px-2.5 py-1 rounded-md font-semibold" style={{ background: "rgba(220,53,69,0.12)", color: "var(--mtp-danger)" }}>
              SHORT: {totalShort} aday
            </span>
          </div>

          <Section title="LONG / Countertrend" setups={longs} />
          <Section title="SHORT" setups={shorts} />

          <p className="text-[11px] text-muted-foreground border-t pt-3">
            Not: TIER-2 setup&apos;lar (Coiled Spring, Bullish Base, Bullish Divergence, Rising
            Wedge) ADAY listesidir — göz kararı/haber teyidi şart (Carr). Ön-filtre: Fiyat&gt;$5
            + hacim&ge;100k. Quanfina long-biased; SHORT bilgi amaçlı.
          </p>
        </>
      )}
    </div>
  );
}

function Section({ title, setups }: { title: string; setups: CarrSummarySetup[] }) {
  if (setups.length === 0) return null;
  return (
    <section className="flex flex-col gap-2">
      <h2 className="text-sm font-semibold text-muted-foreground uppercase tracking-wider">{title}</h2>
      <div className="grid gap-2 sm:grid-cols-2 lg:grid-cols-3">
        {setups.map((s) => (
          <SetupCard key={s.slug} setup={s} />
        ))}
      </div>
    </section>
  );
}

function SetupCard({ setup }: { setup: CarrSummarySetup }) {
  const isLong = setup.direction === "LONG";
  const color = setup.count > 0 ? (isLong ? "var(--mtp-excellent)" : "var(--mtp-danger)") : "var(--mtp-neutral)";
  const bg = setup.count > 0
    ? (isLong ? "rgba(40,167,69,0.07)" : "rgba(220,53,69,0.07)")
    : "rgba(75,156,211,0.05)";

  return (
    <div
      className="rounded-lg border p-3 flex flex-col gap-2"
      style={{ background: bg, borderColor: `${color}44` }}
      data-testid={`carr-setup-${setup.slug}`}
    >
      <div className="flex items-center justify-between gap-2">
        <Link
          href={`/screens?screen=${setup.slug}`}
          className="text-xs font-semibold hover:underline"
          style={{ color }}
        >
          {setup.label}
        </Link>
        <span
          className="text-xs font-bold tabular-nums px-1.5 py-0.5 rounded"
          style={{ background: `${color}22`, color }}
        >
          {setup.count}
        </span>
      </div>

      {setup.candidates.length > 0 ? (
        <div className="flex flex-wrap gap-1.5">
          {setup.candidates.map((c) => (
            <Link
              key={c.symbol}
              href={`/hisse/${c.symbol}`}
              className="text-[11px] font-mono px-1.5 py-0.5 rounded border hover:bg-muted transition-colors"
              title={c.rs_ibd != null ? `RS ${c.rs_ibd}` : undefined}
            >
              {c.symbol}
              {c.rs_ibd != null && <span className="text-muted-foreground ml-1">{c.rs_ibd}</span>}
            </Link>
          ))}
          {setup.count > setup.candidates.length && (
            <span className="text-[11px] text-muted-foreground self-center">
              +{setup.count - setup.candidates.length}
            </span>
          )}
        </div>
      ) : (
        <span className="text-[11px] text-muted-foreground">Bugün aday yok</span>
      )}
    </div>
  );
}
