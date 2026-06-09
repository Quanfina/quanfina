"use client";

/**
 * Paket 228 (27 May 2026, KARAR #480 — UX Bölüm 8 Aksiyon Modu)
 *
 * Pazar Günü Hazırlık Paneli — Sn. Ferit'in haftalık 30 dk ritüeli.
 *
 * Vizyon İLKE #10 (Mod Geçişleri) + Manifesto Özellik #2 (Yer Belirleme):
 * Sn. Ferit hafta başında "Nerede kaldım? Yeni hafta hangi modda?" sorusunu
 * sorar. Bu panel otonom cevap üretir.
 *
 * 5 bölüm iskeleti (sıralı ritüel):
 * 1. Geçen Hafta Özet — pl_dollar/win_rate/trade sayısı (kapalı trade'lerden)
 * 2. Açık Pozisyon Durumu — Pazartesi'ye taşınan pozisyonlar (stop yakınlık)
 * 3. Piyasa Hafta Sonu Okuması — Mark Regime + SPY/QQQ/IWM stage
 * 4. Mod Öngörü — Hafta başlangıç modu (ModBadge full variant)
 * 5. Yeni Hafta Plan — focus list disiplini (max 5 sembol)
 *
 * Mark canon: Vizyon İLKE #10 sat. 1422-1447 (Mod Geçişleri).
 * Disiplin = içsel, sistem = dışsal (Sn. Ferit'in haftalık check-in zinciri).
 */

import { useMemo, useState } from "react";
import Link from "next/link";
import { CalendarDays, ArrowRight, CheckCircle2 } from "lucide-react";
import { useTrades } from "@/hooks/use-trades";
import { useTradesInfo } from "@/hooks/use-trades-info";
import { MockDataBanner } from "@/components/shared/MockDataBanner";
import { useWatchlist } from "@/hooks/use-watchlist";
import { useMarketStatus } from "@/hooks/use-market-status";
import { ModBadge } from "@/components/mark/ModBadge";
import { fmtPLDollar, fmtPLPct } from "@/lib/math";
import { Button } from "@/components/ui/button";
import { Stat } from "@/components/ui/stat";

export default function PazarHazirligiPage() {
  const tradesQuery = useTrades();
  // P418: MOCK fallback şeffaflık (Kural #28 audit DRY paten)
  const { data: tradesInfo } = useTradesInfo();
  const watchlistQuery = useWatchlist();
  const marketQuery = useMarketStatus();

  // React 19 purity: Date.now() render/useMemo içinde impure (react-hooks/purity).
  // useState lazy init mount'ta bir kez hesaplar (render-pure). Haftalık pencere
  // sayfa açılış zamanına sabit — kabul edilebilir (Pazar ritüeli kısa oturum).
  const [now] = useState(() => Date.now());

  // Bölüm 1 — Geçen 7 gün kapalı trade istatistik
  const lastWeekStats = useMemo(() => {
    const trades = tradesQuery.data ?? [];
    const weekMs = 7 * 24 * 60 * 60 * 1000;
    const closed = trades.filter(
      (t) =>
        t.status === "closed" &&
        t.exit_date &&
        now - new Date(t.exit_date).getTime() < weekMs &&
        t.pl_dollar != null
    );
    if (closed.length === 0) {
      return { count: 0, totalPL: 0, wins: 0, losses: 0, winRate: 0 };
    }
    const totalPL = closed.reduce((s, t) => s + (t.pl_dollar ?? 0), 0);
    const wins = closed.filter((t) => (t.pl_dollar ?? 0) > 0).length;
    const losses = closed.filter((t) => (t.pl_dollar ?? 0) < 0).length;
    return {
      count: closed.length,
      totalPL,
      wins,
      losses,
      winRate: (wins / closed.length) * 100,
    };
  }, [tradesQuery.data, now]);

  // Bölüm 2 — Pazartesi'ye taşınan açık pozisyonlar
  const openPositions = useMemo(() => {
    const trades = tradesQuery.data ?? [];
    return trades.filter((t) => t.status === "open");
  }, [tradesQuery.data]);

  // Bölüm 5 — Watchlist focus list disiplini
  const focusCount = useMemo(() => {
    const wl = watchlistQuery.data ?? [];
    return wl.filter((w) => (w as { list_type?: string }).list_type === "focus").length;
  }, [watchlistQuery.data]);

  const isLoading = tradesQuery.isLoading || watchlistQuery.isLoading || marketQuery.isLoading;

  return (
    <div className="flex flex-col h-full">
      {/* P418 DRY: shared MockDataBanner — Pazar Günü 30dk hazırlık MOCK ile yanıltıcı */}
      <MockDataBanner isMock={tradesInfo?.is_mock} context="hafta önizleme + RBA" testId="pazar-hazirligi-mock-banner" />

      {/* Header */}
      <div className="px-6 py-3 border-b flex items-start justify-between gap-3 flex-wrap">
        <div>
          <h1 className="text-xl font-semibold tracking-tight flex items-center gap-2">
            <CalendarDays size={20} />
            Pazar Günü Hazırlığı
          </h1>
          <p className="text-sm text-muted-foreground">
            KARAR #480 — UX Bölüm 8 Aksiyon Modu — Haftalık 30 dk ritüel
          </p>
        </div>
        <ModBadge variant="compact" />
      </div>

      <div className="flex-1 px-6 py-4 flex flex-col gap-4 overflow-auto">
        {isLoading ? (
          <div className="text-sm text-muted-foreground">Yükleniyor...</div>
        ) : (
          <>
            {/* Bölüm 1 — Geçen Hafta Özet */}
            <section className="rounded-lg border p-4">
              <h2 className="text-xs font-semibold uppercase tracking-wider text-muted-foreground mb-3">
                1. Geçen 7 Gün — Kapalı Trade Özet
              </h2>
              {lastWeekStats.count === 0 ? (
                <p className="text-sm text-muted-foreground italic">
                  Bu hafta kapalı trade yok. Sıfır kayıt = öğrenme yok.
                </p>
              ) : (
                <div className="grid grid-cols-2 sm:grid-cols-4 gap-3 text-sm">
                  <Stat label="Trade Sayısı" value={String(lastWeekStats.count)} />
                  <Stat
                    label="Toplam P/L"
                    value={fmtPLDollar(lastWeekStats.totalPL)}
                    color={
                      lastWeekStats.totalPL >= 0
                        ? "var(--mtp-excellent)"
                        : "var(--mtp-danger)"
                    }
                  />
                  <Stat
                    label="Kazan / Kaybet"
                    value={`${lastWeekStats.wins} / ${lastWeekStats.losses}`}
                  />
                  <Stat
                    label="Kazanç Oranı"
                    value={fmtPLPct(lastWeekStats.winRate)}
                    color={
                      lastWeekStats.winRate >= 50
                        ? "var(--mtp-excellent)"
                        : lastWeekStats.winRate >= 35
                        ? "var(--mtp-good, #4B9CD3)"
                        : "var(--mtp-danger)"
                    }
                  />
                </div>
              )}
              <div className="mt-3">
                <Link
                  href="/journal"
                  className="text-xs text-muted-foreground hover:text-foreground inline-flex items-center gap-1"
                >
                  İşlem Günlüğü <ArrowRight size={12} />
                </Link>
              </div>
            </section>

            {/* Bölüm 2 — Açık Pozisyon Durumu */}
            <section className="rounded-lg border p-4">
              <h2 className="text-xs font-semibold uppercase tracking-wider text-muted-foreground mb-3">
                2. Pazartesi&apos;ye Taşınan Açık Pozisyonlar
              </h2>
              {openPositions.length === 0 ? (
                <p className="text-sm text-muted-foreground italic">
                  Açık pozisyon yok. Pazartesi temiz başlangıç.
                </p>
              ) : (
                <div className="text-sm">
                  <p>
                    <b>{openPositions.length}</b>{" "}açık pozisyon Pazartesi&apos;ye
                    taşınıyor. Stop yakınlığı + plan disiplinini kontrol et.
                  </p>
                  <ul className="mt-2 space-y-1">
                    {openPositions.slice(0, 5).map((t) => (
                      <li key={t.id} className="text-xs text-muted-foreground">
                        <Link href={`/hisse/${t.symbol}`} className="hover:text-foreground">
                          {t.symbol}
                        </Link>
                        {" — "}
                        {t.shares} adet @ ${t.entry_price?.toFixed(2)}
                        {t.plan_stop ? ` (stop: $${t.plan_stop.toFixed(2)})` : ""}
                      </li>
                    ))}
                    {openPositions.length > 5 && (
                      <li className="text-xs text-muted-foreground italic">
                        +{openPositions.length - 5} daha...
                      </li>
                    )}
                  </ul>
                </div>
              )}
            </section>

            {/* Bölüm 3 — Piyasa Hafta Sonu Okuması */}
            <section className="rounded-lg border p-4">
              <h2 className="text-xs font-semibold uppercase tracking-wider text-muted-foreground mb-3">
                3. Piyasa Hafta Sonu Okuması (Mark Regime)
              </h2>
              {marketQuery.data ? (
                <div className="grid grid-cols-2 sm:grid-cols-4 gap-3 text-sm">
                  <Stat
                    label="Market Health"
                    value={`${marketQuery.data.market_health_score ?? "—"}/100`}
                    color={
                      (marketQuery.data.market_health_score ?? 0) >= 70
                        ? "var(--mtp-excellent)"
                        : (marketQuery.data.market_health_score ?? 0) >= 40
                        ? "var(--mtp-good, #4B9CD3)"
                        : "var(--mtp-danger)"
                    }
                  />
                  <Stat label="SPY Stage" value={`Stage ${marketQuery.data.spy_stage ?? "—"}`} />
                  <Stat label="QQQ Stage" value={`Stage ${marketQuery.data.qqq_stage ?? "—"}`} />
                  <Stat label="IWM Stage" value={`Stage ${marketQuery.data.iwm_stage ?? "—"}`} />
                </div>
              ) : (
                <p className="text-sm text-muted-foreground italic">Piyasa verisi yok.</p>
              )}
              <div className="mt-3">
                <Link
                  href="/piyasa-durumu"
                  className="text-xs text-muted-foreground hover:text-foreground inline-flex items-center gap-1"
                >
                  Piyasa Durumu detay <ArrowRight size={12} />
                </Link>
              </div>
            </section>

            {/* Bölüm 4 — Hafta Başlangıç Modu */}
            <section className="rounded-lg border p-4">
              <h2 className="text-xs font-semibold uppercase tracking-wider text-muted-foreground mb-3">
                4. Hafta Başlangıç Modu (Vizyon İLKE #10)
              </h2>
              <ModBadge variant="full" />
              <p className="text-xs text-muted-foreground mt-2">
                Mod streak ve piyasa rejimine göre otomatik hesaplandı. Mark canon
                disiplini hafta başında bilinçli giriş.
              </p>
              {/* Paket 232 (27 May 2026): Risk Yönetimi referansı —
                  mod-aware Pyramid Calculator + Tier kilit Sn. Ferit'in
                  haftalık planının somut sayısal sonucu. */}
              <div className="mt-3">
                <Link
                  href="/risk-yonetimi"
                  className="text-xs text-muted-foreground hover:text-foreground inline-flex items-center gap-1"
                >
                  Risk Yönetimi: Pyramid + Tier sizing <ArrowRight size={12} />
                </Link>
              </div>
            </section>

            {/* Bölüm 5 — Yeni Hafta Plan */}
            <section className="rounded-lg border p-4">
              <h2 className="text-xs font-semibold uppercase tracking-wider text-muted-foreground mb-3">
                5. Yeni Hafta Plan (Focus List Disiplini)
              </h2>
              <div className="text-sm space-y-2">
                <div className="flex items-center gap-2">
                  <CheckCircle2
                    size={16}
                    style={{
                      color:
                        focusCount > 0 && focusCount <= 5
                          ? "var(--mtp-excellent)"
                          : "var(--mtp-danger)",
                    }}
                  />
                  <span>
                    Focus listede <b>{focusCount}</b> sembol var.
                    {focusCount === 0 && " Yeni hafta için focus seçimi yap."}
                    {focusCount > 5 && " Mark disiplini: max 5 sembol. Daralt."}
                    {focusCount > 0 && focusCount <= 5 && " Mark canon uyumlu."}
                  </span>
                </div>
                <p className="text-xs text-muted-foreground italic">
                  Mark TLSMW Leaders First — Pazartesi açılış öncesi 5 sembolden
                  fazla yok. Karar yorgunluğunu azaltma felsefesi.
                </p>
                <Link href="/watchlist">
                  <Button size="sm" variant="outline" className="mt-2">
                    İzleme Listesi&apos;ne git
                  </Button>
                </Link>
              </div>
            </section>

            {/* Paket 243 (27 May 2026): Mark Pre-flight uyarısı — Sn. Ferit
                Pazar günü ritüel sonrası ilk trade'i açmadan ÖNCE PreTradeChecklist
                7 koşulunu görmeli (Mark TTLC s.131 + TLSMW Leaders First). */}
            <section
              className="rounded-lg border p-4"
              style={{
                background: "rgba(75,156,211,0.06)",
                borderColor: "rgba(75,156,211,0.30)",
              }}
            >
              <h2 className="text-xs font-semibold uppercase tracking-wider text-muted-foreground mb-2">
                ⚠️ Pazartesi Açılış: Mark Canon Pre-flight
              </h2>
              <p className="text-sm text-muted-foreground leading-relaxed">
                Hafta başlangıcı her trade için <b>7 Mark canon koşulu</b> AddTradeDialog ve
                hisse detay sayfasında otomatik görünür:
              </p>
              <ul className="text-xs text-muted-foreground mt-2 space-y-0.5 ml-4 list-disc">
                <li>Stage 2 piyasa (Carr/Weinstein) — 30W MA üstü</li>
                <li>RS Rating ≥ 70 (Mark TLSMW Leaders First)</li>
                <li>VCP / Pivot Setup mevcut (TTLC Sec 4)</li>
                <li>Plan: Giriş tetikleyicisi net (TTLC Sec 1.6 ZORUNLU)</li>
                <li>Plan: Stop %7 limit içinde (TTLC s.131 mutlak)</li>
                <li>Plan: Hedef R/R ≥ 2 (Mark minimum)</li>
                <li>Mod farkındalığı (Vizyon İLKE #10)</li>
              </ul>
              <p className="text-xs italic mt-2" style={{ color: "var(--mtp-good, #4B9CD3)" }}>
                Disiplin: 7/7 yeşil değilse trade aç&apos;ma — Mark canon felsefesi.
              </p>
            </section>
          </>
        )}
      </div>
    </div>
  );
}

// Paket 237: local Stat çıkarıldı, web/components/ui/stat.tsx DRY tek kaynak
