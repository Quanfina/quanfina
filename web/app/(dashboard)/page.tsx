"use client";

import { useEffect, useState } from "react";
import Link from "next/link";
import { ArrowRight, Activity, ListChecks, NotebookText, Globe, TrendingUp, TrendingDown, CheckCircle2, Circle, Sunrise } from "lucide-react";
import { useSignals } from "@/hooks/use-signals";
import { useTrades } from "@/hooks/use-trades";
import { useTradesInfo } from "@/hooks/use-trades-info";
import { MockDataBanner } from "@/components/shared/MockDataBanner";
import { useMarketStatus } from "@/hooks/use-market-status";
import { useWatchlist } from "@/hooks/use-watchlist";
import { formatDateTR } from "@/lib/format-date";
import { getDoneItems, toggleItem } from "@/lib/daily-checklist";
import { MindsetCardWidget } from "@/components/dashboard/MindsetCard";
import { MarketIndicatorsPanel } from "@/components/market/MarketIndicatorsPanel";
import { PortfolioSummaryCard } from "@/components/dashboard/PortfolioSummaryCard";
import { AlertHistoryCard } from "@/components/dashboard/AlertHistoryCard";
import { MarkRegimeBanner } from "@/components/mark/MarkRegimeBanner";
import { ModBadge } from "@/components/mark/ModBadge";
import { useTradingMode, getModUiTheme } from "@/hooks/use-trading-mode";
import { fmtUsd, fmtPctSigned } from "@/lib/format-currency";
import { MARKET_HEALTH_LABEL_TR } from "@/types/market";

// KARAR #474 (20 May 2026 ~08:15): Ana sayfa POC → Gerçek Dashboard.
// UX Bölüm 3: "Sn. Ferit Quanfina'yı açtığında üstten alta: bakiye + piyasa + sinyaller"
// Bakiye broker entegrasyonu YOK → şu an: açık trade sayısı + toplam P/L (kapanmış)

const STATUS_COLORS: Record<string, string> = {
  buy: "#28A745",
  focus: "#4B9CD3",
  on_deck: "#F59E0B",
  watch: "var(--muted-foreground)",
};

// P382: clean-room (Markets360 Bundle "YESIL/SARI/KIRMIZI" sizma temizligi).
// Enum + TR ceviri types/market.ts'de DRY tek kaynak (MARKET_HEALTH_LABEL_TR).
const HEALTH_COLORS: Record<string, string> = {
  HEALTHY: "var(--mtp-excellent)",
  NEUTRAL: "var(--mtp-neutral)",
  UNDER_PRESSURE: "var(--mtp-danger)",
};

function StatCard({
  label,
  value,
  subValue,
  href,
  color,
}: {
  label: string;
  value: string;
  subValue?: string;
  href: string;
  color?: string;
}) {
  return (
    <Link
      href={href}
      className="rounded-lg border bg-card p-4 flex flex-col gap-1 hover:bg-accent hover:border-accent transition-colors group"
    >
      <span className="text-xs text-muted-foreground uppercase tracking-wider flex items-center gap-1">
        {label}
        <ArrowRight size={11} className="opacity-0 group-hover:opacity-60 transition-opacity" />
      </span>
      <span
        className="text-2xl font-bold tabular-nums"
        style={{ color: color ?? "inherit", fontFamily: "var(--font-jetbrains-mono, monospace)" }}
      >
        {value}
      </span>
      {subValue && (
        <span className="text-xs text-muted-foreground">{subValue}</span>
      )}
    </Link>
  );
}

export default function Home() {
  const signals = useSignals();
  const trades = useTrades();
  // P418 (31 May 2026 — Kural #28 audit): DB unreachable -> MOCK trade fallback
  // Dashboard'da Açık Trade + Toplam P/L + Aksiyon Listesi MOCK üzerinden hesaplanır
  const { data: tradesInfo } = useTradesInfo();
  const market = useMarketStatus();
  const watchlist = useWatchlist();
  // Paket 255 (27 May 2026): Dashboard Defansif uyarı banner (6. sayfa Defansif zinciri)
  const tradingMode = useTradingMode();

  // KARAR #480 (UX Bölüm 8): Pazar Günü Hazırlık Paneli (Aksiyon Modu)
  // Tarih bazlı checklist, localStorage persistence (SSR hydration guard)
  const [doneItems, setDoneItems] = useState<Set<string>>(new Set());
  useEffect(() => {
    setDoneItems(getDoneItems());
  }, []);

  function handleToggle(itemId: string) {
    const next = new Set(doneItems);
    if (next.has(itemId)) {
      next.delete(itemId);
      toggleItem(itemId, false);
    } else {
      next.add(itemId);
      toggleItem(itemId, true);
    }
    setDoneItems(next);
  }

  // Quick stats hesaplamaları
  const openTrades = (trades.data ?? []).filter((t) => t.status === "open");
  const closedTrades = (trades.data ?? []).filter((t) => t.status === "closed");
  const totalPL = closedTrades.reduce((sum, t) => sum + (t.pl_dollar ?? 0), 0);
  const totalPLPct =
    closedTrades.length > 0
      ? closedTrades.reduce((sum, t) => sum + (t.pl_pct ?? 0), 0) / closedTrades.length
      : 0;

  const signalCount = signals.data?.length ?? 0;
  const newTodayCount = signals.data?.filter((s) => s.is_new_today).length ?? 0;
  const watchlistCount = watchlist.data?.length ?? 0;
  // KARAR #733 alt-paket (Paket 37): Stage 4 sayim — sinyaller + watchlist toplam
  const stage4Signals = (signals.data ?? []).filter((s) => s.mark_signals?.carr_stage === 4).length;
  const stage4Watchlist = (watchlist.data ?? []).filter((r) => r.mark_signals?.carr_stage === 4).length;
  const stage4Total = stage4Signals + stage4Watchlist;
  const stage4ListTotal = signalCount + watchlistCount;

  // En iyi 5 sinyal (R/R desc, sonra RS)
  const topSignals = (signals.data ?? []).slice(0, 5);

  // Son 3 açık trade
  const recentOpen = openTrades.slice(0, 3);

  // P382: TR display (Mark birebir "Under Pressure" -> "Baski Altinda"). DRY: tek kaynak types/market.ts
  const marketHealth = market.data?.market_health_label ?? "—";
  const marketHealthTr = market.data?.market_health_label
    ? MARKET_HEALTH_LABEL_TR[market.data.market_health_label]
    : "—";
  const marketHealthColor = HEALTH_COLORS[marketHealth] ?? "inherit";
  const marketScore = market.data?.market_health_score ?? null;
  const marketMode = market.data?.suggested_mode ?? "—";
  // P115 (26 May 2026): DD Severity 5. metrik — O'Neil CLEAN/CAUTION/HEAVY/EXTREME
  const ddSeverity = market.data?.dd_severity;
  const ddCount = market.data?.distribution_days ?? null;
  const DD_SEV_COLOR: Record<string, string> = {
    CLEAN: "var(--mtp-excellent)",
    CAUTION: "var(--mtp-neutral)",
    HEAVY: "#F97316",
    EXTREME: "var(--mtp-danger)",
  };

  // KARAR #480: Aksiyon Modu dinamik checklist (UX Bölüm 8)
  // Veri-bağlamlı 5 madde — Sn. Ferit sabah ne yapacağını görür
  const checklistItems = [
    {
      id: "review-new-signals",
      label: newTodayCount > 0
        ? `${newTodayCount} yeni sinyal incelendi (R/R kontrol + AL/GEÇ)`
        : "Yeni sinyaller kontrol edildi",
      detail: "Sinyaller sayfası → R/R desc sıralı, top 5 değerlendir",
      href: "/signals",
      priority: newTodayCount > 0 ? "high" : "normal",
    },
    {
      id: "check-open-positions",
      label: openTrades.length > 0
        ? `${openTrades.length} açık pozisyon stop loss güncellendi (trailing)`
        : "Açık pozisyon yok",
      detail: openTrades.length > 0
        ? "İşlem Günlüğü → her trade için trailing stop seviyesi gözden geçir"
        : "Bugün için aktif risk yok",
      href: "/journal",
      priority: openTrades.length > 0 ? "high" : "low",
    },
    {
      id: "market-mode-check",
      label: `Piyasa modu doğrulandı (${marketMode}, sağlık ${marketScore ?? "—"})`,
      detail: marketMode === "LONG"
        ? "Long setupları öncelikli, short sinyaller bypass"
        : marketMode === "SHORT"
        ? "Short setupları öncelikli, long pozisyonlar dikkatli"
        : "Mod kararı belirsiz — manuel değerlendirme",
      href: "/piyasa-durumu",
      priority: "normal",
    },
    {
      id: "watchlist-hygiene",
      label: `İzleme Listesi hijyen — ${watchlistCount} satır gözden geçirildi`,
      detail: "Focus/Buy listesi temiz mi, kaldırılacak hisse var mı?",
      href: "/watchlist",
      priority: "normal",
    },
    {
      id: "screens-scan",
      label: "Tarama — günlük scan sonuçları incelendi",
      detail: "25 ekran içinde Stage 2 + RS 90+ adaylar var mı?",
      href: "/screens",
      priority: "low",
    },
  ];
  const doneCount = checklistItems.filter((i) => doneItems.has(i.id)).length;
  const completionPct = Math.round((doneCount / checklistItems.length) * 100);

  return (
    <div className="p-6 flex flex-col gap-6">
      {/* P418 DRY: shared MockDataBanner — Dashboard tüm trade-bağımlı sayım MOCK ile bozuk */}
      <div className="-mx-6 -mt-6">
        <MockDataBanner isMock={tradesInfo?.is_mock} context="Açık Trade + P/L + Aksiyon Listesi" testId="dashboard-mock-banner" />
      </div>

      {/* KARAR #733 alt-paket (Paket 37): Mark Regime üst-uyarı — sn. Ferit
          Quanfina'yi açtığında piyasa rejimini ilk gören öğe (hideOnHealthy=false:
          Dashboard sabit göstersin, HEALTHY iken bile yeşil rozet pekiştirsin). */}
      <div className="-mx-6 -mt-6">
        <MarkRegimeBanner
          stage4Count={stage4Total}
          totalCount={stage4ListTotal}
          hideOnHealthy={false}
        />
      </div>

      {/* Paket 255 (P260 refactor — getModUiTheme DRY helper P259):
          Dashboard Defansif uyarı banner (Vizyon İLKE #10 + Mark TTLC s.187). */}
      {(() => {
        const theme = getModUiTheme(tradingMode.mode);
        if (!theme || tradingMode.mode !== "defansif") return null;
        return (
          <div
            className="-mx-6 px-6 py-2 border-b text-xs flex items-center gap-2"
            style={{
              background: theme.background,
              borderColor: theme.borderColor,
              color: theme.color,
            }}
          >
            <span aria-hidden="true">{theme.emoji}</span>
            <span>
              <b>{theme.shortMessage}</b>{" "}Pazartesi açılış öncesi piyasa Stage 3-4 + MH&lt;30.
              Açık pozisyon stop yönetimi öncelikli, yeni AL aramayı durdur.
            </span>
          </div>
        );
      })()}

      {/* Header */}
      <div className="flex flex-col gap-1">
        <h1 className="text-2xl font-semibold tracking-tight">Bugün Ne Var?</h1>
        <p className="text-sm text-muted-foreground">
          Günlük rutin — Sinyaller → İzleme Listesi → Piyasa → İşlem Günlüğü
        </p>
      </div>

      {/* Quick Stats — 5 metrik (P115: DD Severity eklendi) */}
      <div className="grid grid-cols-2 lg:grid-cols-5 gap-3">
        <StatCard
          label="Sinyaller"
          value={`${signalCount}`}
          subValue={newTodayCount > 0 ? `${newTodayCount} yeni bugün` : "—"}
          href="/signals"
          color="var(--mtp-excellent)"
        />
        <StatCard
          label="İzleme Listesi"
          value={`${watchlistCount}`}
          subValue="aktif takip"
          href="/watchlist"
          color="var(--mtp-neutral)"
        />
        <StatCard
          label="Açık Trade"
          value={`${openTrades.length}`}
          subValue={`${closedTrades.length} kapanmış`}
          href="/journal"
        />
        <StatCard
          label="Toplam P/L"
          value={
            closedTrades.length > 0
              ? `${totalPL >= 0 ? "+" : ""}${fmtUsd(totalPL)}`
              : "—"
          }
          subValue={
            closedTrades.length > 0
              ? `ort. ${fmtPctSigned(totalPLPct)}`
              : "ilk trade bekleniyor"
          }
          href="/journal"
          color={
            closedTrades.length === 0
              ? undefined
              : totalPL >= 0
              ? "var(--mtp-excellent)"
              : "var(--mtp-danger)"
          }
        />
        {/* P115: DD Severity kart — O'Neil CLEAN/CAUTION/HEAVY/EXTREME */}
        <StatCard
          label="Dağıtım Günü"
          value={ddSeverity ?? "—"}
          subValue={ddCount != null ? `${ddCount} DD` : "veri bekleniyor"}
          href="/piyasa-durumu"
          color={ddSeverity ? (DD_SEV_COLOR[ddSeverity] ?? "inherit") : undefined}
        />
      </div>

      {/* Paket 156-157 (26 May 2026): Portfolio canlı özet — paper trading sabah bakışı.
          Açık trade × yfinance quote → toplam değer + unrealized P&L + bugünkü değişim.
          Paket 168 (26 May 2026): AlertHistoryCard — son 24h uyarı timeline.
          Paket 193 (26 May 2026): ModBadge — Trading Mode otomatik (Vizyon İLKE #10). */}
      <div className="grid grid-cols-1 lg:grid-cols-3 gap-4">
        <PortfolioSummaryCard />
        <ModBadge variant="full" />
        <AlertHistoryCard />
      </div>

      {/* 2 kolon: En İyi Sinyaller + Piyasa Özet */}
      <div className="grid grid-cols-1 lg:grid-cols-3 gap-4">
        {/* En İyi Sinyaller (lg col-span-2) */}
        <div className="lg:col-span-2 rounded-lg border bg-card p-4 flex flex-col gap-3">
          <div className="flex items-center justify-between">
            <div className="flex items-center gap-2">
              <Activity size={16} className="text-muted-foreground" />
              <h2 className="text-sm font-semibold">En İyi Sinyaller (R/R sıralı)</h2>
            </div>
            <Link
              href="/signals"
              className="text-xs text-muted-foreground hover:text-foreground flex items-center gap-1"
            >
              Tümünü gör <ArrowRight size={12} />
            </Link>
          </div>
          {signals.isLoading && (
            <div className="text-xs text-muted-foreground py-4">Yükleniyor...</div>
          )}
          {!signals.isLoading && topSignals.length === 0 && (
            <div className="text-xs text-muted-foreground py-4">Sinyal yok</div>
          )}
          {topSignals.length > 0 && (
            <div className="flex flex-col gap-1">
              {topSignals.map((s) => (
                <div
                  key={`${s.symbol}-${s.strategy}`}
                  className="flex items-center justify-between text-sm py-1.5 px-1 hover:bg-accent/50 rounded transition-colors"
                >
                  <div className="flex items-center gap-3">
                    <span className="font-semibold tracking-tight w-14">{s.symbol}</span>
                    {s.is_new_today && (
                      <span
                        className="text-[10px] font-semibold px-1.5 py-0.5 rounded-full"
                        style={{ background: "#28A745", color: "#fff" }}
                      >
                        YENİ
                      </span>
                    )}
                    <span className="text-xs text-muted-foreground capitalize">
                      {s.strategy}
                    </span>
                    <span
                      className="text-xs font-semibold"
                      style={{ color: STATUS_COLORS[s.status] ?? "inherit" }}
                    >
                      {s.status}
                    </span>
                  </div>
                  <div className="flex items-center gap-3 text-xs tabular-nums">
                    {s.risk_reward != null && (
                      <span
                        className="font-semibold"
                        style={{
                          color:
                            s.risk_reward >= 3
                              ? "var(--mtp-excellent)"
                              : s.risk_reward >= 2
                              ? "var(--mtp-good)"
                              : "var(--mtp-neutral)",
                          fontFamily: "var(--font-jetbrains-mono, monospace)",
                        }}
                        title="Risk/Reward"
                      >
                        R/R {s.risk_reward.toFixed(2)}
                      </span>
                    )}
                    <span className="text-muted-foreground">RS {Math.round(s.rs_rating)}</span>
                    {/* P418 (31 May 2026): Bağıl Hacim mini rozet — P416/P417 DRY.
                        Dashboard hızlı bakış için sadece sıra dışı durumlar gösterilir:
                        ≥1.5 yeşil "patlama" / <0.7 kırmızı "sönük". Normal (0.7-1.5)
                        Sn. Ferit'i yormamak için gizli. Mark TLSMW Bol. 6 "Hacim teyit". */}
                    {s.relative_volume != null && (s.relative_volume >= 1.5 || s.relative_volume < 0.7) && (
                      <span
                        className="text-[10px] font-semibold px-1.5 py-0.5 rounded-full"
                        style={{
                          background: s.relative_volume >= 1.5 ? "rgba(40,167,69,0.15)" : "rgba(220,53,69,0.15)",
                          color: s.relative_volume >= 1.5 ? "var(--mtp-excellent)" : "var(--mtp-danger)",
                          fontFamily: "var(--font-jetbrains-mono, monospace)",
                        }}
                        title={
                          s.relative_volume >= 1.5
                            ? `Hacim patlama: ${s.relative_volume.toFixed(2)}× ortalama (Mark TLSMW Bol. 6)`
                            : `Sönük hacim: ${s.relative_volume.toFixed(2)}× ortalama — pivot kırılım teyit eksik`
                        }
                      >
                        {s.relative_volume >= 1.5 ? "🔥" : "💤"} {s.relative_volume.toFixed(2)}×
                      </span>
                    )}
                    <span
                      className="font-semibold"
                      style={{ fontFamily: "var(--font-jetbrains-mono, monospace)" }}
                    >
                      {fmtUsd(s.price)}
                    </span>
                  </div>
                </div>
              ))}
            </div>
          )}
        </div>

        {/* Piyasa Durumu özet */}
        <div className="rounded-lg border bg-card p-4 flex flex-col gap-3">
          <div className="flex items-center justify-between">
            <div className="flex items-center gap-2">
              <Globe size={16} className="text-muted-foreground" />
              <h2 className="text-sm font-semibold">Piyasa Sağlığı</h2>
            </div>
            <Link
              href="/piyasa-durumu"
              className="text-xs text-muted-foreground hover:text-foreground flex items-center gap-1"
            >
              Detay <ArrowRight size={12} />
            </Link>
          </div>
          {market.isLoading && (
            <div className="text-xs text-muted-foreground py-4">Yükleniyor...</div>
          )}
          {!market.isLoading && market.data && (
            <div className="flex flex-col gap-3">
              <div className="flex items-end justify-between">
                <span
                  className="text-4xl font-bold tabular-nums"
                  style={{
                    color: marketHealthColor,
                    fontFamily: "var(--font-jetbrains-mono, monospace)",
                  }}
                >
                  {marketScore ?? "—"}
                </span>
                <span
                  className="text-sm font-semibold px-2 py-0.5 rounded-full"
                  style={{
                    background: `color-mix(in srgb, ${marketHealthColor} 15%, transparent)`,
                    color: marketHealthColor,
                  }}
                >
                  {marketHealthTr}
                </span>
              </div>
              <div className="flex justify-between text-xs text-muted-foreground">
                <span>VIX</span>
                <span className="font-semibold tabular-nums text-foreground">
                  {market.data.vix?.toFixed(1) ?? "—"}
                </span>
              </div>
              <div className="flex justify-between text-xs text-muted-foreground">
                <span>Dist. Days</span>
                <span className="font-semibold tabular-nums text-foreground">
                  {market.data.distribution_days ?? "—"}
                </span>
              </div>
              {/* KARAR #733 alt-paket (Paket 55): Market Breadth A/D Line özet */}
              {market.data.market_breadth && (
                <div className="flex justify-between text-xs text-muted-foreground">
                  <span title={market.data.market_breadth.mark_says}>A/D Line</span>
                  <span className="flex items-center gap-1.5 tabular-nums">
                    <span
                      className="font-semibold"
                      style={{ fontFamily: "var(--font-jetbrains-mono, monospace)" }}
                    >
                      {market.data.market_breadth.ad_ratio.toFixed(2)}
                    </span>
                    <span
                      className="text-[10px] font-bold px-1.5 py-0.5 rounded uppercase tracking-wider"
                      style={{
                        background:
                          market.data.market_breadth.breadth_health === "STRONG"
                            ? "rgba(40,167,69,0.15)"
                            : market.data.market_breadth.breadth_health === "NEUTRAL"
                            ? "rgba(245,158,11,0.15)"
                            : "rgba(220,53,69,0.15)",
                        color:
                          market.data.market_breadth.breadth_health === "STRONG"
                            ? "var(--mtp-excellent)"
                            : market.data.market_breadth.breadth_health === "NEUTRAL"
                            ? "#F59E0B"
                            : "var(--mtp-danger)",
                      }}
                    >
                      {market.data.market_breadth.breadth_health}
                    </span>
                  </span>
                </div>
              )}
              {/* KARAR #733 alt-paket (Paket 60): Divergence kategori shortLabel
                  — BEARISH_DIVERGENCE kritik kırmızı vurgu, P58 widget pateni
                  compact varyant. */}
              {market.data.breadth_divergence &&
                market.data.breadth_divergence.divergence !== "NEUTRAL" && (
                <div className="flex justify-between text-xs text-muted-foreground">
                  <span title={market.data.breadth_divergence.mark_says}>
                    Index × A/D
                  </span>
                  <span
                    className="text-[10px] font-bold px-1.5 py-0.5 rounded uppercase tracking-wider"
                    style={{
                      background:
                        market.data.breadth_divergence.severity === "critical"
                          ? "rgba(220,53,69,0.18)"
                          : market.data.breadth_divergence.severity === "warn"
                          ? "rgba(220,53,69,0.12)"
                          : market.data.breadth_divergence.severity === "ok"
                          ? "rgba(40,167,69,0.15)"
                          : "rgba(75,156,211,0.12)",
                      color:
                        market.data.breadth_divergence.severity === "critical" ||
                        market.data.breadth_divergence.severity === "warn"
                          ? "var(--mtp-danger)"
                          : market.data.breadth_divergence.severity === "ok"
                          ? "var(--mtp-excellent)"
                          : "var(--mtp-good, #4B9CD3)",
                      border:
                        market.data.breadth_divergence.severity === "critical"
                          ? "1px solid var(--mtp-danger)"
                          : undefined,
                    }}
                  >
                    {market.data.breadth_divergence.divergence === "CONFIRMED_UP"
                      ? "ONAYLI ↑"
                      : market.data.breadth_divergence.divergence === "BEARISH_DIVERGENCE"
                      ? "BEARISH ⚠️"
                      : market.data.breadth_divergence.divergence === "BULLISH_DIVERGENCE"
                      ? "BULLISH ↗"
                      : "ONAYLI ↓"}
                  </span>
                </div>
              )}
              {/* KARAR #733 alt-paket (Paket 67): FTD satırı — ftd_detected
                  iken görünür. ONAYLI yeşil (Mark Stage 2 başlangıç sinyali),
                  ZAYIF sarı (hacim eksik). DRY tüketim 2. nokta (MarkRegimeCard
                  P66 widget pateni — compact varyant). */}
              {market.data.follow_through?.ftd_detected && (
                <div className="flex justify-between text-xs text-muted-foreground">
                  <span title={market.data.follow_through.mark_says}>
                    Follow-Through
                  </span>
                  <span className="flex items-center gap-1.5 tabular-nums">
                    {market.data.follow_through.ftd_gain_pct != null && (
                      <span
                        className="font-mono font-semibold"
                        style={{
                          color: market.data.follow_through.volume_confirmed
                            ? "var(--mtp-excellent)"
                            : "#F59E0B",
                        }}
                      >
                        +{market.data.follow_through.ftd_gain_pct.toFixed(2)}%
                      </span>
                    )}
                    <span
                      className="text-[10px] font-bold px-1.5 py-0.5 rounded uppercase tracking-wider"
                      style={{
                        background: market.data.follow_through.volume_confirmed
                          ? "rgba(40,167,69,0.15)"
                          : "rgba(245,158,11,0.15)",
                        color: market.data.follow_through.volume_confirmed
                          ? "var(--mtp-excellent)"
                          : "#F59E0B",
                        border: market.data.follow_through.volume_confirmed
                          ? "1px solid var(--mtp-excellent)"
                          : "1px solid #F59E0B",
                      }}
                    >
                      {market.data.follow_through.volume_confirmed
                        ? "FTD ✓ ONAYLI"
                        : "FTD ⚠️ ZAYIF"}
                    </span>
                  </span>
                </div>
              )}
              <div className="pt-2 border-t flex items-center justify-between">
                <span className="text-xs text-muted-foreground">Önerilen Mod</span>
                <span
                  className="text-xs font-semibold px-2 py-0.5 rounded-full border"
                  style={{
                    color:
                      marketMode === "LONG"
                        ? "var(--mtp-excellent)"
                        : marketMode === "SHORT"
                        ? "var(--mtp-danger)"
                        : "inherit",
                  }}
                >
                  {marketMode}
                </span>
              </div>
            </div>
          )}
        </div>
      </div>

      {/* Açık Pozisyonlar */}
      <div className="rounded-lg border bg-card p-4 flex flex-col gap-3">
        <div className="flex items-center justify-between">
          <div className="flex items-center gap-2">
            <NotebookText size={16} className="text-muted-foreground" />
            <h2 className="text-sm font-semibold">Açık Pozisyonlar</h2>
          </div>
          <Link
            href="/journal"
            className="text-xs text-muted-foreground hover:text-foreground flex items-center gap-1"
          >
            İşlem Günlüğü <ArrowRight size={12} />
          </Link>
        </div>
        {trades.isLoading && (
          <div className="text-xs text-muted-foreground py-4">Yükleniyor...</div>
        )}
        {!trades.isLoading && recentOpen.length === 0 && (
          <div className="text-xs text-muted-foreground py-4">Açık pozisyon yok</div>
        )}
        {recentOpen.length > 0 && (
          <div className="flex flex-col gap-1">
            {recentOpen.map((t) => (
              <div
                key={t.id}
                className="flex items-center justify-between text-sm py-1.5 px-1 hover:bg-accent/50 rounded transition-colors"
              >
                <div className="flex items-center gap-3">
                  <span className="font-semibold tracking-tight w-14">{t.symbol}</span>
                  <span className="text-xs text-muted-foreground capitalize">{t.strategy}</span>
                  <span className="text-xs text-muted-foreground">{t.setup_type}</span>
                  <span className="text-xs text-muted-foreground">
                    {formatDateTR(t.entry_date)}
                  </span>
                </div>
                <div className="flex items-center gap-3 text-xs tabular-nums">
                  <span className="text-muted-foreground">{t.shares} adet</span>
                  <span
                    className="font-semibold"
                    style={{ fontFamily: "var(--font-jetbrains-mono, monospace)" }}
                  >
                    {fmtUsd(t.entry_price)}
                  </span>
                </div>
              </div>
            ))}
          </div>
        )}
      </div>

      {/* KARAR #480 (UX Bölüm 8): Pazar Günü Hazırlık Paneli — Aksiyon Modu */}
      <div className="rounded-lg border bg-card p-4 flex flex-col gap-3">
        <div className="flex items-center justify-between">
          <div className="flex items-center gap-2">
            <Sunrise size={16} className="text-muted-foreground" />
            <h2 className="text-sm font-semibold">Bugün için Aksiyon Listesi</h2>
          </div>
          <div className="flex items-center gap-2">
            <div className="w-24 h-1.5 bg-muted rounded-full overflow-hidden">
              <div
                className="h-full transition-all"
                style={{
                  width: `${completionPct}%`,
                  background:
                    completionPct === 100 ? "var(--mtp-excellent)" :
                    completionPct >= 60 ? "var(--mtp-good)" :
                    "var(--mtp-neutral)",
                }}
              />
            </div>
            <span className="text-xs text-muted-foreground tabular-nums">
              {doneCount} / {checklistItems.length}
            </span>
          </div>
        </div>
        <div className="flex flex-col gap-1">
          {checklistItems.map((item) => {
            const done = doneItems.has(item.id);
            const priorityColor =
              item.priority === "high" ? "var(--mtp-danger)" :
              item.priority === "low" ? "var(--muted-foreground)" :
              "inherit";
            return (
              <div
                key={item.id}
                className="flex items-start gap-3 py-2 px-2 rounded hover:bg-accent/30 transition-colors group"
              >
                <button
                  type="button"
                  onClick={() => handleToggle(item.id)}
                  className="mt-0.5 shrink-0"
                  title={done ? "İşareti kaldır" : "Tamamlandı işaretle"}
                >
                  {done ? (
                    <CheckCircle2 size={18} style={{ color: "var(--mtp-excellent)" }} />
                  ) : (
                    <Circle size={18} className="text-muted-foreground group-hover:text-foreground transition-colors" />
                  )}
                </button>
                <div className="flex-1 min-w-0">
                  <div className="flex items-center gap-2 flex-wrap">
                    <span
                      className={`text-sm ${done ? "line-through text-muted-foreground" : "font-medium"}`}
                      style={{ color: done ? undefined : priorityColor }}
                    >
                      {item.label}
                    </span>
                    {item.priority === "high" && !done && (
                      <span
                        className="text-[10px] font-semibold px-1.5 py-0.5 rounded-full"
                        style={{ background: "rgba(255,80,80,0.15)", color: "var(--mtp-danger)" }}
                      >
                        ÖNCELİK
                      </span>
                    )}
                  </div>
                  {!done && (
                    <Link
                      href={item.href}
                      className="text-xs text-muted-foreground hover:text-foreground transition-colors inline-flex items-center gap-1 mt-0.5"
                    >
                      {item.detail}
                      <ArrowRight size={10} />
                    </Link>
                  )}
                </div>
              </div>
            );
          })}
        </div>
        {completionPct === 100 && (
          <div
            className="text-xs text-center py-2 rounded"
            style={{ background: "rgba(40,167,69,0.1)", color: "var(--mtp-excellent)" }}
          >
            ✅ Bugünün rutini tamamlandı — disiplin kazanıyor!
          </div>
        )}
      </div>

      {/* P425 (31 May 2026): Ek Piyasa Göstergeleri — Ana Sayfa altı ayrı bölüm.
          Derin araştırma sentezi (Faber GTAA + McClellan + Zweig Breadth Thrust),
          her gösterge (?) tooltip açıklamalı. Kural #28: yetersiz veri MOCK değil. */}
      <MarketIndicatorsPanel />

      {/* KARAR ADAY #720 (24 May 2026): Daily Mindset Cards — Mark birebir alıntılı zihinsel disiplin */}
      <MindsetCardWidget />

      {/* Hızlı Erişim 4 buton */}
      <div className="grid grid-cols-2 lg:grid-cols-4 gap-2 pt-2">
        <Link
          href="/signals"
          className="flex items-center gap-2 p-3 rounded-md border hover:bg-accent transition-colors text-sm"
        >
          <Activity size={14} className="text-muted-foreground" />
          <span>Sinyaller</span>
        </Link>
        <Link
          href="/screens"
          className="flex items-center gap-2 p-3 rounded-md border hover:bg-accent transition-colors text-sm"
        >
          <TrendingUp size={14} className="text-muted-foreground" />
          <span>Tarama</span>
        </Link>
        <Link
          href="/watchlist"
          className="flex items-center gap-2 p-3 rounded-md border hover:bg-accent transition-colors text-sm"
        >
          <ListChecks size={14} className="text-muted-foreground" />
          <span>İzleme Listesi</span>
        </Link>
        <Link
          href="/piyasa-durumu"
          className="flex items-center gap-2 p-3 rounded-md border hover:bg-accent transition-colors text-sm"
        >
          <TrendingDown size={14} className="text-muted-foreground" />
          <span>Piyasa</span>
        </Link>
      </div>
    </div>
  );
}
