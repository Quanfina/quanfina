"use client";

import { useEffect, useState } from "react";
import Link from "next/link";
import { ArrowRight, Activity, ListChecks, NotebookText, Globe, TrendingUp, TrendingDown, CheckCircle2, Circle, Sunrise } from "lucide-react";
import { useSignals } from "@/hooks/use-signals";
import { useTrades } from "@/hooks/use-trades";
import { useMarketStatus } from "@/hooks/use-market-status";
import { useWatchlist } from "@/hooks/use-watchlist";
import { formatDateTR } from "@/lib/format-date";
import { getDoneItems, toggleItem } from "@/lib/daily-checklist";
import { MindsetCardWidget } from "@/components/dashboard/MindsetCard";
import { MarkRegimeBanner } from "@/components/mark/MarkRegimeBanner";
import { fmtUsd, fmtPctSigned } from "@/lib/format-currency";

// KARAR #474 (20 May 2026 ~08:15): Ana sayfa POC → Gerçek Dashboard.
// UX Bölüm 3: "Sn. Ferit Quanfina'yı açtığında üstten alta: bakiye + piyasa + sinyaller"
// Bakiye broker entegrasyonu YOK → şu an: açık trade sayısı + toplam P/L (kapanmış)

const STATUS_COLORS: Record<string, string> = {
  buy: "#28A745",
  focus: "#4B9CD3",
  on_deck: "#F59E0B",
  watch: "var(--muted-foreground)",
};

const HEALTH_COLORS: Record<string, string> = {
  YEŞİL: "var(--mtp-excellent)",
  SARI: "var(--mtp-neutral)",
  KIRMIZI: "var(--mtp-danger)",
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
  const market = useMarketStatus();
  const watchlist = useWatchlist();

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

  const marketHealth = market.data?.market_health_label ?? "—";
  const marketHealthColor = HEALTH_COLORS[marketHealth] ?? "inherit";
  const marketScore = market.data?.market_health_score ?? null;
  const marketMode = market.data?.suggested_mode ?? "—";

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

      {/* Header */}
      <div className="flex flex-col gap-1">
        <h1 className="text-2xl font-semibold tracking-tight">Bugün Ne Var?</h1>
        <p className="text-sm text-muted-foreground">
          Günlük rutin — Sinyaller → İzleme Listesi → Piyasa → İşlem Günlüğü
        </p>
      </div>

      {/* Quick Stats — 4 metrik */}
      <div className="grid grid-cols-2 lg:grid-cols-4 gap-3">
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
                  {marketHealth}
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
