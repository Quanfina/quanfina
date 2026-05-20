"use client";

import Link from "next/link";
import { ArrowRight, Activity, ListChecks, NotebookText, Globe, TrendingUp, TrendingDown } from "lucide-react";
import { useSignals } from "@/hooks/use-signals";
import { useTrades } from "@/hooks/use-trades";
import { useMarketStatus } from "@/hooks/use-market-status";
import { useWatchlist } from "@/hooks/use-watchlist";
import { formatDateTR } from "@/lib/format-date";

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

  // En iyi 5 sinyal (R/R desc, sonra RS)
  const topSignals = (signals.data ?? []).slice(0, 5);

  // Son 3 açık trade
  const recentOpen = openTrades.slice(0, 3);

  const marketHealth = market.data?.market_health_label ?? "—";
  const marketHealthColor = HEALTH_COLORS[marketHealth] ?? "inherit";
  const marketScore = market.data?.market_health_score ?? null;
  const marketMode = market.data?.suggested_mode ?? "—";

  return (
    <div className="p-6 flex flex-col gap-6">
      {/* Header */}
      <div className="flex flex-col gap-1">
        <h1 className="text-2xl font-semibold tracking-tight">Bugün Ne Var?</h1>
        <p className="text-sm text-muted-foreground">
          Günlük rutin — Sinyaller → Watchlist → Piyasa → Trade Journal
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
          label="Watchlist"
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
              ? `${totalPL >= 0 ? "+" : ""}$${totalPL.toFixed(2)}`
              : "—"
          }
          subValue={
            closedTrades.length > 0
              ? `ort. ${totalPLPct >= 0 ? "+" : ""}${totalPLPct.toFixed(2)}%`
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
                      ${s.price.toFixed(2)}
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
            Trade Journal <ArrowRight size={12} />
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
                    ${t.entry_price.toFixed(2)}
                  </span>
                </div>
              </div>
            ))}
          </div>
        )}
      </div>

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
          <span>Hisse Tarama</span>
        </Link>
        <Link
          href="/watchlist"
          className="flex items-center gap-2 p-3 rounded-md border hover:bg-accent transition-colors text-sm"
        >
          <ListChecks size={14} className="text-muted-foreground" />
          <span>Watchlist</span>
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
