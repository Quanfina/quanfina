"use client";

import { useMemo, useState } from "react";
import { Quote } from "lucide-react";
import {
  MINDSET_CARDS,
  CATEGORY_LABELS,
  CATEGORY_COLORS,
  type MindsetCategory,
} from "@/data/mindset-cards";

/**
 * KARAR ADAY #730 (24 May 2026) — Mark Zihinsel Disiplin Kütüphanesi.
 *
 * 15 Mark birebir alıntılı mindset kart listesi. Kategori filtresi:
 * Tümü / Risk / Mindset / Setup / Management / Exit
 *
 * Mark MSW + TTLC + TLSMW birebir alıntı + Quanfina'da uygulama notu.
 * KALICI İLKE #4: Her kart kaynak sayfa numarası ile referanslı.
 */

const ALL_FILTER = "all" as const;
type Filter = MindsetCategory | typeof ALL_FILTER;

const FILTER_LABELS: Record<Filter, string> = {
  all: "Tümü",
  risk: "Risk Yönetimi",
  mindset: "Zihinsel Disiplin",
  setup: "Giriş Hazırlığı",
  management: "Trade Yönetimi",
  exit: "Çıkış Disiplini",
};

export default function ZihinselDisiplinPage() {
  const [filter, setFilter] = useState<Filter>(ALL_FILTER);

  const filteredCards = useMemo(() => {
    if (filter === ALL_FILTER) return MINDSET_CARDS;
    return MINDSET_CARDS.filter((c) => c.category === filter);
  }, [filter]);

  // Kategori bazlı sayım (filter button rozetleri için)
  const categoryCounts = useMemo(() => {
    const counts: Record<Filter, number> = {
      all: MINDSET_CARDS.length,
      risk: 0, mindset: 0, setup: 0, management: 0, exit: 0,
    };
    for (const card of MINDSET_CARDS) {
      counts[card.category] += 1;
    }
    return counts;
  }, []);

  return (
    <div className="flex flex-col h-full">
      {/* Header */}
      <div className="px-6 py-3 border-b">
        <div className="flex flex-col gap-1">
          <h1 className="text-xl font-semibold tracking-tight flex items-center gap-2">
            <Quote size={18} className="text-muted-foreground" />
            Mark Zihinsel Disiplin Kütüphanesi
          </h1>
          <p className="text-sm text-muted-foreground">
            Mark Minervini birebir alıntılı disiplin kartları — MSW + TTLC + TLSMW kaynaklı,
            Quanfina&apos;da uygulama notu ile (KARAR #720 + #730).
          </p>
        </div>
      </div>

      {/* Filter bar */}
      <div className="px-6 py-3 border-b flex flex-wrap items-center gap-2">
        {(Object.keys(FILTER_LABELS) as Filter[]).map((key) => {
          const active = filter === key;
          const count = categoryCounts[key];
          const accentColor =
            key === ALL_FILTER ? "var(--foreground)" : CATEGORY_COLORS[key as MindsetCategory];
          return (
            <button
              key={key}
              type="button"
              onClick={() => setFilter(key)}
              className="inline-flex items-center gap-1.5 px-3 py-1.5 rounded-md text-xs font-medium border transition-colors"
              style={{
                background: active ? `${accentColor}22` : "transparent",
                borderColor: active ? `${accentColor}55` : "var(--border)",
                color: active ? accentColor : "var(--muted-foreground)",
              }}
            >
              {FILTER_LABELS[key]}
              <span
                className="text-[10px] tabular-nums opacity-80"
                style={{ color: active ? accentColor : "inherit" }}
              >
                ({count})
              </span>
            </button>
          );
        })}
      </div>

      {/* Cards grid */}
      <div className="flex-1 overflow-auto px-6 py-4">
        {filteredCards.length === 0 ? (
          <p className="text-sm text-muted-foreground text-center py-12">
            Bu kategoride kart yok.
          </p>
        ) : (
          <div className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-3 gap-3">
            {filteredCards.map((card) => {
              const color = CATEGORY_COLORS[card.category];
              return (
                <div
                  key={card.id}
                  className="rounded-lg border bg-card p-4 flex flex-col gap-2 hover:shadow-sm transition-shadow"
                  style={{ borderLeftWidth: "3px", borderLeftColor: color }}
                >
                  <div className="flex items-start gap-2">
                    <span aria-hidden="true" className="text-xl leading-none shrink-0">
                      {card.emoji}
                    </span>
                    <div className="flex-1 min-w-0">
                      <span
                        className="text-[10px] px-2 py-0.5 rounded-full font-medium uppercase tracking-wider"
                        style={{ background: `${color}1A`, color }}
                      >
                        {CATEGORY_LABELS[card.category]}
                      </span>
                    </div>
                  </div>

                  <blockquote
                    className="text-sm font-medium italic leading-relaxed border-l-2 pl-2 py-0.5"
                    style={{ borderLeftColor: color, color: "var(--foreground)" }}
                  >
                    &ldquo;{card.quote}&rdquo;
                  </blockquote>

                  <p className="text-[11px] text-muted-foreground -mt-1">
                    — {card.source}
                  </p>

                  <div className="text-xs bg-muted/40 rounded px-2 py-1.5 leading-relaxed">
                    <span className="font-semibold text-muted-foreground">Quanfina&apos;da: </span>
                    <span>{card.quanfinaNote}</span>
                  </div>
                </div>
              );
            })}
          </div>
        )}
      </div>
    </div>
  );
}
