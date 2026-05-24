"use client";

import { useEffect, useMemo, useState } from "react";
import { Quote, RefreshCw, CheckCircle2 } from "lucide-react";
import {
  MINDSET_CARDS,
  CATEGORY_LABELS,
  CATEGORY_COLORS,
  getTodayMindsetCard,
  type MindsetCard as MindsetCardType,
} from "@/data/mindset-cards";
import { isCardReadToday, markCardReadToday, getReadStreak } from "@/lib/mindset-read-state";
import { Flame } from "lucide-react";

/**
 * KARAR ADAY #720 — Daily Mindset Cards (Dashboard üst bölüm)
 *
 * Mark Minervini birebir alıntılı zihinsel disiplin kartı.
 * Gün başına 1 sabit kart (deterministik tarih hash) — Sn. Ferit'in
 * her sabah Dashboard açtığında "Bugün için Mark hatırlatması" disiplini.
 *
 * Aksiyon: Yenile butonu → rastgele başka bir kart (manuel istek).
 */
export function MindsetCardWidget() {
  const todayCard = useMemo(() => getTodayMindsetCard(), []);
  const [card, setCard] = useState<MindsetCardType>(todayCard);
  const [isManualPick, setIsManualPick] = useState(false);
  // KARAR #720 alt (Paket 28): "okundu" persistence — günlük disiplin
  const [isReadToday, setIsReadToday] = useState(false);
  // KARAR #720 alt (Paket 29): Streak history — gün üst üste okuma
  const [streak, setStreak] = useState(0);

  // Mount + bugünün kartı değişince "okundu mu" + streak yeniden kontrol et
  useEffect(() => {
    setIsReadToday(isCardReadToday(todayCard.id));
    setStreak(getReadStreak().streak);
  }, [todayCard.id]);

  function pickRandom() {
    const others = MINDSET_CARDS.filter((c) => c.id !== card.id);
    if (others.length === 0) return;
    const next = others[Math.floor(Math.random() * others.length)];
    setCard(next);
    setIsManualPick(true);
  }

  function resetToToday() {
    setCard(todayCard);
    setIsManualPick(false);
  }

  function handleMarkRead() {
    markCardReadToday(todayCard.id);
    setIsReadToday(true);
    setStreak(getReadStreak().streak);  // P29: streak güncelle
  }

  const accentColor = CATEGORY_COLORS[card.category];

  return (
    <div
      className="rounded-lg border bg-card p-4 flex flex-col gap-3 relative overflow-hidden"
      style={{
        // Sol kenarda renk şeridi — kategori bağlamı
        borderLeftWidth: "4px",
        borderLeftColor: accentColor,
      }}
    >
      {/* Üst başlık + meta */}
      <div className="flex items-center justify-between gap-2">
        <div className="flex items-center gap-2">
          <span className="text-2xl leading-none" aria-hidden="true">
            {card.emoji}
          </span>
          <div className="flex flex-col">
            <h2 className="text-sm font-semibold flex items-center gap-2">
              <Quote size={14} className="text-muted-foreground" />
              {isManualPick ? "Mark Hatırlatması" : "Bugünün Mark Hatırlatması"}
            </h2>
            <span
              className="text-xs px-2 py-0.5 rounded-full font-medium w-fit mt-1"
              style={{
                background: `${accentColor}1A`, // 10% opacity
                color: accentColor,
              }}
            >
              {CATEGORY_LABELS[card.category]}
            </span>
          </div>
        </div>
        <div className="flex items-center gap-2">
          {/* KARAR #720 alt (Paket 29): Streak rozet — günlük disiplin motivasyon */}
          {streak >= 2 && !isManualPick && (
            <span
              className="inline-flex items-center gap-1 text-xs font-semibold px-2 py-0.5 rounded-full"
              style={{
                background: "rgba(245, 158, 11, 0.15)",
                color: "#F59E0B",
              }}
              title={`${streak} gün üst üste Mark hatırlatması okuma disiplini`}
            >
              <Flame size={12} />
              {streak} gün
            </span>
          )}
          {isManualPick && (
            <button
              type="button"
              onClick={resetToToday}
              className="text-xs text-muted-foreground hover:text-foreground transition-colors px-2 py-1 rounded hover:bg-accent/40"
              title="Bugünün sabit kartına dön"
            >
              Bugüne dön
            </button>
          )}
          <button
            type="button"
            onClick={pickRandom}
            className="p-1.5 rounded hover:bg-accent/40 transition-colors"
            title="Rastgele başka bir kart"
            aria-label="Rastgele kart"
          >
            <RefreshCw size={14} className="text-muted-foreground" />
          </button>
        </div>
      </div>

      {/* Mark birebir alıntı (İngilizce orijinal — kalite korunsun) */}
      <blockquote
        className="text-base font-medium leading-relaxed border-l-2 pl-3 py-1 italic"
        style={{
          borderLeftColor: accentColor,
          color: "var(--foreground)",
        }}
      >
        &ldquo;{card.quote}&rdquo;
      </blockquote>

      {/* Kaynak referansı */}
      <p className="text-xs text-muted-foreground -mt-1">
        — Mark Minervini, <span className="italic">{card.source}</span>
      </p>

      {/* Quanfina'da nasıl uygulandığı (Türkçe pratik) */}
      <div className="text-xs bg-muted/40 rounded px-3 py-2 leading-relaxed">
        <span className="font-semibold text-muted-foreground">Quanfina&apos;da: </span>
        <span>{card.quanfinaNote}</span>
      </div>

      {/* KARAR #720 alt (Paket 28): "Okudum" buton + günlük disiplin pekiştirme */}
      {!isManualPick && (
        <div className="flex items-center justify-between gap-2 pt-1">
          {isReadToday ? (
            <span
              className="inline-flex items-center gap-1.5 text-xs font-medium"
              style={{ color: "var(--mtp-excellent)" }}
            >
              <CheckCircle2 size={14} />
              Bugün için okundu — disiplin pekiştirildi
            </span>
          ) : (
            <button
              type="button"
              onClick={handleMarkRead}
              className="inline-flex items-center gap-1.5 px-3 py-1.5 rounded-md text-xs font-medium border transition-colors hover:bg-accent/40"
              style={{ borderColor: `${accentColor}55`, color: accentColor }}
              title="Bugün için Mark hatırlatmasını okuduğunu işaretle"
            >
              <CheckCircle2 size={14} />
              Okudum, anladım
            </button>
          )}
        </div>
      )}
    </div>
  );
}
