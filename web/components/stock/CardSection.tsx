import type { ReactNode } from "react";

/**
 * Paket 545 (20 Haz 2026): /hisse kart bölümü (research cockpit).
 *
 * 29 kart düz grid'de bunaltıcıydı — başlıklı mantıksal bölümlere ayrıldı (Karar & Risk /
 * Trend & Stage / Teknik / Baz Paternleri / Carr / Fundamental). Her bölüm responsive grid
 * (P436 pateni: md 2 kolon / xl 3 kolon, items-start değişken yükseklik üstten hizalı).
 */
export function CardSection({ title, children }: { title: string; children: ReactNode }) {
  return (
    <section className="flex flex-col gap-2" data-testid={`card-section-${title}`}>
      <h2 className="text-xs font-bold uppercase tracking-wider text-muted-foreground border-b border-muted-foreground/15 pb-1">
        {title}
      </h2>
      <div className="grid grid-cols-1 md:grid-cols-2 xl:grid-cols-3 gap-3 items-start">
        {children}
      </div>
    </section>
  );
}
