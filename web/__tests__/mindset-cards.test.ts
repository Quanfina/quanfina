/**
 * mindset-cards veri bütünlük testi (KARAR ADAY #720 + #730).
 *
 * KALICI İLKE #4 (Matematik/Kaynak Uydurmama) + Kural #26 anayasa:
 * her kart Mark birebir alıntı + kaynak referansı zorunlu, uydurma YOK.
 * 15 kart, 5 kategori, her kategori CATEGORY_LABELS/COLORS'da tanımlı.
 */
import { describe, it, expect } from "vitest";
import {
  MINDSET_CARDS,
  CATEGORY_LABELS,
  CATEGORY_COLORS,
  type MindsetCategory,
} from "@/data/mindset-cards";

const CATEGORIES: MindsetCategory[] = [
  "risk",
  "mindset",
  "setup",
  "management",
  "exit",
];

describe("mindset-cards — kart envanteri", () => {
  it("15 kart mevcut (P720 + P730)", () => {
    expect(MINDSET_CARDS).toHaveLength(15);
  });

  it("Her kartın benzersiz id'si var", () => {
    const ids = MINDSET_CARDS.map((c) => c.id);
    expect(new Set(ids).size).toBe(ids.length);
  });

  it("id format: '{category-kısaltma}-NN' (örn risk-01)", () => {
    for (const card of MINDSET_CARDS) {
      expect(card.id).toMatch(/^[a-z]+-\d{2}$/);
    }
  });
});

describe("mindset-cards — KALICI İLKE #4 kaynak atfı (uydurma YOK)", () => {
  it("Her kartın quote alanı dolu (Mark birebir)", () => {
    for (const card of MINDSET_CARDS) {
      expect(card.quote.trim().length).toBeGreaterThan(10);
    }
  });

  it("Her kartın source alanı dolu (kaynak referansı ZORUNLU)", () => {
    for (const card of MINDSET_CARDS) {
      expect(card.source.trim().length).toBeGreaterThan(3);
    }
  });

  it("source Mark kaynaklarına atıflı (TLSMW/TTLC/MSW/Mindset/Mark X)", () => {
    // Mark kaynak repertuarı: kitaplar (TLSMW/TTLC/MSW/MM) + sayfa atfı +
    // Mark X (Twitter) + video kaynakları (Mark Video/TraderLion/Brandon)
    const validSourcePattern =
      /TLSMW|TTLC|MSW|Mindset|Mark X|MM|s\.\d|Mark Video|TraderLion|Brandon/;
    for (const card of MINDSET_CARDS) {
      expect(card.source).toMatch(validSourcePattern);
    }
  });

  it("Her kartın Quanfina uygulama notu var (pratik karşılık)", () => {
    for (const card of MINDSET_CARDS) {
      expect(card.quanfinaNote.trim().length).toBeGreaterThan(10);
    }
  });

  it("Her kartın emoji görsel ipucu var", () => {
    for (const card of MINDSET_CARDS) {
      expect(card.emoji.trim().length).toBeGreaterThan(0);
    }
  });
});

describe("mindset-cards — kategori bütünlüğü", () => {
  it("Her kartın kategorisi 5 geçerli değerden biri", () => {
    for (const card of MINDSET_CARDS) {
      expect(CATEGORIES).toContain(card.category);
    }
  });

  it("CATEGORY_LABELS 5 kategoriyi de kapsar", () => {
    for (const cat of CATEGORIES) {
      expect(CATEGORY_LABELS[cat]).toBeDefined();
      expect(CATEGORY_LABELS[cat].length).toBeGreaterThan(0);
    }
  });

  it("CATEGORY_COLORS 5 kategoriyi de kapsar (CSS değer)", () => {
    for (const cat of CATEGORIES) {
      expect(CATEGORY_COLORS[cat]).toBeDefined();
      expect(CATEGORY_COLORS[cat].length).toBeGreaterThan(0);
    }
  });

  it("Kategori dağılımı: risk=5, mindset=3 (yorum bloklarına uygun)", () => {
    const counts: Record<string, number> = {};
    for (const card of MINDSET_CARDS) {
      counts[card.category] = (counts[card.category] ?? 0) + 1;
    }
    expect(counts.risk).toBe(5);
    expect(counts.mindset).toBe(3);
    // Toplam 15
    const total = Object.values(counts).reduce((a, b) => a + b, 0);
    expect(total).toBe(15);
  });

  it("Her kategoride en az 1 kart var (boş kategori yok)", () => {
    const usedCategories = new Set(MINDSET_CARDS.map((c) => c.category));
    for (const cat of CATEGORIES) {
      expect(usedCategories.has(cat)).toBe(true);
    }
  });
});

describe("mindset-cards — Mark Wall %10 + position sizing canon (Kural #26)", () => {
  it("Risk kartlarında Mark Wall %10 ve position sizing atfı var", () => {
    const riskCards = MINDSET_CARDS.filter((c) => c.category === "risk");
    const allText = riskCards.map((c) => c.quote + c.quanfinaNote).join(" ");
    // Mark Wall %10 (Uncle Point) ve position sizing 1.25-2.5% canon
    expect(allText).toMatch(/10%|10\.0/);
    expect(allText).toMatch(/1\.25|2\.5/);
  });
});
