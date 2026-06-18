/**
 * SCREEN_CONDITIONS veri bütünlük testi (KARAR #461-467 + #484-485).
 *
 * Tarama sayfası filtre koşulları — Mark Resmi Kural + Quanfina Ek + Mark Ekstra.
 * KALICI İLKE #4 (Matematik/Kaynak Uydurmama): her Mark koşulu kitap sayfa atıflı.
 * Kural #24 (Sağlam Gidelim): UI koşul listesi ↔ Resmî Kural bire-bir.
 */
import { describe, it, expect } from "vitest";
import {
  SCREEN_CATEGORIES,
  SCREEN_CONDITIONS,
  CONDITION_SOURCE_LABEL,
  type ConditionSource,
} from "@/types/screens";

// P502+ "carr" source eklendi (Carr stratejisi 9 setup); SOURCES güncel (4 kaynak).
const SOURCES: ConditionSource[] = ["mark", "quanfina", "mark_ekstra", "carr"];

describe("SCREEN_CATEGORIES — tarama şablonları (Minervini + Carr)", () => {
  // 18 Haz 2026: artık 21 ekran (3 Minervini çekirdek + Minervini lider/momentum + 9 Carr).
  // Brittle toEqual yerine çekirdek + Carr 9 setup varlık kontrolü (gelecek eklemeye dayanıklı).
  it("Minervini çekirdek 3 şablon mevcut", () => {
    expect(Object.keys(SCREEN_CATEGORIES)).toEqual(
      expect.arrayContaining(["stage2_10p", "temel_eleme", "tam_minervini"])
    );
  });

  it("Carr 9 setup mevcut", () => {
    expect(Object.keys(SCREEN_CATEGORIES)).toEqual(
      expect.arrayContaining([
        "mean_reversion", "pullback", "blue_sky", "coiled_spring", "bullish_base",
        "bullish_divergence", "blue_sea", "gap_down", "rising_wedge",
      ])
    );
  });

  it("stage2_10p label = 'Trend Template'", () => {
    expect(SCREEN_CATEGORIES.stage2_10p).toBe("Trend Template");
  });
});

describe("SCREEN_CONDITIONS — koşul source bütünlüğü", () => {
  it("Her şablonun koşulları 3 geçerli source'tan biri", () => {
    for (const [, conditions] of Object.entries(SCREEN_CONDITIONS)) {
      for (const cond of conditions) {
        expect(SOURCES).toContain(cond.source);
      }
    }
  });

  it("Her koşulun text alanı dolu (boş koşul yok)", () => {
    for (const [, conditions] of Object.entries(SCREEN_CONDITIONS)) {
      for (const cond of conditions) {
        expect(cond.text.trim().length).toBeGreaterThan(5);
      }
    }
  });
});

describe("stage2_10p — Trend Template (2 Quanfina + 8 Mark + 1 Mark Ekstra = 11)", () => {
  const conds = SCREEN_CONDITIONS.stage2_10p;

  it("Toplam 11 koşul (Sn. Ferit 22 May sıralama)", () => {
    expect(conds).toHaveLength(11);
  });

  it("İlk 2 koşul Quanfina Ek (evren daraltma — fiyat + hacim ÖNCE)", () => {
    expect(conds[0].source).toBe("quanfina");
    expect(conds[1].source).toBe("quanfina");
    expect(conds[0].text).toMatch(/Fiyat ≥ \$10/);
    expect(conds[1].text).toMatch(/hacim ≥ 500/);
  });

  it("8 Mark Resmi Kural (kitap birebir 8 madde, TLSMW s.79)", () => {
    const markCount = conds.filter((c) => c.source === "mark").length;
    expect(markCount).toBe(8);
  });

  it("8. Mark maddesi RS ≥ 70 (IBD canon)", () => {
    const markConds = conds.filter((c) => c.source === "mark");
    const rsCondition = markConds[markConds.length - 1];
    expect(rsCondition.text).toMatch(/Relative Strength.*≥ 70/);
  });

  it("1 Mark Ekstra Kural (RS ideali 80-90+ kitap tavsiyesi)", () => {
    const ekstra = conds.filter((c) => c.source === "mark_ekstra");
    expect(ekstra).toHaveLength(1);
    expect(ekstra[0].text).toMatch(/80-90/);
  });

  it("Mark %30 dipten + %25 zirveye canon (TLSMW s.79 birebir)", () => {
    const allText = conds.map((c) => c.text).join(" ");
    expect(allText).toMatch(/dipten en az %30/);
    expect(allText).toMatch(/zirveye en fazla %25/);
  });
});

describe("temel_eleme — Fundamental Soft Score (2 Quanfina + 3 Mark Ekstra = 5)", () => {
  const conds = SCREEN_CONDITIONS.temel_eleme;

  it("Toplam 5 koşul", () => {
    expect(conds).toHaveLength(5);
  });

  it("3 Mark Ekstra (Soft Score — EPS/Sales/ROE kitap atıflı)", () => {
    const ekstra = conds.filter((c) => c.source === "mark_ekstra");
    expect(ekstra).toHaveLength(3);
    const text = ekstra.map((c) => c.text).join(" ");
    expect(text).toMatch(/EPS Q\/Q.*≥ %25/);
    expect(text).toMatch(/Sales Q\/Q.*≥ %25/);
    expect(text).toMatch(/ROE ≥ %15-17/);
  });

  it("Kaynak atfı: TLSMW s.127/132 + Momentum Masters s.74 (KALICI İLKE #4)", () => {
    const text = conds.map((c) => c.text).join(" ");
    expect(text).toMatch(/TLSMW s\.127/);
    expect(text).toMatch(/TLSMW s\.132/);
    expect(text).toMatch(/Momentum Masters s\.74/);
  });
});

describe("tam_minervini — Hibrit Pipeline (10 Hard + 5 Soft = 15)", () => {
  const conds = SCREEN_CONDITIONS.tam_minervini;

  it("Toplam 15 koşul (10 Screen Hard + 5 Recipe Soft)", () => {
    expect(conds).toHaveLength(15);
  });

  it("AŞAMA 1 Hard Filter: 2 Quanfina + 8 Mark = 10 madde", () => {
    const hard = conds.filter(
      (c) => c.source === "quanfina" || c.source === "mark"
    );
    expect(hard).toHaveLength(10);
    expect(conds.filter((c) => c.source === "quanfina")).toHaveLength(2);
    expect(conds.filter((c) => c.source === "mark")).toHaveLength(8);
  });

  it("AŞAMA 2 Recipe Soft: 5 Mark Ekstra madde", () => {
    const soft = conds.filter((c) => c.source === "mark_ekstra");
    expect(soft).toHaveLength(5);
  });

  it("Soft Score 5 madde: EPS Q/Q + Sales Q/Q + ROE + Yıllık EPS + Operating Margin", () => {
    const softText = conds
      .filter((c) => c.source === "mark_ekstra")
      .map((c) => c.text)
      .join(" ");
    expect(softText).toMatch(/EPS Q\/Q/);
    expect(softText).toMatch(/Sales Q\/Q/);
    expect(softText).toMatch(/ROE/);
    expect(softText).toMatch(/Yıllık EPS/);
    expect(softText).toMatch(/Operating Margin/);
  });

  it("Mark koşulları kitap atıflı (TLSMW s.79/127/132 + Momentum Masters)", () => {
    const text = conds.map((c) => c.text).join(" ");
    expect(text).toMatch(/TLSMW s\.79/);
    expect(text).toMatch(/Momentum Masters/);
  });
});

describe("CONDITION_SOURCE_LABEL — 4 kaynak insan-okunabilir", () => {
  it("4 source label tanımlı (mark/quanfina/mark_ekstra/carr)", () => {
    for (const src of SOURCES) {
      expect(CONDITION_SOURCE_LABEL[src]).toBeDefined();
      expect(CONDITION_SOURCE_LABEL[src].length).toBeGreaterThan(0);
    }
  });

  it("mark='Mark Resmi Kural', quanfina='Quanfina Ek Filtre', mark_ekstra='Mark Ekstra Kural', carr='Carr Kuralı'", () => {
    expect(CONDITION_SOURCE_LABEL.mark).toBe("Mark Resmi Kural");
    expect(CONDITION_SOURCE_LABEL.quanfina).toBe("Quanfina Ek Filtre");
    expect(CONDITION_SOURCE_LABEL.mark_ekstra).toBe("Mark Ekstra Kural");
    expect(CONDITION_SOURCE_LABEL.carr).toBe("Carr Kuralı");
  });
});
