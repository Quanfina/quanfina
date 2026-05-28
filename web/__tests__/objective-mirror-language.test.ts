/**
 * Objektif Ayna Dil Disiplini guard (Paket 358 — KALICI İLKE #11).
 *
 * CLAUDE.md (Objektif Ayna Dil Disiplini) AÇIKÇA der: "bu disiplin kod katmanına
 * inmeli (string literal taraması, UI komponent yazım rehberi)". Bu test o
 * belgelenmiş ama uygulanmamış kontrolü hayata geçirir.
 *
 * Felsefe: "Sayılar konuşur, hisler değil. Bloomberg Terminal yağcılık yapmaz."
 * UI'da motivasyon/yağcılık/duygusal dil YASAK — sadece somut durum + aksiyon.
 *
 * Tarama: web/components + web/app altındaki tüm .ts/.tsx kaynak metni
 * (testler hariç). İhlal = yasaklı motivasyon ifadesi veya kutlama emojisi.
 */
import { describe, it, expect } from "vitest";
import { readFileSync, readdirSync, statSync } from "node:fs";
import { join, dirname } from "node:path";
import { fileURLToPath } from "node:url";

const HERE = dirname(fileURLToPath(import.meta.url));
const WEB_ROOT = join(HERE, "..");

// Yasaklı motivasyon/yağcılık ifadeleri (Türkçe UX dili — KALICI İLKE #11).
// Mod/durum emojileri (🛡️🚀⚠️🔴🟢●) İZİNLİ — onlar somut durum göstergesi.
// Sadece KUTLAMA/duygusal emojiler yasak.
const FORBIDDEN: { pattern: RegExp; label: string }[] = [
  { pattern: /\baferin\b/i, label: "Aferin (övgü)" },
  { pattern: /\btebrikler\b/i, label: "Tebrikler (övgü)" },
  { pattern: /üzülme/i, label: "Üzülme (duygusal)" },
  { pattern: /harikasın/i, label: "Harikasın (övgü)" },
  { pattern: /merak etme/i, label: "Merak etme (duygusal)" },
  { pattern: /endişelenme/i, label: "Endişelenme (duygusal)" },
  { pattern: /şanslısın/i, label: "Şanslısın (his)" },
  { pattern: /şansın yaver/i, label: "Şansın yaver (his)" },
  { pattern: /bunu hak ettin/i, label: "Bunu hak ettin (övgü)" },
  { pattern: /gurur duy/i, label: "Gurur duy (duygusal)" },
  { pattern: /moralin bozulmasın/i, label: "Moralin bozulmasın (duygusal)" },
  { pattern: /🎉/, label: "🎉 kutlama emoji" },
  { pattern: /🥳/, label: "🥳 kutlama emoji" },
  { pattern: /🙌/, label: "🙌 kutlama emoji" },
  { pattern: /👏/, label: "👏 alkış emoji" },
  { pattern: /🤩/, label: "🤩 duygusal emoji" },
  { pattern: /😊/, label: "😊 duygusal emoji" },
];

function collectSourceFiles(dir: string): string[] {
  const out: string[] = [];
  let entries: string[];
  try {
    entries = readdirSync(dir);
  } catch {
    return out;
  }
  for (const name of entries) {
    if (name === "node_modules" || name === "__tests__" || name === ".next") continue;
    const full = join(dir, name);
    const st = statSync(full);
    if (st.isDirectory()) {
      out.push(...collectSourceFiles(full));
    } else if (/\.(ts|tsx)$/.test(name) && !/\.test\.tsx?$/.test(name)) {
      out.push(full);
    }
  }
  return out;
}

describe("Objektif Ayna Dil Disiplini (KALICI İLKE #11) — yağcılık/motivasyon dili YASAK", () => {
  const files = [
    ...collectSourceFiles(join(WEB_ROOT, "components")),
    ...collectSourceFiles(join(WEB_ROOT, "app")),
    ...collectSourceFiles(join(WEB_ROOT, "lib")),
  ];

  it("tarama kapsamı dolu (komponent dosyaları bulundu)", () => {
    expect(files.length).toBeGreaterThan(20);
  });

  it("UI kaynaklarında yasaklı motivasyon/yağcılık ifadesi yok", () => {
    const violations: string[] = [];
    for (const file of files) {
      const text = readFileSync(file, "utf8");
      for (const { pattern, label } of FORBIDDEN) {
        if (pattern.test(text)) {
          const rel = file.replace(WEB_ROOT, "").replace(/\\/g, "/");
          violations.push(`${rel} → ${label}`);
        }
      }
    }
    // Felsefe: somut sayı + aksiyon; his/övgü değil (Bloomberg Terminal yağcılık yapmaz).
    expect(violations).toEqual([]);
  });
});
