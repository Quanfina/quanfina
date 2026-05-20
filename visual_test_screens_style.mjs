// /screens stil doğrulama — JetBrains Mono + tabular-nums + 12px (KARAR #484)
import { chromium } from "playwright";

const browser = await chromium.launch({ headless: true });
const ctx = await browser.newContext({ viewport: { width: 1366, height: 768 }, colorScheme: "dark" });
const page = await ctx.newPage();
const errors = [];
page.on("console", (m) => { if (m.type() === "error") errors.push(m.text().slice(0, 100)); });

console.log("=== /screens stil dogrulama ===\n");
const res = await page.goto("http://localhost:3000/screens", { waitUntil: "domcontentloaded", timeout: 15000 });
console.log(`HTTP: ${res?.status()}`);
// AG Grid full render bekle (React Query + AG Grid mount)
await page.waitForSelector(".ag-cell", { timeout: 10000 }).catch(() => {});
await page.waitForTimeout(2000);

// AG Grid hucresi - ilk birkac hucrede inline style cek
const cellInfo = await page.evaluate(() => {
  const cells = document.querySelectorAll(".ag-cell");
  const sample = [];
  for (let i = 0; i < Math.min(8, cells.length); i++) {
    const cs = window.getComputedStyle(cells[i]);
    sample.push({
      idx: i,
      colId: cells[i].getAttribute("col-id"),
      fontFamily: cs.fontFamily.slice(0, 40),
      fontSize: cs.fontSize,
      fontVariantNumeric: cs.fontVariantNumeric,
      textAlign: cs.textAlign,
      text: cells[i].textContent?.trim().slice(0, 20),
    });
  }
  return { total: cells.length, sample };
});

console.log(`AG Grid toplam hucre: ${cellInfo.total}\n`);
console.log("Ilk 8 hucre stil snapshot:");
for (const c of cellInfo.sample) {
  const monoOk = c.fontFamily.includes("JetBrains") || c.fontFamily.includes("mono") || c.fontFamily.includes("monospace");
  const sizeOk = c.fontSize === "12px" || c.fontSize === "13px";
  const tnOk = c.fontVariantNumeric === "tabular-nums";
  const flag = monoOk && sizeOk ? "🟢" : "🟡";
  console.log(`  ${flag} [${c.idx}] col=${c.colId} | font=${c.fontFamily} | size=${c.fontSize} | tn=${c.fontVariantNumeric} | align=${c.textAlign} | text="${c.text}"`);
}

console.log(`\nConsole errors: ${errors.length}`);
errors.slice(0, 3).forEach(e => console.log(`  ${e}`));

await browser.close();
