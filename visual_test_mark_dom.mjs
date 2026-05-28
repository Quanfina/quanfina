/**
 * A2 Gorsel Dogrulama (Kural #21 Kanal C — Playwright bagimsiz, DOM-default).
 *
 * Migration + backfill sonrasi /hisse sayfalarinda Mark rozetleri GERCEK DB
 * verisiyle render oluyor mu? AAOI (power_play), AAPL (carr_stage 2), AHCO (stage 1).
 *
 * DOM text analizi (PNG degil — token ekonomik, Kural #21 disiplin).
 */
import { chromium } from "playwright";

const SYMBOLS = [
  { sym: "AAOI", expect: ["Stage 2", "Power Play"] },
  { sym: "AAPL", expect: ["Stage 2"] },
  { sym: "AHCO", expect: ["Stage 1"] },
];

const browser = await chromium.launch({ headless: true });
const ctx = await browser.newContext({
  colorScheme: "dark",
  viewport: { width: 1366, height: 768 },
});

let allPass = true;
for (const { sym, expect } of SYMBOLS) {
  const page = await ctx.newPage();
  const url = `http://localhost:3000/hisse/${sym}`;
  try {
    await page.goto(url, { waitUntil: "domcontentloaded", timeout: 60000 });
    // Mark signals API + yfinance fetch icin bekle
    await page.waitForTimeout(6000);
    const body = await page.innerText("body");
    console.log(`\n=== /hisse/${sym} ===`);
    const found = [];
    const missing = [];
    for (const term of expect) {
      if (body.includes(term)) found.push(term);
      else missing.push(term);
    }
    // Carr Stage Card her zaman gorunmeli (Stage label)
    const hasCarrCard = body.includes("Carr Stage");
    console.log(`  Carr Stage Card: ${hasCarrCard ? "VAR" : "YOK"}`);
    console.log(`  Bulunan rozetler: ${found.join(", ") || "(yok)"}`);
    if (missing.length) {
      console.log(`  EKSIK: ${missing.join(", ")}`);
      allPass = false;
    } else {
      console.log(`  TUM beklenen rozetler GORUNUYOR`);
    }
  } catch (e) {
    console.log(`  [HATA] ${sym}: ${String(e).slice(0, 120)}`);
    allPass = false;
  }
  await page.close();
}

await browser.close();
console.log(`\n=== SONUC: ${allPass ? "TUM GORSEL DOGRULAMA PASS" : "BAZI EKSIK (yukari bak)"} ===`);
process.exit(allPass ? 0 : 1);
