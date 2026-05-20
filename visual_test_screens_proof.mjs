// Hisse Tarama tablo gercek render kaniti - cache YOK
import { chromium } from "playwright";
import { mkdirSync } from "fs";
mkdirSync("test-screenshots/_proof", { recursive: true });

const browser = await chromium.launch({ headless: true });
const ctx = await browser.newContext({
  viewport: { width: 1366, height: 768 },
  colorScheme: "dark",
  // CACHE OFF
  serviceWorkers: "block",
});
const page = await ctx.newPage();
// Cache-control header bypass
await page.route("**/*", (route) => {
  const headers = { ...route.request().headers(), "cache-control": "no-cache, no-store", pragma: "no-cache" };
  route.continue({ headers });
});
await page.goto("http://localhost:3000/screens", { waitUntil: "domcontentloaded", timeout: 15000 });
await page.waitForSelector(".ag-cell", { timeout: 10000 });
await page.waitForTimeout(3000);
await page.screenshot({ path: "test-screenshots/_proof/screens_no_cache.png", fullPage: false });
console.log("Screenshot: test-screenshots/_proof/screens_no_cache.png");

// Fiyat hucresinin gercek render font'unu kanitla
const proof = await page.evaluate(() => {
  const priceCell = document.querySelector('.ag-cell[col-id="price"]');
  if (!priceCell) return { error: "price cell yok" };
  const cs = window.getComputedStyle(priceCell);
  // Inner spans/divs
  const inner = priceCell.firstElementChild;
  const innerCs = inner ? window.getComputedStyle(inner) : null;
  return {
    cellFont: cs.fontFamily,
    cellSize: cs.fontSize,
    cellText: priceCell.textContent?.trim(),
    innerTag: inner?.tagName,
    innerFont: innerCs?.fontFamily,
    innerSize: innerCs?.fontSize,
  };
});
console.log("\nFiyat hucresi gercek render:");
console.log(JSON.stringify(proof, null, 2));

await browser.close();
