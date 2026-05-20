// Tum sayfalardaki AG Grid font dogrulamasi (CCC paket fix yayilim)
import { chromium } from "playwright";
import { mkdirSync } from "fs";

mkdirSync("test-screenshots/_font_proof", { recursive: true });

const browser = await chromium.launch({ headless: true });
const ctx = await browser.newContext({ viewport: { width: 1366, height: 768 }, colorScheme: "dark" });

const PAGES = [
  { name: "signals",   path: "/signals",   expect: "AG Grid: Sinyaller" },
  { name: "screens",   path: "/screens",   expect: "AG Grid: Hisse Tarama" },
  { name: "watchlist", path: "/watchlist", expect: "AG Grid: Watchlist" },
  { name: "journal",   path: "/journal",   expect: "AG Grid: Trade Journal" },
  { name: "minervini", path: "/minervini", expect: "AG Grid: Minervini" },
];

console.log("=== Tum sayfalar font dogrulama (CCC paket) ===\n");

for (const p of PAGES) {
  const tab = await ctx.newPage();
  try {
    await tab.goto(`http://localhost:3000${p.path}`, { waitUntil: "domcontentloaded", timeout: 15000 });
    await tab.waitForSelector(".ag-cell", { timeout: 8000 }).catch(() => {});
    await tab.waitForTimeout(2500);

    const info = await tab.evaluate(() => {
      const cells = document.querySelectorAll(".ag-cell");
      if (cells.length === 0) return { error: "AG Grid hucresi yok" };
      const cs = window.getComputedStyle(cells[0]);
      const hasSynthetic = cs.fontFamily.includes("Fallback");
      return {
        cellCount: cells.length,
        font: cs.fontFamily.slice(0, 60),
        size: cs.fontSize,
        hasSynthetic,
        firstText: cells[0].textContent?.trim().slice(0, 15),
      };
    });

    await tab.screenshot({ path: `test-screenshots/_font_proof/${p.name}.png`, fullPage: false });

    const flag = info.error ? "⚠️" : (info.hasSynthetic ? "🔴" : "🟢");
    const summary = info.error || `cell=${info.cellCount} font=${info.font.slice(0, 35)}... size=${info.size}`;
    console.log(`${flag} ${p.path.padEnd(14)} ${summary}`);
    if (!info.error) {
      console.log(`   sentetik fallback: ${info.hasSynthetic ? "VAR (HALA YANLIS)" : "YOK ✓ (gerçek monospace)"}`);
      console.log(`   ilk hucre text: "${info.firstText}"`);
    }
  } catch (e) {
    console.log(`🔴 ${p.path}: ${e.message.slice(0, 60)}`);
  }
  await tab.close();
}

await browser.close();
console.log("\nScreenshot'lar: test-screenshots/_font_proof/");
