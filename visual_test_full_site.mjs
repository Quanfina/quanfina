// Kural #21 Kanal C — Playwright bağımsız screenshot (Chrome MCP permission bypass)
// 9 sayfayı dolaş, full-page screenshot al, AI okuyabilir PNG'ler bırak
import { chromium } from "playwright";
import { mkdirSync } from "fs";
import { join } from "path";

const BASE_URL = "http://localhost:3000";
const OUT_DIR = "test-screenshots/_full_site";
mkdirSync(OUT_DIR, { recursive: true });

const PAGES = [
  { name: "01_dashboard",      path: "/",             desc: "Ana Sayfa Dashboard (KARAR #474+#480)" },
  { name: "02_signals",        path: "/signals",      desc: "Sinyaller AG Grid (KARAR #469-#479)" },
  { name: "03_screens",        path: "/screens",      desc: "Hisse Tarama (FF paket theme fix)" },
  { name: "04_watchlist",      path: "/watchlist",    desc: "Watchlist (Y paket MOCK)" },
  { name: "05_journal",        path: "/journal",      desc: "Trade Journal (II paket Sinyal Kaynagi)" },
  { name: "06_piyasa_durumu",  path: "/piyasa-durumu",desc: "Piyasa Durumu (4 kart)" },
  { name: "07_minervini",      path: "/minervini",    desc: "Minervini MOCK 30+ hisse" },
  { name: "08_carr",           path: "/carr",         desc: "Carr referans kartlari" },
  { name: "09_hisse_NVDA",     path: "/hisse/NVDA",   desc: "Hisse Detay TradingView" },
];

async function main() {
  console.log("=== Quanfina Full Site Screenshot ===\n");
  const browser = await chromium.launch({ headless: true });
  const ctx = await browser.newContext({
    viewport: { width: 1366, height: 768 },  // Sn. Ferit ekran boyutu
    colorScheme: "dark",                      // KARAR #476 dark default
  });
  const page = await ctx.newPage();
  const errors = [];
  page.on("console", (m) => { if (m.type() === "error") errors.push(`[${m.location().url}] ${m.text()}`); });

  let okCount = 0;
  let failCount = 0;

  for (const p of PAGES) {
    const url = `${BASE_URL}${p.path}`;
    try {
      process.stdout.write(`  ${p.name.padEnd(22)} ${p.path.padEnd(20)} `);
      const res = await page.goto(url, { waitUntil: "networkidle", timeout: 15000 });
      await page.waitForTimeout(1500); // hydration + AG Grid render
      const status = res?.status() ?? 0;
      const fname = join(OUT_DIR, `${p.name}.png`);
      await page.screenshot({ path: fname, fullPage: true });
      console.log(`HTTP ${status} → ${fname}`);
      okCount++;
    } catch (e) {
      console.log(`❌ ${(e instanceof Error ? e.message : String(e)).slice(0, 80)}`);
      failCount++;
    }
  }

  console.log(`\n=== OZET ===`);
  console.log(`  ✅ Basarili: ${okCount}/${PAGES.length}`);
  console.log(`  ❌ Hatali  : ${failCount}/${PAGES.length}`);
  console.log(`  📁 Klasor  : ${OUT_DIR}/`);
  if (errors.length > 0) {
    console.log(`\n  Console Errors (${errors.length}):`);
    errors.slice(0, 5).forEach((e) => console.log(`    - ${e.slice(0, 120)}`));
  }

  await browser.close();
}

main().catch((e) => { console.error(e); process.exit(1); });
