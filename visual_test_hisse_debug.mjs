// Hisse detay sayfası debug — Playwright timeout sebebi tespit (Kural #21 B disiplini)
import { chromium } from "playwright";

const SYMBOL = process.argv[2] || "NVDA";
const URL = `http://localhost:3000/hisse/${SYMBOL}`;

async function main() {
  console.log(`=== /hisse/${SYMBOL} DOM Debug ===\n`);
  const browser = await chromium.launch({ headless: true });
  const ctx = await browser.newContext({
    viewport: { width: 1366, height: 768 },
    colorScheme: "dark",
  });
  const page = await ctx.newPage();

  const errors = [];
  const networkFails = [];
  page.on("console", (m) => { if (m.type() === "error") errors.push(m.text()); });
  page.on("requestfailed", (req) => networkFails.push(`${req.method()} ${req.url()} — ${req.failure()?.errorText}`));

  try {
    // Domcontentloaded yeterli, networkidle değil (sürekli polling olabilir)
    const res = await page.goto(URL, { waitUntil: "domcontentloaded", timeout: 10000 });
    console.log(`HTTP Status: ${res?.status()}`);
    await page.waitForTimeout(3000); // hydration

    // Sayfa başlığı + URL
    const title = await page.title();
    console.log(`Title: ${title}`);

    // Ana içerik
    const h1 = await page.locator("h1").first().textContent().catch(() => null);
    console.log(`H1: ${h1 || "(yok)"}`);

    // TradingView chart konteyner kontrolü
    const chartCount = await page.locator('[class*="lightweight"], [class*="chart"], canvas').count();
    console.log(`Chart elementi (canvas / lightweight-chart): ${chartCount}`);

    // Bilinen komponentler
    const components = [
      { name: "Aktif Stratejiler", selector: 'text=/Aktif Stratejiler/i' },
      { name: "OHLC veri", selector: 'text=/OHLC|Açılış|Kapanış/i' },
      { name: "Setup tipleri", selector: 'text=/VCP|Pullback|Power Play/i' },
      { name: "Sinyaller bölümü", selector: 'text=/Sinyaller/i' },
    ];
    for (const c of components) {
      const count = await page.locator(c.selector).count();
      console.log(`  ${c.name}: ${count} eşleşme`);
    }

    // Sayfa text snapshot (ilk 500 char)
    const bodyText = await page.locator("body").textContent();
    console.log(`\nBody Text (ilk 300 char):\n  ${(bodyText || "").slice(0, 300)}...`);

    console.log(`\nConsole Errors: ${errors.length}`);
    errors.slice(0, 5).forEach((e) => console.log(`  - ${e.slice(0, 150)}`));

    console.log(`\nNetwork Fails: ${networkFails.length}`);
    networkFails.slice(0, 5).forEach((n) => console.log(`  - ${n}`));

  } catch (e) {
    console.log(`❌ HATA: ${e instanceof Error ? e.message : String(e)}`);
    console.log(`\nConsole Errors (hata öncesi): ${errors.length}`);
    errors.slice(0, 3).forEach((er) => console.log(`  - ${er.slice(0, 150)}`));
  }

  await browser.close();
}

main();
