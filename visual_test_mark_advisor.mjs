import { chromium } from "playwright";
import { mkdir } from "fs/promises";
import { join } from "path";

const OUT_DIR = "test-screenshots/_mark_advisor";

async function main() {
  await mkdir(OUT_DIR, { recursive: true });

  const browser = await chromium.launch({ headless: true });
  const ctx = await browser.newContext({
    colorScheme: "dark",
    viewport: { width: 1366, height: 900 },
  });
  const page = await ctx.newPage();

  console.log("=== Mark Risk Advisor UI Visual Test ===\n");

  // 1. Journal sayfasına git
  console.log("[1] /journal yükleniyor...");
  await page.goto("http://localhost:3000/journal", { waitUntil: "networkidle" });
  await page.waitForTimeout(1500);
  const ss1 = join(OUT_DIR, "01_journal_loaded.png");
  await page.screenshot({ path: ss1, fullPage: true });
  console.log(`    -> ${ss1}`);

  // 2. "Trade Ekle" / "Yeni Trade" butonunu bul ve tıkla
  console.log("[2] 'Yeni Trade' butonu aranıyor...");
  // Olası buton text'leri
  const buttonCandidates = [
    'button:has-text("Yeni Trade")',
    'button:has-text("Trade Ekle")',
    'button:has-text("+ Trade")',
    'button:has-text("Ekle")',
    'button[aria-label*="trade"]',
  ];
  let clicked = false;
  for (const sel of buttonCandidates) {
    try {
      const btn = page.locator(sel).first();
      if (await btn.count() > 0) {
        await btn.click();
        console.log(`    -> tıklandı: ${sel}`);
        clicked = true;
        break;
      }
    } catch {}
  }
  if (!clicked) {
    console.log("    -> Yeni Trade butonu bulunamadı, sayfada tüm butonları logla:");
    const btnTexts = await page.locator("button").allInnerTexts();
    console.log("       Mevcut butonlar:", btnTexts.slice(0, 20));
  }

  await page.waitForTimeout(1000);
  const ss2 = join(OUT_DIR, "02_dialog_opened.png");
  await page.screenshot({ path: ss2, fullPage: true });
  console.log(`    -> ${ss2}`);

  // 3. Entry Price ve Adet doldur
  if (clicked) {
    console.log("[3] Entry $ ve Adet dolduruluyor...");
    try {
      await page.fill("#at-eprice", "150");
      await page.fill("#at-shares", "100");
      await page.waitForTimeout(2000); // API call için bekle
      console.log("    -> doldurma OK, advisor API çağrısı beklendi");
    } catch (e) {
      console.log(`    -> doldurma hatası: ${e.message}`);
    }
    const ss3 = join(OUT_DIR, "03_with_entry_price_shares.png");
    await page.screenshot({ path: ss3, fullPage: true });
    console.log(`    -> ${ss3}`);
  }

  // 4. Mark Risk Advisor mavi kartı görünüyor mu?
  console.log("[4] Mark Risk Advisor mavi kartı kontrolü...");
  const advisorCard = page.locator("text=Mark Risk Danışmanı").first();
  const advisorVisible = await advisorCard.count() > 0;
  console.log(`    -> Advisor görünür: ${advisorVisible ? "✅" : "❌"}`);

  if (advisorVisible) {
    // DOM içerik snapshot (token ekonomik analiz için)
    const advisorText = await page.locator('div:has(> div > span:has-text("Mark Risk Danışmanı"))').first().innerText().catch(() => "");
    console.log("\n=== Mark Risk Advisor Content (DOM) ===");
    console.log(advisorText.substring(0, 800));
  }

  // 5. Console log + network errors
  console.log("\n[5] Console log + network errors check...");
  const consoleErrors = [];
  page.on("console", (msg) => {
    if (msg.type() === "error") consoleErrors.push(msg.text());
  });
  await page.waitForTimeout(500);
  console.log(`    -> Console errors: ${consoleErrors.length}`);
  consoleErrors.slice(0, 5).forEach((e) => console.log(`       - ${e}`));

  await browser.close();
  console.log("\n=== BITTI ===");
  console.log(`PNG'ler: ${OUT_DIR}/`);
}

main().catch((e) => {
  console.error("HATA:", e);
  process.exit(1);
});
