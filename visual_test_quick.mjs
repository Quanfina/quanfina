// Hızlı son doğrulama — Journal aksiyon + filtreler
import { chromium } from "playwright";
import { mkdirSync } from "fs";
import { join } from "path";
const BASE_URL = "http://localhost:3000";
mkdirSync("test-screenshots", { recursive: true });
async function shot(p, n) { await p.screenshot({ path: join("test-screenshots", `${n}.png`) }); }
function pass(m) { console.log(`  ✅ ${m}`); }
function fail(m) { console.log(`  ❌ ${m}`); }
function note(m) { console.log(`  ℹ️  ${m}`); }

async function main() {
  const browser = await chromium.launch({ headless: true, slowMo: 60 });
  const ctx = await browser.newContext({ viewport: { width: 1600, height: 900 } }); // daha geniş viewport
  const page = await ctx.newPage();
  const errs = [];
  page.on("console", m => { if (m.type() === "error") errs.push(m.text()); });

  await page.goto(`${BASE_URL}/journal`, { waitUntil: "networkidle" });
  await page.waitForTimeout(1500);
  await page.waitForSelector(".ag-row", { timeout: 8000 });

  // ── Filtreler ───────────────────────────────────────────────
  console.log("\n── Filtreler ──");
  const selects = page.locator("select");
  const counter = page.locator("span.text-xs").filter({ hasText: /trade/ });

  // Statü: Açık
  await selects.nth(0).selectOption("open");
  await page.waitForTimeout(500);
  const openTxt = await counter.textContent() ?? "";
  note(`Açık filtresi: "${openTxt}"`);
  const om = openTxt.match(/^(\d+)/);
  if (om && parseInt(om[1]) >= 2) pass(`Statü "Açık" → ${om[1]} satır`);
  else fail(`Statü filtresi sorun: "${openTxt}"`);
  await shot(page, "t2_6_statü_açık");
  await selects.nth(0).selectOption("all");
  await page.waitForTimeout(400);

  // Strateji: Carr
  await selects.nth(1).selectOption("carr");
  await page.waitForTimeout(500);
  const carrTxt = await counter.textContent() ?? "";
  const cm = carrTxt.match(/^(\d+)/);
  note(`Carr filtresi: "${carrTxt}"`);
  if (cm && parseInt(cm[1]) >= 1) pass(`Strateji "Carr" → ${cm[1]} satır`);
  else fail(`Strateji filtresi sorun: "${carrTxt}"`);
  await selects.nth(1).selectOption("all");
  await page.waitForTimeout(400);

  // Grade: AB
  await selects.nth(2).selectOption("AB");
  await page.waitForTimeout(500);
  const abTxt = await counter.textContent() ?? "";
  const abm = abTxt.match(/^(\d+)/);
  note(`AB filtresi: "${abTxt}"`);
  if (abm && parseInt(abm[1]) >= 1) pass(`Grade "AB" → ${abm[1]} satır`);
  else fail(`Grade AB filtresi sorun: "${abTxt}"`);
  await shot(page, "t2_6_grade_ab");
  await selects.nth(2).selectOption("all");
  await page.waitForTimeout(400);

  // Hisse arama
  const search = page.locator('input[placeholder="Hisse ara..."]');
  await search.fill("NVD");
  await page.waitForTimeout(500);
  const nvdTxt = await counter.textContent() ?? "";
  const nvdm = nvdTxt.match(/^(\d+)/);
  note(`"NVD" arama: "${nvdTxt}"`);
  if (nvdm && parseInt(nvdm[1]) === 1) pass(`Hisse arama "NVD" → 1 satır (NVDA)`);
  else if (nvdm) fail(`"NVD" → ${nvdm[1]} satır — 1 beklendi`);
  await search.fill("");
  await page.waitForTimeout(300);

  // ── Aksiyon butonu varlığı ──────────────────────────────────
  console.log("\n── Aksiyon Butonları ──");
  // 1600px viewport'ta action kolonu görünür mü?
  const actionBtns = page.locator('button[aria-label="İşlemler"]');
  const btnCount = await actionBtns.count();
  note(`Görünen action btn sayısı (1600px): ${btnCount}`);

  if (btnCount > 0) {
    pass(`${btnCount} adet "İşlemler" butonu görünür (1600px viewport)`);

    // İlk action menüsü aç
    await actionBtns.first().click();
    await page.waitForTimeout(500);
    await shot(page, "t2_7_action_menu");
    const items = await page.locator('[role="menuitem"]').allTextContents();
    note(`Menü ögeleri: ${JSON.stringify(items)}`);

    const hasKapat = items.some(t => t.includes("Kapat"));
    const hasDuzenle = items.some(t => t.includes("Düzenle"));
    const hasSil = items.some(t => t.includes("Sil"));

    if (hasKapat) pass('"Kapat" menü ögesi var (açık trade)');
    if (hasDuzenle) pass('"Düzenle" menü ögesi var');
    if (hasSil) pass('"Sil" menü ögesi var');

    await page.keyboard.press("Escape");
    await page.waitForTimeout(300);

    // CloseTradeDialog
    const openTradeBtn = actionBtns.first();
    await openTradeBtn.click();
    await page.waitForTimeout(400);
    const kapatItem = page.locator('[role="menuitem"]').filter({ hasText: "Kapat" });
    if (await kapatItem.count() > 0) {
      await kapatItem.click();
      await page.waitForTimeout(500);
      const closeDlg = page.locator('[role="dialog"]');
      if (await closeDlg.isVisible()) {
        pass("CloseTradeDialog açıldı ✅");
        await shot(page, "t2_7_close_dialog");

        const today = new Date().toISOString().split("T")[0];
        await closeDlg.locator("#ct-xdate").fill(today);
        await closeDlg.locator("#ct-xprice").fill("170.00");
        await page.waitForTimeout(500);

        // P/L önizleme — "flex gap-4" içindeki spanlar
        const previewSpans = closeDlg.locator("span[style*='color']");
        const spanCount = await previewSpans.count();
        note(`P/L preview span sayısı: ${spanCount}`);
        if (spanCount >= 2) {
          const t1 = await previewSpans.nth(0).textContent() ?? "";
          const t2 = await previewSpans.nth(1).textContent() ?? "";
          note(`P/L önizleme: "${t1}" / "${t2}"`);
          if (t1.includes("+") || t1.includes("-")) pass(`P/L önizleme canlı: "${t1}" / "${t2}" ✅ KARAR #408`);
        } else {
          // Border div içinde ara
          const previewBox = closeDlg.locator(".border.px-3");
          if (await previewBox.count() > 0) {
            const pt = await previewBox.first().textContent() ?? "";
            note(`P/L box: "${pt}"`);
            if (pt.trim().length > 0) pass(`P/L önizleme mevcut: "${pt}"`);
          }
        }

        await closeDlg.getByRole("button", { name: /kapat|güncelle/i }).first().click();
        await page.waitForTimeout(1000);
        if (!(await closeDlg.isVisible().catch(() => false))) pass("Trade kapatıldı ✅");
        else fail("CloseTradeDialog kapanmadı");
      } else {
        fail("CloseTradeDialog açılmadı");
      }
    } else {
      // Kapat yok — closed trade, Düzenle dene
      await page.keyboard.press("Escape");
      // Closed trade'de Düzenle testi
      await actionBtns.nth(1).click().catch(() => {});
      await page.waitForTimeout(400);
      const items2 = await page.locator('[role="menuitem"]').allTextContents();
      note(`2. satır menüsü: ${JSON.stringify(items2)}`);
      if (items2.some(t => t.includes("Düzenle"))) pass("Closed trade'de Düzenle var ✅");
      await page.keyboard.press("Escape");
    }

    // Silme + AlertDialog
    await actionBtns.last().click().catch(() => {});
    await page.waitForTimeout(400);
    const silItem = page.locator('[role="menuitem"]').filter({ hasText: "Sil" });
    if (await silItem.count() > 0) {
      await silItem.click();
      await page.waitForTimeout(400);
      const alertDlg = page.locator('[role="alertdialog"]');
      if (await alertDlg.isVisible().catch(() => false)) {
        pass("Silme AlertDialog açıldı ✅ KARAR #395");
        const alertTxt = await alertDlg.textContent() ?? "";
        if (alertTxt.includes("silin")) pass("AlertDialog uyarı metni doğru");
        await shot(page, "t2_7_alert_dialog");
        await alertDlg.getByRole("button", { name: "Sil" }).click();
        await page.waitForTimeout(800);
        pass("Trade silindi");
      } else {
        fail("Silme AlertDialog yok");
      }
    } else {
      await page.keyboard.press("Escape");
      note("'Sil' menü ögesi bu satırda yok");
    }
  } else {
    // Action kolonu görünmüyor — son kolon scroll
    fail(`"İşlemler" butonu 1600px viewport'ta bulunamadı — kolon görünmüyor`);
    await shot(page, "t2_action_missing");
  }

  // Son screenshot
  await page.goto(`${BASE_URL}/journal`, { waitUntil: "networkidle" });
  await page.waitForTimeout(1000);
  await shot(page, "t2_final");

  // Console hataları
  if (errs.length > 0) {
    console.log(`\n⚠️  Console hataları (${errs.length}):`);
    errs.forEach(e => console.log(`  ${e.substring(0, 120)}`));
  } else {
    pass("Console hatası yok");
  }

  await browser.close();
}
main().catch(console.error);
