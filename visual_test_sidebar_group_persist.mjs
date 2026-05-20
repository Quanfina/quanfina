// KARAR #486 Stratejiler grup localStorage persist testi
import { chromium } from "playwright";

const browser = await chromium.launch({ headless: true });
const ctx = await browser.newContext({ viewport: { width: 1366, height: 768 }, colorScheme: "dark" });
const page = await ctx.newPage();
console.log("=== Stratejiler grup localStorage testi ===\n");

// 1. Ilk yukleme - grup kapali default (localStorage bos + route /screens)
await page.goto("http://localhost:3000/screens", { waitUntil: "domcontentloaded", timeout: 15000 });
await page.waitForTimeout(2500);
const s1 = await page.evaluate(() => {
  const btn = Array.from(document.querySelectorAll("button"))
    .find((b) => b.textContent?.includes("Stratejiler"));
  return {
    expanded: btn?.getAttribute("aria-expanded"),
    storage: localStorage.getItem("sidebar-group-strategies"),
  };
});
console.log(`1. Ilk yukleme: expanded=${s1.expanded} storage="${s1.storage}"`);

// 2. Stratejiler'e tikla -> acilir + localStorage="1"
await page.click('button:has-text("Stratejiler")');
await page.waitForTimeout(500);
const s2 = await page.evaluate(() => {
  const btn = Array.from(document.querySelectorAll("button"))
    .find((b) => b.textContent?.includes("Stratejiler"));
  return {
    expanded: btn?.getAttribute("aria-expanded"),
    storage: localStorage.getItem("sidebar-group-strategies"),
    childCount: document.querySelectorAll('nav a[href="/minervini"], nav a[href="/carr"]').length,
  };
});
console.log(`2. Tikla -> ac:      expanded=${s2.expanded} storage="${s2.storage}" children=${s2.childCount}`);

// 3. Sayfa yenile -> hala acik kalmali (localStorage'dan okur)
await page.reload({ waitUntil: "domcontentloaded" });
await page.waitForTimeout(2500);
const s3 = await page.evaluate(() => {
  const btn = Array.from(document.querySelectorAll("button"))
    .find((b) => b.textContent?.includes("Stratejiler"));
  return {
    expanded: btn?.getAttribute("aria-expanded"),
    storage: localStorage.getItem("sidebar-group-strategies"),
    childCount: document.querySelectorAll('nav a[href="/minervini"], nav a[href="/carr"]').length,
  };
});
console.log(`3. Refresh sonrasi:  expanded=${s3.expanded} storage="${s3.storage}" children=${s3.childCount}`);

// 4. Tekrar tikla -> kapan + localStorage="0"
await page.click('button:has-text("Stratejiler")');
await page.waitForTimeout(500);
const s4 = await page.evaluate(() => {
  const btn = Array.from(document.querySelectorAll("button"))
    .find((b) => b.textContent?.includes("Stratejiler"));
  return {
    expanded: btn?.getAttribute("aria-expanded"),
    storage: localStorage.getItem("sidebar-group-strategies"),
  };
});
console.log(`4. Tekrar tikla:     expanded=${s4.expanded} storage="${s4.storage}"`);

// 5. Tekrar refresh -> hala kapali kalmali
await page.reload({ waitUntil: "domcontentloaded" });
await page.waitForTimeout(2500);
const s5 = await page.evaluate(() => {
  const btn = Array.from(document.querySelectorAll("button"))
    .find((b) => b.textContent?.includes("Stratejiler"));
  return {
    expanded: btn?.getAttribute("aria-expanded"),
    storage: localStorage.getItem("sidebar-group-strategies"),
  };
});
console.log(`5. Refresh tekrar:   expanded=${s5.expanded} storage="${s5.storage}"`);

// Sonuc
console.log("\n=== SONUC ===");
const tests = [
  { name: "Ilk yukleme kapali",       pass: s1.expanded === "false" },
  { name: "Tikla -> acilir",          pass: s2.expanded === "true" && s2.storage === "1" && s2.childCount === 2 },
  { name: "Refresh acik kalir",       pass: s3.expanded === "true" && s3.childCount === 2 },
  { name: "Tekrar tikla -> kapanir",  pass: s4.expanded === "false" && s4.storage === "0" },
  { name: "Refresh kapali kalir",     pass: s5.expanded === "false" },
];
let all = true;
for (const t of tests) {
  const flag = t.pass ? "🟢" : "🔴";
  console.log(`  ${flag} ${t.name}`);
  if (!t.pass) all = false;
}
console.log(`\n${all ? "🟢 KARAR #486 localStorage persist DOĞRULANDI" : "🔴 BAZI TESTLER BAŞARISIZ"}`);

await browser.close();
