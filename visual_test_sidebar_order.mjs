// Sidebar nav sirasi dogrulama (KARAR #485)
import { chromium } from "playwright";
const browser = await chromium.launch({ headless: true });
const ctx = await browser.newContext({ viewport: { width: 1366, height: 768 }, colorScheme: "dark" });
const page = await ctx.newPage();
await page.goto("http://localhost:3000", { waitUntil: "domcontentloaded", timeout: 15000 });
await page.waitForTimeout(2500);
const links = await page.evaluate(() => {
  const navs = document.querySelectorAll("nav a");
  return Array.from(navs).map((a, i) => ({ idx: i + 1, label: a.textContent?.trim().slice(0, 25), href: a.getAttribute("href") }));
});
console.log("Sidebar sirasi:");
links.forEach(l => console.log(`  ${l.idx}. ${l.label} -> ${l.href}`));
const piyasa = links.find(l => l.href === "/piyasa-durumu");
const flag = piyasa?.idx === 2 ? "🟢" : "🔴";
console.log(`\n${flag} Piyasa Durumu sirasi: ${piyasa?.idx ?? "(yok)"} (beklenen 2)`);
await browser.close();
