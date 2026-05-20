// Sidebar nav sirasi dogrulama (KARAR #485)
import { chromium } from "playwright";
const browser = await chromium.launch({ headless: true });
const ctx = await browser.newContext({ viewport: { width: 1366, height: 768 }, colorScheme: "dark" });
const page = await ctx.newPage();
await page.goto("http://localhost:3000", { waitUntil: "domcontentloaded", timeout: 15000 });
await page.waitForTimeout(2500);
const items = await page.evaluate(() => {
  const out = [];
  const navChildren = document.querySelectorAll("nav > div, nav > a, nav > button");
  let idx = 0;
  for (const el of navChildren) {
    if (el.tagName === "A") {
      idx++;
      out.push({ idx, kind: "leaf", label: el.textContent?.trim().slice(0, 25), href: el.getAttribute("href") });
    } else if (el.tagName === "DIV") {
      // Group container: button (header) + nested div (children if open)
      const btn = el.querySelector(":scope > button");
      const childWrap = el.querySelector(":scope > div");
      if (btn) {
        idx++;
        const expanded = btn.getAttribute("aria-expanded") === "true";
        out.push({ idx, kind: "group", label: btn.textContent?.trim().slice(0, 25), expanded });
        if (childWrap) {
          const children = childWrap.querySelectorAll("a");
          for (const c of children) {
            idx++;
            out.push({ idx, kind: "child", label: c.textContent?.trim().slice(0, 25), href: c.getAttribute("href") });
          }
        }
      }
    }
  }
  return out;
});
console.log("Sidebar yapisi:");
items.forEach(l => {
  const prefix = l.kind === "group" ? `▸ [${l.expanded ? "OPEN" : "CLOSED"}]` : l.kind === "child" ? "  └" : " ";
  console.log(`  ${l.idx}. ${prefix} ${l.label}${l.href ? " -> " + l.href : ""}`);
});
const piyasa = items.find(l => l.href === "/piyasa-durumu");
const strateji = items.find(l => l.kind === "group" && l.label?.includes("Strateji"));
const flag1 = piyasa?.idx === 2 ? "🟢" : "🔴";
const flag2 = strateji ? "🟢" : "🔴";
console.log(`\n${flag1} Piyasa Durumu sirasi: ${piyasa?.idx ?? "(yok)"} (beklenen 2)`);
console.log(`${flag2} Stratejiler grubu: ${strateji ? "VAR (" + (strateji.expanded ? "acik" : "kapali") + ")" : "YOK"}`);
await browser.close();
