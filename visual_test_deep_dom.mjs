// Derin DOM tarama - cell icindeki HER element'in font'u
import { chromium } from "playwright";

const browser = await chromium.launch({ headless: true });
const ctx = await browser.newContext({ viewport: { width: 1366, height: 768 }, colorScheme: "dark" });
const page = await ctx.newPage();
await page.goto("http://localhost:3000/screens", { waitUntil: "domcontentloaded", timeout: 15000 });
await page.waitForSelector(".ag-cell", { timeout: 10000 });
await page.waitForTimeout(3000);

const dump = await page.evaluate(() => {
  function describe(el) {
    const cs = window.getComputedStyle(el);
    return {
      tag: el.tagName,
      classes: el.className.toString().slice(0, 50),
      font: cs.fontFamily.slice(0, 50),
      size: cs.fontSize,
      weight: cs.fontWeight,
      text: el.children.length === 0 ? el.textContent?.trim().slice(0, 15) : `[${el.children.length} child]`,
    };
  }

  // 3 farkli cell tipi: symbol, price (valueFormatter), scan_date (direct)
  const targets = [
    document.querySelector('.ag-cell[col-id="symbol"]'),
    document.querySelector('.ag-cell[col-id="price"]'),
    document.querySelector('.ag-cell[col-id="scan_date"]'),
    document.querySelector('.ag-header-cell[col-id="symbol"]'),
  ];

  const out = [];
  for (const target of targets) {
    if (!target) continue;
    const colId = target.getAttribute("col-id") || "?";
    const isHeader = target.classList.contains("ag-header-cell");
    out.push({ colId: `${colId}${isHeader ? " (header)" : ""}`, info: describe(target) });
    // tum descendents'i listele
    const walker = document.createTreeWalker(target, NodeFilter.SHOW_ELEMENT, null);
    while (walker.nextNode()) {
      const el = walker.currentNode;
      out.push({ colId: "  └", info: describe(el) });
    }
  }
  return out;
});

console.log("Derin DOM dump:");
for (const item of dump) {
  console.log(`  [${item.colId}] <${item.info.tag}> font="${item.info.font}" size=${item.info.size} w=${item.info.weight} class="${item.info.classes}" text="${item.info.text}"`);
}

await browser.close();
