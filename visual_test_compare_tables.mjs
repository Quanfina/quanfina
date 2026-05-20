// Sinyaller vs Hisse Tarama tablo stil farklarini DOM seviyesinde karsilastir
import { chromium } from "playwright";

const browser = await chromium.launch({ headless: true });
const ctx = await browser.newContext({ viewport: { width: 1366, height: 768 }, colorScheme: "dark" });

async function snapshot(path, label) {
  const tab = await ctx.newPage();
  await tab.goto(`http://localhost:3000${path}`, { waitUntil: "domcontentloaded", timeout: 15000 });
  await tab.waitForSelector(".ag-cell", { timeout: 10000 }).catch(() => {});
  await tab.waitForTimeout(2500);
  const info = await tab.evaluate(() => {
    const headers = document.querySelectorAll(".ag-header-cell");
    const cells = document.querySelectorAll(".ag-cell");
    const headerSample = [];
    for (let i = 0; i < Math.min(6, headers.length); i++) {
      const cs = window.getComputedStyle(headers[i]);
      headerSample.push({
        colId: headers[i].getAttribute("col-id"),
        bg: cs.backgroundColor,
        font: cs.fontFamily.slice(0, 25),
        size: cs.fontSize,
        weight: cs.fontWeight,
        align: cs.textAlign,
        h: cs.height,
      });
    }
    const cellSample = [];
    // Tum kolon tiplerini gormek icin daha cesitli ornek al
    const seen = new Set();
    for (const cell of cells) {
      const colId = cell.getAttribute("col-id");
      if (seen.has(colId)) continue;
      seen.add(colId);
      const cs = window.getComputedStyle(cell);
      // Cell ICINDEKI ilk eleman (renderer cikti)
      const innerEl = cell.querySelector("*") || cell;
      const innerCs = window.getComputedStyle(innerEl);
      cellSample.push({
        colId,
        font: cs.fontFamily.slice(0, 25),
        innerFont: innerCs.fontFamily.slice(0, 25),
        size: cs.fontSize,
        innerSize: innerCs.fontSize,
        weight: cs.fontWeight,
        text: cell.textContent?.trim().slice(0, 18),
      });
      if (cellSample.length >= 8) break;
    }
    // Row arka plan
    const row = document.querySelector(".ag-row");
    const rowCs = row ? window.getComputedStyle(row) : null;
    return {
      headerSample,
      cellSample,
      rowBg: rowCs?.backgroundColor,
      rowH: rowCs?.height,
    };
  });
  console.log(`\n=== ${label} (${path}) ===`);
  console.log(`Row bg=${info.rowBg} h=${info.rowH}`);
  console.log("Headers (ilk 6):");
  info.headerSample.forEach(h => console.log(`  [${h.colId}] bg=${h.bg} font=${h.font} size=${h.size} w=${h.weight} align=${h.align}`));
  console.log("Cells (her col 1 ornek):");
  info.cellSample.forEach(c => {
    const innerDif = c.innerFont !== c.font ? ` INNER=${c.innerFont}/${c.innerSize}` : "";
    console.log(`  [${c.colId}] font=${c.font} size=${c.size} w=${c.weight}${innerDif} text="${c.text}"`);
  });
  await tab.close();
}

await snapshot("/signals", "SINYALLER");
await snapshot("/screens", "HISSE TARAMA");

await browser.close();
