// Kural #21 Kanal C — DOM-only multi-page sağlık taraması (PNG yok, B+C disiplini)
// 9 sayfa için: HTTP, H1, key element count, console errors, network fails özet.
import { chromium } from "playwright";

const BASE_URL = "http://localhost:3000";

// Beklenenler = sayfada gerçekten görünen başlık/etiket (case-insensitive).
// Sn. Ferit UI değişirse buradaki regex güncellenir (yaşayan disiplin).
const PAGES = [
  { path: "/",              name: "Dashboard",       expect: ["Bugün Ne Var", "En İyi Sinyaller", "Aksiyon Listesi"] },
  { path: "/signals",       name: "Sinyaller",       expect: ["Sinyaller", "Manuel Sinyal", "GEÇ"] },
  { path: "/screens",       name: "Hisse Tarama",    expect: ["Hisse Tarama", "Watchlist'e Ekle", "Ready"] },
  { path: "/watchlist",     name: "Watchlist",       expect: ["Watchlist", "Strateji"] },
  { path: "/journal",       name: "Journal",         expect: ["Trade Journal", "Yeni Trade"] },
  { path: "/piyasa-durumu", name: "Piyasa Durumu",   expect: ["Piyasa Durumu", "Sektör"] },
  { path: "/minervini",     name: "Minervini",       expect: ["Minervini", "RS"] },
  { path: "/carr",          name: "Carr",            expect: ["Carr", "Pullback", "Coiled"] },
  { path: "/hisse/NVDA",    name: "Hisse NVDA",      expect: ["NVDA", "Aktif Stratejiler"] },
];

async function checkPage(browser, page) {
  const ctx = await browser.newContext({ viewport: { width: 1366, height: 768 }, colorScheme: "dark" });
  const tab = await ctx.newPage();
  const errors = [];
  const networkFails = [];
  tab.on("console", (m) => { if (m.type() === "error") errors.push(m.text().slice(0, 100)); });
  tab.on("requestfailed", (req) => networkFails.push(`${req.method()} ${req.url().slice(0, 80)}`));

  let httpStatus = 0;
  let title = "";
  let h1 = "(yok)";
  const counts = { canvas: 0, table: 0, button: 0, aggrid: 0 };
  const matches = {};
  try {
    const res = await tab.goto(BASE_URL + page.path, { waitUntil: "domcontentloaded", timeout: 10000 });
    httpStatus = res?.status() || 0;
    await tab.waitForTimeout(2500);
    title = await tab.title();
    h1 = await tab.locator("h1").first().textContent().catch(() => null) || "(yok)";
    counts.canvas = await tab.locator("canvas").count();
    counts.table = await tab.locator('table, [role="grid"]').count();
    counts.button = await tab.locator("button").count();
    counts.aggrid = await tab.locator('.ag-root-wrapper, .ag-theme-quartz, .ag-theme-quartz-dark').count();
    for (const term of page.expect) {
      matches[term] = await tab.locator(`text=/${term}/i`).count();
    }
  } catch (e) {
    errors.push(`NAV: ${e.message.slice(0, 100)}`);
  }
  await ctx.close();
  return { httpStatus, title, h1: h1?.slice(0, 40), counts, matches, errors, networkFails };
}

async function main() {
  console.log("=== Full Site DOM Sağlık Taraması (PNG yok) ===\n");
  const browser = await chromium.launch({ headless: true });
  const results = [];
  for (const page of PAGES) {
    const r = await checkPage(browser, page);
    results.push({ page, ...r });
    const expectSummary = Object.entries(r.matches).map(([k, v]) => `${k}:${v}`).join(" ");
    const flag = r.httpStatus === 200 && r.errors.length === 0 ? "🟢" : (r.httpStatus === 200 ? "🟡" : "🔴");
    console.log(`${flag} ${page.path.padEnd(16)} HTTP=${r.httpStatus} H1="${r.h1}" canvas=${r.counts.canvas} table=${r.counts.table} ag=${r.counts.aggrid} btn=${r.counts.button}`);
    console.log(`   beklenen: ${expectSummary}`);
    if (r.errors.length) console.log(`   ❌ errors: ${r.errors.slice(0, 2).join(" | ")}`);
    if (r.networkFails.length) console.log(`   ⚠️ netfail: ${r.networkFails.slice(0, 2).join(" | ")}`);
  }
  await browser.close();

  // Özet
  const ok = results.filter(r => r.httpStatus === 200 && r.errors.length === 0).length;
  const partial = results.filter(r => r.httpStatus === 200 && r.errors.length > 0).length;
  const fail = results.filter(r => r.httpStatus !== 200).length;
  console.log(`\n--- ÖZET: 🟢 ${ok}   🟡 ${partial}   🔴 ${fail}   / ${results.length}`);

  // Eksik beklenenler (her sayfa için 0 match olan)
  const eksikler = [];
  for (const r of results) {
    const sifir = Object.entries(r.matches).filter(([_, v]) => v === 0).map(([k]) => k);
    if (sifir.length) eksikler.push(`${r.page.path}: ${sifir.join(", ")}`);
  }
  if (eksikler.length) {
    console.log("\n--- BEKLENEN BULUNMADI (UX eksiği adayı):");
    eksikler.forEach(e => console.log(`  ${e}`));
  }
}

main();
