# Frontend Test Senaryoları (Vitest hazırlık)

> **Durum:** Vitest henüz kurulmadı. Bu belge Sn. Ferit paper trading başlattığında
> ya da Aşama 4 deploy öncesi Vitest kurulumu yapılırken **şablon kaynak**.
>
> **Paket 230 (27 May 2026):** P197 (useTradingMode) + P171 (usePositionAlerts) pattern'ini
> tüm yeni hook + komponentlere yaymak için belge — kanon test set tek yerden okunur.

## Kapsam — Test edilmesi gereken birimler

### Hook'lar

#### `useTradingMode()` (P193 + P227 AKSİYON kilidi)
- ✅ **Defansif tetik:** market_health <30 → mode='defansif', recommendedSizingPct=0
- ✅ **Defansif tetik:** spy_stage ∈ {3,4} → mode='defansif'
- ✅ **Rehab tetik:** consecutiveLosses ≥ 3 → mode='rehab', recommendedSizingPct=0.5
- ✅ **Agresif tetik:** market_health >70 + consecutiveWins ≥ 5 → mode='agresif'
- ✅ **Normal default:** hiçbir koşul yoksa → mode='normal', recommendedSizingPct=1.0
- ✅ **Streak hesap:** kapalı trade'ler exit_date desc, en yeni'den geriye sayım, kayıp/kazanç kırılma noktasında dur
- ✅ **Boş trade listesi:** consecutiveWins=0, consecutiveLosses=0, totalClosedTrades=0

#### `usePositionAlerts()` (P160 + P161)
- ✅ **STOP_HIT:** current_price < plan_stop (long), toast.error
- ✅ **STOP_NEAR:** current_price < plan_stop * 1.02 (long), toast.warning
- ✅ **MINERVINI_7PCT:** loss_pct >= 7% from entry, toast.error (Mark TTLC s.131 mutlak limit)
- ✅ **TARGET_NEAR:** current_price >= plan_target * 0.98, toast.success
- ✅ **Dismiss localStorage:** dismissedKey persist (24h TTL)
- ✅ **History FIFO 50:** eski kayıt çıkarılır, yeni eklenir

### Komponentler

#### `<ModBadge variant="compact" />` (P193)
- ✅ **Render:** mode='normal' → mavi rozet "NORMAL"
- ✅ **Render:** mode='defansif' → kırmızı rozet "DEFANSİF"
- ✅ **Tooltip:** reason field gösterilmeli
- ✅ **9 sayfa entegrasyon:** Dashboard, Risk, Sinyaller, Watchlist, Journal, Hisse, Piyasa, AddTradeDialog, CloseTradeDialog

#### `<ModBadge variant="full" />` (P228 Pazar Hazırlığı)
- ✅ **Render:** emoji + büyük başlık + reason + uiBehavior + sizing tablosu

#### `<PreTradeChecklist />` (P229)
- ✅ **7 koşul render:** Stage, RS, Setup, Plan trigger, Stop, Target, Mode
- ✅ **Stage 2 OK:** stage=2 → status='ok'
- ✅ **Stage 4 FAIL:** stage=4 → status='fail' "UZAK DUR"
- ✅ **RS ≥ 70 OK:** rsRating=85 → status='ok'
- ✅ **RS < 50 FAIL:** rsRating=30 → status='fail' "Laggard"
- ✅ **Stop %7 limit:** entry=100, stop=92 → fail (8% > 7% mutlak)
- ✅ **Stop %7 limit:** entry=100, stop=94 → ok (6% < 7%)
- ✅ **R/R hesap:** entry=100, stop=95, target=115 → R=5, Reward=15, R/R=3 → ok
- ✅ **R/R < 1:** entry=100, stop=95, target=102 → R/R=0.4 → fail
- ✅ **Mode defansif:** tradingMode.mode='defansif' → fail
- ✅ **Mode rehab:** tradingMode.mode='rehab' → warn
- ✅ **Toplam:** okCount + warnCount + failCount = rows.length

#### `<AddTradeDialog />` Defansif AKSİYON kilidi (P227)
- ✅ **Defansif modda submit BLOK:** tradingMode.mode='defansif' + defansifOverride=false → submit disabled
- ✅ **Override checkbox aktive:** defansifOverride=true → submit aktif
- ✅ **isClosed istisna:** trade kapalı kayıt geriye dönük → override gerekmez
- ✅ **Reset:** dialog kapanışında defansifOverride sıfırlanır

#### `<PortfolioSummaryCard />` sektör konsantrasyon (P190 + P202)
- ✅ **Sektör max ≤25%:** OK (yeşil)
- ✅ **Sektör >25% <30%:** Sarı uyarı (Mark TTLC s.85 yaklaşım)
- ✅ **Sektör >30%:** Kırmızı (Mark TTLC s.85 limit aşıldı)
- ✅ **Map agregasyon:** aynı sektörden çoklu trade toplam %

### Sayfalar

#### `/pazar-hazirligi` (P228)
- ✅ **5 bölüm render:** Geçen Hafta + Açık Pozisyon + Piyasa + Mod + Plan
- ✅ **Boş trade:** "Bu hafta kapalı trade yok" mesajı
- ✅ **Boş watchlist:** focusCount=0 → "Yeni hafta için focus seçimi yap"
- ✅ **focusCount > 5:** "Mark disiplini: max 5 sembol. Daralt." uyarı
- ✅ **Market loading:** ısLoading=true → "Yükleniyor..."

## Kurulum (gelecek)

```bash
cd web
pnpm add -D vitest @vitest/ui @testing-library/react @testing-library/jest-dom jsdom
```

`vitest.config.ts`:
```ts
import { defineConfig } from 'vitest/config';
import react from '@vitejs/plugin-react';

export default defineConfig({
  plugins: [react()],
  test: { environment: 'jsdom', globals: true, setupFiles: ['./vitest.setup.ts'] },
});
```

`vitest.setup.ts`:
```ts
import '@testing-library/jest-dom/vitest';
```

## P227-P253 ek senaryolar (27 May 2026)

### AddTradeDialog Defansif AKSİYON kilidi (P227)
- ✅ Defansif modda submit BLOK (override checkbox yok → disabled)
- ✅ Override checkbox işaretle → submit aktif
- ✅ isClosed=true → blok yok (geçmiş kayıt için override gerekmez)
- ✅ Reset: dialog kapanışında defansifOverride=false sıfırlanır

### PreTradeChecklist (P229)
- ✅ Stage 2 → ok, Stage 4 → fail "UZAK DUR", Stage null → warn
- ✅ RS ≥ 70 → ok "Leader", 50-69 → warn "Average", <50 → fail "Laggard"
- ✅ Setup: vcpPass=true → ok "VCP onaylı"; pivotPass=true → ok "Pivot breakout"
- ✅ planEntryTrigger boş → fail "TTLC Sec 1.6 ZORUNLU"
- ✅ Stop %7+ → fail "TTLC s.131 limit AŞILDI"
- ✅ Stop entry üstünde → fail "long için imkansız"
- ✅ R/R ≥ 2 → ok; 1-2 → warn "zayıf"; <1 → fail "kabul edilemez"
- ✅ Mod defansif → fail; rehab → warn; agresif → ok; normal → ok

### Sinyaller AL Defansif disabled (P235)
- ✅ alBlocked=true → button disabled + gri background
- ✅ tooltip "Defansif mod aktif — yeni AL BLOK"
- ✅ Normal/Rehab/Agresif → button aktif
- ✅ columnDefs deps [alBlocked] → mode değişince re-render

### Tarama Defansif uyarı banner (P253)
- ✅ mode=defansif → header altında kırmızı banner görünür
- ✅ 🛡️ emoji + "Mark TTLC s.187: yeni AL BLOK"
- ✅ Normal/Rehab/Agresif → banner görünmez

### Mark Otomatik Plan buton (P250)
- ✅ entry_price boş → buton görünmez
- ✅ entry_price=100 + tık → planStop=94 (%6), planTarget=112 (2R)
- ✅ Tooltip: "stop = entry × 0.94, target = entry + 2R"
- ✅ ATR-based default (P163) korunuyor — Mark buton manuel override

### R/R Live Preview (P251)
- ✅ entry+stop+target dolu → rozet görünür
- ✅ R/R ≥ 2 → yeşil "Mark uyumlu"
- ✅ R/R 1-2 → sarı "Zayıf"
- ✅ R/R <1 → kırmızı "Kabul edilemez"
- ✅ Stop ≤ 7% → yeşil ✓
- ✅ Stop > 7% → kırmızı "TTLC s.131 LİMİT AŞILDI"
- ✅ Risk $ + Reward $ side-by-side gösterilir

### Watchlist Defansif Leaders First (P252)
- ✅ getRowStyle: mode=defansif + rs<70 → opacity 0.45
- ✅ mode=defansif + rs≥70 → normal opacity
- ✅ Normal/Rehab/Agresif → tüm satırlar normal
- ✅ AG Grid re-render: useTradingMode değişince getRowStyle yenilenir

## P259-P262 DRY helper senaryolar (27 May 2026)

### useTradingMode helper'ları (P233+P234+P259)
- ✅ `isNewAlBlocked('defansif')` → true; diğer 3 mod → false
- ✅ `getDefansifBlockMessage('defansif')` → "DEFANSİF mod aktif..." mesaj
- ✅ `getDefansifBlockMessage('normal'|'rehab'|'agresif')` → null
- ✅ `getModUiTheme('defansif')` → ModUiTheme nesnesi (kırmızı tema, 🛡️ emoji)
- ✅ `getModUiTheme('rehab')` → sarı tema (#F59E0B, 🩹 emoji)
- ✅ `getModUiTheme('agresif')` → yeşil tema (--mtp-excellent, 🚀 emoji)
- ✅ `getModUiTheme('normal')` → null (banner gizlenir)

### Dashboard + Tarama + Sinyaller Defansif banner (P260+P261+P262)
- ✅ mode=defansif → kırmızı banner görünür (3 sayfa: Dashboard, Tarama, Sinyaller)
- ✅ mode=normal → banner gizlidir (3 sayfada da)
- ✅ Helper çağrısı yenileme: mode değişince useState/useEffect react renders
- ✅ Banner içeriği: theme.shortMessage + sayfa-özel ek metin

### PreTradeChecklist compact prop (P257)
- ✅ `compact=true` → 4 koşul (Stage + RS + Setup + Mod)
- ✅ `compact=false` (default) → 8 koşul (P265 ile genişledi: yukarıdaki 4 + Plan trigger + Stop + R/R + Pozisyon ≤25%)
- ✅ Plan alanları compact'ta gizli — "Stop $ tanımlı değil" fail yok

### PreTradeChecklist 8. koşul planSizePct (P265+P266)
- ✅ `planSizePct=null` → warn "Pozisyon % tanımlı değil"
- ✅ `planSizePct=20` → ok yeşil "20% (Mark TTLC s.85 sektör limiti içinde)"
- ✅ `planSizePct=25` → ok (sınırda)
- ✅ `planSizePct=28` → warn sarı "28% — Mark canon 25-30% sınır bölgesi"
- ✅ `planSizePct=35` → fail kırmızı "35% — Mark TTLC s.85 LİMİT AŞILDI"
- ✅ compact=true → 8. koşul gizli (sembol seviyesi sayfa)
- ✅ useMemo deps array `planSizePct` dahil — form input değişiminde rerender

## İlişkili

- Kural #24 (Sağlam Gidelim) — 6 Aşama Aşama 5 PYTEST
- Vizyon KALICI İLKE #11 (Objektif Ayna Dili) — test sonucu sayı + renk
- KARAR #197 + #214 (Backend pytest)
- Manifesto Özellik #8 (Öğrenen) — sistem kendi disiplinini test eder
- Bilgi Mimarisi İlke #4 (Tekrarsızlık/DRY) — getModUiTheme tek kaynak
