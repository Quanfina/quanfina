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

## İlişkili

- Kural #24 (Sağlam Gidelim) — 6 Aşama Aşama 5 PYTEST
- Vizyon KALICI İLKE #11 (Objektif Ayna Dili) — test sonucu sayı + renk
- KARAR #197 + #214 (Backend pytest)
- Manifesto Özellik #8 (Öğrenen) — sistem kendi disiplinini test eder
