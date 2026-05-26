# useTradingMode Test Senaryoları (Manuel Doğrulama)

> **Paket 197 (26 May 2026):** Frontend Jest/Vitest yok. Trading mode hook'u
> pure deterministic — input → output deterministic. Bu belge manual
> verification senaryolarını listeler. Gelecek: Vitest kurulumu sonrası
> bu senaryolar direkt test'e çevrilir.

## Tetik Tablosu

| # | Senaryo | Beklenen Mod | Recommended Sizing |
|---|---|---|---|
| 1 | Hiç açık trade yok, MH 50 | Normal | %1 R |
| 2 | 2 ardışık kayıp, MH 50 | Normal | %1 R (henüz 3 olmadı) |
| 3 | **3 ardışık kayıp**, MH 50 | **Rehab** | **%0.5 R** |
| 4 | 5 ardışık kayıp, MH 75 | Rehab | %0.5 R (3+ tetikler) |
| 5 | **MH 25** (< 30) | **Defansif** | **%0 R (BLOK)** |
| 6 | MH 25 + 5 ardışık kazanç | Defansif | %0 R (piyasa öncelikli) |
| 7 | MH 75 (> 70) + 3 ardışık kazanç | Normal | %1 R (5+ değil) |
| 8 | **MH 75 + 5 ardışık kazanç** | **Agresif** | **%1.5 R** |
| 9 | MH 75 + 10 ardışık kazanç | Agresif | %1.5 R |
| 10 | Karışık seri (W-L-W-W) | Normal | %1 R (streak kırık) |

## Streak Hesap Mantığı

```ts
// En yeni'den geriye, ilk farklı yönde dur
trades: [Loss, Loss, Loss, Win, Win, Win, Loss]
        ↑ en yeni
// Sonuç: consecutiveLosses = 3 (ilk 3 ardışık L)
// consecutiveWins = 0 (Win serisine ulaşmadan kıırldı)
```

## Tetik Sırası (Önemli — Disiplin)

1. **Defansif** önce (piyasa zayıf, her şeyden öncelikli)
2. **Rehab** ikinci (kişisel zarar disiplini)
3. **Agresif** üçüncü (piyasa güçlü + streak)
4. **Normal** default

## Manuel Test Adımları

1. Dashboard aç → ModBadge görünür
2. Trade kayıt aç → eğer mod != Normal → uyarı banner üstte
3. Journal'da pl_dollar'lı kapalı trade'leri filtrele → streak hesap

## Backend Doğrulama (P196 UI tetik kontrolü)

Mevcut /api/trades 8 trade MOCK fallback:
- NVDA closed +$1330 (Win)
- AAPL closed -$744 (Loss)
- AMD closed +$1169 (Win)
- META closed +$747 (Win)

En yeni → en eski sort (exit_date desc):
- META 2026-04-30 +$747 → Win 1
- AAPL 2026-04-25 -$744 → Loss kırıldı, consecutiveWins=1
- AMD 2026-04-18 +$1169 → (zaten Loss kıırldı, dur)
- NVDA 2026-05-15 +$1330 (exit_date sort yanlış olabilir)

**Sonuç:** Şu an Normal mod, piyasa MH 75, 2 ardışık kazanç → "Normal" tetik PASS (5+ değil, 3 değil).

## Gelecek: Vitest Kurulumu

```bash
cd web
pnpm add -D vitest @testing-library/react @testing-library/jest-dom jsdom
```

Bu yapı eklendiğinde bu senaryolar `trading-mode.test.tsx` dosyasına geçer.
