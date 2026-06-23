import { Sidebar } from "@/components/layout/sidebar";
import { DbStatusBanner } from "@/components/layout/DbStatusBanner";
import { DataFreshnessBanner } from "@/components/layout/DataFreshnessBanner";
import { QuickSummaryBar } from "@/components/layout/QuickSummaryBar";

// P587 (23 Haz 2026) #418 deneyi: Dashboard segment'i force-dynamic. Tüm sayfalar
// build-time STATIK prerender (○) yerine per-request render (ƒ). Gözlenen fresh-browser
// #418 prod-build-static-prerender'a özgüydü (TZ/frozen-date elendi). Bu segment zaten
// auth-gated + tamamen client-query-driven (SSR sadece skeleton) → static prerender sıfır
// değer; force-dynamic doğru tercih. #418 elenirse kalır, elenmezse geri alınır.
export const dynamic = "force-dynamic";

// KARAR ADAY (22 May 2026): DbStatusBanner — Cloud SQL erişilemez ise sarı
// uyarı banner'ı gösterir. Sn. Ferit talimat "Mockdan gerçek veriye geçmeyi
// ayarla" → Seçenek B (MOCK koru + frontend banner). useDbStatus hook
// /api/health'i 30 sn poll ile dinler.

export default function DashboardLayout({
  children,
}: {
  children: React.ReactNode;
}) {
  return (
    <div className="flex min-h-screen">
      <Sidebar />
      <main className="flex-1 min-w-0 pt-14 md:pt-0 overflow-auto flex flex-col">
        <DbStatusBanner />
        <DataFreshnessBanner />
        {/* P404: Tüm sayfalarda sticky kompakt özet — Trading Mode + Market Health
            + Açık Risk + Realized P&L. Markets360 üst Portfolio sayılar pattern uyarlaması. */}
        <QuickSummaryBar />
        <div className="flex-1 min-h-0">{children}</div>
      </main>
    </div>
  );
}
