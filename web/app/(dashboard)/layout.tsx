import { Sidebar } from "@/components/layout/sidebar";
import { DbStatusBanner } from "@/components/layout/DbStatusBanner";
import { DataFreshnessBanner } from "@/components/layout/DataFreshnessBanner";

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
        <div className="flex-1 min-h-0">{children}</div>
      </main>
    </div>
  );
}
