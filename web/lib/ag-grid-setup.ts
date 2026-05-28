/**
 * AG Grid v35 modül kaydı (Paket 355 — bundle optimizasyonu).
 *
 * Önceden bu kayıt components/providers.tsx (global root provider) içindeydi →
 * AllCommunityModule (~500KB ag-grid-community) TÜM route'ların paylaşılan
 * client chunk'ına giriyordu. Dashboard / piyasa-durumu / sektör-rotasyonu /
 * istatistikler / pazar-hazırlığı / risk-yönetimi gibi grid'siz sayfalar bile
 * bu yükü taşıyordu.
 *
 * Çözüm: kayıt grid kullanan 5 sayfaya (journal/minervini/screens/signals/
 * watchlist) taşındı — `import "@/lib/ag-grid-setup"` ile. ES modül side-effect
 * olarak sayfa chunk'ı yüklenince (grid render'dan ÖNCE) bir kez çalışır.
 * ModuleRegistry idempotent — birden fazla sayfa import etse de tek kayıt.
 *
 * Sonuç: ag-grid artık sadece grid sayfalarının route chunk'ında; grid'siz
 * sayfaların First Load JS'i ~500KB azalır.
 */
import { AllCommunityModule, ModuleRegistry } from "ag-grid-community";

ModuleRegistry.registerModules([AllCommunityModule]);
