"use client";

import { useEffect, useState } from "react";
import { Bell, AlertCircle, AlertTriangle, Info, X } from "lucide-react";
import { getAlertHistory, type AlertHistoryEntry } from "@/hooks/use-position-alerts";

/**
 * Paket 168 (26 May 2026): Dashboard alert history timeline kart.
 *
 * Son 24h içinde tetiklenen position alert'lerini gösterir.
 * Sn. Ferit sabah Dashboard açtığında "dün ne tetiklendi" özetini görür.
 *
 * Veri kaynağı: localStorage `position-alerts-history` (FIFO max 50, 24h TTL).
 * SSR uyumu: useEffect ile client-side hydration.
 *
 * Render kuralları:
 * - 0 entry → boş durum mesajı ("Son 24 saatte uyarı tetiklenmedi")
 * - 1-50 entry → ters kronolojik liste (en yeni üstte)
 * - Severity ikonu + relative time ("5 dk önce", "2 sa önce")
 */

function formatRelative(ts: number): string {
  const diffMs = Date.now() - ts;
  const min = Math.floor(diffMs / 60000);
  if (min < 1) return "şimdi";
  if (min < 60) return `${min} dk önce`;
  const hr = Math.floor(min / 60);
  if (hr < 24) return `${hr} sa önce`;
  return `${Math.floor(hr / 24)} gün önce`;
}

function SeverityIcon({ severity }: { severity: AlertHistoryEntry["severity"] }) {
  if (severity === "critical")
    return <AlertCircle size={14} style={{ color: "var(--mtp-danger)" }} />;
  if (severity === "warning")
    return <AlertTriangle size={14} style={{ color: "#F59E0B" }} />;
  return <Info size={14} style={{ color: "var(--mtp-good, #4B9CD3)" }} />;
}

export function AlertHistoryCard() {
  const [entries, setEntries] = useState<AlertHistoryEntry[] | null>(null);

  // SSR hydration guard
  useEffect(() => {
    setEntries(getAlertHistory());
    // 30s'de bir refresh — yeni alert tetiklenirse görünsün
    const interval = setInterval(() => {
      setEntries(getAlertHistory());
    }, 30000);
    return () => clearInterval(interval);
  }, []);

  function handleClear() {
    if (typeof window === "undefined") return;
    localStorage.removeItem("position-alerts-history");
    setEntries([]);
  }

  if (entries === null) {
    // Hydration öncesi (skeleton yerine null — flash önleme)
    return null;
  }

  return (
    <div className="rounded-lg border bg-card p-4 flex flex-col gap-3">
      <div className="flex items-center justify-between">
        <div className="flex items-center gap-2">
          <Bell size={14} className="text-muted-foreground" />
          <h3 className="text-xs font-semibold tracking-wider uppercase text-muted-foreground">
            Uyarı Geçmişi (24h)
          </h3>
        </div>
        {entries.length > 0 && (
          <button
            type="button"
            onClick={handleClear}
            className="text-[10px] text-muted-foreground hover:text-foreground flex items-center gap-1 transition-colors"
            title="Tüm geçmişi temizle"
          >
            <X size={10} />
            Temizle
          </button>
        )}
      </div>

      {entries.length === 0 ? (
        <div className="py-4 text-sm text-muted-foreground text-center">
          Son 24 saatte uyarı tetiklenmedi.
          <div className="text-xs mt-1 opacity-70">
            Stop hit / %7 kuralı / hedef yakın bildirimleri burada görünür.
          </div>
        </div>
      ) : (
        <ul className="flex flex-col gap-2 max-h-64 overflow-y-auto">
          {entries.map((e) => (
            <li
              key={e.id}
              className="flex items-start gap-2 text-xs py-1.5 px-2 rounded border"
              style={{ borderColor: "var(--border)" }}
            >
              <SeverityIcon severity={e.severity} />
              <div className="flex-1 min-w-0">
                <div className="font-semibold truncate">{e.title}</div>
                <div className="text-muted-foreground text-[11px] mt-0.5 line-clamp-2">
                  {e.message}
                </div>
                <div className="text-[10px] text-muted-foreground mt-1 opacity-70">
                  {formatRelative(e.timestamp)} · {e.symbol}
                </div>
              </div>
            </li>
          ))}
        </ul>
      )}
    </div>
  );
}
