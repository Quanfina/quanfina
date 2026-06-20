"use client";

import { useMemo } from "react";
import type { SectorRotationEntry } from "@/hooks/use-sector-rotation";

/**
 * Paket 554 (20 Haz 2026): Sektör Rotasyon Kadranı — görsel rotasyon yorumlama grafiği.
 *
 * Profesyonel sektör-rotasyon görselinin (Görece Güç × Momentum, 4 kadran, saat yönünde
 * dönüş) gerçek-veri uyarlaması. Tescilli isim KULLANILMAZ (clean-room — Kural #10/#3);
 * Quanfina adı "Sektör Rotasyon Kadranı". Mevcut tabloyu BOZMAZ — altına eklenir.
 *
 * METODOLOJİ (Kural #26 — uydurma yok, gerçek perf'ten türetilir):
 *   X (Görece Güç)  = perf_3m − ortalama(perf_3m)            [orta-vade görece güç trendi]
 *   Y (Momentum)    = (perf_1m − ort perf_1m) − (perf_3m − ort perf_3m)/3
 *                     [son ayın görece temposu − 3 aylık ortalama aylık tempo = ivme]
 *   Sektör ortalaması benchmark yerine geçer (cross-sector). Eksenler 0'da kesişir.
 *   4 kadran: Lider (sağ-üst) / Zayıflıyor (sağ-alt) / Geride (sol-alt) / İyileşiyor (sol-üst).
 *   Saat yönü: Lider → Zayıflıyor → Geride → İyileşiyor → Lider.
 *
 * Veri yoksa/yetersizse grafik gizlenir (MOCK YOK — Kural #28).
 */

type QuadKey = "LEADING" | "WEAKENING" | "LAGGING" | "IMPROVING";

const QUAD: Record<QuadKey, { label: string; color: string; bg: string }> = {
  LEADING: { label: "Lider", color: "var(--mtp-excellent)", bg: "rgba(40,167,69,0.07)" },
  WEAKENING: { label: "Zayıflıyor", color: "#F59E0B", bg: "rgba(245,158,11,0.07)" },
  LAGGING: { label: "Geride", color: "var(--mtp-danger)", bg: "rgba(220,53,69,0.07)" },
  IMPROVING: { label: "İyileşiyor", color: "var(--mtp-good, #4B9CD3)", bg: "rgba(75,156,211,0.07)" },
};

function quadOf(x: number, y: number): QuadKey {
  if (x >= 0 && y >= 0) return "LEADING";
  if (x >= 0 && y < 0) return "WEAKENING";
  if (x < 0 && y < 0) return "LAGGING";
  return "IMPROVING";
}

interface Point {
  ticker: string;
  sector: string;
  x: number;
  y: number;
  rank: number | null;
  quad: QuadKey;
}

export function SectorRotationQuadrant({ data }: { data: SectorRotationEntry[] }) {
  const points = useMemo<Point[]>(() => {
    const valid = data.filter((s) => s.perf_1m != null && s.perf_3m != null);
    if (valid.length < 2) return [];
    const mean1m = valid.reduce((a, s) => a + (s.perf_1m as number), 0) / valid.length;
    const mean3m = valid.reduce((a, s) => a + (s.perf_3m as number), 0) / valid.length;
    return valid.map((s) => {
      const relMed = (s.perf_3m as number) - mean3m; // X
      const relShort = (s.perf_1m as number) - mean1m;
      const momentum = relShort - relMed / 3; // Y
      return {
        ticker: s.ticker,
        sector: s.sector_name,
        x: relMed,
        y: momentum,
        rank: s.rs_rank,
        quad: quadOf(relMed, momentum),
      };
    });
  }, [data]);

  if (points.length < 2) return null;

  // SVG geometri — simetrik domain (0 merkezde dursun)
  const W = 640;
  const H = 460;
  const M = { top: 28, right: 24, bottom: 44, left: 48 };
  const plotW = W - M.left - M.right;
  const plotH = H - M.top - M.bottom;
  const xMax = Math.max(...points.map((p) => Math.abs(p.x)), 1) * 1.18;
  const yMax = Math.max(...points.map((p) => Math.abs(p.y)), 1) * 1.18;
  const sx = (x: number) => M.left + ((x + xMax) / (2 * xMax)) * plotW;
  const sy = (y: number) => M.top + ((yMax - y) / (2 * yMax)) * plotH; // y ters (SVG)
  const cx0 = sx(0);
  const cy0 = sy(0);

  return (
    <section
      className="rounded-lg border p-4 flex flex-col gap-3"
      data-testid="sector-rotation-quadrant"
    >
      <div className="flex flex-col gap-0.5">
        <h2 className="text-sm font-semibold">Sektör Rotasyon Kadranı</h2>
        <p className="text-[11px] text-muted-foreground leading-relaxed">
          Görece Güç × Momentum — sektörler saat yönünde döner:{" "}
          <span style={{ color: QUAD.LEADING.color }}>Lider</span> →{" "}
          <span style={{ color: QUAD.WEAKENING.color }}>Zayıflıyor</span> →{" "}
          <span style={{ color: QUAD.LAGGING.color }}>Geride</span> →{" "}
          <span style={{ color: QUAD.IMPROVING.color }}>İyileşiyor</span>.
        </p>
      </div>

      <svg
        viewBox={`0 0 ${W} ${H}`}
        className="w-full max-w-3xl mx-auto"
        role="img"
        aria-label="Sektör rotasyon kadranı: görece güç ve momentum"
      >
        {/* Kadran arka planları */}
        <rect x={cx0} y={M.top} width={M.left + plotW - cx0} height={cy0 - M.top} fill={QUAD.LEADING.bg} />
        <rect x={cx0} y={cy0} width={M.left + plotW - cx0} height={M.top + plotH - cy0} fill={QUAD.WEAKENING.bg} />
        <rect x={M.left} y={cy0} width={cx0 - M.left} height={M.top + plotH - cy0} fill={QUAD.LAGGING.bg} />
        <rect x={M.left} y={M.top} width={cx0 - M.left} height={cy0 - M.top} fill={QUAD.IMPROVING.bg} />

        {/* Dış çerçeve */}
        <rect x={M.left} y={M.top} width={plotW} height={plotH} fill="none" stroke="var(--border)" strokeWidth={1} />

        {/* Merkez eksenler (0 çizgileri) */}
        <line x1={cx0} y1={M.top} x2={cx0} y2={M.top + plotH} stroke="var(--muted-foreground)" strokeWidth={1} strokeDasharray="4 4" opacity={0.5} />
        <line x1={M.left} y1={cy0} x2={M.left + plotW} y2={cy0} stroke="var(--muted-foreground)" strokeWidth={1} strokeDasharray="4 4" opacity={0.5} />

        {/* Kadran etiketleri (köşeler) */}
        <text x={M.left + plotW - 6} y={M.top + 14} textAnchor="end" fontSize={11} fontWeight={700} fill={QUAD.LEADING.color}>LİDER</text>
        <text x={M.left + plotW - 6} y={M.top + plotH - 6} textAnchor="end" fontSize={11} fontWeight={700} fill={QUAD.WEAKENING.color}>ZAYIFLIYOR</text>
        <text x={M.left + 6} y={M.top + plotH - 6} textAnchor="start" fontSize={11} fontWeight={700} fill={QUAD.LAGGING.color}>GERİDE</text>
        <text x={M.left + 6} y={M.top + 14} textAnchor="start" fontSize={11} fontWeight={700} fill={QUAD.IMPROVING.color}>İYİLEŞİYOR</text>

        {/* Eksen başlıkları */}
        <text x={M.left + plotW / 2} y={H - 8} textAnchor="middle" fontSize={11} fill="var(--muted-foreground)">Görece Güç (3A) →</text>
        <text x={14} y={M.top + plotH / 2} textAnchor="middle" fontSize={11} fill="var(--muted-foreground)" transform={`rotate(-90 14 ${M.top + plotH / 2})`}>Momentum (İvme) ↑</text>

        {/* Sektör noktaları */}
        {points.map((p) => {
          const r = p.rank === 1 ? 7 : 5.5;
          const col = QUAD[p.quad].color;
          return (
            <g key={p.ticker}>
              <title>{`${p.sector} (${p.ticker}) — ${QUAD[p.quad].label} · RS #${p.rank ?? "—"}`}</title>
              <circle cx={sx(p.x)} cy={sy(p.y)} r={r} fill={col} fillOpacity={0.85} stroke="var(--background)" strokeWidth={1.5} />
              <text x={sx(p.x) + r + 3} y={sy(p.y) + 3.5} fontSize={10} fontFamily="var(--font-mono, monospace)" fontWeight={600} fill="var(--foreground)">{p.ticker}</text>
            </g>
          );
        })}
      </svg>

      {/* Legend */}
      <div className="flex flex-wrap gap-x-4 gap-y-1 text-[10px] text-muted-foreground justify-center">
        {(Object.keys(QUAD) as QuadKey[]).map((k) => (
          <span key={k} className="inline-flex items-center gap-1">
            <span className="inline-block w-2.5 h-2.5 rounded-full" style={{ background: QUAD[k].color }} />
            {QUAD[k].label}
          </span>
        ))}
      </div>

      <p className="text-[10px] text-muted-foreground/80 leading-relaxed border-t border-muted-foreground/10 pt-2">
        Yöntem: <span className="font-mono">Görece Güç = perf_3A − sektör ortalaması</span>;{" "}
        <span className="font-mono">Momentum = (perf_1A − ort) − (perf_3A − ort)/3</span> (görece gücün değişim hızı).
        Sektör ortalaması benchmark yerine geçer. Profesyonel rotasyon grafiği yaklaşımının basitleştirilmiş,
        gerçek-veri uyarlamasıdır (lisanslı RS-Ratio/Momentum formülü değil).
      </p>
    </section>
  );
}
