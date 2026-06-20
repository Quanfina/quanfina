export type TradeStatus = 'open' | 'closed';
export type TradeGrade = 'A+' | 'A' | 'B' | 'C' | 'D' | 'F';
export type ExitReason =
  | 'stop_loss'
  | 'target_hit'
  | 'trailing_stop'
  | 'discretionary'
  | 'time_exit';

// KARAR #477 (20 May 2026): Sinyal Kaynağı zorunlu (UX Bölüm 7).
// "Trade Kayit Formu (Sinyal Kaynagi Vurgusu)" — disiplin için trade'in kökeni izlenir.
// Sn. Ferit'in trade kalitesi analizi (manuel vs strateji): hangi giriş tipi daha kazançlı?
export type SignalSource = 'strategy' | 'manual_self' | 'manual_external';

export const SIGNAL_SOURCE_LABELS: Record<SignalSource, string> = {
  strategy:        'Strateji Sinyali',
  manual_self:     'Manuel — Kendi Gözlemim',
  manual_external: 'Manuel — Dış Kaynak',
};

export const SIGNAL_SOURCE_DESCRIPTIONS: Record<SignalSource, string> = {
  strategy:        'Sistem sinyallerinden (Sinyaller sayfası AL butonu) tetiklendi',
  manual_self:     'Kendi tarama/analizimle bulduğum giriş',
  manual_external: 'Başkasından (sosyal medya, forum, tavsiye) gelen fikir',
};

// KARAR ADAY #717 (24 May 2026): Mark TTLC Sec 1 6 zorunlu plan alanı.
// Mark birebir: "Without a written plan, you have only hope" / "Always go in with a plan"
// Time Horizon — Mark'ın 3 trade tipi: swing (1-2 hafta), position (1-6 ay), core (6+ ay)
export type TimeHorizon = 'swing' | 'position' | 'core';

export const TIME_HORIZON_LABELS: Record<TimeHorizon, string> = {
  swing:    'Swing (1-2 hafta)',
  position: 'Pozisyon (1-6 ay)',
  core:     'Core (6+ ay)',
};

export const TIME_HORIZON_DESCRIPTIONS: Record<TimeHorizon, string> = {
  swing:    'Kısa vadeli — daralma → kırılım → 2R/3R hedef',
  position: 'Orta vadeli — trend takibi 50MA üzerinde, sektör lideri',
  core:     'Uzun vadeli — sermayenin %25-50 büyük posisyon, 200MA disiplin',
};

import type { MarkSignals } from "@/components/mark/MarkBadgeStrip";
import type { PivotBreakoutStatus } from "@/hooks/use-pivot-breakout";

export interface Trade {
  id: number;
  symbol: string;
  strategy: string;
  setup_type: string;
  // P539 (20 Haz 2026): pozisyon yönü — 1=LONG (default) / 2=SHORT (Carr SHORT setupları)
  invest_type?: 1 | 2;
  signal_source?: SignalSource | null;  // KARAR #477: UX Bölüm 7 (geriye dönük uyum için optional)
  entry_date: string;
  entry_price: number;
  exit_date: string | null;
  exit_price: number | null;
  shares: number;
  status: TradeStatus;
  pl_dollar: number | null;
  pl_pct: number | null;
  grade: TradeGrade | null;
  exit_reason: ExitReason | null;
  lessons: string | null;
  // KARAR ADAY #717 — Mark TTLC Sec 1 6 zorunlu plan alanı.
  // Eski trade'ler için null (geriye uyum), yeni kayıtlarda zorunlu.
  plan_entry_trigger?: string | null;
  plan_stop?: number | null;
  plan_target?: number | null;
  plan_size_pct?: number | null;
  plan_exit_strategy?: string | null;
  plan_time_horizon?: TimeHorizon | null;
  // Migration 011 / KARAR ADAY #960 (16 Haz 2026): AKTIF/editable stop+hedef + audible sebep.
  // plan_* orijinal plan (değişmez); bunlar pozisyon yönetiminde ayarlanır. audible_reason =
  // son stop/hedef değişiklik sebebi (Mark "audible" disiplini).
  stop_loss?: number | null;
  target_price?: number | null;
  audible_reason?: string | null;
  // P477 (#976): açık pozisyon sell-strength enrichment (entry-aware — Hard Stop %10 ateşlenir).
  // Sadece open trade'lerde dolu (backend _enrich_trade). category → journal "Satış" kolonu rozeti.
  sell_strength?: {
    category: "HOLD" | "WATCH" | "REDUCE" | "SELL" | null;
    score: number;
    signals: string[];
    mark_says: string;
  } | null;
  // KARAR #733 alt-paket (Paket 41): Mark Profili enrichment (Watchlist + Signals pateni)
  // Journal sayfasinda stage4Count gerçek hesaplama + gelecek MarkBadgeStrip kolonu.
  mark_signals?: MarkSignals;
  // KARAR #733 alt-paket (Paket 84, 26 May 2026): Pivot status enrichment
  pivot_status?: PivotBreakoutStatus | null;
  // Paket 151 (26 May 2026): Paper trading canlı P&L — client-side enriched.
  // useStockQuote ile fetch edilen güncel piyasa fiyatı (yfinance + 5dk cache).
  // Sadece açık trade'lerde dolu. Closed trade için pl_dollar/pl_pct geçerli.
  current_price?: number | null;       // yfinance son kapanış
  unrealized_pl_dollar?: number | null;  // (current_price - entry_price) × shares
  unrealized_pl_pct?: number | null;     // ((current_price / entry_price) - 1) × 100
  quote_source?: "yfinance" | "mock" | null;
  // Paket 190 (26 May 2026): Sektör konsantrasyon uyarısı (Mark TTLC s.85).
  // Backend _enrich_trade_with_mark_signals _STOCK_META lookup ile doldurur.
  sector?: string | null;
}

export interface TradeCreate {
  symbol: string;
  strategy: string;
  setup_type: string;
  invest_type?: 1 | 2;          // P539: pozisyon yönü (1=LONG default / 2=SHORT)
  signal_source: SignalSource;  // KARAR #477: ZORUNLU — UX Bölüm 7
  entry_date: string;
  entry_price: number;
  shares: number;
  status?: TradeStatus;
  exit_date?: string | null;
  exit_price?: number | null;
  grade?: TradeGrade | null;
  exit_reason?: ExitReason | null;
  lessons?: string | null;
  // KARAR ADAY #717 — Mark felsefesi: plan'sız trade YASAK. 6 alan ZORUNLU.
  plan_entry_trigger: string;
  plan_stop: number;
  plan_target: number;
  plan_size_pct: number;
  plan_exit_strategy: string;
  plan_time_horizon: TimeHorizon;
}

export interface TradeUpdate {
  exit_date?: string | null;
  exit_price?: number | null;
  status?: TradeStatus;
  grade?: TradeGrade | null;
  exit_reason?: ExitReason | null;
  lessons?: string | null;
  setup_type?: string;
  // KARAR ADAY #717 — Plan alanları sonradan revize edilebilir (Optional).
  plan_entry_trigger?: string | null;
  plan_stop?: number | null;
  plan_target?: number | null;
  plan_size_pct?: number | null;
  plan_exit_strategy?: string | null;
  plan_time_horizon?: TimeHorizon | null;
  // Migration 011 / #960: aktif stop/hedef düzenleme + audible sebep (değişiyorsa zorunlu).
  stop_loss?: number | null;
  target_price?: number | null;
  audible_reason?: string | null;
}

export interface SetupType {
  key: string;
  label: string;
  description: string;
}

export const GRADE_OPTIONS: TradeGrade[] = ['A+', 'A', 'B', 'C', 'D', 'F'];

export const EXIT_REASON_LABELS: Record<ExitReason, string> = {
  stop_loss:    'Stop Loss',
  target_hit:   'Hedef Ulaşıldı',
  trailing_stop:'Trailing Stop',
  discretionary:'Takdiri',
  time_exit:    'Süre Çıkışı',
};

export const SETUP_LABELS: Record<string, string> = {
  vcp:           'VCP',
  pivot:         'Pivot',
  pocket_pivot:  'Pocket Pivot',
  power_play:    'Power Play',
  cup_and_handle:'Cup & Handle',
  flat_base:     'Flat Base',
  pullback:      'Pullback',
  coiled_spring: 'Coiled Spring',
  // P530 (18 Haz 2026): Carr 9-setup label hizalama (backend SETUP_TYPES ile birebir).
  // Journal SETUP sütunu Carr paper trade'lerde ham slug yerine düzgün etiket gösterir.
  mean_reversion:     'Mean Reversion',
  blue_sky:           'Blue Sky Breakout',
  bullish_base:       'Bullish Base',
  bullish_divergence: 'Bullish Divergence',
  blue_sea:           'Blue Sea Breakdown',
  gap_down:           'Gap Down',
  rising_wedge:       'Rising Wedge',
};

// P558 (20 Haz 2026): Carr SHORT setup'ları (kanonik — backend P518/P520/P522 bulk SHORT
// slug'larıyla birebir). Bu 3 setup dışındaki her setup LONG (Minervini + Carr LONG setupları).
// Sinyaller sayfası YÖN sütunu setup_type'tan yön türetir (invest_type pre-trade'de yok).
export const SHORT_SETUPS: ReadonlySet<string> = new Set([
  "blue_sea",
  "gap_down",
  "rising_wedge",
]);

// DİKKAT (P558, gerçek-veri doğrulaması): /api/signals setup_type KARIŞIK gelir — bazı
// kayıtlar slug ("vcp"), bazıları label ("Bullish Divergence"). Bu yüzden SHORT eşleşmesi
// hem slug hem label formunu (case-insensitive) tanımalı, yoksa label-formlu SHORT sinyal
// yanlışlıkla LONG görünür. SHORT label'ları SETUP_LABELS'ten türetilir (DRY, uydurma yok).
const _SHORT_NORM: ReadonlySet<string> = new Set([
  ...[...SHORT_SETUPS].map((s) => s.toLowerCase()),
  ...[...SHORT_SETUPS].map((s) => SETUP_LABELS[s].toLowerCase()),
]);

/** setup_type'tan pozisyon yönü (slug VEYA label formu). SHORT dışı / null → "LONG". */
export function setupDirection(setupType: string | null | undefined): "LONG" | "SHORT" {
  if (!setupType) return "LONG";
  return _SHORT_NORM.has(setupType.trim().toLowerCase()) ? "SHORT" : "LONG";
}
