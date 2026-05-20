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

export interface Trade {
  id: number;
  symbol: string;
  strategy: string;
  setup_type: string;
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
}

export interface TradeCreate {
  symbol: string;
  strategy: string;
  setup_type: string;
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
}

export interface TradeUpdate {
  exit_date?: string | null;
  exit_price?: number | null;
  status?: TradeStatus;
  grade?: TradeGrade | null;
  exit_reason?: ExitReason | null;
  lessons?: string | null;
  setup_type?: string;
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
};
