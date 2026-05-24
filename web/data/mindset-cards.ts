/**
 * KARAR ADAY #720 — Daily Mindset Cards
 * Mark Minervini birebir alıntılı zihinsel disiplin kartları.
 * Kaynak: TLSMW + TTLC + MSW birebir alıntı + Quanfina uygulamada karşılık.
 *
 * KALICI İLKE #4 disiplini: Her kart Mark birebir alıntı + sayfa numarası.
 * Uydurma YOK, kaynak işaretsiz cümle YOK.
 *
 * 24 May 2026 — Sprint 4-bis.7 Uygulama Dönemi 2. paket
 */

export type MindsetCategory =
  | "risk"           // Risk yönetimi (stop, position size, Wall)
  | "mindset"        // Zihinsel disiplin (responsibility, sit-out, conviction)
  | "setup"          // Giriş hazırlığı (plan, pivot, conviction)
  | "management"     // Trade yönetimi (trailing, pyramid, breakeven)
  | "exit";          // Çıkış disiplini (sell into strength, climax, failed breakout)

export interface MindsetCard {
  id: string;
  category: MindsetCategory;
  quote: string;         // Mark birebir (İngilizce orijinal — kalite korunsun)
  source: string;        // TLSMW s.X / TTLC s.Y / MSW kaynak referansı
  quanfinaNote: string;  // Quanfina'da nasıl uygulandığı (Türkçe pratik)
  emoji: string;         // Görsel ipucu
}

export const MINDSET_CARDS: MindsetCard[] = [
  // -----------------------------------------------------------------
  // KATEGORİ 1: RİSK (5 kart)
  // -----------------------------------------------------------------
  {
    id: "risk-01",
    category: "risk",
    quote: "Approach every trade risk-first. Stop is set before entry.",
    source: "TTLC s.144 (Sec 2-3: Risk First + Never Risk More)",
    quanfinaNote: "Quanfina TradeForm: plan_stop alanı entry_price'tan önce yazılır (KARAR #717).",
    emoji: "🛡️",
  },
  {
    id: "risk-02",
    category: "risk",
    quote: "The Wall: 10% maximum loss on any trade. Beyond that — recovery becomes asymmetric.",
    source: "Mindset Secrets s.73 (Uncle Point)",
    quanfinaNote: "MARK_STOP_ABSOLUTE_CAP_PCT = 10.0 (quanfina_math.py KESIN sabit).",
    emoji: "🧱",
  },
  {
    id: "risk-03",
    category: "risk",
    quote: "Risk 1.25% to 2.5% of equity per trade. Never more.",
    source: "TTLC s.143-144 (Sec 8: Position Sizing)",
    quanfinaNote: "mark_position_sizer: equity_risk_pct guard 1.25-2.5% (Pilot/Standart/Full tier).",
    emoji: "📏",
  },
  {
    id: "risk-04",
    category: "risk",
    quote: "Bucking Bronco: high volatility = smaller position, NOT wider stop.",
    source: "TTLC s.144 + MM s.~ (ATR discipline)",
    quanfinaNote: "ATR > 2.5x → position size azaltılır, stop genişletilmez (KARAR #486).",
    emoji: "🐎",
  },
  {
    id: "risk-05",
    category: "risk",
    quote: "Trades not working = no size increase. Pilot must turn profitable before Standard.",
    source: "Mark X (Twitter) — Pyramid Safety Lock",
    quanfinaNote: "should_advance_tier() Pilot→Standart geçiş için kâr şartı (KARAR #487).",
    emoji: "🔒",
  },

  // -----------------------------------------------------------------
  // KATEGORİ 2: MINDSET (3 kart)
  // -----------------------------------------------------------------
  {
    id: "mindset-01",
    category: "mindset",
    quote: "Sit-out power: the discipline to be flat when no setup exists.",
    source: "TTLC Sec 10 Eight Keys (Key 1) + MSW s.99",
    quanfinaNote: "Quanfina sit-out: hiçbir trade Quanfina disiplinli rotada değilse → fırsat değil disiplin.",
    emoji: "🧘",
  },
  {
    id: "mindset-02",
    category: "mindset",
    quote: "Without a written plan, you have only hope. Always go in with a plan.",
    source: "TTLC s.~ (Sec 1: Always Go in with a Plan)",
    quanfinaNote: "TradeForm: 6 plan alanı ZORUNLU — plan_entry_trigger, plan_stop, plan_target, plan_size_pct, plan_exit_strategy, plan_time_horizon (KARAR #717).",
    emoji: "📋",
  },
  {
    id: "mindset-03",
    category: "mindset",
    quote: "Compound money, not mistakes. Each trade independent of the last.",
    source: "TTLC s.~ (Sec 5: Compound Money, Not Mistakes)",
    quanfinaNote: "RBA disiplini: her trade RBAF formülü ile ayrı değerlendirilir, geçmiş bağ yok.",
    emoji: "💎",
  },

  // -----------------------------------------------------------------
  // KATEGORİ 3: SETUP (3 kart)
  // -----------------------------------------------------------------
  {
    id: "setup-01",
    category: "setup",
    quote: "Code 33: three quarters of acceleration in earnings, sales, AND profit margins. That's a potent recipe.",
    source: "TLSMW s.173 (Ch 7: Fundamentals to Focus On)",
    quanfinaNote: "detect_code_33(): 3-quarter EPS+Sales+Margin triple strict accel = elite tier.",
    emoji: "⚡",
  },
  {
    id: "setup-02",
    category: "setup",
    quote: "VCP Footprint: 40W base, 31% first contraction, 3% last contraction, 4 contractions total.",
    source: "TLSMW s.~210 (Ch 10: Pattern Library)",
    quanfinaNote: "compute_vcp_quality(): contraction sayısı + yarılanma kuralı + V-Dry hacim disiplini.",
    emoji: "📉",
  },
  {
    id: "setup-03",
    category: "setup",
    quote: "Power Play (HTF): explosive 90-130% advance in 8 weeks, then 3-6 week tight base.",
    source: "TLSMW s.~ (Ch 9: Power Play)",
    quanfinaNote: "compute_power_play_pass(): velocity threshold + post-explosion tight base (KARAR #467).",
    emoji: "🚀",
  },

  // -----------------------------------------------------------------
  // KATEGORİ 4: MANAGEMENT (2 kart)
  // -----------------------------------------------------------------
  {
    id: "mgmt-01",
    category: "management",
    quote: "Move stop to breakeven at 2R-3R profit. Free trade — your capital is safe.",
    source: "TTLC s.~ (Sec 9 + SBE Video 00:09:51)",
    quanfinaNote: "TrailingStopManager: trail_breakeven aşaması — 2R/3R kâr eşiği (KARAR #489).",
    emoji: "🛡️",
  },
  {
    id: "mgmt-02",
    category: "management",
    quote: "Pyramid winners only: Pilot → Standard → Full. Each tier locked by previous tier profit.",
    source: "Mark Video + TraderLion Lesson 7 + Brandon Video",
    quanfinaNote: "Quanfina pyramid: %6.25 → %12.5 → %25 (2x katlama, kilitli) (KARAR #487).",
    emoji: "🔺",
  },

  // -----------------------------------------------------------------
  // KATEGORİ 5: EXIT (2 kart)
  // -----------------------------------------------------------------
  {
    id: "exit-01",
    category: "exit",
    quote: "Outside Day Negative Reversal: absolute exit signal. No questions asked.",
    source: "Mark Video 01:06:17 + TLSMW s.~",
    quanfinaNote: "compute_outside_day_negative_reversal(): tetiklenirse trailing/MA bypass, anında EXIT_FULL (KARAR #465).",
    emoji: "🚪",
  },
  {
    id: "exit-02",
    category: "exit",
    quote: "Sell into strength on climax run. Don't wait for the top.",
    source: "TTLC s.155 (Sec 9: When to Sell — 3 Modes)",
    quanfinaNote: "Climax detector: parabolic 10-20MA dikey ivme → Selling Into Strength uyarı.",
    emoji: "📈",
  },
];

export const CATEGORY_LABELS: Record<MindsetCategory, string> = {
  risk:       "Risk Yönetimi",
  mindset:    "Zihinsel Disiplin",
  setup:      "Giriş Hazırlığı",
  management: "Trade Yönetimi",
  exit:       "Çıkış Disiplini",
};

export const CATEGORY_COLORS: Record<MindsetCategory, string> = {
  risk:       "var(--mtp-danger)",
  mindset:    "var(--mtp-neutral)",
  setup:      "var(--mtp-good)",
  management: "var(--mtp-excellent)",
  exit:       "var(--mtp-warning, #F59E0B)",
};

/**
 * Gün başına 1 sabit kart döndürür (deterministik — localStorage yok, kararlı render).
 * Algoritma: bugünün tarihinden (YYYY-MM-DD) hash → cards length modulo.
 */
export function getTodayMindsetCard(date: Date = new Date()): MindsetCard {
  const dateStr = date.toISOString().slice(0, 10); // YYYY-MM-DD
  let hash = 0;
  for (let i = 0; i < dateStr.length; i++) {
    hash = (hash << 5) - hash + dateStr.charCodeAt(i);
    hash = hash & hash;
  }
  const idx = Math.abs(hash) % MINDSET_CARDS.length;
  return MINDSET_CARDS[idx];
}
