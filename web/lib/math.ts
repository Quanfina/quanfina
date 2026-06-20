import Decimal from 'decimal.js';

export function calcPL(
  entryPrice: number | string,
  exitPrice: number | string,
  shares: number | string,
  investType: 1 | 2 = 1,   // P539: 1=LONG / 2=SHORT (fiyat düşünce kâr) — backend _calc_pl simetri
): { plDollar: number; plPct: number } {
  const entry = new Decimal(entryPrice);
  const exit  = new Decimal(exitPrice);
  const qty   = new Decimal(shares);
  const sign  = investType === 2 ? new Decimal(-1) : new Decimal(1);  // SHORT: kâr/zarar tersine

  const plDollar = sign.times(exit.minus(entry)).times(qty).toDecimalPlaces(2).toNumber();
  const plPct    = sign.times(exit.minus(entry)).dividedBy(entry).times(100).toDecimalPlaces(2).toNumber();

  return { plDollar, plPct };
}

export function fmtPLDollar(value: number): string {
  const abs = Math.abs(value).toFixed(2);
  return value >= 0 ? `+$${abs}` : `-$${abs}`;
}

export function fmtPLPct(value: number): string {
  return (value >= 0 ? '+' : '') + value.toFixed(2) + '%';
}
