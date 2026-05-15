import Decimal from 'decimal.js';

export function calcPL(
  entryPrice: number | string,
  exitPrice: number | string,
  shares: number | string,
): { plDollar: number; plPct: number } {
  const entry = new Decimal(entryPrice);
  const exit  = new Decimal(exitPrice);
  const qty   = new Decimal(shares);

  const plDollar = exit.minus(entry).times(qty).toDecimalPlaces(2).toNumber();
  const plPct    = exit.minus(entry).dividedBy(entry).times(100).toDecimalPlaces(2).toNumber();

  return { plDollar, plPct };
}

export function fmtPLDollar(value: number): string {
  const abs = Math.abs(value).toFixed(2);
  return value >= 0 ? `+$${abs}` : `-$${abs}`;
}

export function fmtPLPct(value: number): string {
  return (value >= 0 ? '+' : '') + value.toFixed(2) + '%';
}
