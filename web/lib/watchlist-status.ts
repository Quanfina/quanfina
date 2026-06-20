export const STATUS_HIERARCHY = ["watch", "on_deck", "focus", "buy"] as const;
type Status = (typeof STATUS_HIERARCHY)[number];

export function promoteStatus(current: string): string {
  const idx = STATUS_HIERARCHY.indexOf(current as Status);
  if (idx === -1 || idx === STATUS_HIERARCHY.length - 1) return current;
  return STATUS_HIERARCHY[idx + 1];
}

export function demoteStatus(current: string): string {
  const idx = STATUS_HIERARCHY.indexOf(current as Status);
  if (idx <= 0) return current;
  return STATUS_HIERARCHY[idx - 1];
}

export function canPromote(status: string): boolean {
  return status !== "buy";
}

export function canDemote(status: string): boolean {
  return status !== "watch";
}

export function statusRank(status: string): number {
  return STATUS_HIERARCHY.indexOf(status as Status);
}

/**
 * P562 (List Degradation): pivot_status + pocket_pivot sinyallerinden ÖNERİLEN tier.
 *
 * 4-liste hiyerarşisi Quanfina tasarımı (ux_tarama C_hedef). Eşleme pivot YAKINLIĞINA
 * dayanır (Mark TLSMW Ch 10 Pivot Breakout — canon sinyal, uydurma yok):
 *   CONFIRMED (kırılım tetiklendi)  → buy     (trade-hazır)
 *   NEAR_PIVOT (pivota çok yakın)   → focus   (odak — kırılım yakın)
 *   WEAK / Pocket Pivot (kurumsal güç) → on_deck (izle)
 *   BELOW_PIVOT / sinyal yok        → watch
 *
 * Advisory — otomatik DEĞİŞTİRMEZ (Kural #4); UI nudge gösterir, Sn. Ferit karar verir.
 */
export function suggestStatus(
  pivotStatus: string | null | undefined,
  pocketPivot: string | null | undefined,
): Status {
  if (pivotStatus === "CONFIRMED") return "buy";
  if (pivotStatus === "NEAR_PIVOT") return "focus";
  if (pivotStatus === "WEAK" || pocketPivot === "GOOD" || pocketPivot === "CANDIDATE") {
    return "on_deck";
  }
  return "watch";
}
