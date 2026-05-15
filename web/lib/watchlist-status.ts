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
