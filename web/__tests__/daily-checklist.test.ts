/**
 * daily-checklist (KARAR #480 — UX Bölüm 8 "Aksiyon Modu").
 *
 * Günlük rutin disiplin: localStorage tarih bazlı + 7 gün TTL.
 */
import { describe, it, expect, beforeEach } from "vitest";
import { getDoneItems, toggleItem } from "@/lib/daily-checklist";

const STORAGE_KEY = "quanfina_daily_checklist";

function todayKey(): string {
  return new Date().toISOString().split("T")[0];
}

beforeEach(() => {
  localStorage.clear();
});

describe("getDoneItems — bugünkü tamamlananlar (Set)", () => {
  it("Boş localStorage → boş Set", () => {
    const done = getDoneItems();
    expect(done.size).toBe(0);
  });

  it("Bugün için 2 item kayıtlı → Set 2 eleman", () => {
    localStorage.setItem(
      STORAGE_KEY,
      JSON.stringify({ [todayKey()]: ["item-1", "item-3"] })
    );
    const done = getDoneItems();
    expect(done.size).toBe(2);
    expect(done.has("item-1")).toBe(true);
    expect(done.has("item-3")).toBe(true);
  });

  it("Sadece dünün kaydı varsa → boş Set (yeni gün sıfırlanır)", () => {
    const yesterday = new Date();
    yesterday.setDate(yesterday.getDate() - 1);
    const yKey = yesterday.toISOString().split("T")[0];
    localStorage.setItem(
      STORAGE_KEY,
      JSON.stringify({ [yKey]: ["item-1"] })
    );
    expect(getDoneItems().size).toBe(0);
  });

  it("Geçersiz JSON → boş Set (defansif)", () => {
    localStorage.setItem(STORAGE_KEY, "not-json{");
    expect(getDoneItems().size).toBe(0);
  });
});

describe("toggleItem — işaretle/kaldır", () => {
  it("done=true → item Set'e eklenir, localStorage'da görünür", () => {
    toggleItem("item-1", true);
    expect(getDoneItems().has("item-1")).toBe(true);
    const raw = localStorage.getItem(STORAGE_KEY);
    expect(raw).toContain("item-1");
  });

  it("done=false → item Set'ten çıkarılır", () => {
    toggleItem("item-1", true);
    toggleItem("item-2", true);
    toggleItem("item-1", false);
    const done = getDoneItems();
    expect(done.has("item-1")).toBe(false);
    expect(done.has("item-2")).toBe(true);
  });

  it("Aynı item iki kez done=true → tek seferli (Set dedup)", () => {
    toggleItem("item-1", true);
    toggleItem("item-1", true);
    expect(getDoneItems().size).toBe(1);
  });

  it("done=false zaten yok → no-op (hata vermez)", () => {
    toggleItem("nonexistent", false);
    expect(getDoneItems().size).toBe(0);
  });
});

describe("7-gün TTL temizliği (saklama disiplini)", () => {
  it("8 gün eski entry → toggleItem sonrası silinir", () => {
    const old = new Date();
    old.setDate(old.getDate() - 8);
    const oldKey = old.toISOString().split("T")[0];
    localStorage.setItem(
      STORAGE_KEY,
      JSON.stringify({
        [oldKey]: ["eski-item"],
        [todayKey()]: ["bugun-item"],
      })
    );
    // toggleItem 7+ gün cleanup tetikler
    toggleItem("yeni-item", true);
    const parsed = JSON.parse(localStorage.getItem(STORAGE_KEY)!);
    expect(parsed[oldKey]).toBeUndefined();
    expect(parsed[todayKey()]).toContain("bugun-item");
    expect(parsed[todayKey()]).toContain("yeni-item");
  });

  it("6 gün eski entry KORUNUR (cutoff = 7 gün)", () => {
    const recent = new Date();
    recent.setDate(recent.getDate() - 6);
    const recentKey = recent.toISOString().split("T")[0];
    localStorage.setItem(
      STORAGE_KEY,
      JSON.stringify({ [recentKey]: ["6-gunluk"] })
    );
    toggleItem("bugun-yeni", true);
    const parsed = JSON.parse(localStorage.getItem(STORAGE_KEY)!);
    expect(parsed[recentKey]).toEqual(["6-gunluk"]);
  });
});

describe("Çoklu gün izolasyon", () => {
  it("Birden çok günün state'i bağımsız tutulur", () => {
    const yesterday = new Date();
    yesterday.setDate(yesterday.getDate() - 1);
    const yKey = yesterday.toISOString().split("T")[0];
    localStorage.setItem(
      STORAGE_KEY,
      JSON.stringify({
        [yKey]: ["dun-item-a", "dun-item-b"],
      })
    );
    toggleItem("bugun-item", true);
    const parsed = JSON.parse(localStorage.getItem(STORAGE_KEY)!);
    // Dün KORUNDU + bugün eklenmiş (cutoff 7 gün, 1 gün dışında)
    expect(parsed[yKey]).toContain("dun-item-a");
    expect(parsed[todayKey()]).toContain("bugun-item");
  });
});
