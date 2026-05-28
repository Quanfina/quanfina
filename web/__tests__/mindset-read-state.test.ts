/**
 * mindset-read-state (KARAR ADAY #720 alt P28+P29).
 *
 * Sn. Ferit sabah Mindset kart okuma persistence + streak.
 * localStorage tarih bazlı + 60 gün history + kesintisiz streak hesap.
 */
import { describe, it, expect, beforeEach } from "vitest";
import {
  isCardReadToday,
  markCardReadToday,
  getReadStreak,
  isReadTodayAny,
} from "@/lib/mindset-read-state";

const STORAGE_KEY = "quanfina-mindset-read";

function todayStr(date: Date = new Date()): string {
  return date.toISOString().slice(0, 10);
}

function daysAgo(n: number): string {
  const d = new Date();
  d.setDate(d.getDate() - n);
  return d.toISOString().slice(0, 10);
}

beforeEach(() => {
  localStorage.clear();
});

describe("isCardReadToday — bugünün belirli kartı okundu mu?", () => {
  it("Boş localStorage → false", () => {
    expect(isCardReadToday("risk-01")).toBe(false);
  });

  it("Bugün başka kart okundu → false (cardId eşleşmez)", () => {
    localStorage.setItem(
      STORAGE_KEY,
      JSON.stringify({ date: todayStr(), cardId: "risk-02" })
    );
    expect(isCardReadToday("risk-01")).toBe(false);
  });

  it("Dün aynı kart okundu → false (tarih eşleşmez)", () => {
    localStorage.setItem(
      STORAGE_KEY,
      JSON.stringify({ date: daysAgo(1), cardId: "risk-01" })
    );
    expect(isCardReadToday("risk-01")).toBe(false);
  });

  it("Bugün + cardId eşleşir → true", () => {
    localStorage.setItem(
      STORAGE_KEY,
      JSON.stringify({ date: todayStr(), cardId: "risk-01" })
    );
    expect(isCardReadToday("risk-01")).toBe(true);
  });

  it("Geçersiz JSON → false (defansif)", () => {
    localStorage.setItem(STORAGE_KEY, "not-json{");
    expect(isCardReadToday("risk-01")).toBe(false);
  });
});

describe("markCardReadToday — kart işaretle + history güncelle", () => {
  it("İlk işaretleme → date + cardId + history[0] = today", () => {
    markCardReadToday("risk-01");
    const state = JSON.parse(localStorage.getItem(STORAGE_KEY)!);
    expect(state.date).toBe(todayStr());
    expect(state.cardId).toBe("risk-01");
    expect(state.history).toContain(todayStr());
  });

  it("Aynı gün ikinci işaretleme (farklı kart) → history dedup", () => {
    markCardReadToday("risk-01");
    markCardReadToday("risk-02");
    const state = JSON.parse(localStorage.getItem(STORAGE_KEY)!);
    expect(state.cardId).toBe("risk-02"); // son kart
    expect(state.history.filter((d: string) => d === todayStr())).toHaveLength(1);
  });

  it("Dün okundu, bugün ilk işaretleme → history 2 gün", () => {
    const yesterday = daysAgo(1);
    localStorage.setItem(
      STORAGE_KEY,
      JSON.stringify({ date: yesterday, cardId: "risk-09", history: [yesterday] })
    );
    markCardReadToday("risk-10");
    const state = JSON.parse(localStorage.getItem(STORAGE_KEY)!);
    expect(state.history).toContain(yesterday);
    expect(state.history).toContain(todayStr());
    expect(state.history.length).toBe(2);
  });

  it("History 60 gün üzeri → kırpılır (FIFO)", () => {
    // 65 günlük dizi oluştur
    const history: string[] = [];
    for (let i = 65; i >= 1; i--) {
      history.push(daysAgo(i));
    }
    localStorage.setItem(
      STORAGE_KEY,
      JSON.stringify({ date: daysAgo(1), cardId: "old", history })
    );
    markCardReadToday("risk-yeni");
    const state = JSON.parse(localStorage.getItem(STORAGE_KEY)!);
    expect(state.history.length).toBe(60); // HISTORY_MAX_DAYS
    // En eski 6 gün düşmüş olmalı (65+1=66 → kırp 60)
    expect(state.history).not.toContain(daysAgo(65));
    expect(state.history).toContain(todayStr());
  });
});

describe("getReadStreak — kesintisiz okuma günleri", () => {
  it("Boş history → streak=0", () => {
    expect(getReadStreak()).toEqual({ streak: 0, todayRead: false });
  });

  it("Sadece bugün okundu → streak=1, todayRead=true", () => {
    localStorage.setItem(
      STORAGE_KEY,
      JSON.stringify({ date: todayStr(), cardId: "r1", history: [todayStr()] })
    );
    const r = getReadStreak();
    expect(r.streak).toBe(1);
    expect(r.todayRead).toBe(true);
  });

  it("Bugün + dün + önceki gün → streak=3", () => {
    const history = [daysAgo(2), daysAgo(1), todayStr()];
    localStorage.setItem(
      STORAGE_KEY,
      JSON.stringify({ date: todayStr(), cardId: "r1", history })
    );
    expect(getReadStreak().streak).toBe(3);
  });

  it("Bugün okunmadı + dün okundu → streak=1, todayRead=false", () => {
    const history = [daysAgo(1)];
    localStorage.setItem(
      STORAGE_KEY,
      JSON.stringify({ date: daysAgo(1), cardId: "r1", history })
    );
    const r = getReadStreak();
    expect(r.streak).toBe(1);
    expect(r.todayRead).toBe(false);
  });

  it("Kesintili: dün okundu, 2 gün önce yok, 3 gün önce var → streak=1 (bugün boş)", () => {
    const history = [daysAgo(3), daysAgo(1)];
    localStorage.setItem(
      STORAGE_KEY,
      JSON.stringify({ date: daysAgo(1), cardId: "r1", history })
    );
    expect(getReadStreak().streak).toBe(1); // dünden geriye 2 gün önce yok
  });

  it("Bugün ve önceki gün var, dün yok → streak=1 (todayRead=true ama dün boş)", () => {
    const history = [daysAgo(2), todayStr()];
    localStorage.setItem(
      STORAGE_KEY,
      JSON.stringify({ date: todayStr(), cardId: "r1", history })
    );
    const r = getReadStreak();
    expect(r.streak).toBe(1); // bugünden geriye dün yok → kır
    expect(r.todayRead).toBe(true);
  });

  it("Geçersiz JSON → streak=0 defansif", () => {
    localStorage.setItem(STORAGE_KEY, "{");
    expect(getReadStreak().streak).toBe(0);
  });
});

describe("isReadTodayAny — sadece tarih kontrolü (cardId fark etmez)", () => {
  it("Boş → false", () => {
    expect(isReadTodayAny()).toBe(false);
  });

  it("Bugün herhangi kart okundu → true", () => {
    localStorage.setItem(
      STORAGE_KEY,
      JSON.stringify({ date: todayStr(), cardId: "any-card" })
    );
    expect(isReadTodayAny()).toBe(true);
  });

  it("Dün okundu → false", () => {
    localStorage.setItem(
      STORAGE_KEY,
      JSON.stringify({ date: daysAgo(1), cardId: "any" })
    );
    expect(isReadTodayAny()).toBe(false);
  });
});
