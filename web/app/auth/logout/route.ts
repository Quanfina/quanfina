import { NextResponse } from "next/server";

/**
 * P575 (21 Haz 2026): POST /auth/logout — qf_session çerezini sil (Çıkış).
 * Basic Auth'ta imkansız olan temiz logout: çerez maxAge=0 ile silinir, middleware /login'e atar.
 */
export async function POST() {
  const res = NextResponse.json({ ok: true });
  res.cookies.set("qf_session", "", {
    httpOnly: true,
    secure: true,
    sameSite: "lax",
    path: "/",
    maxAge: 0,
  });
  return res;
}
