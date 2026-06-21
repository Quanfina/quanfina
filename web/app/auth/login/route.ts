import { NextRequest, NextResponse } from "next/server";

/**
 * P575 (21 Haz 2026): POST /auth/login — parola doğrula, qf_session çerezi set et.
 * Çerez değeri = SESSION_TOKEN (rastgele secret, parola DEĞİL). httpOnly+secure+sameSite.
 * Middleware + api bu token'ı kontrol eder. APP_PASSWORD/SESSION_TOKEN tanımsızsa reddet.
 */
export async function POST(req: NextRequest) {
  const body = await req.json().catch(() => ({}) as { password?: string });
  const password = (body as { password?: string }).password ?? "";

  const expected = process.env.APP_PASSWORD || "";
  const token = process.env.SESSION_TOKEN || "";

  if (!expected || !token || password !== expected) {
    return new NextResponse("unauthorized", { status: 401 });
  }

  const res = NextResponse.json({ ok: true });
  res.cookies.set("qf_session", token, {
    httpOnly: true,
    secure: true,
    sameSite: "lax",
    path: "/",
    maxAge: 60 * 60 * 24 * 30, // 30 gün
  });
  return res;
}
