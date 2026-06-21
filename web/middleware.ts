import { NextRequest, NextResponse } from "next/server";

/**
 * P575 (21 Haz 2026): Çerez-tabanlı oturum gate (Basic Auth → cookie + Çıkış butonu).
 *
 * qf_session çerezi == SESSION_TOKEN (env, Secret Manager) → erişim. Login/auth handler'ları +
 * statik muaf. Yetkisiz: sayfa → /login redirect; /api/* → 401. next.config rewrite Cookie
 * header'ını api'ye iletir → api de aynı token'ı kontrol eder. SESSION_TOKEN tanımsızsa gate
 * KAPALI (lokal dev). Çıkış: /auth/logout çerezi siler (Basic Auth'ın çözemediği logout).
 */
const COOKIE = "qf_session";

export function middleware(req: NextRequest) {
  const token = process.env.SESSION_TOKEN || "";
  if (!token) return NextResponse.next(); // gate kapalı (lokal/dev)

  const { pathname } = req.nextUrl;
  if (pathname === "/login" || pathname.startsWith("/auth/")) {
    return NextResponse.next();
  }

  const session = req.cookies.get(COOKIE)?.value;
  if (session === token) return NextResponse.next();

  // Yetkisiz
  if (pathname.startsWith("/api/")) {
    return new NextResponse("Yetkisiz", { status: 401 });
  }
  const url = req.nextUrl.clone();
  url.pathname = "/login";
  url.searchParams.set("next", pathname);
  return NextResponse.redirect(url);
}

export const config = {
  matcher: ["/((?!_next/static|_next/image|favicon.ico).*)"],
};
