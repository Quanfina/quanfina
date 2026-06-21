import { NextRequest, NextResponse } from "next/server";

/**
 * P573 (21 Haz 2026): HTTP Basic Auth gate — Cloud Run public deploy koruması.
 *
 * Web sayfaları + /api/* proxy'sini parola ile kapatır (tarayıcı native prompt — login sayfası
 * gerekmez). next.config rewrite Authorization header'ını api'ye iletir → api de aynı parolayla
 * korunur (tek kimlik). APP_PASSWORD tanımsızsa gate KAPALI (lokal dev). Statik asset'ler hariç.
 */
export function middleware(req: NextRequest) {
  const user = process.env.APP_USER || "";
  const pass = process.env.APP_PASSWORD || "";
  if (!pass) return NextResponse.next(); // gate kapalı (lokal/dev)

  const auth = req.headers.get("authorization") || "";
  if (auth.startsWith("Basic ")) {
    try {
      const decoded = atob(auth.slice(6));
      const i = decoded.indexOf(":");
      const u = decoded.slice(0, i);
      const p = decoded.slice(i + 1);
      if (u === user && p === pass) return NextResponse.next();
    } catch {
      /* malformed header → 401 below */
    }
  }
  return new NextResponse("Quanfina — kimlik doğrulama gerekli.", {
    status: 401,
    headers: { "WWW-Authenticate": 'Basic realm="Quanfina", charset="UTF-8"' },
  });
}

export const config = {
  // Statik asset + favicon hariç her şey (sayfalar + /api/* proxy dahil)
  matcher: ["/((?!_next/static|_next/image|favicon.ico).*)"],
};
