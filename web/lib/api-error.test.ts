/**
 * parsePydanticError + parseErrorBody — Paket 390 ortak helper testleri.
 *
 * Saf fonksiyon (hızlı, network/DOM yok). DRY tek kaynak (P388 use-trades.ts'de
 * doğdu, P390'da lib/'e taşındı; watchlist mutation hook'u da kullanır).
 */
import { describe, it, expect } from "vitest";
import { parsePydanticError, parseErrorBody } from "@/lib/api-error";


describe("parsePydanticError — saf string formatlama", () => {
  it("Array (Pydantic 422) -> 'field: msg' join", () => {
    const detail = [
      {
        type: "greater_than",
        loc: ["body", "entry_price"],
        msg: "Input should be greater than 0",
      },
    ];
    expect(parsePydanticError(detail, 422)).toBe("entry_price: Input should be greater than 0");
  });

  it("Multi-field array -> semicolon separator", () => {
    const detail = [
      { loc: ["body", "entry_price"], msg: "must be > 0" },
      { loc: ["body", "shares"], msg: "must be > 0" },
    ];
    const result = parsePydanticError(detail, 422);
    expect(result).toBe("entry_price: must be > 0; shares: must be > 0");
  });

  it("String detail (FastAPI 503/404) -> dogrudan dondur", () => {
    expect(parsePydanticError("Trade bulunamadı", 404)).toBe("Trade bulunamadı");
    expect(parsePydanticError("Cloud SQL down", 503)).toBe("Cloud SQL down");
  });

  it("Undefined/null detail -> HTTP <status> fallback", () => {
    expect(parsePydanticError(undefined, 500)).toBe("HTTP 500");
    expect(parsePydanticError(null, 502)).toBe("HTTP 502");
  });

  it("Bos array -> bos string (degerli field yok)", () => {
    expect(parsePydanticError([], 422)).toBe("");
  });

  it("Array elemani eksik loc/msg -> '?' fallback + 'gecersiz'", () => {
    const detail = [{}];
    expect(parsePydanticError(detail, 422)).toBe("?: geçersiz");
  });

  it("Array elemani sadece loc (msg yok) -> 'gecersiz' default", () => {
    const detail = [{ loc: ["body", "symbol"] }];
    expect(parsePydanticError(detail, 422)).toBe("symbol: geçersiz");
  });

  it("Number/object detail -> HTTP <status> fallback", () => {
    expect(parsePydanticError(42, 500)).toBe("HTTP 500");
    expect(parsePydanticError({ foo: "bar" }, 500)).toBe("HTTP 500");
  });
});


describe("parseErrorBody — Response wrapper", () => {
  function makeResponse(body: unknown, status: number, contentType = "application/json"): Response {
    return new Response(
      typeof body === "string" ? body : JSON.stringify(body),
      { status, headers: { "Content-Type": contentType } }
    );
  }

  it("Valid JSON + detail array -> field-bazli mesaj", async () => {
    const res = makeResponse(
      { detail: [{ loc: ["body", "x"], msg: "must be > 0" }] },
      422,
    );
    expect(await parseErrorBody(res)).toBe("x: must be > 0");
  });

  it("Valid JSON + detail string -> dogrudan dondur", async () => {
    const res = makeResponse({ detail: "Bulunamadı" }, 404);
    expect(await parseErrorBody(res)).toBe("Bulunamadı");
  });

  it("Valid JSON + detail yok -> HTTP <status>", async () => {
    const res = makeResponse({ message: "Something else" }, 500);
    expect(await parseErrorBody(res)).toBe("HTTP 500");
  });

  it("Malformed body (JSON parse fail) -> HTTP <status>", async () => {
    const res = makeResponse("not valid json", 503, "text/plain");
    expect(await parseErrorBody(res)).toBe("HTTP 503");
  });

  it("Bos body -> HTTP <status>", async () => {
    const res = new Response(null, { status: 502 });
    expect(await parseErrorBody(res)).toBe("HTTP 502");
  });
});
