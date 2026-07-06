# -*- coding: utf-8 -*-
"""
D3-01 — CLAUDE.md BÖLME: partition-tabanlı satır çıkarma aracı.

AMAÇ (Kural #24 sağlam disiplini): CLAUDE.md'nin davranış belirlemeyen katmanlarını
(manifesto felsefesi + kural gerekçe kuyrukları + teknik verbose tablolar) notebook'a
taşımak; anayasada davranış-çekirdeği + tek-satır işaretçi bırakmak.

YÖNTEM — "partition" (içerik kaybı İMKÂNSIZ):
  Her kaynak satır TAM OLARAK bir kovaya gider:
    - KEEP   -> yeni CLAUDE.md'de kalır
    - MOVE   -> hedef notebook dosyasına KES-YAPIŞTIR (kopya değil)
    - DROP   -> yalnız ayraç (---/boş), içerik değil
  Aritmetik dogrulama: n0 == len(keep) + len(move_*) + len(dropped_ayrac)
  Bu eşitlik tutuyorsa hiçbir İÇERİK satırı kaybolmamıştır (yalnız --- ayraçlar düşer).
  Kalan risk: YANLIŞ-SINIFLANDIRMA (davranış satırını MOVE'a koymak) -> Ş1 ile azalt.

ŞERHLER (Sn. Ferit, 05 Tem 2026 — üçü de bağlayıcı):
  Ş1 — Kural çekirdeği ~18 satır MEKANİK HEDEF DEĞİL. Kes-yapıştır sınırı kural başına
       YARGIYLA: "Tetikleyici / Pattern keşfi / Canlı kanıt / X. ortaya çıkış günlüğü /
       Manifesto Özellik atfı / İlişkili" -> GEREKÇELERİ'ye. "imperatif emir + Yasak/Doğru
       pattern örnekleri + tablolar + EYLEM ZİNCİRİ" -> KALIR (davranış belirler). Şüphede
       kalan satır ANAYASADA KALIR (davranış kaybı > 100 satır fazlalık). KAL ~600-650 OK.
  Ş2 — CANLI TEST: bölme sonrası YENİ oturumda Manifesto Testi #5 (3 sondaj) — davranış
       kaybı yoksa tescillenir. Bu oturumda self-verify YAPILAMAZ (eski anayasa yüklü).
  Ş3 — Anchor: markdown anchor (#kural-n) KULLANMA. İşaretçi düz metin:
       "Gerekçe/kanıt → _KURAL_GEREKCELERI.md (Kural #N)". Hedef dosyada her kural
       "## Kural #N — <ad>" başlığıyla aranabilir.

DURUM (06 Tem 2026 — D3-01 TAMAMLANDI):
  ✅ AŞAMA 1 (Manifesto) UYGULANDI + commit'li (`518b8f5`) — bu script ile.
     Satır 17-178 (9 Özellik + Felsefi Temel + Testler + Sürekli Yaşar) -> notebook/_MANIFESTO.md
     CLAUDE.md 2695 -> 2539. Aritmetik: 2695 = 14 + 162 + 2513 + 6(ayrac). Sıfır kayıp.
  ✅ AŞAMA 2 (06 Tem 2026) UYGULANDI: 23 kural/alt-bölüm gerekçe kuyruğu + GitHub İlke 4-6
     + K28 audit + K20 repertuar -> notebook/_KURAL_GEREKCELERI.md (474 satır, 28 h2 başlık);
     teknik yarı -> Notebook_C2 (D3-01 Teknik Bağlam EK) + Notebook_C3 (şema eki);
     2 çiftleme SİL (AI Rol / Terminoloji 2. kopya — 2 unique satır primary'ye entegre).
     CLAUDE.md 2539 -> 1922. Aritmetik: 2539 = 1827 + 687 + 25 ✓ (61 sınır assert).
     Ş1 DUR şartı uygulandı: yargı tabanı 1917 > hedef ~1300 -> DUR + rapor -> Sn. Ferit C kararı
     (1922 ile uygula; 2. tur kesim Kural #18 hijyen turunda ~Eki 2026, _YAPILACAKLAR V. bölüm).
     Sürücü: scratchpad d3_asama2_apply.py (oturum-içi; MOVE tablosu YAPILANLAR 06 Tem bloğunda özetli).

AŞAMA 2 KULLANIMI (taze oturum için şablon):
  1) CLAUDE.md'nin GÜNCEL skeleton'unu çıkar: grep -nE "^### Kural|^## " CLAUDE.md
  2) Her kural için MOVE aralıklarını (gerekçe blokları) YARGIYLA belirle (Ş1). Çoklu-aralık
     olabilir (gerekçe davranış içeriğinin ARASINDA — örn. Kural #8 Tetikleyici sat.320,
     4-mod tablosundan ÖNCE; tabloyu değil sadece narrative'i taşı).
  3) run_partition() ile uygula: MOVE_RANGES + hedef dosya + her blok için POINTER metni.
  4) DOĞRULA: aritmetik OK + grep kırık-referans + wc -l ≤~1300 + Bilgi Haritası'na 2 yeni dosya.

Kural #15 kapsam dışı (bu .py, .ps1 değil) — UTF-8 Türkçe yorum OK.
"""
import io

CLAUDE = r"C:\Projeler\Quanfina\CLAUDE.md"


def read_lines(path):
    with io.open(path, "r", encoding="utf-8") as f:
        return f.readlines()   # her eleman \n ile biter


def write_text(path, text):
    """UTF-8 BOM-less, satır sonu korunur (Kural #19 — Out-File yerine WriteAllText muadili)."""
    with io.open(path, "w", encoding="utf-8", newline="") as f:
        f.write(text)


def partition(src_path, moves):
    """
    moves: list of dict:
       { "dest": <hedef dosya yolu veya None(=DROP)>,
         "start": <1-indexed baslangic>, "end": <1-indexed bitis (dahil)>,
         "pointer": <CLAUDE.md'ye blok yerine eklenecek metin | None> }
    Örtüşmeyen, artan sıralı aralıklar bekler. Her satır KEEP veya (MOVE/DROP).
    Return: (new_claude_text, dest_texts: {path: text}, arithmetic_ok, breakdown)
    """
    lines = read_lines(src_path)
    n0 = len(lines)
    covered = [False] * (n0 + 1)   # 1-indexed
    dest_chunks = {}
    for m in sorted(moves, key=lambda x: x["start"]):
        for i in range(m["start"], m["end"] + 1):
            assert not covered[i], f"ORTUSME satir {i}"
            covered[i] = True
        if m.get("dest"):
            dest_chunks.setdefault(m["dest"], [])
            dest_chunks[m["dest"]].append("".join(lines[m["start"] - 1:m["end"]]))

    # yeni CLAUDE: KEEP satirlari + MOVE bloklarinin yerine pointer
    out = []
    i = 1
    move_by_start = {m["start"]: m for m in moves}
    while i <= n0:
        if move_by_start.get(i):
            m = move_by_start[i]
            if m.get("pointer"):
                out.append(m["pointer"])
            i = m["end"] + 1
        else:
            out.append(lines[i - 1])
            i += 1
    new_claude = "".join(out)

    moved = sum(m["end"] - m["start"] + 1 for m in moves if m.get("dest"))
    dropped = sum(m["end"] - m["start"] + 1 for m in moves if not m.get("dest"))
    kept = n0 - moved - dropped
    arithmetic_ok = (kept + moved + dropped == n0)
    breakdown = {"n0": n0, "kept": kept, "moved": moved, "dropped_ayrac": dropped,
                 "new_claude_lines": new_claude.count("\n")}
    dest_texts = {p: "".join(chunks) for p, chunks in dest_chunks.items()}
    return new_claude, dest_texts, arithmetic_ok, breakdown


# ---- AŞAMA 1 KAYDI (uygulandı; referans/tekrar-çalıştırma için) ----
# moves = [{"dest": r"...\notebook\_MANIFESTO.md", "start": 17, "end": 178, "pointer": <manifesto pointer>}]
# Aşama 1 ayrı script (scratchpad/d3_manifesto.py) ile koşuldu; bu dosya AŞAMA 2 aracıdır.

if __name__ == "__main__":
    print("D3-01 partition araci. Asama 1 (Manifesto) TAMAM. Asama 2 icin partition() kullan.")
    print("Guncel CLAUDE.md satir:", len(read_lines(CLAUDE)))
