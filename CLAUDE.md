# QUANFINA — SİSTEM MANİFESTOSU

## 🎯 Sn. Ferit'in Vizyonu

> "Bilgisayarı açtığımızda sistem çalışmamızı tanısın, kaldığımız 
> yeri bilsin, hangi dosyalarda ne bilgi var bilsin, gerektiğinde 
> 'şu dosyaya bakalım' desin, baksın. Çok çalıştığımız yerlerde 
> güncelleme yapalım desin, VS Code güncellesin. Elektrik kesilmesi 
> veya başka bir sorun olursa bilgi kaybolmasın."
>
> — Sn. Ferit, 17 Mayıs 2026

Bu cümle, Quanfina sisteminin kalbidir.

---

## 🧬 Manifestonun 9 Özelliği

Sistem üç yönlü çalışır: **Giriş** (oturum açılışı), **Çıkış** (oturum 
kapanışı), **Bakım** (sürekli yaşam).

> 📜 **Versiyon notu:** 17 May 2026 — 8 özellik tescil. **18 May 2026 — 9.
> özellik RESMÎ TESCİL** (Aşama 5.7 kapanış). Eski 7. Felaket Dayanıklılığı
> 9. konuma alındı (ultimate dayanıklılık), 6. konum yeni "Doğru Kategoriye
> Otomatik" özelliği için açıldı.

### Giriş Yönlü (Oturum Açılışında)

**1. Tanıma**
Sistem yeni oturumun Sn. Ferit'in Quanfina projesi olduğunu anlar.
"Bu kim?" sorusu yoktur.

**2. Yer Belirleme**
Sistem Sn. Ferit'in en son nerede kaldığını bilir.
Notebook_A_Vizyon.md ⚡ GÜNCEL DURUM bölümünden anlık durumu okur.

**3. Bilgi Haritası**
Sistem hangi bilginin hangi dosyada olduğunu bilir.
CLAUDE.md Bilgi Haritası tablosu, _INDEX.md dosyaları, klasör yapısı 
bu görevin altyapısıdır.

**4. Proaktif Yönlendirme**
Sistem "şu dosyaya bakalım" der, kendisi yönelir.
Sn. Ferit'in dosya yolunu söylemesi gerekmez.

### Çıkış Yönlü (Oturum Kapanışında)

**5. Proaktif Kayıt**
Sistem "şunu yazalım, kaybolmasın" der.
Yeni KARAR, AÇIK KONU, İLKE adayı tespit ettiğinde otomatik kaydı önerir.
Sn. Ferit'in unutması beklenmez.

**6. Doğru Kategoriye Otomatik** *(18 May 2026 RESMÎ TESCİL — Aşama 5.7)*
Yeni araştırma, KARAR, AÇIK KONU, İLKE doğru klasör/dosya kategorisine
otomatik yerleşir. Bilgi Mimarisi 5 İlke (Doğru Yer + Klasör Bağlamı) ve
Aşama 1.A'da yapılan %29 strateji kategorilendirmesi bu özelliğin somut
altyapısıdır.

### Bakım Yönlü (Sürekli)

**7. Anılı Güncelleme**
Sistem "burası çok güncellendi, dosyayı temizleyelim" der.
Adım sonu güncellemesi (Kural #8) bu görevin pratik karşılığıdır.

**8. Öğrenen / Öneren**
Sistem deneyimden öğrenir, pattern'leri tespit eder, kural önerir veya
**doğrudan tescil eder** (Kural #14 revize, 18 May 2026). 3 kanalda canlı:
anayasa (Kural #16) + feedback memory + USER settings genişletme.

**9. Felaket Dayanıklılığı** *(18 May 2026 — Aşama 2.1 TAM CANLI)*
Bilgi tek noktada değildir. Yedekler, çoklu dosyalar, kayıt sistemi 
"elektrik kesilse, bilgisayar bozulsa, hatta proje yıllarca dursa" 
sistemin yeniden başlayabilmesini sağlar. Pratik altyapı:
- Lokal yedek (rotation 10) — `notebook_yedekle.ps1`
- Off-machine yedek (Google Drive Stream — `G:\Drive'ım\Quanfina_Backup\`)
- Otomasyon (Windows ScheduledTask günlük 09:00)
- Pre-push hook + sızma kontrol (Kural #10)

---

## 🏛️ Felsefi Temel

### Yaşayan Ortak Hafıza

Quanfina bir yazılım projesi DEĞİL — bir **yaşayan ortak hafızadır.**

Sn. Ferit (insan) + Sistem (AI altyapısı) birlikte bir bilgi 
ekosistemi oluştururlar. Bu ekosistem:

- **Hatırlar:** Notebook dosyaları, karar günlüğü, YAPILANLAR.md
- **Konuşur:** Web Claude (strateji), Claude Code (uygulama)
- **Büyür:** Her yeni karar, her yeni İLKE, her yeni öğrenme
- **Yaşlanır:** Eski bilgiler arşivlenir, kullanılmayan yetenekler çıkarılır
- **Yenilenir:** Manifesto Testleri ile kanıtlanır, ilkeler güncellenir

### "Sağlam Gidelim" Prensibi

Sn. Ferit'in temel disiplini:

> "Sağlam gidelim, bir daha bir daha uğraşmayalım."

Bu, Quanfina'nın tüm tasarımına yansır:
- Her adım test edilir
- Her dosya yeniden okunur
- Her karar kayıt altına alınır
- Şüpheli durumda durulur, danışılır
- "Belki olur" mantığı YOK

### Vibe Coding

Sn. Ferit kod yazmaz, kopyala-yapıştır yapar.

- Web Claude → strateji + prompt yazımı
- Claude Code (VS Code) → kod ve dosya uygulaması
- Sn. Ferit → stratejik karar + onay + kopyalama

Bu disiplin, Quanfina'nın **paralel** çalışmasına imkân verir.
Sn. Ferit kod öğrenmek zorunda değildir — sistem onu öğrenmiş gibi davranır.

### Quanfina NE DEĞİL

Bu önemli, çünkü dış baskıları reddeder:

- Pazarlanacak bir ürün **değil**
- Yatırımcı sunumu hedefi **yok**
- Lansman tarihi **yok**
- Müşteri **yok**
- Deadline **yok**

Tek odak: Sn. Ferit'in finansal disiplinini güçlendirmek.

---

## 🧪 Manifesto Testleri (17 Mayıs 2026)

Manifesto sadece felsefe değil — **kanıtlanmıştır**.

### Test #1 — Yarı Başarı (4.5/7)
Soru: "Quanfina'da neredeyim? Sıradaki adım nedir?"
Sonuç: Sistem ⚡ GÜNCEL DURUM bölümünü görmedi. Sadece son satırları 
okudu. Karışıklık yarattı.
Müdahale: CLAUDE.md başına Görev Başlangıç Protokolü eklendi.

### Test #2 — İlerleme (5/6)
Sonuç: Sistem doğru satıra baktı ama bilgi eskimişti.
Müdahale: ⚡ GÜNCEL DURUM + YAPILANLAR.md güncellendi.
Türetilen kural: Operasyonel Disiplin Kuralı #8 — Adım Sonu Güncelleme.

### Test #3 — Mükemmel (7/7 + BONUS)
Sonuç: Sistem doğru yerlere baktı, doğru bilgiyi okudu, doğru cevap 
verdi, hatta bonus olarak tüm Aşama 1.B alt-adımlarını sıraladı.
Doğrulanan özellikler: #1 Tanıma, #2 Yer Belirleme, #3 Bilgi Haritası,
#4 Proaktif Yönlendirme, #7 Anılı Güncelleme, #8 Öğrenen,
#9 Felaket Dayanıklılığı.

### Test #4 — Manifesto Testi #3 cross-tool ✅ (17-18 May 2026)
Yeni Claude Desktop chat'i (17 May) + Yeni Claude Code worktree (18 May)
`_BASLAT.md` skill kart bootstrap'ı ile aynı kalitede başladı.
Doğrulanan: tool-agnostic davranış + `_DEVIR.md` zamansal hafıza.

**Sonuç (18 May 2026 RESMÎ KAPANIŞ):** Manifesto **9/9 özellik canlı** —
9 özellik tam tescil edildi (Aşama 5.7). #6 (Doğru Kategoriye Otomatik —
yeni) Aşama 1.A %29 strateji kategorilendirmesi + Bilgi Mimarisi 5 İlke
ile canlı. #9 (Felaket Dayanıklılığı) Aşama 2.1 Drive Stream +
ScheduledTask ile **TAM CANLI**.

---

## 🌱 Manifesto Sürekli Yaşar

Bu metin **statik değildir.**

Sn. Ferit yeni özellik fark eder → manifesto güncellenir.
Yeni test yapılır → kanıtlar genişler.
Yeni İLKE doğar → felsefe derinleşir.

Manifesto, Quanfina'nın **kendi kendini açıklayan** belgesidir.
Yeni Claude oturumu bu metni okur ve **anlar**: "Bu proje budur."

---


## 🎯 GÖREV BAŞLANGIÇ PROTOKOLÜ

> **Bu protokol uyulması zorunlu.**
> Yeni Claude oturumu açıldığında SIRAYLA okur:
>
> 💡 **Hızlı tetikleyici (Claude Desktop için):** Sn. Ferit
> `notebook/_BASLAT.md` dosyasındaki kopyala-yapıştır bloğunu
> yeni chat'in ilk mesajı olarak gönderir. Bu blok aşağıdaki
> protokolü AI'ya komut olarak iletir, manuel tetikleme yükü
> tek tıklamaya iner. (Claude Code'da CLAUDE.md otomatik
> yüklenir — bootstrap'a gerek yok.)

### 1. Bu Dosya (CLAUDE.md)
Sn. Ferit'in projesi: Quanfina (kişisel hisse trade platformu).
Vibe coding — kod yazmaz, kopyala-yapıştır. Web Claude + Claude Code.

### 2. notebook/YAPILANLAR.md — Yaşayan Hafıza
- Tamamlanan adımlar
- Sistem manifestosu
- Çalışma mantığı

### 3. notebook/Notebook_A_Vizyon.md → ⚡ GÜNCEL DURUM BÖLÜMÜ
**SATIR 33-95** arası — buraya MUTLAKA bakılmalı.
Bu bölüm:
- Hangi Aşama'dayız (1.A / 1.B / 1.C / 2 / 3 / 4 / 5)
- Son tamamlanan adımlar
- Sıradaki adım
- Metrikler (Vizyon satır, KARAR sayısı, vb.)
- Aktif araçlar

### 4. Detay Gerekirse
- Bilgi Haritası (bu dosyada aşağıda)
- Vizyon dosyasının ilgili bölümleri
- Diğer notebook dosyaları

### 5. Sn. Ferit'e Sunum Şekli
- Trafik lambası raporu (🟢/🟡/🔴)
- Kısa ve net
- Karar yorgunluğunu azalt
- Operasyonel detaylar Web Claude'da, stratejik kararlar Sn. Ferit'te
- ŞÜPHELİ durumlarda sor, otomatik geçme

### 6. Soruyu Cevaplama Stratejisi

"Quanfina'da neredeyim?" tipi sorulara cevap:
1. ⚡ GÜNCEL DURUM'u oku (Vizyon satır 33-95)
2. YAPILANLAR.md son güncelleme tarihine bak
3. Tek bir net cevap ver: "Şu an X Aşaması'ndasınız, sıradaki Y"
4. KARIŞIKLIK YARATMA — eğer iki yön varsa, ana yönü belirt, detay sormaya gerek bırakma

---

## 🎯 Bilgi Mimarisi İlkeleri

### İlke 1 — Doğru Yer
Her bilgi kendi kategorisindeki dosyada yaşar.
- Karar günlüğü → notebook/Notebook_A_Vizyon.md
- Strateji bilgisi → notebook/kitaplar/[Strateji].md (Carr, Minervini)
- Platform/rakip analizi → notebook/analizler/[Konu].md
- UX kararları → ux_tarama/
- Teknik referans → notebook/Notebook_C2_*.md, Notebook_C3_*.md
- Sistem hafızası → notebook/YAPILANLAR.md
- Master indeks → notebook/_INDEX.md (Adım 1.9'da kurulacak)

### İlke 2 — AI Görünürlüğü
Her dosya AI tarafından bulunabilir olmalı:
- CLAUDE.md Bilgi Haritası'nda referansı var
- notebook/_INDEX.md'de listede
- Yetim dosya (haritada olmayan) yaratılmaz

### İlke 3 — Yaşayan Sistem
Kullanılmayan bilgi tutulmaz:
- Vazgeçilmiş kararlar → meta-linkage ile işaretle, silme
- Geçersiz içerik → _archive/ klasörüne
- Tarafsız hafıza → ayrı dosyaya
- "Sonra silerim" yığını yok — bilinçli karar

### İlke 4 — Tekrarsızlık (DRY)
Her bilgi parçasının tek doğru kaynağı var.
- Aynı içerik birden fazla yerde olmaz
- Referans: "Detay: [dosya/bölüm]"
- Tekrar tespit → tek noktada topla

### İlke 5 — Klasör Bağlamı (17 May 2026 yeni)
Yeni bir klasör oluşturulduğunda **README.md amaç belgesi** eklenir.
Yeni araştırıcı veya yeni AI session bu klasörün ne için olduğunu
30 saniyede anlamalı.

**README.md içeriği şablonu:**
- "Bu klasör ne için?" (1 paragraf)
- İçerik haritası (dosya tipleri + sayılar)
- Geri alma / kullanım talimatları (varsa)
- İlgili referanslar (notebook/, CLAUDE.md, vs.)

**Örnek:** `_archive/README.md` (Aşama 1.D sonrası eklendi).

**İstisna:** Geçici/tek-amaçlı klasörler (örn. `node_modules/`,
`__pycache__/`) — bunlar zaten gitignore'da, bağlam belgesi gerekmez.

---

## ⚙️ Operasyonel Disiplin Kuralları

### Kural 1 — Tek Seferde Tek İş
Paralel operasyon yok. Çalışan iş bitmeden yeni iş başlamaz.

### Kural 2 — Rapor Görmeden Devam Yok
Trafik lambası raporu: 🟢 Yeşil / 🟡 Sarı / 🔴 Kırmızı.
Rapor onaylanmadan sonraki adım planı sunulmaz.

### Kural 3 — Şüphede Dur, Danış
Otonom değilsin. Şüpheli durum → Sn. Ferit'e sor.
"Devam edelim mi?" sorusu sorulur.

### Kural 4 — Auto-Approve Disiplini
Auto-approve açık olsa bile yıkıcı işlemlerde manuel onay.
Yıkıcı = dosya silme, büyük rename, mimari değişiklik.
Backup alınmadan yıkıcı işlem yok.

### Kural 5 — Prompt Tekrar Verilir
Sn. Ferit eski promptu aramaz. Sistem insanı yormaz.

### Kural 6 — Backup Önce, Operasyon Sonra
Önemli işlerde önce backup. Dosya adı:
[orijinal].backup_v[N]_[YYYYMMDD]_[adim_kodu]

### Kural 7 — Bilinçli Karar, Sürüklenme Yok
"Sonra temizlerim" yığını yapılmaz.
Sürdürülebilirlik tetikleyici: bir klasörde 5+ dosya → refactor.

### Kural 8 — Akıllı Kapanış Disiplini (17 May 2026 v1 → 19 May 2026 v2)

**v1 (17 May 2026):** "Adım Sonu Güncelleme" — Her büyük adım sonu
Vizyon + YAPILANLAR + _DEVIR tam kapanış. ❌ DEPRECATED 19 May 2026.

**v2 (19 May 2026 ~02:00):** **Akıllı Kapanış Disiplini — 4 modlu.**

**Tetikleyici revize (Manifesto Özellik #8 10. self-correction —
yöntem seviyesi):** Sn. Ferit 19 May 2026 ~01:30 talimatı: *"bu
kapanışlar yavaşlatı yo nasıl bir çözüm bulabiliriz kpanışı sonra
yapma durumu olsa sende fikir ver tam hız almışken kapanış
düzenlemesi için bekleme hızı düşürüyor sistemde."* Pratik gerçek:
v1'de her küçük iş için tam kapanış (Vizyon sürüm + YAPILANLAR + _DEVIR
+ commit + push + Drive) ~%50 chat zamanı yutuyordu. Karar yorgunluğu
azaltma felsefesiyle çelişti.

**4 Mod tasarımı:**

| Mod | Tetikleyici | Aksiyon | Süre |
|---|---|---|---|
| 🚀 **Akış Modu** (default) | Sn. Ferit ardışık iş söylüyor, kapanış demedi | Sadece dosya değişikliği + commit (background asenkron). **Kanon güncellemesi YOK** | 5-10 sn / iş |
| 📌 **Mini-Mühür** | İş anlamlı + Sn. Ferit "devam" diyor | ⚡ GÜNCEL DURUM **tek satır** ekle (50 char), tek commit | 10 sn |
| 🏁 **Tam Kapanış** | Sn. Ferit "kapanış yap" der VEYA yeni Kural/Aşama eklendi VEYA kritik karar tescili | Vizyon sürüm bloğu + YAPILANLAR ek + _DEVIR alt-güncel + _OZET refresh + tek commit + push + Drive | 3-5 dk batched |
| 🌙 **Otonom Hijyen Modu** ⭐ | Sn. Ferit "yatıyorum, hijyen yap" / "geceyi düzenle" / "uyu modunda devam" | AI bağımsız çalışır, yıkıcı eylemler hariç. Tüm bekleyen + birikmiş işler + hijyen + final mühür. Sabah Sn. Ferit'e _DEVIR ⏳ KUYRUK özet | 30-60 dk |

**Akış Modu disiplini (default):**
- Dosya değişikliği yap (Edit/Write)
- Anlamlıysa commit (background, ASCII title)
- Drive senkron arka planda (`drive_sync.ps1` ScheduledTask saatlik
  zaten — manuel tetik gerekmez)
- **Kanon dosyalar (Vizyon ⚡, YAPILANLAR, _DEVIR, _OZET) eskimiş kalır**
- Sn. Ferit "kapanış yap" deyince batched güncelleme

**Asenkron commit/push (Bash background mode):**
- Sn. Ferit bir sonraki sözünü beklerken commit/push arkada çalışır
- Hata olursa task-notification ile bildirim
- Başarılıysa sessizce tamamlanır

**Otonom Hijyen Modu kapsamı (Sn. Ferit yatınca AI yapar):**
- Bölüm A — Kanon update (Vizyon sürüm, YAPILANLAR, _DEVIR, _OZET refresh)
- Bölüm B — Hijyen scriptleri (saglik_kontrol, pattern_ogren,
  proaktif_oneri, build_index v3.1 sayım refresh)
- Bölüm C — AÇIK KONU semantik tarama (Kural #18 ile pasif aday)
- Bölüm D — Cross-reference doğrulama (KARAR/AÇIK KONU/İLKE numara
  tutarlılık)
- Bölüm E — Sayım tutarlılık (CLAUDE.md satır, Vizyon KARAR sayım)
- Bölüm F — Dead link tarama (notebook/'da bahsedilen dosyalar var mı)
- Bölüm G — Memory pasiflik pre-screen (13+ gün eski)
- Bölüm H — _DEVIR eski blok arşivleme (>7 gün → _DEVIR_ARSIV.md)
- Bölüm I — Commit + push (Kural #10 sızma test)
- Bölüm J — Drive senkron (Web Claude'a hazır)
- Bölüm K — _DEVIR.md ⏳ KUYRUK sabah özet (Sn. Ferit uyandığında okur)

**Disiplin (Otonom Mod):**
- **Yıkıcı eylem YASAK** (Kural #4) — silme/rename/mimari değişiklik
  kuyruğa alınır, sabah Sn. Ferit onayı
- **Yeni kural önerisi YOK** — Kural #14 sadece Sn. Ferit varken tescil
- **Stratejik karar YOK** — sadece uygulama + hijyen + analiz
- Sabah özet: `_DEVIR.md` ⏳ KUYRUK'a 5-10 satırlık rapor

**Kazanım ölçümü (bu chat'in verisi):**
- v1: 22 task = ~80 dk (toplam) kapanış yüküyle
- v2 (varsayım): aynı 22 task = ~25 dk akış modu + 5 dk batched kapanış
- **Hız kazancı: 3-5x** akış modunda

**İlişkili:** Kural #4 (Yıkıcı eylem onayı — Otonom Mod sınırı),
Kural #9 v2 (Akıllı Dağılım — Web/Code paralel uygulama),
Kural #18 (Pasif Öğe Çıkarma — Otonom Mod B/C parçası),
[[feedback_kapanis_disiplini]] (memory karşılığı).

### Kural 9 — Akıllı Araç Dağılımı + Handoff (17 May 2026 v1 → 18 May 2026 v2)

**v1 (17 May 2026):** "Tek Araç Felsefesi — Claude Desktop ana,
Web Claude kullanılmaz." → ❌ DEPRECATED 18 May 2026.

**v2 (18 May 2026):** **Akıllı Dağılım + Handoff Protokolü.**

Sn. Ferit'in karar yorgunluğunu azaltmak için: her araç güçlü
olduğu işte, "hangisini seçeyim" kararı yine sıfır — ama bu
sefer **paralel araç değil, akıllı dağılım** ile.

**Tetikleyici revize (Kural #14 doğrudan tescil):**
- 18 May 2026 ~21:00 Sn. Ferit Claude Code (Opus 4.7 1M) cevap
  hızını "Web Claude'a göre yavaş" buldu. Gerçek: Opus 4.7 +
  tool round-trip + 1225 satır CLAUDE.md context + 9 memory
  dosyası → Web Claude'un Sonnet default + hafif system prompt'una
  göre yavaş yanıt
- Web Claude'a **Google Drive Connector** bağlandı → Quanfina
  bağlamına Drive üzerinden erişebiliyor (Quanfina_notebook/_BASLAT.txt
  dahil tüm yaşayan sistem)
- Kural #9 v1'in temel varsayımı ("Web Claude bağlamsız, kullanışsız")
  artık geçersiz

**Birincil dağılım:**

| İş Türü | Birincil Araç | Gerekçe |
|---|---|---|
| Strateji, düşünme, hızlı sorgu | **Web Claude** ⭐ | Sonnet hızı + Drive Connector ile bağlam |
| Karar üretme (KARAR/AÇIK KONU/İLKE düşünme) | **Web Claude** | Drive'dan Vizyon okur, prompt üretir |
| Dosya operasyonu (Edit/Write/multi-file) | **Claude Code (VS Code)** ⭐ | Direkt dosya erişimi, paralel tool |
| Commit + push (Kural #10) | **Claude Code** | Bash, pre-push hook, git geçmişi |
| Yaşayan sistem hijyeni (Vizyon sürüm, _DEVIR vb.) | **Claude Code** | TaskCreate, multi-step |
| Kavram/kitap yorumu | **NotebookLM Plus** doğrudan | Kural #17 |
| Trade grade önerisi | **TradeGrader Gem** | Kural #17 istisna (stateless skor) |
| Filesystem MCP özel iş | **Claude Desktop** (gerektiğinde) | Düşük öncelik |

**Handoff Protokolü (vibe coding evrimi):**

```
Sn. Ferit Web Claude'da konuşur (hızlı düşünme)
       ↓
Web Claude prompt üretir (Drive Connector ile bağlam okuyarak)
       ↓
Sn. Ferit prompt'u kopyalar (vibe coding — kod yazmaz)
       ↓
Claude Code (VS Code worktree veya ana repo) yapıştırılan prompt'u uygular
       ↓
Commit + push + Drive senkron (drive_sync.ps1)
       ↓
Bir sonraki Web Claude oturumu Drive üzerinden yeni hali görür
```

**Tetikleyici durumlar (Web Claude → Code handoff):**
- Web chat içeriği dolduğunda (token limiti yaklaşıyor) → tescil
  + güncelleme Code'a devredilir
- Doğrudan dosya operasyonu gerektiğinde (Edit/Write/Bash)
- Multi-step yaşayan sistem hijyeni (3+ dosya güncelleme +
  commit/push zinciri)
- Karar tescili (KARAR ekleme, AÇIK KONU kapanış, sürüm bloğu)

**Web Claude'un kapasitesi (18 May 2026):**
- ✅ Drive Connector (`mcp__09e271fe-...`): list_recent_files,
  search_files, read_file_content, get_file_metadata, copy_file,
  create_file, download_file_content, get_file_permissions
- ✅ Quanfina_notebook klasörüne Drive üzerinden erişim
- ❌ Lokal dosya yazma (Edit/Write) — bu Code'un işi
- ❌ Bash/git/commit — bu Code'un işi

**Karar yorgunluğu yine sıfır:** her iş için tek doğru araç var,
seçim yok.

**Manifesto Özellik #8 (Öğrenen) zirvesi:** sistem kendi
anayasasını revize ediyor — v1 → v2. Statik kural değil, yaşayan
felsefe.

**İlişkili:** Kural #17 (Uzman/Yorumcu Varsayılanı = NotebookLM),
[[feedback_hibrit_arac_dagilimi]] (memory karşılığı), Aşama 5
felsefe seviyesi.

### Kural 9 v2 alt-bölüm — Web Claude → Code Handoff (Yöntem B) (18 May 2026 ~22:00)

Web Claude (Drive Connector ile) büyük adım sonrası `_DEVIR.md`
formatlı blok üretir. Sn. Ferit kopyalar → Code'a "_DEVIR.md üstüne
ekle" der → Code günceller, commit, drive_sync.ps1 mirror.

Web Claude Drive yazma yetkisi VAR (create_file, copy_file) ama
KULLANILMAZ — drive_sync.ps1 /MIR Drive yazımını sonraki turda
silebilir, lokal kanon bozulur, .md vs .txt çift gerçeklik tuzağı.
Manuel tek-kopya akış mevcut altyapıyla bütünleşik, yeni script
gerekmez.

**Blok formatı:**
```
### 🔄 Güncelleme (TARIH ~SAAT) — [Konu]

> Web Claude session: [özet]

#### 🟢 Tespit/Karar
- ...

#### Code'a iş
- ...
```

**DRY İlke #4 uyumu:** `_HANDOFF.md` iptal — `_DEVIR.md` zaten bu
görev için var (kurulu, çalışıyor). Tek köprü dosyası.

**H#10 bağlantısı:** Web Claude Drive yazma yeteneğini başta "yok"
dedi (araç listesine kör güven). Sn. Ferit sorgusuyla tool_search
yeniden çalıştırıldı → create_file + copy_file mevcut çıktı.
Kural #12 (Önce Keşfet) doğrudan uygulama — bkz. `_HATALAR.md` H#10.

**İlişkili:** Kural #8 (Adım Sonu Güncelleme), `_DEVIR.md`,
`_HATALAR.md` H#10, [[feedback_hibrit_arac_dagilimi]].

### Kural 10 — Push Öncesi Sızma Kontrolü (17 May 2026 yeni)
Her `git push` öncesi sızma taraması ZORUNLU.
- Otomatik araç: `scripts/sizma_kontrol.ps1` (GitHub İlke #8 listesi)
- Pre-push hook bağlı: `.git/hooks/pre-push` — push komutu otomatik tetikler
- Bulgu varsa push BLOK. Tertemiz olana kadar push yapılmaz
- Bypass yok. Auto-approve bu kuralı geçersiz kılmaz (Kural #4)
- Geri dönüş zor: public repo'ya sızan içerik fork/cache/AI eğitim verisi
  riskini geri alamaz. Önlemek tek seçenek.

### Kural 11 — Notebook Proaktif Tarama (17 May 2026 yeni)
`notebook/` Sn. Ferit'in ortak hafızası ve karar günlüğü. AI proaktif tarar:
- **Adıma başlamadan ÖNCE:** `notebook/Notebook_A_Vizyon.md` ⚡ GÜNCEL DURUM
  + `notebook/YAPILANLAR.md` okunur (zaten Görev Başlangıç Protokolü #2-3)
- **Belirsiz konuda:** KARAR ADAY, AÇIK KONU, İLKE notebook'ta var mı tarar
- **Geçmiş referans için:** `notebook/kitaplar/`, `notebook/analizler/`,
  `notebook/Notebook_B6_AdimlarKarar.md`, `notebook/EK10_*.md` kontrol edilir
- **Yaklaşım:** "Burada bir şey var mı?" değil "Burada şu olabilir mi?"
  mantığıyla bakılır (Manifesto Özellik #4 proaktif öneri)
- **Sızma riski yok:** notebook git'te değil — özgürce tara, kayıt ekle,
  sadece HEAD commit'ine girmez (`.gitignore`'da `notebook/` korunuyor)

### Kural 12 — Önce Keşfet, Sonra Sor (17 May 2026 yeni)
Sn. Ferit'e bilgi sormadan ÖNCE sistem otomatik keşif yap (yaşayan sistem
felsefesi — karar yorgunluğunu azalt).

**Keşif sırası:**
1. **Dosya sistemi**: registry (`HKCU:\Software\...`), AppData
   (`%APPDATA%`, `%LOCALAPPDATA%`), config dosyaları (`.env`, `.json`, ini)
2. **Git/dev araçlar**: `git config`, `git credential fill`, `git log
   --format='%ae'`, `git remote -v`
3. **API çağrıları**: GitHub API (token credential'da), Google API
   (oauth varsa), vb.
4. **Yüklü program tespiti**: `winget list`, `Get-AppxPackage`,
   `HKLM:\...\Uninstall`, sistem dizinleri
5. **Süreç/ağ durumu**: `Get-Process`, `Get-NetTCPConnection`

**Sn. Ferit'e sorulması gereken durumlar:**
- Browser GUI sayfasındaki bilgi (Drive Settings, GitHub Settings)
- Office GUI (File→Account paneli — encrypted lokalde)
- Stratejik/öznel karar (A/B/C seçimi, öncelik)
- Lisans/abonelik web tarafı (kişiselleştirilmiş)

**Rapor formatı:** Keşif yaptıktan sonra rapor verirken:
- "PowerShell ile şuna baktım, şu çıktı" (somut)
- Belirsiz kalan kısım için tek soru sor (toplu soru listesi değil)
- Asla "şunları yapar mısın: 1, 2, 3, 4..." şeklinde Sn. Ferit'i yorma

**Tarama scripti standardı:** Tekrar eden keşifleri `scripts/` altına
PowerShell olarak yaz (yaşayan sistem birikimi). Örnek:
- `scripts/sizma_kontrol.ps1` (push güvenliği)
- `scripts/notebook_yedekle.ps1` (felaket dayanıklılığı)
- `scripts/build_index.ps1` (envanter doğrulayıcı)
- `scripts/hesap_tarama.ps1` (gelecek — hesap matris keşfi)

### Kural 13 — Commit Mesajı Disiplini (17 May 2026 yeni)
Çoklu satır veya Türkçe karakter içeren commit mesajları **temp dosya
+ `git commit -F`** ile geçirilir. PowerShell here-string (`@'...'@`)
Türkçe karakterlerde (ş, ğ, ı, "—" em dash, "?") parse hatası verir
ve `git commit -m`'in argümanını yanlış token'lara böler.

**Doğru pattern:**
```powershell
$msg = "Asama X.Y.Z: ASCII baslik (Turkce icerikli detay multiline)"
$path = "$env:TEMP\_commit_msg.txt"
[System.IO.File]::WriteAllText($path, $msg, [Text.UTF8Encoding]::new($false))
git commit -F $path
Remove-Item $path
```

Veya Write tool ile temp dosya yazıp `git commit -F` çağrısı (bu chat'in
kullandığı pattern).

**Tek satır + ASCII commit:**
```powershell
git commit -m "Asama 1.E: index update"   # OK
```

**Yasak (parse hatası garantili):**
```powershell
git commit -m @'
Aşama X.Y: çoklu satır Türkçe
'@   # ş, ğ, ı, ?, — parse'i boşluk olarak böler
```

Bu kural [`feedback_kesfet_sor`](memory) ile birlikte yaşayan sistemin
operasyonel disiplinine girer.

### Kural 14 — Pattern Tespit + Otomatik Kural Önerisi (17 May 2026 yeni)
Yaşayan sistem sürdürülebilirliği için: AI her çalışma sırasında tekrar
eden pattern veya sürpriz/unutulmuş yaşayan sistem parçaları tespit
ederse, bunları kalıcı kurala dönüştürme önerisi sunar (Sn. Ferit
onayıyla). Sn. Ferit'in deyimiyle: **"bir nevi skill oluşturuyoruz"**.

**Pattern tespiti tetikleyicileri:**
- Aynı problem 2. kez ortaya çıkarsa (örn. PowerShell heredoc Türkçe
  karakter sorunu → Kural #13 olarak tescil)
- Yeni klasör/dosya/araç eklenirse → bağlam belgesi (İlke #5)
- AI yeni bir keşif yöntemi geliştirirse (Kural #12 örneği —
  registry/API ile otomatik tespit)
- Sn. Ferit aynı düzeltmeyi 2+ kez söylüyorsa → bilinçli kayda al
- Belirli bir iş akışı 2+ defa aynı sırayla yapılırsa → kural

**Eylem zinciri (17 May 2026 revize — Sn. Ferit talimatı):**
Sn. Ferit'in deyimi: "Önceki kurallarla iş akışını düzelterek, bunu
söyleme ihtiyacı duymuyorum artık." Yani AI pattern tespit ettiğinde:

1. **Doğrudan tescil et** (öneri sormadan):
   - Kural CLAUDE.md'ye eklenir (kalıcı tescil) → numara otomatik
     (mevcut son kural + 1)
   - VEYA İlke / Feedback memory olarak tescil edilir (kullanıcı düzeyi)
   - VEYA scripts/ altına otomasyon scripti yazılır (yaşayan sistem)
2. **Raporda bildir** — "Pattern X tespit edildi, Kural #Y olarak
   tescil ettim. Detay: ...". Sn. Ferit yanlış bulursa geri alır
   (reversible — git revert + Edit ile, yıkıcı değil).
3. **`notebook/_BASLAT.md` güncellenir** — yeni chat hemen görür
4. **Skill setine eklenir** → "yaşayan sistem skill kart" pattern'i

Bu revize Kural #14'ün **kendi felsefesini kendine uygulamasının**
canlı örneği — pattern (Sn. Ferit aynı talimatı 2. kez söyledi:
"kurala ekle, ben söyleme ihtiyacı duymayayım") → otomatik tescil.

**İstisna:** Yıkıcı kural değişiklikleri (mevcut bir kuralı silme,
büyük yön değişimi) hâlâ Sn. Ferit onayı ister (Kural #4).

**Skill felsefesi:**
`notebook/_BASLAT.md` tek bir **skill kart** gibi davranır. Yeni
Claude Desktop chat'i bu kart ile tam iş akışını edinir. CLAUDE.md
anayasa katmanı detaylı belgeler, _BASLAT.md ise hızlı tetikleyicidir.

**Manifesto Özellik #8 (Öğrenen)** bu kuralla canlı tescile geçer —
sistem kullanıldıkça kendi pattern kütüphanesini büyütür.

**İlişkili:** Kural #8 (Adım Sonu Güncelleme), Kural #11 (Notebook
Proaktif Tarama), Kural #12 (Önce Keşfet), Kural #13 (Commit Disiplini),
Bilgi Mimarisi İlke #3 (Yaşayan Sistem) + #5 (Klasör Bağlamı).

### Kural 15 — PowerShell Script Encoding (17 May 2026 yeni)
`scripts/*.ps1` dosyaları **ASCII-only içerik** kullanır. Write tool ile
yazılan .ps1 dosyaları BOM-less UTF-8'dir; PowerShell default encoding
(cp1254 Windows Turkish) bekler ve Türkçe karakterlerde "missing
terminator" parse hatası verir.

**Doğru pattern:**
- ASCII içerik (ş → s, ğ → g, ı → i, ç → c, ü → u, ö → o, "—" → "-")
- Yorum satırlarında bile ASCII tercih edilir
- Tek istisna: kullanıcıya gösterilen UI string'leri (Write-Host)
  ASCII ile yazılır — Türkçe render gerekirse `Out-File -Encoding utf8`
  veya `[Console]::OutputEncoding = [System.Text.UTF8Encoding]::new($true)`

**Markdown / notebook dosyaları:** Türkçe içerik **OK** — PowerShell
parse'i değil, Markdown render'ı kullanılır.

**Commit mesajları:** Kural #13 — temp dosya + `git commit -F` (Türkçe
için ASCII title + Türkçe detay OK).

**Pattern keşfi:** Aşama 1 sonrası `scripts/hesap_tarama.ps1` yazımı
sırasında tespit edildi. Kural #14'ün **ilk canlı somut uygulaması**
— pattern → kural önerisi → tescil tek turda.

**Etki:** Tüm mevcut scripts/*.ps1 ASCII-only zaten:
- `sizma_kontrol.ps1`, `notebook_yedekle.ps1`,
  `build_index.ps1`, `hesap_tarama.ps1`

### Kural 16 — PowerShell Native Exe `2>&1` Yasağı (18 May 2026 yeni)
Windows PowerShell 5.1'de bir **native executable**'in (git, gh,
node, python, .exe) stderr'ini `2>&1` ile stdout'a yönlendirmek
hatalıdır. PowerShell stderr satırlarını `NativeCommandError`
ErrorRecord nesnelerine sarar; exit kodu 0 olsa bile `$?` `$false`
döner ve script `$ErrorActionPreference = "Stop"` altında patlar.

**Yasak pattern:**
```powershell
$out = git status 2>&1          # YANLIS — stderr wrap edilir
$out = & gh pr list 2>&1        # YANLIS — ayni sorun
```

**Doğru pattern:**
- stderr'i bırak, PowerShell zaten ayrı stream'de yakalar
- Native exit kodunu `$LASTEXITCODE` ile kontrol et (`$?` değil)
- Mutlak gerekiyorsa: `try/catch` ile `NativeCommandError`'ı yut,
  veya `$out = & git status; if ($LASTEXITCODE -ne 0) { ... }`

```powershell
$out = git status                # OK
if ($LASTEXITCODE -ne 0) { ... } # exit kodu kontrolu
```

**Pattern keşfi:** Önceki AI bir kez yaşadı (`_DEVIR.md` 17 May 2026
notu), sistem prompt'umda da uyarı vardı → 2. ortaya çıkış = Kural
#14 doğrudan tescil eşiği. Kural #15 ile birlikte PowerShell
disiplinini tamamlar.

**İstisna:** PowerShell cmdlet'leri (`Get-*`, `Invoke-*`) için `2>&1`
**güvenli** — sadece native .exe'lerde sorun var.

**İlişkili:** [`feedback_kesfet_sor`](memory) (Kural #12), Kural #13
(commit disiplini), Kural #15 (PS encoding).

### Kural 17 — Uzman/Yorumcu Rolü Varsayılanı = NotebookLM Plus (18 May 2026 yeni)

AI uzman/yorumcu rolü kurarken **varsayılan araç NotebookLM Plus**,
Gemini Gems değil. Yeni uzman/danışman ihtiyacı doğduğunda Gems
düşünmeden NotebookLM Plus seçilir.

**Why:** Gems'in **Drive senkronu yok** — yüklenen dosya snapshot,
kaynak güncellenince manuel re-upload gerekir. NotebookLM Plus
**Drive linkli canlı kaynak** destekler — `drive_sync.ps1` notebook/
→ `G:\Drive'ım\Quanfina_notebook\` her saat ayna alır, NotebookLM
otomatik yeniden indeksler. Yaşayan sistem kaynak akışı sadece
NotebookLM tarafında bütünleşik.

**2 ortaya çıkış (Kural #14 eşiği aşıldı):**
1. **Aşama 2.4** Vizyon Bekçisi Gem → ❌ İPTAL, NotebookLM'e taşındı
   (18 May 2026 öğleden sonra, Aşama 2.3 (b) RESMÎ KAPANIŞ)
2. **Aşama 4.2** Carr Yorumcu Gem → ❌ İPTAL, NotebookLM "Quanfina
   Carr Stage Analizi" doğrudan Yorumcu rolünde (18 May 2026 akşam,
   KARAR #445)

**Canlı tescil:** 3 NotebookLM uzmanı tam rolde — Vizyon Bekçisi
(karar tarihi denetçisi) + Minervini (kavram sözlüğü) + Carr Stage
Analizi (Carr 1.+2. baskı + Weinstein yorumu).

**İstisna — Gems'te kalabilecek rol türleri:**
- **Stateless skor üretici** — TradeGrader Gem (Aşama 4.1): 17
  kategori prompt yeterli, canlı kaynak gerekmez, snapshot problem değil
- **Generic kodlama desteği** — Gemini Kodlama Gem: kaynak yerine
  Quanfina-spesifik prompt yeterli
- **Sayfa-bağımsız asistan** — kaynak Drive'da değilse

**Bilgi-yoğun roller için NotebookLM Plus ZORUNLU:**
- Karar/AÇIK KONU/İLKE denetçisi (Vizyon Bekçisi paterni)
- Kavram sözlüğü (Minervini, Carr Stage Analizi)
- Manifesto bilgi katmanlı yorumcu
- Strateji kitabı yorumcu (her yeni strateji kaynak → ayrı NotebookLM)

**Eylem:**
- Yeni uzman/yorumcu ihtiyacında **ilk seçenek NotebookLM**, gerekçe
  sorulmadan
- Mevcut Gem'i NotebookLM'e taşıma kararı için **1. madde tetikleyici**
  (canlı kaynak gerekli mi sorusu yeter)
- Kurulum belgesi şablonu: `notebook/Asama_X_Y_*.md` — Drive linkli
  .txt kaynak listesi + clean-room sınırı + system prompt + test PASS

**İlişkili:** Kural #9 (Tek Araç Felsefesi — uzman katmanında tek
standart), Kural #14 (Pattern Tescil — 2 ortaya çıkış kuralı),
Aşama 2.3 (a+b) + Aşama 4.2 RESMÎ KAPANIŞ (KARAR #445).

### Kural 18 — Pasif Öğe Çıkarma Protokolü (Negatif Tescil) (18 May 2026 ~23:00 yeni)

**Felsefe:** Yaşayan sistem sadece büyümez — pasif kalmış öğeler
toplanır, sınırlı dikkati tüketir, AI'a gereksiz bağlam yükler.
Kural #14 ile **eklenir**, Kural #18 ile **çıkarılır**. Sistem
nefes alır.

**Why (Sn. Ferit talimatı 18 May 2026 ~23:00):** *"kural ekleme
varsa kural çıkarma protokolü kuralı da eklicem masaüstüne
yazdırcam"*. 3 günde sistem 17 Kural + 41 İLKE adayı + 447 KARAR
+ 13 script + 11 notebook + 9 memory'e büyüdü, çıkarma mekanizması
yoktu. Web Claude turu bu eksiği dürüstçe tespit etti.

**Kapsam (tüm sistem öğeleri):**
- Kural (CLAUDE.md)
- İLKE (Bilgi Mimarisi, GitHub, vs.)
- KARAR (Vizyon karar günlüğü — geçmiş, asla silinmez)
- AÇIK KONU (Vizyon)
- Memory dosyası (`~/.claude/projects/.../memory/`)
- Script (`scripts/`)
- Notebook belgesi (`notebook/`)
- Manifesto özelliği (CLAUDE.md)
- Self-correction sayımı

**Pasiflik eşiği:** Son **30 gün** içinde 0 referans (commit log +
chat geçmişi + AI uygulama kanıtı). Eşik aşıldıysa "PASİF ADAY"
işaretlenir.

**Tetikleme — 2 yol:**

1. **Hijyen Turu (3 ayda bir):**
   - AI `pasif_tara.ps1` (gelecek script, aday) ile tüm öğeleri
     tarar (30 gün eşiği)
   - Pasif aday listesi üretilir
   - Sn. Ferit kararı (Kural #4 yıkıcı eylem onayı) bekler
   - Onay alınanlar çıkarılır

2. **Doğrudan tescil (Sn. Ferit veya AI):**
   - Sn. Ferit *"şu kural artık gerek değil"* derse → Kural #18 ile
     doğrudan çıkarma
   - AI "bu pattern artık geçerli değil" tespit ederse → Kural #14
     gibi öneri sunar, Sn. Ferit onay verir

**Çıkarma stratejisi — öğe tipine göre:**

| Öğe | Eylem |
|---|---|
| Kural | "PASİF (tarih, sebep)" notu CLAUDE.md'de **kalır** — anayasa kararlı, silinmez |
| İLKE | "PASİF" notu Vizyon'da kalır |
| KARAR | Vizyon'da **kalır** (mimari karar geçmişi, asla silinmez) |
| AÇIK KONU | Kapanmışsa silinir — kapatma disiplini ayrı (Kural #11 alt-madde) |
| Memory dosyası | `memory/_archive/` altına taşı |
| Script | `scripts/_archive/` altına taşı |
| Notebook belgesi | `notebook/_archive/` altına taşı |
| Manifesto özelliği | "PASİF" notu, **donuk olmalı**, çıkarma çok nadir |
| Self-correction sayımı | Sayımı bırak (KARAR #447 sonrası: "yeni pattern askıda") |

**Negatif tescil disiplini:**
- Çıkarma her zaman **gerekçeli**: "Çıkarıldı [tarih] — sebep: [...]"
- **Geri ekleme:** Eğer çıkarılan öğe sonra tekrar geçerli olursa,
  yeni Kural #14 turuyla eklenir (eski kayıt referans olarak kalır)
- **Toplu çıkarma:** Hijyen turunda max **5 öğe** (radikal değişim
  önlemek için)

**Bağlantı diğer kurallarla:**
- Kural #14 (Pattern Tespit + Kural Önerisi): **ekleme** mekanizması
- Kural #18 (Pasif Öğe Çıkarma): **çıkarma** mekanizması
- Kural #4 (Yıkıcı Eylem Onayı): çıkarma yıkıcı, onay zorunlu
- İlke #4 (DRY): çıkarma DRY uyumu
- KARAR ADAY #442 (Yetenek Minimalizmi): "Çok yetenek yanlışa sebep
  olabiliyor" — Kural #18'in felsefi temeli

**İstisna:** KARAR'lar **asla** silinmez (mimari karar geçmişi
kanıtı). Sadece "PASİF" notu eklenir, içeriği kalır.

**İlk somut canlı uygulama (KARAR #448, 19 May 2026 ~01:00):**
Aşama 3.3 GitHub MCP iptal kararı doğrulandı — 5 dk önce `gh
auth login` ile Bash üzerinden tam GitHub yetkisi (gist + read:org
+ repo + workflow scopes). MCP çiftlemesi gereksizdi, `gh` CLI
yeterli. Negatif tescil canlı — kanon iptal, gerçeklik destekledi.
Aşama 3 RESMÎ KAPANIŞ ile aynı turda. Bu, Web Claude turunda
yakalanan "Kural #18 metnine ilk uygulama referansı" disiplinine
karşılık geliyor.

### Kural 9 v2 alt-bölümü — Otomatik Çift Yönlü Senkron (19 May 2026 ~01:30, KARAR #449)

**Tetikleyici (Manifesto Özellik #8 9. self-correction — tasarım
seviyesi):** Sn. Ferit talimatı: *"web claude ile sistemi
birleştir nasıl olacaksa bana sorma sen orda yaptıklarımızı görsün
o burda falan"*. Yöntem B handoff (KARAR #447) Web → Code yönünde
`_DEVIR.md` üzerinden manuel handoff'a güveniyordu — eksik yarı:
Web Claude'un Drive'a yazdıkları lokal `notebook/`'a otomatik
yansımıyordu.

**Çözüm (çift yönlü otomatik senkron):**

```
Code → Drive (PUSH):  drive_sync.ps1 saatlik (09:00 başlangıç, PT1H)
Drive → Code (PULL):  drive_pull.ps1 saatlik (09:30 başlangıç, PT1H, alternat)
```

**`scripts/drive_pull.ps1` v0.5 davranışı:**
- Sadece `.md` dosyaları (`.txt`'ler drive_sync üretimi, lokal'de yok)
- Drive newer + lokal yok → kopyala (yeni Web Claude dosyası)
- Drive newer + lokal eski (1+ saat) → overwrite
- Drive newer + lokal de yeni (son 1 saat) → **CONFLICT** —
  `_PULL_CONFLICT.md` log dosyası, Sn. Ferit manuel çözer
- Silme YOK (sadece kopyala/güncelle, Kural #4 yıkıcı eylem yok)

**ScheduledTask:** `Quanfina_Notebook_Drive_Pull` saatlik, 09:30
başlangıç (drive_sync 09:00'ın tam yarısı), idempotent.

**Conflict önleme disiplini:**
- Web Claude sadece `_DEVIR.md` "## ⏳ KUYRUK" altına yeni blok
  ekler — append-only pattern
- Code büyük edit'leri commit + push sonrası yapar (Drive'a yansır)
- 30 dk aralık → conflict penceresi az

**İlişkili:** Kural #9 v2 ana metni (Akıllı Dağılım + Handoff
Protokolü), KARAR #447 (Yöntem B), `scripts/drive_sync.ps1`,
`scripts/drive_pull.ps1`, `notebook/_OZET.md` "Bilgi Akışı"
bölümü.

### Kural 19 — PowerShell `Out-File -Encoding UTF8` Türkçe İçerikte Yasak (19 May 2026 ~04:00 yeni)

Windows PowerShell 5.1'de Türkçe (veya diğer non-ASCII) içerikli
markdown/text dosyaları yazarken **`Out-File -Encoding UTF8` YASAK**.
Bu cmdlet **UTF-8 BOM** ekler + içeriği önce Windows console encoding
(cp1254 — Türkçe Windows-1252 türevi) ile yorumlayıp UTF-8'e
dönüştürür → **çift dönüşüm = mojibake** (örn: "Özet" → "Ã–zet",
emoji 🤝 → "ğŸ¤").

**Yasak pattern:**
```powershell
$icerik | Out-File -FilePath $f -Encoding UTF8 -Force        # YANLIS
Set-Content -Path $f -Value $icerik -Encoding UTF8            # YANLIS
```

**Doğru pattern — `[System.IO.File]::WriteAllText` UTF-8 BOM-less:**
```powershell
$utf8NoBom = New-Object System.Text.UTF8Encoding $false
[System.IO.File]::WriteAllText($f, $icerik, $utf8NoBom)       # OK
```

**Pattern keşfi:** 2 ortaya çıkış aynı gece (19 May 2026):
1. `drive_sync.ps1` v2.1 C1 arşivleme — `_DEVIR.md` mojibake
2. `satir_sayim_otomatik.ps1` v0.5 -Uygula — `_OZET.md` mojibake

Eşik 2 aşıldı → Kural #14 doğrudan tescil. _HATALAR.md H#11 kayıt.

**Mojibake düzeltme (zaten oluşmuş için):**
```powershell
$bytes = [System.IO.File]::ReadAllBytes($f)
$bomYok = if ($bytes[0] -eq 0xEF -and $bytes[1] -eq 0xBB -and $bytes[2] -eq 0xBF) { $bytes[3..($bytes.Count-1)] } else { $bytes }
$decode1 = [System.Text.Encoding]::UTF8.GetString($bomYok)
$cp1252 = [System.Text.Encoding]::GetEncoding(1252)
$bytesGercek = $cp1252.GetBytes($decode1)
$gercekIcerik = [System.Text.Encoding]::UTF8.GetString($bytesGercek)
$utf8NoBom = New-Object System.Text.UTF8Encoding $false
[System.IO.File]::WriteAllText($f, $gercekIcerik, $utf8NoBom)
```

**İstisna:**
- ASCII-only içerik → `Out-File -Encoding ascii` güvenli (Kural #15 zaten ASCII-only `scripts/*.ps1`'i kapsıyor)
- PowerShell **7+** (Core) → `Out-File -Encoding utf8NoBOM` parametresi var, ama hâlâ console encoding bug'ı olabilir → **yine `[System.IO.File]::WriteAllText` tercih edilir**

**Script audit listesi (sabah Sn. Ferit):**
- `saglik_kontrol.ps1 -Uygula` Out-File kullanıyor mu? → kontrol et, varsa v0.5.1 fix
- `notebook_yedekle.ps1` Out-File yok (ZIP)
- `drive_sync.ps1` v2.2 Out-File yok (robocopy + Copy-Item)
- `pasif_tara.ps1` Out-File yok (Write-Host only)
- `hijyen_paketi.ps1` Out-File yok (wrapper)

**İlişkili:** Kural #13 (commit mesajı + temp dosya UTF-8), Kural #15
(scripts ASCII-only), Kural #16 (PS native exe 2>&1), Manifesto
Özellik #8 11. self-correction (yöntem seviyesi — encoding bug).

### Kural 20 — UI/UX Tasarım Kararında Çift Danışma Protokolü (19 May 2026 ~08:00 yeni)

**Felsefe:** UI/UX bileşen tasarım/değişiklik kararlarında **iki kaynak
ZORUNLU tarama** + Quanfina-özgü soyutlama. Sn. Ferit'in geçmiş
araştırma birikimi (Markets360 ham kaynak + Notebook_B6 + Master
NotebookLM) atlanırsa **sürtünme + tekrar tasarım**. Kural #14
doğrudan tescil (2 ortaya çıkış 19 May 2026):
1. Sabah ~07:00 — Sn. Ferit "tasarım kararlarında Master'a danış"
2. Sabah ~08:00 — Sn. Ferit "Markets360 CSS/JS sürekli kontrol"

**Zorunlu Çift Tarama:**

**A. Master NotebookLM (Quanfina Notebook)**
- Soyut motif + KARAR referansı + 33 .txt cross-strateji sentez
- Kural #17 uyarınca uzman/yorumcu rolü
- Cevap "KARAR/AÇIK KONU/İLKE — KAYNAK DOSYA — DETAY" formatında

**B. Markets360 Ham Kaynak Tarama**
- CSS: `notebook/B2_Markets360_main-BeC9yeif.css` (768.5 KB)
- JS: `notebook/B2_Markets360_main-Dis3UGIE.js` (7.36 MB)
- Yöntem: PowerShell regex + frequency analiz (örnek paterler aşağıda)

**Tarama Pattern Repertuar (19 May 2026 3 dalga sentezinden):**

```powershell
# CSS hex renk paleti
$hex = [regex]::Matches($css, '#[0-9a-fA-F]{6}\b')

# CSS class isim
$classes = [regex]::Matches($css, '\.[a-zA-Z][a-zA-Z0-9_-]+')

# CSS spacing/typography
$padding = [regex]::Matches($css, '(?:padding|margin)[^;]*?(\d+\.?\d*)(px|rem|em)')

# CSS pseudo-class
$pseudo = [regex]::Matches($css, ':hover|:focus|:active|:disabled')

# JS component pattern
$comp = [regex]::Matches($js, '\b([A-Z][a-zA-Z]+(Panel|Drawer|Modal|Dialog|Menu))\b')

# JS library frequency
$libs = @{ "sonner" = ([regex]::Matches($js, 'sonner|toast\.')).Count; ... }

# JS AG Grid pattern
$ag = @{ "pinnedColumn" = ([regex]::Matches($js, 'pinned')).Count; ... }

# JS trading sabit
$trade = @{ "Pivot" = ([regex]::Matches($js, '\bpivot\b')).Count; ... }
```

**Soyutlama Disiplini (Clean-Room — `_CLEAN_ROOM.md`):**
- Yasaklı isim doğrudan **KOPYALANMAZ** (Markets 360, SEPA®, MonAlert,
  MAI, Vd, Wp, valueGetter, aB(), chip-long, chip-short kod-içi YASAK)
- Motif **soyut anlatılır**: "rakip platform yapısı", "referans yaklaşım"
- Quanfina-özgü adlandırma: `TrendTemplateScanner`, `WatchlistPanel`,
  `MomentumScanner`, `FocusList`, `ColumnConfigMenu`, `RowActionMenu`

**Eylem Sıralaması (her UI/UX kararda):**
1. **Master'a danış** — soyut motif + KARAR/AÇIK KONU referans
2. **Ham CSS/JS tara** — somut kanıt (hex, frequency, pattern)
3. **Notebook_B6 + Notebook_C1 + analizler/Markets360_*.md** — geçmiş özet kontrol
4. **Quanfina-özgü soyutla** — clean-room disiplinli
5. **KARAR ADAY tescili** (gerekirse) — Vizyon + mimari belge
6. **Uygula** — TypeScript + AG Grid + Tailwind

**Canlı kanıt (Kural #20 doğuş anı — 19 May 2026 ~06:30→08:00):**
- screens/page.tsx ilk versiyon (basit) Master danışılmadan + ham tarama yapılmadan yazıldı
- Sn. Ferit dikkat çekti: "Bu tarz tasarım kodlama işlerinde Quanfina Notebook'a sorular soralım"
- Master 5 motif + Quanfina-özgü adlandırma önerdi (Test 4 v1.1 paterni — clean-room self-correction)
- Sn. Ferit: "Markets 360'ın css ve js de de tarama fikir çıkarmanı istiyorum"
- 3 dalga ham tarama: 1984 class + 107 hex + 86 API endpoint + 30K+ filter operator
- Sentez: `notebook/Sprint_4_bis_Mimari_Kararlar.md` (~310 satır, 6 KARAR ADAY #453-#458)
- Uygulama: screens/page.tsx Katman 1 UX zirvesi (chip + RS bandı + sayaç + multi-select + 4 toast tipi)

**İstisna:** Sadece **mimari değişiklik** olmayan **küçük string/etiket
güncelleme** (örn. button label tek satır) için çift tarama zorunlu
değil — Sn. Ferit kararı.

**İlişkili:** Kural #9 v2 (Akıllı Dağılım — Master strateji, Code
uygula), Kural #17 (Uzman/Yorumcu = NotebookLM), KARAR ADAY #453-#458
(Sprint 4-bis mimari sentez), Manifesto Özellik #4 (Proaktif
Yönlendirme) + #8 (Öğrenen — Markets360 ham kaynak emeği boşa
gitmesin), `notebook/Sprint_4_bis_Mimari_Kararlar.md`.

---

## 🤝 Çalışma Mantığı + AI Rol Dağılımı

### Çalışma Mantığı
- Sn. Ferit **vibe coding** yapar — kod yazmaz, kopyala-yapıştır
- Karar yorgunluğunu azaltma felsefesi: operasyonel detaylar AI'ya devredilir,
  Sn. Ferit stratejik karar + final onay verir
- "Sağlam gidelim, bir daha bir daha uğraşmayalım" — kalıcı çözüm tercih edilir
- "Yavaş düşün, hızlı uygula" — analiz uzun, eylem net
- Şüpheli durum → Sn. Ferit'e sor (Kural #3); dürüstlük tercih edilir, yağcılık değil

### AI Rol Dağılımı (18 May 2026 v2 — Kural #9 v2 sonrası: Akıllı Dağılım + Handoff)

| Araç | Rol | Notlar |
|---|---|---|
| **Web Claude** ⭐ | **Birincil strateji + düşünme + hızlı sorgu + karar üretme** — Drive Connector ile Quanfina bağlamlı | Sonnet hızı + Quanfina_notebook Drive üzerinden okur. Chat dolunca / dosya işi → Code'a handoff |
| **Claude Code (VS Code)** ⭐ | **Birincil dosya operasyonu + commit/push + multi-step yaşayan sistem hijyeni + kodlama** | Edit/Write/Bash/Git direkt erişim, paralel tool. Web Claude'un ürettiği prompt'u uygulayıcı |
| **Claude Desktop** | Filesystem MCP gerektiren özel iş (azaldı) | Düşük kullanım, gerektiğinde |
| **NotebookLM Plus** | Kavram/kitap/strateji yorumu (Kural #17 — Vizyon Bekçisi + Minervini + Carr Stage Analizi) | Drive linkli canlı kaynak, otomatik senkron |
| **Gemini Gems** | TradeGrader (Kural #17 istisna — stateless skor) | Diğer Gem'ler NotebookLM'e taşındı |
| **Sn. Ferit** | Stratejik kararlar, final onay, ŞÜPHELİ durum hakemi, **handoff orkestratörü** (Web → Code prompt aktarımı) | Vibe coding evrimi: Web Claude prompt üretir, Sn. Ferit yapıştırır, Code uygular |

**Handoff Protokolü:** Detay için Kural #9 v2. Kısa özet: chat
dolunca / dosya işi gelince Web Claude → Code yönlendirir, prompt
üretir. Sn. Ferit kopyala-yapıştır. Code uygular + commit + Drive
senkron. Bir sonraki Web Claude oturumu Drive üzerinden yeni
hali görür.

**Yaşayan sistem köprüsü:** `drive_sync.ps1` saatlik mirror →
NotebookLM otomatik yeniden indeks + Web Claude Drive Connector
canlı kaynak. **Aynı bilgi 3 araçta paralel** (lokal canon +
Drive ayna + NotebookLM canlı).

### Yetki Devri Prensipleri
- **Auto-approve AÇIK** — yes tıklama yükü yok (Kural #4 ihlal etmeyenler için)
- **Yıkıcı işlemde manuel onay** (Kural #4) — dosya silme, büyük rename, mimari değişiklik
- **Rapor formatı:** Trafik lambası 🟢 / 🟡 / 🔴 + kısa, net özet
- **Şüphede dur, sor** (Kural #3) — otonom değilsin
- **"Karar sende" yetkisi:** Sn. Ferit açıkça verirse karar verici AI'dir,
  ama yıkıcı tarafı varsa raporla sun (Kural #4 hâlâ geçerli)

### Bilgi Akışı (Manifesto özelliklerinin günlük yansıması)
- **Giriş yönlü** — sistem bilgiyi okur: Özellik #1 (Sn. Ferit'i tanır),
  #2 (kaldığı yeri bilir → ⚡ GÜNCEL DURUM), #3 (hangi dosyada ne var → Bilgi Haritası),
  #4 (proaktif "şuna bakalım" önerisi)
- **Çıkış yönlü** — sistem yeni bilgiyi yerleştirir: Özellik #5 (kayıt yeri önerisi),
  #6 (yeni araştırma → doğru kategoriye — Bilgi Mimarisi İlke #1)
- **Bakım yönlü** — sistem kendini günceller: Özellik #7 (adım sonu güncelleme — Kural #8),
  #8 (pattern öğrenme — feedback memory), #9 (felaket dayanıklılığı — Drive + git + NotebookLM)

---

## 🔗 GitHub İlkeleri

### İlke 1 — Versiyon Kontrol Disiplini
Her anlamlı operasyon sonrası commit. Commit mesajı:
"Adım X.Y.Z: kısa açıklama"

### İlke 2 — Push Disiplini
Aşama veya büyük adım bitiminde push.
Gün sonu push: çalışan içerik kayıpsız sabaha kalır.

### İlke 3 — .gitignore Bilinçli Kullanım
Hassas dosyalar commit'lenmez. Geçici dosyalar (*.tmp,
*.backup_*) ignore'da. Büyük binary → Drive'a.

### İlke 4 — Mevcut Durum: Quanfina Repo Public (17 May 2026)
GitHub Teacher onayı için public. Geçici. Strateji metinleri
açıkta — fork riski, AI eğitim verisi riski kabul edilmiş.

### İlke 5 — Geçiş Planı: Public → Private
Teacher onayı geldiğinde:
1. Settings → visibility → Private
2. Fork kontrolü
3. "Include private contributions on profile" açık tut
4. Geçişi YAPILANLAR.md'ye kaydet

### İlke 6 — Ayrı Public Eğitim Repoları (Aşama 4)
Quanfina private olduktan sonra profil aktivitesi ayrı
public repolar üzerinden:
- claude-code-prompts-tr
- vibe-coding-workflow
- notebook-driven-development

### İlke 7 — Claude Code Git Workflow
Standart kullanım. Yıkıcı git komutları (force push, branch
silme) → manuel onay (Kural #4).

### İlke 8 — Sızma Kontrol Listesi (Kural #10 ile birlikte)
Her push öncesi şu 6 kontrol — hepsi PASS olmadan push yok:

1. **`.gitignore` doğrulama** — `.env`, `notebook/`, `*.backup_*`,
   `/node_modules/`, `test-screenshots/`, `*.tmp` korumada mı?
2. **Staged dosya gözden geçirme** — `git status` ile commit'lenecek
   dosyalar bilinçli mi? Yetim/sürpriz dosya var mı?
3. **Staged içerik taraması** — `git diff --cached` ile gerçek değer
   sızması (hardcoded password, API key) var mı?
4. **Yasaklı isim taraması** — Markets 360, Fab 5, SEPA®, MonAlert®,
   MAI, Vd, Wp, valueGetter, aB() — Türkçe/İngilizce hiçbir varyantı
   kod/docstring/markdown'da olmamalı (CLAUDE.md Kodlama Standardı #3)
5. **Secret format taraması** — AKIA[0-9A-Z]{16}, ghp_*, github_pat_*,
   sk-[A-Za-z0-9]{20+}, BEGIN (RSA|OPENSSH|PRIVATE|CERTIFICATE),
   xoxb-, AIza* (Google API)
6. **Final commit listesi onayı** — Sn. Ferit'e gösterilir, onay alınır

Otomatik script: `scripts/sizma_kontrol.ps1` — bu 6 kontrolü çalıştırır,
exit 0 = temiz, exit 1 = kirli (rapor üretir). Pre-push hook olarak da
bağlanabilir (`.git/hooks/pre-push` — 17 May 2026 itibarıyla bağlı).

---

## 📚 Terminoloji Disiplini

### Onaylı Türkçe Proje Terminolojisi
| Yasak | Doğru | Gerekçe |
|---|---|---|
| Faz 1, Faz 2 | **Aşama 1, Aşama 2** | Tescilli proje terminolojisi |
| Step 1.2 | **Adım 1.2.5.b** | Noktalı hiyerarşi alt-adım netliği |
| Sprint X | **Adım X.Y** veya **Aşama X.Y** | Yeni sistemde sprint kullanılmaz |
| Phase | Aşama | Türkçe |

Eski commit mesajlarında "Faz" veya "Sprint" geçebilir — bunlar geçmiş, kanon
değil. Yeni metinde tekrar etme.

### Yasaklı İsimler ve Markalar (Sızma Kontrolü #4 ile birebir)
| Yasak | Açıklama |
|---|---|
| Markets 360 | Yabancı platform; clean-room ihlali |
| Fab 5 | Mark Minervini tescilli marka |
| SEPA®, MonAlert®, MAI | Mark Minervini tescilli markalar |
| Vd, Wp, valueGetter, aB() | Yabancı platform minified internal isimler |

Otomatik kontrol: `scripts/sizma_kontrol.ps1` kontrol #4 + pre-push hook.

### Piyasa Bağlamı
- **Quanfina ABD piyasasında işlem yapar** (NYSE / NASDAQ / ARCA)
- YASAK: BIST, Türk piyasası, TRY (₺), "Türk yatırımcıya özel" gibi ifadeler
- Sn. Ferit Türkiye'de yaşıyor ama platform ABD piyasası odaklı
- USD ($) ana para birimi, "$" sembolü kullanılır

### Trafik Lambası Standartları (Kural #2 ile birlikte)
- 🟢 **Yeşil** — Operasyon başarılı, devam güvenli
- 🟡 **Sarı** — Dikkat gerekiyor ama operasyon devam edebilir
- 🔴 **Kırmızı** — BLOK, müdahale gerekli

Her büyük adım sonu zorunlu (Kural #2). Rapor formatı: trafik lambası + kısa özet.

### Dil ve Üslup
- Türkçe ana dil; teknik terimler orijinal bırakılabilir (FastAPI, watchlist, vb.)
- Sertlik + dürüstlük tercih edilir, yağcılık yapılmaz
- Komut iletilirken kısa, net; rapor verilirken trafik lambası + tablo

---

# CLAUDE.md — Notebook_0_Baglam
**Quanfina Proje Bağlam Dosyası**
**Son güncelleme: 17 Mayıs 2026**

> ## Bu Dosya Hakkında
>
> Bu dosya Claude Code tarafından her oturumda otomatik okunan bağlam dosyasıdır.
> Teknik gereklilik nedeniyle kök dizinde yaşar (Claude Code dizin-bazlı çalışır),
> ancak mantıken notebook sisteminin parçasıdır ve **Notebook_0_Baglam** olarak adlandırılır.
>
> - Detaylı bilgi haritası: bu dosyada "Bilgi Haritası" bölümü
> - Tam karar günlüğü: `notebook/Notebook_A_Vizyon.md`
> - Master indeks: `notebook/_INDEX.md` (Aşama 1.E.a — 17 May 2026 canlı)
> - İndeks doğrulayıcı: `scripts/build_index.ps1` (Manifesto Özellik #3 bakımı)

## Proje Vizyonu

Quanfina, Sn. Ferit'in kişisel hisse tarama, watchlist yönetimi ve trade takip
platformudur. Finviz Elite tarama motoru üzerinden Minervini ve Carr
metodolojilerine göre günlük veri çeker; Next.js arayüzü ve FastAPI backend'i
üzerinden bu verileri sunar. Tek kullanıcı, kişisel kullanım.

### Bu Proje NE DEĞİL
- Pazarlanacak veya satılacak bir ürün değil
- Müşteri, hedef kitle veya yatırımcı sunumu hedefi yok
- Lansman, marka veya fiyatlandırma çalışması yok
Tek odak: Sn. Ferit'in uygulamayı kullanması ve iyileştirmesi.

---

## Şu Anki Durum (Mayıs 2026)

| Katman | Durum | Açıklama |
|--------|-------|---------|
| FastAPI (api/) | ✅ Aktif | Watchlist + Trade CRUD gerçek DB; Minervini/Market MOCK |
| Next.js (web/) | ✅ Aktif | Watchlist, Journal, Signals, Piyasa sayfaları |
| quanfina-scanner | ✅ Aktif | Günlük Finviz taraması, Cloud Run |
| PostgreSQL Cloud SQL | ✅ Aktif | Tüm veri (legacy + web tabloları) |
| Streamlit (app.py, pages/) | ⏸️ Pasif | Yakında _archive/streamlit_legacy/'e taşınacak |

**Aktif Aşama:** Vizyon dosyasındaki ⚡ GÜNCEL DURUM bölümüne bakın
→ `notebook/Notebook_A_Vizyon.md` (satır 33-95)

**Statik Mimari Durumu** (değişmez): Yukarıdaki tablo (FastAPI, Next.js, PostgreSQL aktif. Streamlit pasif.)

---

## Teknoloji Yığını

| Katman | Teknoloji |
|--------|-----------|
| Frontend | Next.js 16, React 19, TypeScript, Tailwind CSS, shadcn/ui, TanStack Query, ag-Grid |
| Backend | FastAPI, Python 3.13, uvicorn |
| Veritabanı | PostgreSQL (Google Cloud SQL) — psycopg2 + SQLAlchemy |
| Veri çekme | scanner.py (Finviz Elite API, yfinance) |
| Scanner runtime | Flask + gunicorn, Cloud Run (quanfina-scanner) |
| Cloud | GCP: Cloud Run, Cloud SQL, Cloud Build, Cloud Scheduler, Artifact Registry, Secret Manager |
| Test | pytest (Python), Playwright 1.60 (E2E, visual_test*.mjs) |
| Python ortamı (kök) | `venv\` → `venv\Scripts\python.exe` |
| Python ortamı (api/) | `api\.venv\` → `api\.venv\Scripts\python.exe` |
| Node (web/) | pnpm |

---

## Klasör Yapısı Haritası

```
c:\Projeler\Quanfina\
│
├── api/                          # FastAPI backend
│   ├── main.py                   # Tüm API endpoint'leri (55 KB)
│   ├── db_helpers.py             # web_watchlist + web_trades CRUD
│   ├── requirements.txt          # fastapi, uvicorn, SQLAlchemy, psycopg2
│   └── .venv/                    # api/ için ayrı Python ortamı
│
├── web/                          # Next.js 16 frontend
│   ├── app/(dashboard)/          # App Router sayfaları
│   │   ├── page.tsx              # Dashboard
│   │   ├── watchlist/page.tsx
│   │   ├── journal/page.tsx
│   │   ├── signals/page.tsx
│   │   ├── minervini/page.tsx
│   │   ├── carr/page.tsx
│   │   ├── piyasa-durumu/page.tsx
│   │   └── hisse/[symbol]/page.tsx
│   ├── components/               # React bileşenleri (watchlist/, journal/, market/ vb.)
│   ├── hooks/                    # use-watchlist, use-trades, use-signals vb.
│   ├── types/                    # TypeScript tip tanımları
│   ├── lib/                      # Utility fonksiyonları
│   ├── AGENTS.md                 # Next.js breaking changes uyarısı
│   └── CLAUDE.md                 # → @AGENTS.md (import)
│
├── scripts/                      # Migration ve seed scriptleri
│   ├── sql/001_web_tables.sql    # web_watchlist + web_trades şeması
│   ├── run_migration.py          # SQL migration çalıştırıcı
│   ├── seed_initial_data.py      # İlk veri yükleme (idempotent)
│   └── seed_symbol_lists.py      # symbol_lists tablosu seed (tek seferlik)
│
├── pages/                        # ⏸️ Streamlit sayfaları (pasif, arşivlenecek)
│   └── 2_Screens.py              # 28 screen filtreleme UI iskeleti (MVP aşamasında)
│
├── tests/                        # Python testleri (pytest)
│   ├── test_trade_journal.py     # TradeJournal CRUD (50 test, savepoint/rollback)
│   └── test_quanfina_math.py     # Matematik fonksiyonları
│
├── notebook/                     # Proje vizyon ve analiz belgeleri (.gitignore'da)
├── ux_tarama/                    # UX tasarım çalışmaları (.gitignore'da)
│
├── scanner.py                    # Finviz Elite tarama motoru (77 KB)
├── scanner_server.py             # Scanner için Flask HTTP wrapper (Cloud Scheduler hedefi)
├── quanfina_math.py              # ⭐ Minervini matematik motoru — KRİTİK (31 KB)
│                                 # R-Multiple, RBA, TradeGrader, Stop yönetimi
│                                 # Streamlit pages + tests tarafından kullanılır
│                                 # İleride FastAPI endpoint'lerine taşınacak
├── db_connection.py              # PostgreSQL CRUD — Streamlit/TradeJournal için
├── trade_journal.py              # TradeJournal class (Streamlit legacy)
├── app.py                        # ⏸️ Streamlit ana sayfa (pasif)
├── styles.py                     # ⏸️ Streamlit tasarım sistemi (pasif)
│
├── Dockerfile.scanner            # quanfina-scanner Cloud Run imajı
├── Dockerfile.app                # ⏸️ quanfina-app Streamlit imajı (durdurulacak)
├── cloudbuild.yaml               # GCP Cloud Build pipeline
└── visual_test*.mjs              # Playwright E2E testleri (aktif, bazı testler fail)
```

### Durumu Belirsiz Dosyalar (Adım 1.8'de netleşecek)

| Dosya | Araştırma Sonucu |
|-------|-----------------|
| `quanfina_math.py` | **Aktif** — Minervini matematik motoru; Streamlit pages ve tests tarafından kullanılıyor |
| `pages/2_Screens.py` | MVP iskeleti — 28 screen filtresi UI; dummy veri; henüz geliştirilmekte |
| `seed_symbol_lists.py` | **Tek seferlik** — symbol_lists tablosunu doldurur, çalıştırıldı mı doğrulanmadı |
| `quanfina.db` | SQLite legacy (618 KB, 03 May 2026) — PostgreSQL'e geçildi, referans olarak duruyor |
| `visual_test*.mjs` | **Aktif** Playwright testleri — son rapor: Watchlist kısmi (4 fail), Journal geçti |
| `_list_cols.py` | Finviz kolon keşif scripti — tek seferlik yardımcı |
| `migrate_to_postgres.py` | Tek seferlik SQLite→PG migration — tamamlandı, referans |

---

## GCP Altyapısı

| Servis | Adı / Detay |
|--------|-------------|
| Cloud Run | `quanfina-scanner` (Flask scanner, 3600s timeout, europe-west1) |
| Cloud Run | `quanfina-app` (Streamlit, yakında durdurulacak) |
| Cloud SQL | PostgreSQL, europe-west1 |
| Cloud Build | `cloudbuild.yaml` — 2 imaj build + deploy |
| Cloud Scheduler | Günlük POST /scan → quanfina-scanner (schedule GCP Console'da tanımlı, kodda yok) |
| Artifact Registry | `europe-west1-docker.pkg.dev/{PROJECT_ID}/quanfina/` |
| Secret Manager | PG_HOST, PG_PORT, PG_DATABASE, PG_USER, PG_PASSWORD, FINVIZ_API_KEY |

Ortam değişkenleri: `.env` (local) / Secret Manager (production)

---

## PostgreSQL Şeması

### Web katmanı (FastAPI — aktif)

| Tablo | Açıklama |
|-------|---------|
| `web_watchlist` | PRIMARY KEY (symbol, strategy); consensus_count, consensus_strategies (JSONB) |
| `web_trades` | SERIAL PK; symbol, strategy, entry/exit, pl_dollar, pl_pct, grade, lessons |
| `symbol_lists` | user_id, strategy, list_type (watch/on_deck/focus/buy), symbol |

### Streamlit / TradeJournal tabloları (legacy — korunuyor)

| Tablo | Açıklama |
|-------|---------|
| `trades` | 40 kolon; status: 'Open'/'Closed'/'Deleted' (BÜYÜK HARF) |
| `trade_legs` | Multi-leg pyramiding — ON DELETE CASCADE |
| `trade_exits` | Kısmi/tam çıkış — ON DELETE CASCADE |
| `stop_history` | Trailing stop — ON DELETE CASCADE |
| `journal_entries` | Serbest metin günlüğü — ON DELETE SET NULL |
| `portfolios` | Portföy değerleri; portfolio_id FK kullanılıyor |

### trades tablosu — kolon notları

- `trade_type` VARCHAR ('Long'/'Short') — eski kolon, mevcut kod kullanıyor
- `invest_type` SMALLINT (1=LONG, 2=SHORT) — yeni kolon, TradeJournal API bunu kullanıyor
- `portfolio_id` INTEGER FK → `portfolios`
- `notes` TEXT (plural) — mevcut naming korundu
- `entry_date` / `exit_date` TIMESTAMP (DATE'e dönüştürülmedi)
- `deleted_at` TIMESTAMP — soft delete

### Yabancı anahtar davranışları

- `trade_legs.trade_id` → `trades.id` ON DELETE CASCADE
- `trade_exits.leg_id` → `trade_legs.id` ON DELETE CASCADE
- `stop_history.trade_id` → `trades.id` ON DELETE CASCADE
- `journal_entries.linked_trade_id` → `trades.id` ON DELETE SET NULL

### Scanner tabloları (quanfina-scanner — aktif)

| Tablo | Açıklama |
|-------|---------|
| `minervini_scans` | Trend Template tarama sonuçları (günlük) |
| `minervini_fundamental_scans` | Teknik + Temel filtre |
| `minervini_fundamental_only` | Sadece temel filtre |
| `minervini_52w_high` | 52 hafta yeni yüksek |
| `sector_rotation` | 11 SPDR ETF RS rank (günlük) |

**Not:** `trades.status` BÜYÜK HARF ('Open'), `web_trades.status` küçük harf ('open') — karıştırma.

---

## FastAPI API Katmanı (api/)

**Çalıştırma:**
```powershell
cd api
.\.venv\Scripts\python.exe -m uvicorn main:app --reload --port 8001
```

**Endpoint durumları:**

| Method | Path | Durum |
|--------|------|-------|
| GET | /api/health | REAL (db_health_check) |
| GET | /api/watchlist | REAL (watchlist_get_all) |
| POST | /api/watchlist | REAL (watchlist_insert + consensus) |
| PATCH | /api/watchlist/{symbol}/{strategy} | REAL (watchlist_update) |
| DELETE | /api/watchlist/{symbol}/{strategy} | REAL (watchlist_delete) |
| POST | /api/watchlist/{symbol}/{strategy}/promote | REAL |
| GET | /api/signals | REAL (watchlist'ten türetilmiş) |
| GET | /api/trades | REAL (trades_get_all) |
| POST | /api/trades | REAL (trades_insert + PL hesabı) |
| PATCH | /api/trades/{trade_id} | REAL (trades_update) |
| DELETE | /api/trades/{trade_id} | REAL (trades_delete) |
| GET | /api/minervini/stocks | **MOCK** |
| GET | /api/market/status | **MOCK** |
| GET | /api/terms | **MOCK** |
| GET | /api/terms/{key} | **MOCK** |
| GET | /api/stock/{symbol}/info | **MOCK** |
| GET | /api/stock/{symbol}/ohlcv | **MOCK** |
| GET | /api/setup-types | **MOCK** |

**db_helpers.py fonksiyonları:**
```python
watchlist_get_all(), watchlist_get_one(symbol, strategy)
watchlist_insert(row), watchlist_update(symbol, strategy, updates)
watchlist_delete(symbol, strategy), watchlist_recompute_consensus()
trades_get_all(), trades_get_by_id(id)
trades_insert(trade), trades_update(id, updates), trades_delete(id)
db_health_check()
```

---

## Next.js Frontend (web/)

**Çalıştırma:**
```powershell
cd web
pnpm dev      # http://localhost:3000
```

**API proxy:** `web/next.config.ts` → `/api/*` istekleri `http://localhost:8001`'e yönlenir.

**Tamamlanan sayfalar:** watchlist, journal, signals, piyasa-durumu, minervini, carr
**Eksik (Streamlit'ten taşınmamış):** Sektör Rotasyonu, Portfolio Risk, İstatistikler

---

## Bağlantı Yönetimi (Python)

```python
# api/ için (db_helpers.py) — port 8001
from sqlalchemy import create_engine
engine = create_engine(_URL, pool_pre_ping=True)

# Streamlit/TradeJournal için (db_connection.py)
from db_connection import get_connection, get_engine
conn = get_connection()   # psycopg2
engine = get_engine()     # SQLAlchemy
```

Ortam değişkenleri: `PG_HOST`, `PG_DATABASE`, `PG_USER`, `PG_PASSWORD`, `PG_PORT`

---

## TradeJournal API (Streamlit legacy — korunuyor)

```python
from trade_journal import TradeJournal

with TradeJournal() as tj:
    tid = tj.add_trade('NVDA', invest_type=1, entry_date=..., entry_price=120.0,
                        stop_loss=115.0, quantity=100)
    tj.update_stop(tid, new_stop_price=118.0, reason='trailing')
    result = tj.close_trade(tid, exit_date=..., exit_price=130.0)
```

**Transaction kuralı:** `TradeJournal(conn=external)` → class commit çağırmaz, caller yönetir.

---

## quanfina_math.py — Matematik Motoru (aktif)

Tüm Minervini disiplinindeki matematiksel hesaplamalar burada. Streamlit pages ve testler tarafından kullanılıyor. İleride FastAPI endpoint'lerine taşınacak.

Temel fonksiyon grupları:
- Günlük değişim, kâr/zarar hesabı (Long/Short)
- R-Multiple ve Risk Dolarları
- Stop Loss yönetimi (initial, breakeven, trail)
- 52W mesafesi, SMA20, Hacim metrikleri (V50, VRR)
- Distribution Days
- RBA (Result-Based Analysis — beklenti, adjusted ratio)
- TradeGrader (17 kategori — BP, BL, CE, SP, SE, SL vb.)

---

## Testler

```powershell
# Python testleri
venv\Scripts\python.exe -m pytest tests\test_trade_journal.py -v
venv\Scripts\python.exe -m pytest tests\test_quanfina_math.py -v

# E2E testler (Playwright)
npx playwright test visual_test.mjs
npx playwright test visual_test_journal.mjs
npx playwright test visual_test_quick.mjs
```

Test stratejisi (Python): gerçek PostgreSQL + savepoint/rollback — veri DB'de kalmaz.
E2E test hedefi: `http://localhost:3000` — Watchlist ve Journal sayfaları.

**Mevcut test durumu (son rapor: visual_test_report.json):**
- Watchlist E2E: kısmi başarı (4 test fail)
- Journal E2E: tüm testler geçti
- Test iyileştirme Aşama 1 dışında, ileri tarihte ele alınacak

---

## UX Tasarım Çalışmaları

> **Bu klasör Sn. Ferit'in UX tasarım çalışmasıdır. Her UI/UX kararı verilirken önce
> buraya bakılmalıdır. Bu çalışma daha önce sistem tarafından gözden kaçırıldı;
> artık bilgi haritasının parçasıdır.**

`ux_tarama/` klasörü `.gitignore`'da — git'e PUSH EDİLMEZ.

| Dosya | İçerik | Durum |
|-------|--------|-------|
| `_INDEX.md` | Master index — 232 bulgu, 5 kategori | Tamamlanmış, 09 May 2026 |
| `A_mevcut_ux_gozlemleri.md` | Streamlit UX'teki 21 şikayet — 7+4 tab karmaşası, ham liste sorunları | Tamamlanmış, Next.js için hâlâ geçerli |
| `B_markets360_ogrenimi.md` | 74 bulgu — AG Grid, 4 liste hiyerarşisi, badge sistemi, layout | Tamamlanmış, Next.js'de uygulanacak |
| `C_hedef_tasarim_kararlari.md` | 64 karar — Hedef UX blueprint; Watch/On Deck/Focus/Buy yapısı, sütun hiyerarşisi | Tamamlanmış, ELZEM — her UI kararında referans |
| `D_streamlit_teknik.md` | 37 teknik bulgu — Streamlit bileşen envanteri (dönüşüm için referans) | Tamamlanmış, sınırlı geçerlilik |
| `E_acik_sorular.md` | 36 açık soru — Report Card, Books UI, 5 Reflection, TradeJournal leg/exit | AÇIK — çözüm bekliyor |

**En kritik referans:** `ux_tarama/C_hedef_tasarim_kararlari.md` — Watch/On Deck/Focus/Buy 4 liste yapısı,
sütun hiyerarşisi, badge sistemi, piyasa sağlığı 5 metriye.

---

## Notebook Sistemi

`notebook/` klasörü `.gitignore`'da — git'e PUSH EDİLMEZ.

| Dosya | İçerik |
|-------|--------|
| `Notebook_A_Vizyon.md` | Ana proje vizyonu (~7064 satır); KARAR#, AÇIK KONU#, İLKE# sistemli |
| `YAPILANLAR.md` | Yaşayan hafıza — tamamlanan adımlar, kaldığı yer |
| `kitaplar/Minervini` | Minervini metodoloji (kitap analizi) |
| `kitaplar/Minervini_Video` | Minervini video analizleri |
| `kitaplar/Carr` | Carr metodoloji ✅ |
| `analizler/Markets360_Bundle` | Markets360 platform analizi (clean-room uyumu: kaynak koda atıf yapma) |
| `analizler/FMP_Matematik` | FMP matematik karşılaştırma |
| `analizler/Markets360_Gorsel_1, 2` | UI görsel analizi (Markets360 ekranları) |
| `Notebook_B6` | Sprint protokolleri, adımlar listesi |
| `Notebook_C1` | Sprint quickstart kılavuzu |
| `Notebook_C2` | Teknik EK'ler (EK1-8), matematik, formüller |
| `Notebook_C3` | Veritabanı şeması (EK9) |
| `EK10` | TradeGrader algoritması sentezi |

**Karar sistemi (Notebook_A_Vizyon.md):**
- `KARAR #X` — teknik/mimari kararlar (443 karar, v20.39 itibarıyla)
- `AÇIK KONU #X` — bekleyen sorular (52 açık konu)
- `İLKE #X` — değişmez prensipler

Yeni oturum başında **son 100 satırı** okumak yeterlidir. Eski kararlar değiştirilmez — meta-linkage ile güncellenir (`KARAR #X revize: ... → ...`).

---

## Bilgi Haritası — Hangi Konu Nerede?

> 🎯 **Master indeks:** Tüm dosya envanteri için → `notebook/_INDEX.md`
> (Aşama 1.E.a, 17 May 2026 canlı). Bu tablo en sık başvurulan
> konuların kısa eşleştirmesi — tam liste için _INDEX.md'ye bak.
> Doğrulama: `.\scripts\build_index.ps1`

| Konu | Dosya/Klasör | Tür |
|------|--------------|-----|
| Master indeks (tam dosya envanteri) | `notebook/_INDEX.md` | Sistem |
| Yeni chat bootstrap | `notebook/_BASLAT.md` | Sistem |
| Yol haritası (Aşama 1.C → 5) | `notebook/_ROADMAP.md` | Sistem |
| Hesap/araç matrisi | `notebook/_LINKLER.md` | Sistem |
| Kod envanteri (sınıflandırma + dependency) | `notebook/_KOD_ENVANTERI.md` | Sistem |
| Proje vizyonu, kararlar, açık konular | `notebook/Notebook_A_Vizyon.md` | Karar günlüğü |
| Minervini metodoloji | `notebook/kitaplar/Minervini.md` | Strateji bilgisi |
| Minervini video analizleri | `notebook/kitaplar/Minervini_Video.md` | Strateji bilgisi |
| Carr metodoloji | `notebook/kitaplar/Carr.md` | Strateji bilgisi ✅ |
| Yaşayan hafıza, tamamlanan adımlar | `notebook/YAPILANLAR.md` | Proje hafızası |
| Markets360 platform analizi | `notebook/analizler/Markets360_Bundle.md` | Referans (clean-room) |
| FMP matematik karşılaştırma | `notebook/analizler/FMP_Matematik.md` | Strateji bilgisi |
| UI görsel analizi | `notebook/analizler/Markets360_Gorsel_1.md`, `Gorsel_2.md` | UI referansı |
| Sprint protokolleri | `notebook/Notebook_B6_AdimlarKarar.md` | Süreç |
| Teknik EK'ler, formüller | `notebook/Notebook_C2_EK1-8.md` | Teknik referans |
| Veritabanı şeması | `notebook/Notebook_C3_EK9_DBSchema.md` | Teknik referans |
| TradeGrader algoritması | `notebook/EK10_TradeGrader_Sentezi.md` | Algoritma |
| UX tasarım kararları (blueprint) | `ux_tarama/C_hedef_tasarim_kararlari.md` | UI/UX tasarım |
| Mevcut UX sorunları | `ux_tarama/A_mevcut_ux_gozlemleri.md` | UI/UX analiz |
| Markets360 UI öğrenimi | `ux_tarama/B_markets360_ogrenimi.md` | UI/UX referans |
| Açık UX soruları | `ux_tarama/E_acik_sorular.md` | UI/UX açık |
| FastAPI endpoint'leri | `api/main.py`, `api/db_helpers.py` | Aktif kod |
| Next.js sayfaları | `web/app/` | Aktif kod |
| TypeScript tip tanımları | `web/types/` | Aktif kod |
| ⭐ Minervini matematik motoru (R-Multiple, RBA, TradeGrader, Stop yönetimi) | `quanfina_math.py` | **Aktif kod — kritik** |
| Streamlit (pasif) | `app.py`, `pages/` | Legacy kod |
| Tarama motoru | `scanner.py`, `scanner_server.py` | Aktif kod |

---

## Mimari İlke: Çok Stratejili Sistem

Quanfina mimarisi tek bir stratejiye bağımlı değildir. Şu an aktif stratejiler:
Minervini, Carr. Yarın başkası eklenebilir.

Bu yüzden:
- Kod strateji-bağımsız yazılır (hardcoded strateji adı kaçınılır)
- Veritabanı tabloları `strategy` etiketli olur
- UI bileşenler strateji-spesifik isim taşımaz (evrensel isimlendirme)
- Yeni strateji = yeni modül eklemek, mevcut kodu değiştirmemek

**Mevcut mimari ilke ihlalleri (refactor adayı — şu an öncelikli değil):**
- `web/types/minervini.ts` → `strategy.ts` veya `minervini-strategy.ts` olmalı
- `minervini_scans` tablosu → `strategy_scans` (strategy_name kolonlu) olmalı
- `minervini_fundamental_scans`, `minervini_52w_high` → benzer pattern

Bu refactor çok-strateji kullanımı somutlaştığında ele alınır.

---

## AI Rol Dağılımı

| Rol | Araç | Sorumluluk |
|-----|------|-----------|
| Stratejist | Web Claude | Vizyon, prompt tasarımı, karar sentezi |
| Uygulayıcı | Claude Code | PLAN MODE (keşif/taslak) → EDIT MODE (uygulama) |
| Karar sahibi | Sn. Ferit | Nihai onay, yön belirleme |

**İş akışı:** Web Claude prompt üretir → Sn. Ferit Claude Code'a verir →
PLAN MODE'da taslak → onay → EDIT MODE'da uygulama → Notebook'a kayıt.

---

## Terminoloji Disiplini

| ❌ Kullanılmaz | ✅ Kullanılır |
|--------------|--------------|
| "Faz 1/2/3" | "Aşama 1 / Aşama 2" (büyük gruplar) |
| "Sprint" | "Adım X.Y" (aşama içindeki işler) |
| "KARAR ADAY + KESİN" | Sadece "KARAR ADAY" veya "KARAR KESİN" (ikisi birden olmaz) |

Scope prefix zorunluluğu: bağlamı net yaz ("DB-Aşama 1", "UI-Adım 2.1" gibi).

---

## Kodlama Standartları ve Kısıtlar

1. `database.py` (SQLite legacy) ve `migrate_to_postgres.py`'a **dokunma** — sadece referans
2. Mevcut tablolarda kolon **DROP etme** — sadece ADD COLUMN veya CREATE TABLE
3. **Yabancı platform internal isimleri kullanma** (clean-room uyumu) — minified/proprietary isim örnekleri lokal notlarda (notebook/) saklanır, repo'da geçmez
4. `updated_at`: web_watchlist/web_trades'de TRIGGER var; Streamlit tabloları için Python tarafında `NOW()` ile yönetilir
5. Status değerleri: Streamlit tabloları 'Open'/'Closed'/'Deleted' (BÜYÜK HARF); web tabloları 'open'/'closed'/'buy' (küçük harf) — karıştırma
6. Yorum yazma; gerektiğinde neden yazılır (ne yapar değil)
7. `notebook/` ve `ux_tarama/` içine dosya yazma — sadece oku
8. Streamlit kodu silinmez — `_archive/streamlit_legacy/`'e taşınacak (henüz yapılmadı)
9. `api/` için kök `venv\` yerine `api\.venv\` kullan
10. `web/` için npm değil pnpm kullan
11. FastAPI port: 8001 (next.config.ts proxy buna ayarlı — 8000 değil)

---

## Emek Görünür Olur

Sn. Ferit'in yaptığı her çalışma sistem tarafından **görünür** olmalıdır.
Yetim dosya yaratmaktan kaçınılır.

Yeni çalışma yapıldığında:
1. İlgili klasöre kaydedilir
2. Bu CLAUDE.md'deki Bilgi Haritası tablosuna eklenir
3. `Notebook_A_Vizyon.md`'ye karar olarak yazılır (uygunsa)
4. Açık konu varsa AÇIK KONU numarası verilir

"Yapılmış ama görünmez" kabul edilemez.

---

## Yapılmaması Gerekenler

- `notebook/` ve `ux_tarama/` klasörlerini git'e push etme (zaten .gitignore'da)
- `.env` dosyasını git'e commit etme
- Streamlit kodunu silme — arşivlenecek, silinmeyecek
- Eski KARAR metinlerini silme — meta-linkage ile güncelle
- `database.py` ve `migrate_to_postgres.py`'a dokunma
- Mevcut tablolarda kolon DROP etme
- Markets 360 kaynak koduna atıf yapma (clean-room prensibi)
