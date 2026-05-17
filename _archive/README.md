# 🗄️ Quanfina — Arşiv Klasörü

> **Bu klasör ne için?**
> Aşama 1.D (Streamlit Emeklilik, 17 May 2026) sonrasında yaşayan
> kod tabanından çıkarılan ama git history'den de silinmemiş
> dosyaların kalıcı evi. Manifesto Özellik #9 (Felaket dayanıklılığı)
> ile uyumlu — bilgi kaybedilmez, sadece yaşayan koddan ayrılır.

---

## 📂 İçerik

### `streamlit_legacy/`
Eski Streamlit UI katmanı. Next.js'e geçişle birlikte 17 May 2026'da
emekliye alındı. 16 dosya, 3,517 satır.

- `pages/` — 13 Streamlit sayfası (1_Genel_Bakis → 11_Sektor_Rotasyonu + 3_Minervini_old)
- `app.py` — Streamlit giriş noktası
- `styles.py` — Streamlit tasarım sistemi (CSS + helper)
- `database.py` — SQLite legacy (1_Genel_Bakis'in tek kullanıcısı)

**Commit serisi:** `a9f307d` → `fed11c7` → `f13d18b` → `4078263` (Aşama 1.D.1-4)

### `scratch/`
Geçici / scratch dosyalar. 5 dosya, 318 satır.

- `_list_cols.py` — tracked scratch (git history korundu, commit `ecd3944`)
- `test_finviz_sectors.py`, `test_pg.py`, `test_sector_match.py`, `test_sectors_full.py` — ignore'daki scratch testler (fiziksel mv ile burada, git history'de yok)

---

## 🔄 Geri alma — bir dosyayı tekrar yaşayan koda nasıl alırım?

```bash
# Git history'den geçmişi görmek için:
git log --follow _archive/streamlit_legacy/pages/3_Minervini.py

# Belirli bir versiyonu geri almak:
git show <commit>:_archive/streamlit_legacy/pages/3_Minervini.py > pages/3_Minervini.py

# Veya direkt eski yere geri taşımak:
git mv _archive/streamlit_legacy/pages/3_Minervini.py pages/
```

---

## 🗑️ Tamamen silme zamanlaması

Sn. Ferit'in kararı (17 May 2026): "Streamlit önemli değil, sonradan tamamen silinecek."

**Şu anki konum:** Aşama 1.D KAPANIŞ — `_archive/` git'te kalıyor.

**Tamamen silme:** Aşama 5 (Yaşayan Sistem Zirvesi) veya repo Private dönüşü sonrası değerlendirilir. O zamana kadar:
- Bilgi kaybı yok (git history + arşiv klasörü)
- Yaşayan koddan ayrı (developer odaklanması temiz)
- Madencilik kapısı açık (gerektiğinde özellik notebook'a aktarılabilir)

---

## 📚 İlgili referanslar

- `notebook/_KOD_ENVANTERI.md` → 5'li sınıflandırma
- `notebook/_ROADMAP.md` → Aşama 1.D detayları
- `notebook/YAPILANLAR.md` → Aşama 1.D RESMÎ KAPANIŞ
- `CLAUDE.md` → Kodlama Standardı #8 (arşiv kuralı)
