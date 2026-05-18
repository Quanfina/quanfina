# Quanfina YAPILANLAR.md Adim Sonu Kapanis Sablonu
# Referans: CLAUDE.md Kural #8 (Adim Sonu Guncelleme)
#           notebook/_ROADMAP.md Asama 3.4
#
# Amac: Kural #8 (Adim Sonu Guncelleme) icin somut otomasyon yardimcisi.
# AI veya Sn. Ferit bir adim tamamladiginda bu script:
# 1. Mevcut baglam bilgisini otomatik toplar (commit, kural sayim, sayfa sayim)
# 2. Kapanis sablonu uretir (markdown)
# 3. Sn. Ferit/AI sablonu YAPILANLAR.md sonuna yapistirir
#
# v0.5 sinir: sablon UReTICI (YAZMA yok, guvenli). v1.0 sonra: -Uygula ile
# YAPILANLAR.md'ye otomatik ekleme.
#
# Kullanim:
#   .\scripts\yapilanlar_otomatik_guncelle.ps1 -Asama "5.8" -Baslik "Test"
#   .\scripts\yapilanlar_otomatik_guncelle.ps1 -Asama "X.Y" -Baslik "..." -Detay "..."
#   .\scripts\yapilanlar_otomatik_guncelle.ps1 -SonDurum  # Sadece baglam bilgisi
#
# Versiyon: v0.5 (18 May 2026, Asama 3.4 ilk uretim)
# Kural uyumu: #15 (ASCII-only), #16 (native exe 2>&1 yok)

param(
    [string]$Asama = "X.Y",
    [string]$Baslik = "<BASLIK>",
    [string]$Detay = "",
    [switch]$SonDurum,
    [switch]$ToFile
)

$ErrorActionPreference = "Continue"
$repoRoot = (git rev-parse --show-toplevel).Trim()
if (-not $repoRoot) { Write-Host "Git deposu degil." -ForegroundColor Red; exit 1 }

# ============================================================
# Otomatik baglam bilgisi
# ============================================================
$tarih = Get-Date -Format "dd MMMM yyyy"
$saat = Get-Date -Format "HH:mm"
$tarihSaat = "$tarih, ~$saat"

$sonCommitHash = (git -C $repoRoot log -1 --format="%h").Trim()
$sonCommitMesaj = (git -C $repoRoot log -1 --format="%s").Trim()
$toplamCommit = (git -C $repoRoot rev-list --count HEAD).Trim()

# CLAUDE.md kural sayimi
$claudeMd = Join-Path $repoRoot "CLAUDE.md"
$claudeLines = Get-Content -Path $claudeMd -Encoding UTF8
$kuralSayim = ($claudeLines | Where-Object { $_ -match '^### Kural \d+' }).Count
$ilkeSayim = ($claudeLines | Where-Object { $_ -match '^### .lke \d+' }).Count
$claudeSatir = $claudeLines.Count

# Vizyon durumu
$vizyon = Join-Path $repoRoot "notebook\Notebook_A_Vizyon.md"
$vizyonSatir = (Get-Content -Path $vizyon -Encoding UTF8).Count

# Memory
$memoryDir = "$env:USERPROFILE\.claude\projects\C--Projeler-Quanfina\memory"
$memSayim = if (Test-Path $memoryDir) { (Get-ChildItem -Path $memoryDir -Filter "*.md").Count } else { 0 }

# Son 7 gun commit sayimi
$son7Gun = (git -C $repoRoot log --since="7 days ago" --format="%h" | Measure-Object).Count

# Son YAPILANLAR.md guncelleme tarihi
$yapilanlarMd = Join-Path $repoRoot "notebook\YAPILANLAR.md"
$yapilanlarSonGuncelleme = ""
if (Test-Path $yapilanlarMd) {
    $yapilanlarLines = Get-Content -Path $yapilanlarMd -Encoding UTF8
    $sonSatir = $yapilanlarLines | Where-Object { $_ -match '^\*\*Son guncelleme' } | Select-Object -Last 1
    if ($sonSatir) { $yapilanlarSonGuncelleme = $sonSatir }
}

# ============================================================
# -SonDurum modu: sadece baglam goster
# ============================================================
if ($SonDurum) {
    Write-Host ""
    Write-Host "=== Quanfina Son Durum (yapilanlar_otomatik_guncelle.ps1 -SonDurum) ===" -ForegroundColor Cyan
    Write-Host ""
    Write-Host "Tarih       : $tarihSaat"
    Write-Host "Son commit  : $sonCommitHash - $sonCommitMesaj"
    Write-Host "Toplam commit (HEAD): $toplamCommit"
    Write-Host "Son 7 gun   : $son7Gun commit"
    Write-Host ""
    Write-Host "Anayasa     : CLAUDE.md $claudeSatir satir, $kuralSayim Kural, $ilkeSayim Ilke"
    Write-Host "Karar gunlugu: Vizyon $vizyonSatir satir"
    Write-Host "Memory      : $memSayim dosya"
    Write-Host ""
    if ($yapilanlarSonGuncelleme) {
        Write-Host "YAPILANLAR son guncelleme satiri:" -ForegroundColor DarkGray
        Write-Host "  $yapilanlarSonGuncelleme" -ForegroundColor DarkGray
    }
    Write-Host ""
    exit 0
}

# ============================================================
# Sablon uretimi (default)
# ============================================================
$sablon = New-Object System.Text.StringBuilder
function S { param([string]$l) [void]$sablon.AppendLine($l) }

S "---"
S ""
# NOT: ASCII-only baslik (Kural #15). Sn. Ferit YAPILANLAR.md'de manuel "ASAMA" -> "AŞAMA" yapabilir.
S "## ASAMA $Asama -- $Baslik ($tarihSaat)"
S ""
if ($Detay) {
    S "$Detay"
    S ""
}
S "### Cikti / yansimalar"
S ""
S "- [Madde 1 - yeni dosya/script]"
S "- [Madde 2 - guncelleme]"
S "- [Madde 3 - test sonucu]"
S ""
S "### Sistem durumu (otomatik)"
S ""
S "| Konu | Deger |"
S "|---|---|"
S "| Son commit | ``$sonCommitHash`` - $sonCommitMesaj |"
S "| Toplam commit (HEAD) | $toplamCommit |"
S "| Son 7 gun | $son7Gun commit |"
S "| CLAUDE.md | $claudeSatir satir, $kuralSayim Kural, $ilkeSayim Ilke |"
S "| Vizyon | $vizyonSatir satir |"
S "| Memory | $memSayim dosya |"
S ""
S "**Siradaki:** [Bir sonraki adim aciklamasi]"
S ""
S "**Son guncelleme:** $tarihSaat -- Asama $Asama RESMI KAPANIS"

$ciktiMetni = $sablon.ToString()

if ($ToFile) {
    $outPath = Join-Path $env:TEMP "quanfina_yapilanlar_sablon.md"
    [System.IO.File]::WriteAllText($outPath, $ciktiMetni, [System.Text.UTF8Encoding]::new($false))
    Write-Host "Sablon TEMP'e yazildi: $outPath" -ForegroundColor Green
    Write-Host "Kopyala/yapistirip YAPILANLAR.md sonuna ekle." -ForegroundColor DarkGray
} else {
    Write-Host ""
    Write-Host "=== YAPILANLAR.md icin kapanis sablonu ===" -ForegroundColor Cyan
    Write-Host "(Kopyala, YAPILANLAR.md sonuna yapistir, koseli parantez maddelerini doldur)" -ForegroundColor DarkGray
    Write-Host ""
    Write-Host $ciktiMetni
}

exit 0
