# Quanfina Pasif Oge Tarama (Kural #18 destek scripti)
# Referans: CLAUDE.md Kural #18 (Pasif Oge Cikarma Protokolu - Negatif Tescil)
#           Hijyen turu (3 ayda bir) PASIF aday listesi uretici
#
# Amac: 30 gun referans yoksa PASIF aday isaretle. ACIK KONU, memory,
# script, notebook belge gibi ogeleri tara, eskimis olanlari rapor et.
# Yikici eylem YAPMAZ - sadece aday listesi Sn. Ferit karari icin.
#
# Versiyon: v0.6 (06 Tem 2026, D2-02) - Kural #15 on-ihlali kapatildi:
#   script icerigi ASCII-only'e cevrildi (Turkce regex'ler [char] kodlariyla),
#   Vizyon okuma Get-Content -Raw -> [IO.File]::ReadAllText UTF8 (H#11 dersi)
# v0.5 (19 May 2026, ilk surum)
# Kural #15 + #16 uyumlu
#
# Kullanim:
#   .\scripts\pasif_tara.ps1                      # 30 gun default
#   .\scripts\pasif_tara.ps1 -EsikGun 60          # 60 gun
#   .\scripts\pasif_tara.ps1 -Detayli             # her oge icin son referans

param(
    [int]$EsikGun = 30,
    [switch]$Detayli
)

$ErrorActionPreference = "Continue"
$scriptPath = $MyInvocation.MyCommand.Path
$repoRoot = Split-Path -Parent (Split-Path -Parent $scriptPath)
$simdi = Get-Date
$esik = $simdi.AddDays(-$EsikGun)

Write-Host ""
Write-Host "=== Quanfina Pasif Oge Tarama (v0.6) ===" -ForegroundColor Cyan
Write-Host "Esik: son $EsikGun gun (referans yoksa PASIF aday)"
Write-Host "Tarama zamani: $($simdi.ToString('yyyy-MM-dd HH:mm:ss'))"
Write-Host ""

# 1. Memory dosyalari (LastWriteTime + git referans)
Write-Host "--- Bolum 1: Memory dosyalari ---" -ForegroundColor Yellow
$memoryPath = "C:\Users\Ferit\.claude\projects\C--Projeler-Quanfina\memory"
if (Test-Path $memoryPath) {
    $memDosyalari = Get-ChildItem $memoryPath -File -Filter "*.md" | Where-Object { $_.Name -ne "MEMORY.md" }
    $pasifMem = $memDosyalari | Where-Object { $_.LastWriteTime -lt $esik }
    if ($pasifMem) {
        foreach ($m in $pasifMem) {
            $gunSayisi = [math]::Round(($simdi - $m.LastWriteTime).TotalDays, 0)
            Write-Host "  [PASIF aday] $($m.Name) - $gunSayisi gun once" -ForegroundColor DarkYellow
        }
    } else {
        Write-Host "  Pasif memory yok" -ForegroundColor DarkGray
    }
}
Write-Host ""

# 2. Script dosyalari (scripts/*.ps1 son git commit zamani)
Write-Host "--- Bolum 2: Script dosyalari ---" -ForegroundColor Yellow
$scriptsPath = Join-Path $repoRoot "scripts"
$pasifScript = @()
foreach ($s in (Get-ChildItem $scriptsPath -File -Filter "*.ps1")) {
    $sonCommit = git -C $repoRoot log -1 --format="%ai" -- "scripts/$($s.Name)" 2>$null
    if ($sonCommit) {
        $sonCommitDate = [DateTime]::Parse($sonCommit)
        if ($sonCommitDate -lt $esik) {
            $gunSayisi = [math]::Round(($simdi - $sonCommitDate).TotalDays, 0)
            $pasifScript += [pscustomobject]@{ Ad = $s.Name; GunSayisi = $gunSayisi }
        }
    }
}
if ($pasifScript) {
    foreach ($s in $pasifScript) {
        Write-Host "  [PASIF aday] $($s.Ad) - son commit $($s.GunSayisi) gun once" -ForegroundColor DarkYellow
    }
} else {
    Write-Host "  Pasif script yok (hepsi son $EsikGun gun icinde commit)" -ForegroundColor DarkGray
}
Write-Host ""

# 3. Notebook sistem dosyalari
Write-Host "--- Bolum 3: Notebook sistem dosyalari ---" -ForegroundColor Yellow
$notebookPath = Join-Path $repoRoot "notebook"
$sistemDosyalari = Get-ChildItem $notebookPath -File -Filter "_*.md"
$pasifNotebook = $sistemDosyalari | Where-Object { $_.LastWriteTime -lt $esik }
if ($pasifNotebook) {
    foreach ($n in $pasifNotebook) {
        $gunSayisi = [math]::Round(($simdi - $n.LastWriteTime).TotalDays, 0)
        Write-Host "  [PASIF aday] $($n.Name) - $gunSayisi gun once" -ForegroundColor DarkYellow
    }
} else {
    Write-Host "  Pasif notebook sistem dosyasi yok" -ForegroundColor DarkGray
}
Write-Host ""

# 4. ACIK KONU semantik tarama (Vizyon icinde "COZULDU" yazan ama OPEN)
# Kural #15: Turkce karakterler [char] kodlariyla kurulur (dosya ASCII-only kalir)
Write-Host "--- Bolum 4: ACIK KONU semantik ---" -ForegroundColor Yellow
$vizyon = Join-Path $repoRoot "notebook\Notebook_A_Vizyon.md"
if (Test-Path $vizyon) {
    # H#11 dersi: Get-Content -Raw PS 5.1'de cp1254 okur, Turkce bozulur -> ReadAllText UTF8 zorunlu
    $vizyonIcerik = [System.IO.File]::ReadAllText($vizyon, [System.Text.Encoding]::UTF8)
    # ACIK KONU #X paragraflarini tara
    # ASCII-only kaynak (Kural #15): Turkce karakterler [char] kodlariyla kurulur
    $chC  = [char]0x00C7   # C-cedil buyuk
    $chc  = [char]0x00E7   # c-cedil kucuk
    $chI  = [char]0x0131   # noktasiz i
    $chO  = [char]0x00D6   # O-uml
    $chU  = [char]0x00DC   # U-uml
    $chOk = [char]0x2705   # yesil tik emoji
    $acikBuyuk = "A${chC}IK KONU"
    $acikKucuk = "A${chc}${chI}k Konu"
    $cozuldu   = "${chC}${chO}Z${chU}LD${chU}"
    $regex = '\*\*(' + $acikBuyuk + '|' + $acikKucuk + ') #(\d+).*?\*\*[^*]+?(?=\*\*(' + $acikBuyuk + '|' + $acikKucuk + ') #|\n## |\n---)'
    $matches = [regex]::Matches($vizyonIcerik, $regex, [System.Text.RegularExpressions.RegexOptions]::Singleline)
    $semanticAday = @()
    foreach ($m in $matches) {
        $blok = $m.Value
        # "RESOLVED", "COZULDU", "KAPANDI" yazan ama OPEN durumda kalan
        if ($blok -match "($cozuldu|RESOLVED|KAPANDI|KAPATILDI)" -and $blok -notmatch $chOk) {
            $no = [regex]::Match($blok, '#(\d+)').Groups[1].Value
            $semanticAday += "#$no"
        }
    }
    if ($semanticAday) {
        Write-Host "  Cozulmus gibi gozuken ama acik: $($semanticAday -join ', ')" -ForegroundColor DarkYellow
    } else {
        Write-Host "  Semantik tutarsiz ACIK KONU yok" -ForegroundColor DarkGray
    }
}
Write-Host ""

# Ozet
Write-Host "=== OZET ===" -ForegroundColor Cyan
Write-Host "Memory pasif aday  : $(if ($pasifMem) { $pasifMem.Count } else { 0 })"
Write-Host "Script pasif aday  : $(if ($pasifScript) { $pasifScript.Count } else { 0 })"
Write-Host "Notebook pasif aday: $(if ($pasifNotebook) { $pasifNotebook.Count } else { 0 })"
Write-Host "ACIK KONU semantik : $(if ($semanticAday) { $semanticAday.Count } else { 0 })"
Write-Host ""
Write-Host "Kural #18 hijyen turu karari icin Sn. Ferit'e listele." -ForegroundColor Yellow
Write-Host ""

exit 0
