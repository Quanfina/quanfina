# Quanfina Proaktif Oneri Sistemi
# Referans: CLAUDE.md Manifesto Ozellik #8 (Ogrenen) - tam canlilik
#           notebook/_ROADMAP.md Asama 5.6
#           scripts/saglik_kontrol.ps1 (girdi 1)
#           scripts/pattern_ogren.ps1 (girdi 2)
#
# Amac: saglik_kontrol + pattern_ogren raporlarini birlestir, ust seviye
# proaktif oneriler uret. Asama 5'in SON adimi - Manifesto Ozellik #8
# (Ogrenen) tam canli.
#
# Kullanim:
#   .\scripts\proaktif_oneri.ps1
#   .\scripts\proaktif_oneri.ps1 -ToFile
#   .\scripts\proaktif_oneri.ps1 -DriveYazma  # Drive'a haftalik rapor
#
# Versiyon: v0.6 (21 May 2026, Sonraki kuyruk DINAMIK guncellendi)
# v0.5 (18 May 2026): Asama 5.6 ilk uretim
# v0.6 (21 May 2026): "Sonraki kuyruk" statik listesi guncellendi.
#   Asama 1-5 TUMU TAMAMLANDI. Eski script "Asama 2.3 + Asama 2 RESMI KAPANIS"
#   diye eski tamamlanmis isleri oneriyordu (P#3 izlenen pattern). Yeni statik
#   liste Sprint 4-bis.7+ kuyruk + Sn. Ferit stratejik eli + AI otonom rezerv.
#   Tam dinamik _ROADMAP parse Asama 6'da (gelecek).
# Kural uyumu: #15 (ASCII-only), #16 (native exe 2>&1 yok), #19 (UTF8 BOM-less)

param(
    [switch]$ToFile,
    [switch]$DriveYazma
)

$ErrorActionPreference = "Continue"

# Worktree-aware repo root tespiti (saglik_kontrol v0.5.1 + build_index v2.1 + notebook_yedekle pateni)
# $PSScriptRoot: script'in bulundugu yer = scripts/, parent = repo root
$scriptPath = $MyInvocation.MyCommand.Path
$scriptsDir = Split-Path -Parent $scriptPath
$repoRoot = Split-Path -Parent $scriptsDir
if (-not (Test-Path (Join-Path $repoRoot "CLAUDE.md"))) {
    Write-Host "Repo kokunde CLAUDE.md bulunamadi: $repoRoot" -ForegroundColor Red
    exit 1
}

# notebook/ + memory/ .gitignore'da, worktree'de yok. Ana repoya fallback
$notebookCheck = Join-Path $repoRoot "notebook\Notebook_A_Vizyon.md"
if (-not (Test-Path $notebookCheck)) {
    try {
        $gitCommonDir = (git -C $repoRoot rev-parse --git-common-dir 2>$null).Trim()
        if ($gitCommonDir) {
            if (-not [System.IO.Path]::IsPathRooted($gitCommonDir)) {
                $gitCommonDir = Join-Path $repoRoot $gitCommonDir
            }
            $mainRepo = Split-Path -Parent $gitCommonDir
            if (Test-Path (Join-Path $mainRepo "notebook\Notebook_A_Vizyon.md")) {
                Write-Host "[bilgi] worktree icindeyim, ana repo CLAUDE.md/notebook kullaniliyor: $mainRepo" -ForegroundColor DarkCyan
                $repoRoot = $mainRepo
            }
        }
    } catch { }
}

$tarihStr = Get-Date -Format "dd MMM yyyy HH:mm"
$tarihKisa = Get-Date -Format "yyyyMMdd_HHmm"

Write-Host ""
Write-Host "=== Quanfina Proaktif Oneri Sistemi ===" -ForegroundColor Cyan
Write-Host "Uretim: $tarihStr (proaktif_oneri.ps1 v0.5)" -ForegroundColor DarkGray
Write-Host ""

# Tek seferlik birlestirilmis rapor buffer
$report = New-Object System.Text.StringBuilder
function Add-Line { param([string]$line) [void]$report.AppendLine($line) }

Add-Line "# Quanfina Proaktif Oneri Raporu"
Add-Line ""
Add-Line "**Uretim:** $tarihStr (proaktif_oneri.ps1 v0.5)"
Add-Line ""
Add-Line "Bu rapor Asama 5'in SON ciktisi - Manifesto Ozellik #8 (Ogrenen)"
Add-Line "**tam canli** kanitin gunluk takibi. saglik_kontrol + pattern_ogren"
Add-Line "raporlarini birlestirip ust seviye oneri uretir."
Add-Line ""

# ============================================================
# 1. Hizli durum (saglik_kontrol.ps1 kompakt)
# ============================================================
Add-Line "## 1. Hizli Durum Ozeti"
Add-Line ""

$claudeMd = Join-Path $repoRoot "CLAUDE.md"
$claudeLines = Get-Content -Path $claudeMd -Encoding UTF8
$claudeSatir = $claudeLines.Count
$kuralSayim = ($claudeLines | Where-Object { $_ -match '^### Kural \d+(?! v\d+ alt)' }).Count
$ilkeSayim = ($claudeLines | Where-Object { $_ -match '^### .lke \d+' }).Count

$vizyon = Join-Path $repoRoot "notebook\Notebook_A_Vizyon.md"
$vizyonSatir = (Get-Content -Path $vizyon -Encoding UTF8).Count

$memoryDir = "$env:USERPROFILE\.claude\projects\C--Projeler-Quanfina\memory"
$memSayim = if (Test-Path $memoryDir) { (Get-ChildItem -Path $memoryDir -Filter "*.md").Count } else { 0 }
$feedbackSayim = if (Test-Path $memoryDir) { (Get-ChildItem -Path $memoryDir -Filter "feedback_*.md").Count } else { 0 }

$toplamCommit = (git -C $repoRoot rev-list --count HEAD).Trim()
$sonCommit = (git -C $repoRoot log -1 --format="%h %s").Trim()
$branch = (git -C $repoRoot branch --show-current).Trim()

Add-Line "| Konu | Deger |"
Add-Line "|---|---|"
Add-Line "| CLAUDE.md satir | $claudeSatir |"
Add-Line "| Operasyonel Kural | $kuralSayim |"
Add-Line "| Bilgi Mimarisi + GitHub Ilke | $ilkeSayim |"
Add-Line "| Vizyon satir | $vizyonSatir |"
Add-Line "| Memory dosyasi | $memSayim ($feedbackSayim feedback) |"
Add-Line "| Branch | $branch |"
Add-Line "| Toplam commit (HEAD) | $toplamCommit |"
Add-Line "| Son commit | $sonCommit |"
Add-Line ""

# ============================================================
# 2. Yedek + Manifesto #9
# ============================================================
Add-Line "## 2. Felaket Dayanikiligi (Manifesto #9)"
Add-Line ""

$lokalYedek = "$env:USERPROFILE\Quanfina_Backup"
$lokalYedekler = if (Test-Path $lokalYedek) {
    Get-ChildItem -Path $lokalYedek -Filter "notebook_yedek_*.zip" | Sort-Object LastWriteTime -Descending
} else { @() }

$driveYedekPath = $null
$gDrive = Get-PSDrive -Name "G" -ErrorAction SilentlyContinue
if ($gDrive) {
    $kokKlasorler = Get-ChildItem -Path "G:\" -Directory -ErrorAction SilentlyContinue
    foreach ($k in $kokKlasorler) {
        $tryPath = Join-Path $k.FullName "Quanfina_Backup"
        if (Test-Path $tryPath) { $driveYedekPath = $tryPath; break }
    }
}
$driveYedekler = if ($driveYedekPath) {
    Get-ChildItem -Path $driveYedekPath -Filter "notebook_yedek_*.zip" | Sort-Object LastWriteTime -Descending
} else { @() }

$task = Get-ScheduledTask -TaskName "Quanfina_Notebook_Yedek_Gunluk" -ErrorAction SilentlyContinue
$nextRun = if ($task) { (Get-ScheduledTaskInfo -TaskName "Quanfina_Notebook_Yedek_Gunluk").NextRunTime } else { $null }

Add-Line "| Katman | Yedek sayisi | Son yedek |"
Add-Line "|---|---:|---|"
$lokalSon = if ($lokalYedekler.Count -gt 0) { $lokalYedekler[0].LastWriteTime.ToString("dd MMM HH:mm") } else { "yok" }
$driveSon = if ($driveYedekler.Count -gt 0) { $driveYedekler[0].LastWriteTime.ToString("dd MMM HH:mm") } else { "yok" }
Add-Line "| Lokal | $($lokalYedekler.Count) | $lokalSon |"
Add-Line "| Drive (off-machine) | $($driveYedekler.Count) | $driveSon |"
Add-Line "| ScheduledTask | $(if ($task) { 'Aktif, ' + $nextRun } else { 'KURULU DEGIL' }) | - |"
Add-Line ""

# ============================================================
# 3. Pattern + Tescil ozetı
# ============================================================
Add-Line "## 3. Pattern + Tescil Ozeti (Manifesto #8)"
Add-Line ""

$hatalarMd = Join-Path $repoRoot "notebook\_HATALAR.md"
$aktifPattern = 0
$izlenenPattern = 0
if (Test-Path $hatalarMd) {
    $hatalarLines = Get-Content -Path $hatalarMd -Encoding UTF8
    $aktifPattern = ($hatalarLines | Where-Object { $_ -match '^### H#\d+' }).Count
    $izlenenPattern = ($hatalarLines | Where-Object { $_ -match '^### P#\d+' }).Count
}

$userSettings = "$env:USERPROFILE\.claude\settings.json"
$addlDirSayim = 0
if (Test-Path $userSettings) {
    $settings = Get-Content -Path $userSettings -Raw | ConvertFrom-Json
    $addlDirSayim = $settings.permissions.additionalDirectories.Count
}

Add-Line "| Kanal | Sayim |"
Add-Line "|---|---:|"
Add-Line "| Aktif pattern (H#) - kalici tescil edilmis | $aktifPattern |"
Add-Line "| Izlenen pattern (P#) - 2. ortaya cikis bekliyor | $izlenenPattern |"
Add-Line "| Anayasa Kural (CLAUDE.md) | $kuralSayim |"
Add-Line "| Memory feedback | $feedbackSayim |"
Add-Line "| Settings additionalDirectories | $addlDirSayim |"
Add-Line ""

# ============================================================
# 4. Son 7 gun commit aktivitesi (trend)
# ============================================================
Add-Line "## 4. Son 7 Gun Commit Aktivitesi"
Add-Line ""
$gunlukCommit = git -C $repoRoot log --since="7 days ago" --format="%ai" | ForEach-Object {
    ($_ -split ' ')[0]
} | Group-Object | Sort-Object Name -Descending

if ($gunlukCommit) {
    Add-Line "| Tarih | Commit |"
    Add-Line "|---|---:|"
    foreach ($g in $gunlukCommit) {
        Add-Line "| $($g.Name) | $($g.Count) |"
    }
    Add-Line ""
    $toplam7Gun = ($gunlukCommit | Measure-Object Count -Sum).Sum
    Add-Line "**Son 7 gun toplam:** $toplam7Gun commit"
} else {
    Add-Line "Son 7 gunde commit yok."
}
Add-Line ""

# ============================================================
# 5. Sistem Onerisi (proaktif)
# ============================================================
Add-Line "## 5. Sistem Onerisi"
Add-Line ""

$oneriler = @()

# Yedek kontrol
if ($lokalYedekler.Count -eq 0) {
    $oneriler += "[YEDEK] Lokal yedek yok. .\scripts\notebook_yedekle.ps1 calistir."
}
if ($driveYedekler.Count -eq 0) {
    $oneriler += "[DRIVE] Drive yedek yok. Drive Desktop App kurulu mu kontrol et."
}
if (-not $task) {
    $oneriler += "[TASK] ScheduledTask kurulu degil. .\scripts\notebook_yedekle.ps1 -ScheduledTask -Hedef <path>"
} elseif ($lokalYedekler.Count -gt 0) {
    $sonLokalGun = ($lokalYedekler[0].LastWriteTime - (Get-Date)).TotalDays * -1
    if ($sonLokalGun -gt 2) {
        $oneriler += "[YEDEK] Son lokal yedek $([math]::Round($sonLokalGun,1)) gun once. Tazeleme oneririm."
    }
}

# Pattern kontrol
if ($izlenenPattern -gt 0) {
    $oneriler += "[PATTERN] $izlenenPattern izlenen pattern var. 2. ortaya cikis Kural #14 tetikler."
}
if ($aktifPattern -ge 7) {
    $oneriler += "[OLGUNLUK] $aktifPattern aktif pattern tescil edilmis - yasayan sistem olgun durumda."
}

# Kural sayimi
if ($kuralSayim -ge 16) {
    $oneriler += "[ANAYASA] $kuralSayim Operasyonel Kural - guclu anayasa katmani."
}

# Komite uretim
if ($oneriler.Count -gt 0) {
    foreach ($o in $oneriler) { Add-Line "- $o" }
} else {
    Add-Line "Sistem tum kontrol noktalarinda saglikli. Yeni oneri yok."
}
Add-Line ""

# ============================================================
# 6. Sonraki adim onerisi (yol haritasi)
# ============================================================
Add-Line "## 6. Sonraki Adim Onerisi"
Add-Line ""

# Asama 5 durum kontrolu
$saglikDosya = Join-Path $repoRoot "notebook\_SAGLIK_KONTROL.md"
$hatalarDosya = Join-Path $repoRoot "notebook\_HATALAR.md"
$felsefeDosya = Join-Path $repoRoot "notebook\_FELSEFE.md"
$saglikScript = Join-Path $repoRoot "scripts\saglik_kontrol.ps1"
$patternScript = Join-Path $repoRoot "scripts\pattern_ogren.ps1"

$asama5Tamam = (Test-Path $saglikDosya) -and (Test-Path $hatalarDosya) -and (Test-Path $felsefeDosya) -and (Test-Path $saglikScript) -and (Test-Path $patternScript)

if ($asama5Tamam) {
    Add-Line "**Asama 1-5 TUMU TAMAMLANDI** (5.7 yaşayan sistem zirvesi 18 May, bu script 5.6'nin kendisi)."
    Add-Line "**Sprint 4-bis.6 'Sistem otursun' stabilizasyon aktif** (20-21 May): 60+ paket boyunca anayasa 23 Kural sabit."
    Add-Line ""
    Add-Line "**Sn. Ferit Stratejik Eli (oncelik sirasi):**"
    Add-Line "1. ACIK KONU #70 Cloud SQL erisim stratejisi (Auth Proxy / Connector / IP Whitelist) - DB up site canli demek"
    Add-Line "2. Risk Academy NotebookLM 8 PDF (Tharp/Basso/Vince/Elder) - KARAR ADAY #455 bagim"
    Add-Line "3. ACIK KONU #72 React 19 ESLint 13 stratejik error (UI mimari sprint)"
    Add-Line "4. ACIK KONU #74 Chrome MCP localhost permission (eklenti Site Erisimi)"
    Add-Line "5. 6 PENDING soru (bildirim/evren/saat/risk%/heat%/Polygon)"
    Add-Line ""
    Add-Line "_Not: ACIK KONU #22 (Carr 1./2. baski) v20.43'te KAPANDI - KARAR #444 hibrit politika._"
    Add-Line ""
    Add-Line "**Sprint 4-bis.7+ Kuyruk (KARAR ADAY, Sn. Ferit yon gerek):**"
    Add-Line "- #453 lightweight-charts (hisse detay TradingView yerine)"
    Add-Line "- #455 Risk-Merkez UI (Risk Academy PDF gerekli)"
    Add-Line "- #456 Filter Operator (Watchlist filtreleme)"
    Add-Line "- #457 Superscreen ❌ IPTAL (tek kullanici)"
    Add-Line ""
    Add-Line "**AI Otonom Rezerve (talimat bekler):**"
    Add-Line "- scanner.py refactor 1955 -> moduller (~2-3 saat)"
    Add-Line "- Sektor Rotasyonu / Portfolio Risk / Istatistikler sayfasi (Streamlit'ten port)"
    Add-Line "- Hisse detay TradingView timeout fix"
}
Add-Line ""

# ============================================================
# Cikti
# ============================================================
Add-Line "---"
Add-Line ""
Add-Line "_Bu rapor proaktif_oneri.ps1 v0.6 tarafindan uretildi._"
Add-Line "_Asama 5.6 ciktisi - Manifesto Ozellik #8 (Ogrenen) tam canli kanit._"
Add-Line "_Girdi: scripts/saglik_kontrol.ps1 + scripts/pattern_ogren.ps1_"

$ciktiMetni = $report.ToString()

if ($DriveYazma) {
    # Drive yedek path auto-detect
    if ($driveYedekPath) {
        $outPath = Join-Path $driveYedekPath "proaktif_rapor_$tarihKisa.md"
        [System.IO.File]::WriteAllText($outPath, $ciktiMetni, [System.Text.UTF8Encoding]::new($false))
        Write-Host "Rapor Drive'a yazildi: $outPath" -ForegroundColor Green
        Write-Host "(Stream otomatik upload eder)" -ForegroundColor DarkGray
    } else {
        Write-Host "[uyari] Drive yedek path bulunamadi. TEMP'e yaziliyor." -ForegroundColor Yellow
        $outPath = Join-Path $env:TEMP "quanfina_proaktif_rapor.md"
        [System.IO.File]::WriteAllText($outPath, $ciktiMetni, [System.Text.UTF8Encoding]::new($false))
    }
} elseif ($ToFile) {
    $outPath = Join-Path $env:TEMP "quanfina_proaktif_rapor.md"
    [System.IO.File]::WriteAllText($outPath, $ciktiMetni, [System.Text.UTF8Encoding]::new($false))
    Write-Host "Rapor TEMP'e yazildi: $outPath" -ForegroundColor Green
} else {
    Write-Host $ciktiMetni
}

exit 0
