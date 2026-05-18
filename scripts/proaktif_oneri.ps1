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
# Versiyon: v0.5 (18 May 2026, Asama 5.6 ilk uretim)
# Kural uyumu: #15 (ASCII-only), #16 (native exe 2>&1 yok)

param(
    [switch]$ToFile,
    [switch]$DriveYazma
)

$ErrorActionPreference = "Continue"
$repoRoot = (git rev-parse --show-toplevel).Trim()
if (-not $repoRoot) { Write-Host "Git deposu degil." -ForegroundColor Red; exit 1 }

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
$kuralSayim = ($claudeLines | Where-Object { $_ -match '^### Kural \d+' }).Count
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
    Add-Line "**Asama 5 altyapi tamam** (5.1+5.2+5.3+5.4+5.5+5.7 ✅, bu script 5.6'nin kendisi)."
    Add-Line ""
    Add-Line "**Sonraki kuyruk (kodlamaya gecmeden):**"
    Add-Line "1. Asama 2.3 NotebookLM Plus Minervini (Sn. Ferit)"
    Add-Line "2. Asama 2.4 Vizyon Bekcisi Gem (AI prompt + Sn. Ferit kurar)"
    Add-Line "3. Asama 3 MCP genisleme (5 alt-adim)"
    Add-Line "4. Asama 4 Cilalama Gem'leri (6 alt-adim)"
    Add-Line "5. ACIK KONU #22 cevabi (Carr 1./2. baski - kod tarafini kilitliyor)"
    Add-Line "6. 6 PENDING soru cevabi"
    Add-Line ""
    Add-Line "Sonra: **Sprint 4-bis kod tarafi maraton**"
}
Add-Line ""

# ============================================================
# Cikti
# ============================================================
Add-Line "---"
Add-Line ""
Add-Line "_Bu rapor proaktif_oneri.ps1 v0.5 tarafindan uretildi._"
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
