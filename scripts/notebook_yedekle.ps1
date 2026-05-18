# Quanfina Notebook Yedekleme
# Referans: CLAUDE.md → Sistem Manifestosu Özellik #9 (Felaket Dayanıklılığı)
#           notebook/_ROADMAP.md → Aşama 2.1 (Drive Backup)
#
# Amaç: notebook/ klasörünü timestamp'li ZIP olarak yedek dizine kopyalar.
# Drive Desktop App kuruluysa hedef Drive senkron klasörü olabilir.
# Cron veya manuel tetikli.
#
# Kullanım:
#   .\scripts\notebook_yedekle.ps1
#   .\scripts\notebook_yedekle.ps1 -Hedef "G:\Drive\Quanfina_Backup"
#   .\scripts\notebook_yedekle.ps1 -SonNAdet 5
#   .\scripts\notebook_yedekle.ps1 -ScheduledTask -Hedef "G:\Drive\Quanfina_Backup"  # Task Scheduler kayit
#   .\scripts\notebook_yedekle.ps1 -UnregisterTask  # Task Scheduler kayit sil
#
# Default hedef: $env:USERPROFILE\Quanfina_Backup
# Default SonN: 10 (en son 10 yedek tutulur, eskileri silinir)
# Default Saat: 09:00 (ScheduledTask icin)

param(
    [string]$Hedef = (Join-Path $env:USERPROFILE "Quanfina_Backup"),
    [int]$SonNAdet = 10,
    [switch]$ScheduledTask,
    [switch]$UnregisterTask,
    [string]$Saat = "09:00"
)

$ErrorActionPreference = "Stop"

$TaskName = "Quanfina_Notebook_Yedek_Gunluk"

# ============================================================
# ScheduledTask kurulum/kaldirma modu
# ============================================================
if ($ScheduledTask -or $UnregisterTask) {
    # Repo kokunu bul (script path tabanli, git'e bagimli degil)
    $scriptPath = $MyInvocation.MyCommand.Path
    $scriptsDir = Split-Path -Parent $scriptPath
    $repoBase = Split-Path -Parent $scriptsDir

    Write-Host ""
    Write-Host "=== Quanfina Notebook Yedek ScheduledTask ===" -ForegroundColor Cyan
    Write-Host ""

    # Once mevcut task var mi temizle (idempotent)
    $mevcut = Get-ScheduledTask -TaskName $TaskName -ErrorAction SilentlyContinue
    if ($mevcut) {
        Write-Host "[bakim] Mevcut task siliniyor: $TaskName" -ForegroundColor Yellow
        Unregister-ScheduledTask -TaskName $TaskName -Confirm:$false
    }

    if ($UnregisterTask) {
        if ($mevcut) {
            Write-Host "[ok] Task silindi: $TaskName" -ForegroundColor Green
        } else {
            Write-Host "[bilgi] Task zaten yoktu: $TaskName" -ForegroundColor DarkGray
        }
        exit 0
    }

    # ScheduledTask kurma
    Write-Host "Task adi : $TaskName"
    Write-Host "Tetik    : Gunluk $Saat"
    Write-Host "Hedef    : $Hedef"
    Write-Host "Script   : $scriptPath"
    Write-Host ""

    $argList = @(
        "-NoProfile",
        "-NonInteractive",
        "-ExecutionPolicy", "Bypass",
        "-File", "`"$scriptPath`"",
        "-Hedef", "`"$Hedef`""
    )
    $action = New-ScheduledTaskAction `
        -Execute "powershell.exe" `
        -Argument ($argList -join " ")

    $trigger = New-ScheduledTaskTrigger -Daily -At $Saat

    $settings = New-ScheduledTaskSettingsSet `
        -StartWhenAvailable `
        -DontStopOnIdleEnd `
        -ExecutionTimeLimit (New-TimeSpan -Minutes 10)

    $principal = New-ScheduledTaskPrincipal `
        -UserId $env:USERNAME `
        -LogonType Interactive `
        -RunLevel Limited

    Register-ScheduledTask `
        -TaskName $TaskName `
        -Action $action `
        -Trigger $trigger `
        -Settings $settings `
        -Principal $principal `
        -Description "Quanfina notebook/ klasoru gunluk ZIP yedek (Manifesto Ozellik #9)" | Out-Null

    Write-Host "[ok] ScheduledTask kuruldu: $TaskName" -ForegroundColor Green
    Write-Host ""
    Write-Host "Dogrulama: Get-ScheduledTask -TaskName '$TaskName'" -ForegroundColor DarkGray
    Write-Host "Manuel calistirma: Start-ScheduledTask -TaskName '$TaskName'" -ForegroundColor DarkGray
    Write-Host "Silme: .\scripts\notebook_yedekle.ps1 -UnregisterTask" -ForegroundColor DarkGray
    Write-Host ""
    exit 0
}

# Repo kökünü bul
try {
    $repoRoot = (git rev-parse --show-toplevel 2>$null).Trim()
} catch {
    $repoRoot = ""
}
if (-not $repoRoot) {
    Write-Host "[hata] Git deposu bulunamadi." -ForegroundColor Red
    exit 1
}

$notebookDir = Join-Path $repoRoot "notebook"
if (-not (Test-Path $notebookDir)) {
    Write-Host "[hata] notebook/ klasoru yok: $notebookDir" -ForegroundColor Red
    exit 1
}

# Hedef yoksa olustur
if (-not (Test-Path $Hedef)) {
    Write-Host "[setup] Hedef klasor olusturuluyor: $Hedef" -ForegroundColor Yellow
    New-Item -ItemType Directory -Path $Hedef -Force | Out-Null
}

# Zip dosya adi
$stamp = Get-Date -Format "yyyyMMdd_HHmmss"
$zipName = "notebook_yedek_$stamp.zip"
$zipPath = Join-Path $Hedef $zipName

Write-Host ""
Write-Host "=== Quanfina Notebook Yedekleme ===" -ForegroundColor Cyan
Write-Host "Kaynak : $notebookDir"
Write-Host "Hedef  : $zipPath"
Write-Host ""

# Notebook icindeki yasayan dosyalari say
$fileCount = (Get-ChildItem -Path $notebookDir -Recurse -File).Count
$totalSize = (Get-ChildItem -Path $notebookDir -Recurse -File | Measure-Object -Property Length -Sum).Sum
$totalMB = [math]::Round($totalSize / 1MB, 2)
Write-Host "Yedeklenecek: $fileCount dosya, $totalMB MB" -ForegroundColor White

# ZIP olustur
try {
    Compress-Archive -Path "$notebookDir\*" -DestinationPath $zipPath -Force -ErrorAction Stop
} catch {
    Write-Host "[hata] ZIP olusturulamadi: $_" -ForegroundColor Red
    exit 1
}

$zipSize = (Get-Item $zipPath).Length
$zipMB = [math]::Round($zipSize / 1MB, 2)
Write-Host "[ok] Yedeklendi: $zipMB MB" -ForegroundColor Green

# Eski yedekleri temizle (son N adet kalir)
$tumYedekler = Get-ChildItem -Path $Hedef -Filter "notebook_yedek_*.zip" |
               Sort-Object LastWriteTime -Descending
if ($tumYedekler.Count -gt $SonNAdet) {
    $silinecek = $tumYedekler | Select-Object -Skip $SonNAdet
    Write-Host ""
    Write-Host "[bakim] Eski yedekler temizleniyor (son $SonNAdet kalsin)" -ForegroundColor Yellow
    foreach ($eski in $silinecek) {
        Remove-Item $eski.FullName -Force
        Write-Host "  silindi: $($eski.Name)" -ForegroundColor DarkGray
    }
}

# Ozet
$mevcutSayisi = (Get-ChildItem -Path $Hedef -Filter "notebook_yedek_*.zip").Count
Write-Host ""
Write-Host "=== OZET ===" -ForegroundColor Cyan
Write-Host "Yedek hedefi  : $Hedef"
Write-Host "Tutulan yedek : $mevcutSayisi adet"
Write-Host "Son yedek     : $zipName"
Write-Host ""
Write-Host "TAVSIYE: Bu klasor Google Drive Desktop senkron klasoru altinda" -ForegroundColor Yellow
Write-Host "olursa otomatik Drive'a yuklenir. Kontrol et:" -ForegroundColor Yellow
Write-Host "  - Drive Desktop App kurulu mu" -ForegroundColor Yellow
Write-Host "  - Hedef '$Hedef' Drive senkron yolu icinde mi" -ForegroundColor Yellow
Write-Host "  - notebook/_LINKLER.md'a Drive klasor URL'i eklenecek (Asama 2.5)" -ForegroundColor Yellow

exit 0
