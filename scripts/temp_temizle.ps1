# Quanfina Temp Temizleme - Lokal debug clutter hijyen
# Referans: CLAUDE.md Kural #8 v2 (Otonom Hijyen Modu B bolumu) + Kural #18 (Pasif Oge Cikarma)
#           Kural #7 (Bilincli Karar - 5+ dosya birikimi refactor tetik)
#
# Amac: Her paket dogrulamada biriken tek-seferlik debug dosyalarini temizler.
# _*.mjs (P436/P437/... paket dogrulama), runtime log, test json, eski backup.
# Hepsi GITIGNORED (repo'ya girmez) ama lokal birikir - 42'ye ulasmisti (P580).
#
# GUVENLIK (asla silinmez):
#   - .env (secret), quanfina.db (legacy referans)
#   - Kanonik test (visual_test*.mjs - underscore YOK)
#   - _archive/ klasorleri (bilincli arsiv)
#   - 7 gunden yeni *.backup_* (Kural #6 backup guvenligi)
#
# Versiyon: v1.0 (22 Haz 2026, P581 - Sn. Ferit "ekle" talimati)
# Kural #15 (ASCII-only) + #16 (native exe 2>&1 yok) uyumlu
#
# Kullanim:
#   .\scripts\temp_temizle.ps1            # Dry-run (sadece raporlar, SILMEZ)
#   .\scripts\temp_temizle.ps1 -Uygula    # Gercek silme
#   .\scripts\temp_temizle.ps1 -BackupGun 14  # backup esigi (default 7)

param(
    [switch]$Uygula,
    [int]$BackupGun = 7
)

$ErrorActionPreference = "Continue"
$scriptPath = $MyInvocation.MyCommand.Path
$scriptsDir = Split-Path -Parent $scriptPath
$repoRoot = Split-Path -Parent $scriptsDir

Write-Host ""
Write-Host "=== Quanfina Temp Temizleme ===" -ForegroundColor Cyan
if ($Uygula) {
    Write-Host "Mod: UYGULA (gercek silme)" -ForegroundColor Yellow
} else {
    Write-Host "Mod: KURU CALISMA (dry-run - sadece rapor, -Uygula ile sil)" -ForegroundColor DarkGray
}
Write-Host ""

$adaylar = New-Object System.Collections.ArrayList

# A. Temp debug _*.mjs (root + web/)
foreach ($dir in @($repoRoot, (Join-Path $repoRoot "web"))) {
    Get-ChildItem -Path $dir -Filter "_*.mjs" -File -ErrorAction SilentlyContinue | ForEach-Object {
        [void]$adaylar.Add([PSCustomObject]@{ Kategori = "A temp .mjs"; Dosya = $_.FullName })
    }
}

# A2. _tmp_*.py (gitignore patterni)
Get-ChildItem -Path $repoRoot -Filter "_tmp_*.py" -File -ErrorAction SilentlyContinue | ForEach-Object {
    [void]$adaylar.Add([PSCustomObject]@{ Kategori = "A temp .py"; Dosya = $_.FullName })
}

# B. Runtime log + test json (regenerable, acik isimle - over-match yok)
$loglar = @("scanner_err.txt", "scanner_log.txt", "uvicorn_err.log", "uvicorn_out.log", "visual_test_report.json")
foreach ($name in $loglar) {
    $p = Join-Path $repoRoot $name
    if (Test-Path $p) {
        [void]$adaylar.Add([PSCustomObject]@{ Kategori = "B log/json"; Dosya = $p })
    }
}

# C. Eski backup (sadece BackupGun'den eski - Kural #6 yeni backup korunur)
$esik = (Get-Date).AddDays(-$BackupGun)
Get-ChildItem -Path $repoRoot -Filter "*.backup_*" -File -ErrorAction SilentlyContinue |
    Where-Object { $_.LastWriteTime -lt $esik } | ForEach-Object {
        [void]$adaylar.Add([PSCustomObject]@{ Kategori = "C eski backup (>$BackupGun gun)"; Dosya = $_.FullName })
    }

# Guvenlik filtresi - kritik dosyalar asla
$korunan = @(".env", "quanfina.db")
$adaylar = $adaylar | Where-Object {
    $ad = Split-Path $_.Dosya -Leaf
    ($korunan -notcontains $ad) -and ($_.Dosya -notmatch '[\\/]_archive[\\/]')
}

$toplam = @($adaylar).Count
if ($toplam -eq 0) {
    Write-Host "[ok] Temizlenecek temp dosya yok (zaten temiz)" -ForegroundColor Green
    Write-Host ""
    exit 0
}

# Kategori ozet
$adaylar | Group-Object Kategori | ForEach-Object {
    Write-Host ("  {0,-28} {1} dosya" -f $_.Name, $_.Count) -ForegroundColor Gray
}
Write-Host ""

$silindi = 0
$hata = 0
foreach ($a in $adaylar) {
    $ad = Split-Path $a.Dosya -Leaf
    if ($Uygula) {
        try {
            Remove-Item -LiteralPath $a.Dosya -Force -ErrorAction Stop
            $silindi++
        } catch {
            Write-Host "  [hata] $ad" -ForegroundColor Red
            $hata++
        }
    } else {
        Write-Host "  [kuru-sil] $ad" -ForegroundColor DarkGray
    }
}

Write-Host ""
if ($Uygula) {
    Write-Host "[ok] Silindi: $silindi / $toplam" -ForegroundColor Green
    if ($hata -gt 0) { Write-Host "[uyari] Hata: $hata" -ForegroundColor Red }
} else {
    Write-Host "[kuru] $toplam dosya silinecek (-Uygula ile gercek sil)" -ForegroundColor Yellow
}
Write-Host ""
exit 0
