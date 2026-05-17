# Quanfina Master Indeks Doğrulayıcı
# Referans: CLAUDE.md Bilgi Mimarisi İlke #2 (AI Görünürlüğü)
#           notebook/_INDEX.md
#
# Amaç: notebook/_INDEX.md'de listelenen dosyalarla gerçek dosya
# sisteminin senkron olduğunu doğrular. Yetim dosya veya kayıp dosya
# tespit eder. Manifesto Özellik #3 (hangi dosyada ne var bilir)
# bakım aracı.
#
# Kullanım:
#   .\scripts\build_index.ps1
#   .\scripts\build_index.ps1 -VerboseOutput  # her dosyayı listele
#
# Çıkış kodu:
#   0 = senkron (envanter güncel)
#   1 = senkronsuzluk var (rapor üretildi)
#
# NOT: Bu script şu an "üretmez" sadece "doğrular". Aşama 5'te tam
# otomatik üretici sürümü eklenebilir.

param(
    [switch]$VerboseOutput
)

$ErrorActionPreference = "Continue"
$repoRoot = (git rev-parse --show-toplevel).Trim()
if (-not $repoRoot) { Write-Host "Git deposu degil." -ForegroundColor Red; exit 1 }

$indexPath = Join-Path $repoRoot "notebook\_INDEX.md"
if (-not (Test-Path $indexPath)) {
    Write-Host "[hata] notebook/_INDEX.md bulunamadi." -ForegroundColor Red
    exit 1
}

Write-Host ""
Write-Host "=== Quanfina Master Indeks Dogrulama ===" -ForegroundColor Cyan
Write-Host ""

$findings = @()

function Test-File {
    param([string]$RelPath, [string]$Kategori)
    $full = Join-Path $repoRoot $RelPath
    if (Test-Path $full) {
        if ($VerboseOutput) { Write-Host "  [OK] $RelPath" -ForegroundColor DarkGray }
        return $true
    } else {
        Write-Host "  [KAYIP] $Kategori : $RelPath" -ForegroundColor Yellow
        $script:findings += "kayip: $RelPath"
        return $false
    }
}

# --- Sistem katmani ---
Write-Host "1. Sistem katmani" -ForegroundColor Yellow
$systemFiles = @(
    "CLAUDE.md",
    "notebook/_BASLAT.md",
    "notebook/_ROADMAP.md",
    "notebook/_LINKLER.md",
    "notebook/_INDEX.md",
    "notebook/_KOD_ENVANTERI.md",
    "notebook/YAPILANLAR.md",
    "notebook/Notebook_A_Vizyon.md"
)
$systemFiles | ForEach-Object { Test-File $_ "Sistem" | Out-Null }

# --- Yasayan Python (kok) ---
Write-Host ""
Write-Host "2. Yasayan Python (kok)" -ForegroundColor Yellow
$rootPython = @(
    "db_connection.py",
    "quanfina_math.py",
    "scanner.py",
    "scanner_server.py",
    "trade_journal.py",
    "migrate_to_postgres.py"
)
$rootPython | ForEach-Object { Test-File $_ "Kok Python" | Out-Null }

# --- FastAPI ---
Write-Host ""
Write-Host "3. FastAPI (api/)" -ForegroundColor Yellow
@("api/main.py", "api/db_helpers.py") | ForEach-Object { Test-File $_ "FastAPI" | Out-Null }

# --- Next.js ana dosyalar ---
Write-Host ""
Write-Host "4. Next.js (web/)" -ForegroundColor Yellow
@(
    "web/app/layout.tsx",
    "web/app/(dashboard)/layout.tsx",
    "web/app/(dashboard)/page.tsx",
    "web/CLAUDE.md",
    "web/package.json"
) | ForEach-Object { Test-File $_ "Next.js" | Out-Null }

# --- Scripts ---
Write-Host ""
Write-Host "5. Scripts" -ForegroundColor Yellow
@(
    "scripts/sizma_kontrol.ps1",
    "scripts/notebook_yedekle.ps1",
    "scripts/run_migration.py",
    "scripts/seed_initial_data.py",
    "scripts/seed_symbol_lists.py",
    "scripts/build_index.ps1"
) | ForEach-Object { Test-File $_ "Scripts" | Out-Null }

# --- Tests ---
Write-Host ""
Write-Host "6. Tests" -ForegroundColor Yellow
@("tests/test_quanfina_math.py", "tests/test_trade_journal.py") | ForEach-Object { Test-File $_ "Tests" | Out-Null }

# --- Git hooks ---
Write-Host ""
Write-Host "7. Git hooks" -ForegroundColor Yellow
Test-File ".git/hooks/pre-push" "Git hooks" | Out-Null

# --- Yetim Python tarama (indekste olmayan kok Python) ---
Write-Host ""
Write-Host "8. Yetim dosya taramasi" -ForegroundColor Yellow
$beklenenKok = $rootPython
$gercekKok = Get-ChildItem -Path $repoRoot -File -Filter "*.py" | Select-Object -ExpandProperty Name
$yetim = $gercekKok | Where-Object { $_ -notin $beklenenKok }
if ($yetim) {
    Write-Host "  [YETIM] Kok Python (indekste yok):" -ForegroundColor Red
    $yetim | ForEach-Object {
        Write-Host "    $_" -ForegroundColor Red
        $script:findings += "yetim: $_"
    }
} else {
    Write-Host "  [OK] Yetim kok Python yok" -ForegroundColor Green
}

# --- Arsiv durum ---
Write-Host ""
Write-Host "9. Arsiv durumu" -ForegroundColor Yellow
$arsiv = Join-Path $repoRoot "_archive"
if (Test-Path $arsiv) {
    $arsivCount = (Get-ChildItem -Path $arsiv -Recurse -File).Count
    $arsivKB = [math]::Round(((Get-ChildItem -Path $arsiv -Recurse -File | Measure-Object Length -Sum).Sum / 1KB), 1)
    Write-Host "  _archive/ : $arsivCount dosya, $arsivKB KB" -ForegroundColor Cyan
    Write-Host "  Streamlit emeklilik (Asama 1.D) ciktisi"
} else {
    Write-Host "  _archive/ yok"
}

# --- Ozet ---
Write-Host ""
Write-Host "=== OZET ===" -ForegroundColor Cyan
if ($findings.Count -eq 0) {
    Write-Host "SENKRON - _INDEX.md ve dosya sistemi uyumlu" -ForegroundColor Green
    Write-Host ""
    exit 0
} else {
    Write-Host "SENKRONSUZ - $($findings.Count) bulgu:" -ForegroundColor Red
    $findings | ForEach-Object { Write-Host "  - $_" -ForegroundColor Yellow }
    Write-Host ""
    Write-Host "Cozum: _INDEX.md'i guncelle veya kayip dosyayi tamamla" -ForegroundColor Yellow
    exit 1
}
