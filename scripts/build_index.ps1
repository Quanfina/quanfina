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
# Surum: 2 (18 May 2026) - yetim tespit + _INDEX.md patch onerisi
# - scripts/, notebook/*.md ve kok Python icin yetim taramasi
# - Yetim bulunursa OZET bolumunde _INDEX.md icin satir onerisi uretilir
# - Asama 5'te tam otomatik uretici (mevcut _INDEX.md overwrite) eklenebilir
# Kural #15 (ASCII-only) + Kural #16 (native exe 2>&1 yasagi) uyumlu

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

# --- Yetim dosya tarama (indekste olmayan) ---
Write-Host ""
Write-Host "8. Yetim dosya taramasi" -ForegroundColor Yellow

$yetimList = @()

# 8a. Kok Python
$gercekKok = Get-ChildItem -Path $repoRoot -File -Filter "*.py" | Select-Object -ExpandProperty Name
$yetimKok = $gercekKok | Where-Object { $_ -notin $rootPython }
if ($yetimKok) {
    Write-Host "  [YETIM] Kok Python:" -ForegroundColor Red
    $yetimKok | ForEach-Object {
        Write-Host "    $_" -ForegroundColor Red
        $script:findings += "yetim-kok-py: $_"
        $yetimList += @{ Path = $_; Kategori = "Yasayan Python (kok)" }
    }
} else {
    Write-Host "  [OK] Yetim kok Python yok" -ForegroundColor Green
}

# 8b. Scripts klasoru (PS1 + PY)
$beklenenScript = @(
    "sizma_kontrol.ps1", "notebook_yedekle.ps1", "build_index.ps1",
    "hesap_tarama.ps1", "run_migration.py", "seed_initial_data.py",
    "seed_symbol_lists.py"
)
$scriptsDir = Join-Path $repoRoot "scripts"
if (Test-Path $scriptsDir) {
    $gercekScript = Get-ChildItem -Path $scriptsDir -File | Where-Object {
        $_.Extension -in ".ps1", ".py"
    } | Select-Object -ExpandProperty Name
    $yetimScript = $gercekScript | Where-Object { $_ -notin $beklenenScript }
    if ($yetimScript) {
        Write-Host "  [YETIM] scripts/:" -ForegroundColor Red
        $yetimScript | ForEach-Object {
            Write-Host "    scripts/$_" -ForegroundColor Red
            $script:findings += "yetim-scripts: scripts/$_"
            $yetimList += @{ Path = "scripts/$_"; Kategori = "Scripts" }
        }
    } else {
        Write-Host "  [OK] Yetim scripts dosyasi yok" -ForegroundColor Green
    }
}

# 8c. notebook/*.md (sistem katmani markdown'lari)
$beklenenNotebook = @(
    "_BASLAT.md", "_ROADMAP.md", "_LINKLER.md", "_INDEX.md",
    "_KOD_ENVANTERI.md", "_DEVIR.md", "_kisisel_okuma.md",
    "YAPILANLAR.md", "Notebook_A_Vizyon.md",
    "Notebook_B6_AdimlarKarar.md", "Notebook_C1_Sprint_QuickStart.md",
    "Notebook_C2_EK1-8.md", "Notebook_C3_EK9_DBSchema.md",
    "EK10_TradeGrader_Sentezi.md"
)
$notebookDir = Join-Path $repoRoot "notebook"
if (Test-Path $notebookDir) {
    $gercekNotebook = Get-ChildItem -Path $notebookDir -File -Filter "*.md" | Select-Object -ExpandProperty Name
    $yetimNotebook = $gercekNotebook | Where-Object { $_ -notin $beklenenNotebook }
    if ($yetimNotebook) {
        Write-Host "  [YETIM] notebook/*.md:" -ForegroundColor Red
        $yetimNotebook | ForEach-Object {
            Write-Host "    notebook/$_" -ForegroundColor Red
            $script:findings += "yetim-notebook: notebook/$_"
            $yetimList += @{ Path = "notebook/$_"; Kategori = "Sistem katmani" }
        }
    } else {
        Write-Host "  [OK] Yetim notebook/*.md yok" -ForegroundColor Green
    }
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

    # _INDEX.md patch onerisi (uretici davranis)
    if ($yetimList.Count -gt 0) {
        Write-Host ""
        Write-Host "=== _INDEX.md PATCH ONERISI ===" -ForegroundColor Cyan
        Write-Host "Yetim dosyalar icin uygun bolumlere ekle:" -ForegroundColor Yellow
        Write-Host ""
        $grouped = $yetimList | Group-Object -Property { $_.Kategori }
        foreach ($g in $grouped) {
            Write-Host "  [$($g.Name)]" -ForegroundColor Cyan
            foreach ($item in $g.Group) {
                $name = Split-Path $item.Path -Leaf
                $href = $item.Path -replace "^notebook/", ""
                Write-Host "    | [``$($item.Path)``]($href) | <ACIKLAMA EKLE> |"
            }
        }
    }
    Write-Host ""
    Write-Host "Cozum: _INDEX.md'i yukaridaki onerilerle guncelle" -ForegroundColor Yellow
    exit 1
}
