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
    "hesap_tarama.ps1", "saglik_kontrol.ps1", "pattern_ogren.ps1",
    "proaktif_oneri.ps1", "drive_sync.ps1", "yapilanlar_otomatik_guncelle.ps1",
    "aciklama_konsolide.ps1",
    "run_migration.py", "seed_initial_data.py", "seed_symbol_lists.py"
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
# NOT: *_backup.md pattern'i _INDEX.md "Backup" bolumunde belgelenir, yetim sayilmaz
$beklenenNotebook = @(
    "_BASLAT.md", "_ROADMAP.md", "_LINKLER.md", "_INDEX.md",
    "_KOD_ENVANTERI.md", "_DEVIR.md", "_kisisel_okuma.md",
    "_SAGLIK_KONTROL.md", "_HATALAR.md", "_FELSEFE.md",
    "YAPILANLAR.md", "Notebook_A_Vizyon.md",
    "Notebook_B6_AdimlarKarar.md", "Notebook_C1_Sprint_QuickStart.md",
    "Notebook_C2_EK1-8.md", "Notebook_C3_EK9_DBSchema.md",
    "EK10_TradeGrader_Sentezi.md"
)
$notebookDir = Join-Path $repoRoot "notebook"
if (Test-Path $notebookDir) {
    $gercekNotebook = Get-ChildItem -Path $notebookDir -File -Filter "*.md" | Select-Object -ExpandProperty Name
    $yetimNotebook = $gercekNotebook | Where-Object {
        $_ -notin $beklenenNotebook -and $_ -notlike "*_backup.md"
    }
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

# --- Belge satir sayim raporu (v3.0 — manuel hijyen yardimcisi) ---
Write-Host ""
Write-Host "9. Belge satir sayim raporu" -ForegroundColor Yellow
Write-Host "   (Bu sayilar _INDEX.md ve _BASLAT.md'deki referanslarla manuel karsilastir)" -ForegroundColor DarkGray
$belgeDosyalari = @(
    @{ Path = "CLAUDE.md"; Etiket = "CLAUDE.md (anayasa)" },
    @{ Path = "notebook/_BASLAT.md"; Etiket = "_BASLAT.md" },
    @{ Path = "notebook/_INDEX.md"; Etiket = "_INDEX.md" },
    @{ Path = "notebook/_DEVIR.md"; Etiket = "_DEVIR.md" },
    @{ Path = "notebook/_ROADMAP.md"; Etiket = "_ROADMAP.md" },
    @{ Path = "notebook/_LINKLER.md"; Etiket = "_LINKLER.md" },
    @{ Path = "notebook/_KOD_ENVANTERI.md"; Etiket = "_KOD_ENVANTERI.md" },
    @{ Path = "notebook/_SAGLIK_KONTROL.md"; Etiket = "_SAGLIK_KONTROL.md" },
    @{ Path = "notebook/_HATALAR.md"; Etiket = "_HATALAR.md" },
    @{ Path = "notebook/_FELSEFE.md"; Etiket = "_FELSEFE.md" },
    @{ Path = "notebook/YAPILANLAR.md"; Etiket = "YAPILANLAR.md" },
    @{ Path = "notebook/Notebook_A_Vizyon.md"; Etiket = "Notebook_A_Vizyon.md" }
)
foreach ($belge in $belgeDosyalari) {
    $fullPath = Join-Path $repoRoot $belge.Path
    if (Test-Path $fullPath) {
        # NOT: (Get-Content).Count tum satirlari sayar (bos dahil).
        # Measure-Object -Line bos satirlari atlar (yanlis sonuc verir, KULLANMA).
        $satir = (Get-Content -Path $fullPath -Encoding UTF8).Count
        $boyutKB = [math]::Round((Get-Item $fullPath).Length / 1KB, 1)
        $etiketPadded = $belge.Etiket.PadRight(28)
        Write-Host "  $etiketPadded : $satir satir, $boyutKB KB" -ForegroundColor Cyan
    } else {
        Write-Host "  $($belge.Etiket) : [BULUNAMADI] $($belge.Path)" -ForegroundColor Red
        $script:findings += "kayip-belge: $($belge.Path)"
    }
}

# --- Arsiv durum ---
Write-Host ""
Write-Host "10. Arsiv durumu" -ForegroundColor Yellow
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
