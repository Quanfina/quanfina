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
# Surum: 3.1 (29 May 2026) - STATIK orphan-listesi -> DINAMIK envanter redesign
# - v2.1 (20 May): worktree-aware repo root ($PSScriptRoot + git common-dir fallback)
# - v3.1 (29 May): Bolum 8 statik $beklenenScript/$beklenenNotebook orphan-check
#   KALDIRILDI (proje buyudukce stale -> ~24 notebook + yeni script + market_calendar
#   false orphan/kayip uretiyordu). Yerine dinamik kategori sayimi (liste bakimi YOK).
#   Cekirdek dosya VARLIK kontrolu (Bolum 1-7) korundu; gone dosyalar (_INDEX,
#   _KOD_ENVANTERI, _SAGLIK_KONTROL, _FELSEFE) cikarildi. Tam dinamik felsefe:
#   "envanterin kendisi" (P117 notu) artik gercek - liste eskime problemi cozuldu.
# Kural #15 (ASCII-only) + Kural #16 (native exe 2>&1 yasagi) uyumlu

param(
    [switch]$VerboseOutput
)

$ErrorActionPreference = "Continue"

# v2.1 fix (20 May 2026): worktree-aware repo root tespiti
# $PSScriptRoot pattern: script'in bulundugu yer = scripts/ klasoru,
# onun parent'i = repo root (worktree veya ana repo)
$scriptPath = $MyInvocation.MyCommand.Path
$scriptsDir = Split-Path -Parent $scriptPath
$repoRoot = Split-Path -Parent $scriptsDir
if (-not (Test-Path (Join-Path $repoRoot "CLAUDE.md"))) {
    Write-Host "Repo kokunde CLAUDE.md bulunamadi: $repoRoot" -ForegroundColor Red
    exit 1
}

# worktree-aware: git common-dir uzerinden ana repoyu bul
$gitCommonDir = (git rev-parse --git-common-dir).Trim()
if ($gitCommonDir -and ($gitCommonDir -ne ".git")) {
    if (-not [System.IO.Path]::IsPathRooted($gitCommonDir)) {
        $gitCommonDir = Join-Path $repoRoot $gitCommonDir
    }
    $mainRepo = Split-Path -Parent $gitCommonDir
    if (Test-Path (Join-Path $mainRepo "notebook")) {
        Write-Host "[bilgi] worktree icindeyim, ana repo notebook/ kullaniliyor: $mainRepo" -ForegroundColor DarkCyan
        $repoRoot = $mainRepo
    }
}

# P117 (26 May 2026): notebook/_INDEX.md 22 May konsolidasyon sonrasi arsivlendi.
# build_index.ps1 artik envanterin kendisidir — _INDEX.md kontrol kaldirildi.

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
    # v3.1: _INDEX.md + _KOD_ENVANTERI.md 22 May konsolidasyonunda arsivlendi (CIKARILDI)
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
    "market_calendar.py",   # v3.1: ABD borsa takvimi (P362/P363 canli modul)
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

# --- Dinamik envanter (v3.1: statik "beklenen" orphan-check KALDIRILDI) ---
# Eski 8a/8b/8c hardcoded $beklenenScript + $beklenenNotebook listelerine karsi
# orphan ariyordu. Proje buyudukce listeler STALE oldu -> false orphan (22 May
# konsolidasyon ~24 notebook dosyasi + yeni scriptler + market_calendar.py
# listelerde yoktu; _INDEX.md zaten arsivlendi -> dogrulanacak referans da yok).
# v3.1: orphan-check kaldirildi, dinamik KATEGORI SAYIMI geldi (liste bakimi YOK,
# hicbir zaman eskimez). Cekirdek dosya VARLIK kontrolu (Bolum 1-7) kaliyor;
# envanter artik "neyin var oldugunu" dinamik raporlar (Manifesto #3 self-update).
Write-Host ""
Write-Host "8. Dinamik envanter (kategori bazli sayim)" -ForegroundColor Yellow

$yetimList = @()  # v3.1: orphan-check yok; OZET geriye-uyum referansi icin bos

$kategoriler = @(
    @{ Etiket = "Kok Python (*.py)";      Dir = $repoRoot;                              Mode = "py" }
    @{ Etiket = "Scripts (*.ps1 + *.py)"; Dir = (Join-Path $repoRoot "scripts");        Mode = "psorpy" }
    @{ Etiket = "notebook/*.md";          Dir = (Join-Path $repoRoot "notebook");       Mode = "md" }
    @{ Etiket = "tests/*.py";             Dir = (Join-Path $repoRoot "tests");          Mode = "py" }
    @{ Etiket = "web Vitest (__tests__)"; Dir = (Join-Path $repoRoot "web\__tests__");  Mode = "test" }
)
foreach ($k in $kategoriler) {
    if (Test-Path $k.Dir) {
        $items = switch ($k.Mode) {
            "py"     { Get-ChildItem -Path $k.Dir -File -Filter "*.py" }
            "md"     { Get-ChildItem -Path $k.Dir -File -Filter "*.md" }
            "test"   { Get-ChildItem -Path $k.Dir -File -Filter "*.test.*" }
            "psorpy" { Get-ChildItem -Path $k.Dir -File | Where-Object { $_.Extension -in ".ps1", ".py" } }
        }
        Write-Host ("  {0,-26} : {1} dosya" -f $k.Etiket, @($items).Count) -ForegroundColor Cyan
    } else {
        Write-Host ("  {0,-26} : (klasor yok)" -f $k.Etiket) -ForegroundColor DarkGray
    }
}

# --- Belge satir sayim raporu (v3.0 — manuel hijyen yardimcisi) ---
Write-Host ""
Write-Host "9. Belge satir sayim raporu" -ForegroundColor Yellow
Write-Host "   (Bu sayilar _BASLAT.md'deki referanslarla manuel karsilastir)" -ForegroundColor DarkGray
# v3.1: gone dosyalar (_INDEX/_KOD_ENVANTERI/_SAGLIK_KONTROL/_FELSEFE) CIKARILDI
# (her biri "[BULUNAMADI]" + false kayip-belge -> exit 1 yapiyordu). _KOD_PATTERNLERI EKLENDI.
$belgeDosyalari = @(
    @{ Path = "CLAUDE.md"; Etiket = "CLAUDE.md (anayasa)" },
    @{ Path = "notebook/_BASLAT.md"; Etiket = "_BASLAT.md" },
    @{ Path = "notebook/_DEVIR.md"; Etiket = "_DEVIR.md" },
    @{ Path = "notebook/_ROADMAP.md"; Etiket = "_ROADMAP.md" },
    @{ Path = "notebook/_LINKLER.md"; Etiket = "_LINKLER.md" },
    @{ Path = "notebook/_HATALAR.md"; Etiket = "_HATALAR.md" },
    @{ Path = "notebook/_KOD_PATTERNLERI.md"; Etiket = "_KOD_PATTERNLERI.md" },
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
    Write-Host "SENKRON - cekirdek dosyalar mevcut, envanter dinamik" -ForegroundColor Green
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
