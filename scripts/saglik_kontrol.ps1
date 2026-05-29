# Quanfina Sistem Saglik Kontrolu
# Referans: CLAUDE.md Manifesto Ozellik #8 (Ogrenen)
#           notebook/_SAGLIK_KONTROL.md (Asama 5.1 iskelet)
#           notebook/_ROADMAP.md Asama 5.2
#
# Amac: Otomatik olculebilir metrikleri tarayip markdown rapor olarak uretir.
# v0.5 (18 May 2026): sadece ekran raporu (kopyala-yapistir)
# v1.0 (gelecek): _SAGLIK_KONTROL.md auto-rewrite
#
# Kullanim:
#   .\scripts\saglik_kontrol.ps1
#   .\scripts\saglik_kontrol.ps1 -ToFile  # ciktiyi .saglik_son_rapor.md dosyasina yazar
#
# Kural uyumu: #15 (ASCII-only), #16 (native exe 2>&1 yok)

param(
    [switch]$ToFile,
    [switch]$Uygula,
    [string]$DriveYedekPath = $null
)

$ErrorActionPreference = "Continue"
# v0.5.1 fix (19 May 2026 ~04:00): worktree-aware repo root tespiti
# $PSScriptRoot pattern (drive_sync.ps1 gibi). git rev-parse worktree'de worktree path donduruyor, ana repoyu degil.
$scriptPath = $MyInvocation.MyCommand.Path
$scriptsDir = Split-Path -Parent $scriptPath
$repoRoot = Split-Path -Parent $scriptsDir
if (-not (Test-Path (Join-Path $repoRoot "CLAUDE.md"))) {
    Write-Host "Repo kokunde CLAUDE.md bulunamadi: $repoRoot" -ForegroundColor Red
    exit 1
}

# Drive yedek path auto-detect (Kural #15: scriptte Turkce karakter yasak,
# bu yuzden literal path yazmiyoruz; G:\ kokunde Drive benzeri klasor + Quanfina_Backup ara)
if (-not $DriveYedekPath) {
    $gDrive = Get-PSDrive -Name "G" -ErrorAction SilentlyContinue
    if ($gDrive) {
        $kokKlasorler = Get-ChildItem -Path "G:\" -Directory -ErrorAction SilentlyContinue
        foreach ($k in $kokKlasorler) {
            $tryPath = Join-Path $k.FullName "Quanfina_Backup"
            if (Test-Path $tryPath) { $DriveYedekPath = $tryPath; break }
        }
    }
}

# Ciktinin tek bir yerde toplanmasi icin buffer
$report = New-Object System.Text.StringBuilder
function Add-Line { param([string]$line) [void]$report.AppendLine($line) }

Add-Line "# Quanfina Saglik Raporu"
Add-Line ""
Add-Line "**Uretim:** $(Get-Date -Format 'dd MMM yyyy HH:mm') (saglik_kontrol.ps1 v0.5)"
Add-Line ""

# ============================================================
# 1. Anayasa Katmani
# ============================================================
Add-Line "## Anayasa Katmani"
Add-Line ""
$claudeMd = Join-Path $repoRoot "CLAUDE.md"
if (Test-Path $claudeMd) {
    $satir = (Get-Content -Path $claudeMd -Encoding UTF8).Count
    # Get-Content + -match Turkce karakter regex sorununu cozer
    $claudeLines = Get-Content -Path $claudeMd -Encoding UTF8
    # v0.5.2 fix (19 May 2026): unique kural numarasi (alt-bolum sayilmasin)
    # Eski: Where-Object Count → "### Kural 9 v2 alt-bolumu" gibi alt-bolumler dahil sayim sisirilir
    $kuralSayim = ($claudeLines | ForEach-Object { if ($_ -match '^### Kural (\d+)') { $matches[1] } } | Sort-Object -Unique).Count
    # v0.5.3 fix (19 May 2026 ~05:00): Ilke total = Bilgi Mim 5 + GitHub 8 = 13
    # Sort-Object -Unique YANLIS — iki ayri bolumde numara cakisiyor (BM #1-5 + GH #1-8)
    # Where-Object Count dogru — her "### Ilke N" satiri ayri ilkedir
    $ilkeSayim = ($claudeLines | Where-Object { $_ -match '^### .lke \d+' }).Count
    $githubIlke = $ilkeSayim  # Toplam (Bilgi Mimarisi 5 + GitHub 8 = 13 beklenir)
    Add-Line "| Olcum | Deger |"
    Add-Line "|---|---:|"
    Add-Line "| CLAUDE.md satir | $satir |"
    Add-Line "| Operasyonel Kural sayisi | $kuralSayim |"
    Add-Line "| Toplam Ilke (Bilgi Mimarisi + GitHub) | $ilkeSayim |"
} else {
    Add-Line "[hata] CLAUDE.md bulunamadi"
}
Add-Line ""

# ============================================================
# 2. Karar Sistemi (Vizyon dosyasi)
# ============================================================
Add-Line "## Karar Sistemi (Notebook_A_Vizyon)"
Add-Line ""
$vizyon = Join-Path $repoRoot "notebook\Notebook_A_Vizyon.md"
if (Test-Path $vizyon) {
    $vSatir = (Get-Content -Path $vizyon -Encoding UTF8).Count
    # Turkce karakter regex'te problem cikariyor (Select-String + UTF-8) -
    # Get-Content + -match daha guvenilir
    $vLines = Get-Content -Path $vizyon -Encoding UTF8
    $kararAday = (($vLines -join "`n") | Select-String -Pattern 'KARAR ADAY #\d+' -AllMatches).Matches.Count
    $kararKesin = (($vLines -join "`n") | Select-String -Pattern 'KARAR KESIN #\d+' -AllMatches).Matches.Count
    $acikKonu = (($vLines -join "`n") | Select-String -Pattern 'A.IK KONU #\d+' -AllMatches).Matches.Count
    $ilkeAday = (($vLines -join "`n") | Select-String -Pattern '.LKE #\d+' -AllMatches).Matches.Count
    Add-Line "| Olcum | Deger |"
    Add-Line "|---|---:|"
    Add-Line "| Vizyon satir | $vSatir |"
    Add-Line "| KARAR ADAY mention | $kararAday |"
    Add-Line "| KARAR KESIN mention | $kararKesin |"
    Add-Line "| ACIK KONU mention | $acikKonu |"
    Add-Line "| ILKE mention | $ilkeAday |"
} else {
    Add-Line "[hata] Vizyon bulunamadi"
}
Add-Line ""

# ============================================================
# 3. Yasayan Kod Tabani
# ============================================================
Add-Line "## Yasayan Kod Tabani"
Add-Line ""
function Count-Lines { param([string]$path, [string]$filter, [switch]$Recurse)
    $files = if ($Recurse) {
        Get-ChildItem -Path $path -Filter $filter -Recurse -File -ErrorAction SilentlyContinue
    } else {
        Get-ChildItem -Path $path -Filter $filter -File -ErrorAction SilentlyContinue
    }
    $count = 0
    foreach ($f in $files) { $count += (Get-Content -Path $f.FullName -Encoding UTF8).Count }
    return @{ DosyaSayisi = $files.Count; Satir = $count }
}

$kokPy = Count-Lines -path $repoRoot -filter "*.py"
$apiPy = Count-Lines -path (Join-Path $repoRoot "api") -filter "*.py"
$scriptsPs = Get-ChildItem -Path (Join-Path $repoRoot "scripts") -File -ErrorAction SilentlyContinue | Where-Object { $_.Extension -in ".ps1", ".py" }
$scriptsSatir = 0
foreach ($s in $scriptsPs) { $scriptsSatir += (Get-Content -Path $s.FullName -Encoding UTF8).Count }
$testsPy = Count-Lines -path (Join-Path $repoRoot "tests") -filter "*.py"

Add-Line "| Kategori | Dosya | Satir |"
Add-Line "|---|---:|---:|"
Add-Line "| Kok Python | $($kokPy.DosyaSayisi) | $($kokPy.Satir) |"
Add-Line "| FastAPI (api/) | $($apiPy.DosyaSayisi) | $($apiPy.Satir) |"
Add-Line "| Scripts | $($scriptsPs.Count) | $scriptsSatir |"
Add-Line "| Tests | $($testsPy.DosyaSayisi) | $($testsPy.Satir) |"
Add-Line ""

# ============================================================
# 4. Belge Katmani
# ============================================================
Add-Line "## Belge Katmani"
Add-Line ""
# v0.6 (29 May 2026): gone dosyalar (_INDEX/_KOD_ENVANTERI/_SAGLIK_KONTROL) CIKARILDI
# (22 May konsolidasyon -> CLAUDE.md). _HATALAR + _KOD_PATTERNLERI EKLENDI.
$belgeler = @(
    @{ Path = "CLAUDE.md"; Etiket = "CLAUDE.md" },
    @{ Path = "notebook\_BASLAT.md"; Etiket = "_BASLAT.md" },
    @{ Path = "notebook\_DEVIR.md"; Etiket = "_DEVIR.md" },
    @{ Path = "notebook\_ROADMAP.md"; Etiket = "_ROADMAP.md" },
    @{ Path = "notebook\_LINKLER.md"; Etiket = "_LINKLER.md" },
    @{ Path = "notebook\_HATALAR.md"; Etiket = "_HATALAR.md" },
    @{ Path = "notebook\_KOD_PATTERNLERI.md"; Etiket = "_KOD_PATTERNLERI.md" },
    @{ Path = "notebook\YAPILANLAR.md"; Etiket = "YAPILANLAR.md" },
    @{ Path = "notebook\Notebook_A_Vizyon.md"; Etiket = "Notebook_A_Vizyon.md" }
)
Add-Line "| Dosya | Satir | KB |"
Add-Line "|---|---:|---:|"
foreach ($b in $belgeler) {
    $fp = Join-Path $repoRoot $b.Path
    if (Test-Path $fp) {
        $satir = (Get-Content -Path $fp -Encoding UTF8).Count
        $kb = [math]::Round((Get-Item $fp).Length / 1KB, 1)
        Add-Line "| $($b.Etiket) | $satir | $kb |"
    }
}
Add-Line ""

# ============================================================
# 5. Yedek (Manifesto Ozellik #9)
# ============================================================
Add-Line "## Yedek (Manifesto Ozellik #9)"
Add-Line ""
$lokalYedek = "$env:USERPROFILE\Quanfina_Backup"
$driveYedek = $DriveYedekPath  # Auto-detect veya parametreden

function Show-Yedek { param([string]$path, [string]$etiket)
    if (Test-Path $path) {
        $zips = Get-ChildItem -Path $path -Filter "notebook_yedek_*.zip" -ErrorAction SilentlyContinue | Sort-Object LastWriteTime -Descending
        if ($zips.Count -gt 0) {
            $sonTarih = $zips[0].LastWriteTime.ToString("dd MMM HH:mm")
            $sonBoyut = [math]::Round($zips[0].Length / 1MB, 2)
            Add-Line "| $etiket | $($zips.Count) | $sonTarih | $sonBoyut MB |"
        } else {
            Add-Line "| $etiket | 0 | yok | - |"
        }
    } else {
        Add-Line "| $etiket | [yok klasor] | - | - |"
    }
}

Add-Line "| Konum | Yedek sayisi | Son yedek | Son boyut |"
Add-Line "|---|---:|---|---:|"
Show-Yedek -path $lokalYedek -etiket "Lokal (`$USERPROFILE\Quanfina_Backup)"
if ($driveYedek) {
    Show-Yedek -path $driveYedek -etiket "Drive ($driveYedek)"
} else {
    Add-Line "| Drive | [auto-detect basarisiz] | - | - |"
}

$task = Get-ScheduledTask -TaskName "Quanfina_Notebook_Yedek_Gunluk" -ErrorAction SilentlyContinue
if ($task) {
    $nextRun = (Get-ScheduledTaskInfo -TaskName "Quanfina_Notebook_Yedek_Gunluk").NextRunTime
    Add-Line ""
    Add-Line "**ScheduledTask:** $($task.State) - sonraki tetik: $nextRun"
} else {
    Add-Line ""
    Add-Line "**ScheduledTask:** KURULU DEGIL (`.\scripts\notebook_yedekle.ps1 -ScheduledTask -Hedef ...` ile kur)"
}
Add-Line ""

# ============================================================
# 6. Git + Repo
# ============================================================
Add-Line "## Git + Repo"
Add-Line ""
$branch = (git -C $repoRoot branch --show-current).Trim()
$sonCommit = (git -C $repoRoot log -1 --format="%h %s").Trim()
$toplamCommit = (git -C $repoRoot rev-list --count HEAD).Trim()
$worktreeSayisi = (git -C $repoRoot worktree list | Measure-Object).Count

Add-Line "| Olcum | Deger |"
Add-Line "|---|---|"
Add-Line "| Branch | $branch |"
Add-Line "| Son commit | $sonCommit |"
Add-Line "| Toplam commit (HEAD) | $toplamCommit |"
Add-Line "| Worktree sayisi | $worktreeSayisi |"
Add-Line ""

# ============================================================
# 7. Memory + Settings
# ============================================================
Add-Line "## Memory + Settings"
Add-Line ""
$memoryDir = "$env:USERPROFILE\.claude\projects\C--Projeler-Quanfina\memory"
if (Test-Path $memoryDir) {
    $memCount = (Get-ChildItem -Path $memoryDir -File -Filter "*.md").Count
    Add-Line "| Olcum | Deger |"
    Add-Line "|---|---:|"
    Add-Line "| Memory dosyasi | $memCount |"
}

$userSettings = "$env:USERPROFILE\.claude\settings.json"
if (Test-Path $userSettings) {
    $settings = Get-Content -Path $userSettings -Raw | ConvertFrom-Json
    $allowCount = $settings.permissions.allow.Count
    $addlDirCount = $settings.permissions.additionalDirectories.Count
    Add-Line "| USER permissions.allow entry | $allowCount |"
    Add-Line "| USER additionalDirectories entry | $addlDirCount |"
}
Add-Line ""

# ============================================================
# 8. Yetim dosya (build_index ile uyumlu)
# ============================================================
Add-Line "## Yetim Dosya Taramasi"
Add-Line ""
Add-Line "Detay: ``.\scripts\build_index.ps1`` ile (bu rapora dahil edilmedi)"
Add-Line ""

# ============================================================
# Cikti
# ============================================================
Add-Line "---"
Add-Line ""
Add-Line "_Bu rapor saglik_kontrol.ps1 v0.6 tarafindan uretildi._"
Add-Line "_NOT: _SAGLIK_KONTROL.md 22 May konsolidasyonunda CLAUDE.md'ye tasindi; -Uygula raporu TEMP'e yazar._"

$ciktiMetni = $report.ToString()

if ($Uygula) {
    # v1.0 - _SAGLIK_KONTROL.md sonuna "Otomatik Olcum" bolumu ekle veya guncelle
    # Marker-based: manuel statik bolumler korunur, sadece otomatik kisim degisir
    $saglikMd = Join-Path $repoRoot "notebook\_SAGLIK_KONTROL.md"
    if (-not (Test-Path $saglikMd)) {
        # v0.6 (29 May 2026): _SAGLIK_KONTROL.md 22 May konsolidasyonunda arsivlendi
        # (icerik CLAUDE.md'ye tasindi). Auto-rewrite hedefi yok -> graceful degrade:
        # raporu TEMP'e yaz (eski "exit 1 hata" yerine). Feature obsolete ama crash yok.
        $outPath = Join-Path $env:TEMP "quanfina_saglik_son_rapor.md"
        [System.IO.File]::WriteAllText($outPath, $ciktiMetni, [System.Text.UTF8Encoding]::new($false))
        Write-Host "[bilgi] _SAGLIK_KONTROL.md konsolide edildi (22 May) -> auto-rewrite hedefi yok." -ForegroundColor Yellow
        Write-Host "        Rapor TEMP'e yazildi: $outPath" -ForegroundColor DarkGray
        exit 0
    }

    $mevcut = Get-Content -Path $saglikMd -Raw -Encoding UTF8
    $marker = "<!-- AUTO-SAGLIK:START -->"
    $markerEnd = "<!-- AUTO-SAGLIK:END -->"

    $yeniBolum = @"
$marker

> Bu bolum ``scripts/saglik_kontrol.ps1 -Uygula`` ile otomatik uretildi.
> Manuel duzenleme yapma - bir sonraki calismada uzerine yazilir.

$ciktiMetni

$markerEnd
"@

    if ($mevcut -match [regex]::Escape($marker)) {
        # Mevcut bolumu replace et
        $pattern = "(?s)$([regex]::Escape($marker)).*?$([regex]::Escape($markerEnd))"
        $yeni = $mevcut -replace $pattern, $yeniBolum
        Write-Host "[ok] _SAGLIK_KONTROL.md otomatik bolum guncellendi" -ForegroundColor Green
    } else {
        # Sona ekle
        $yeni = $mevcut.TrimEnd() + "`n`n---`n`n" + $yeniBolum + "`n"
        Write-Host "[ok] _SAGLIK_KONTROL.md sonuna otomatik bolum eklendi" -ForegroundColor Green
    }

    [System.IO.File]::WriteAllText($saglikMd, $yeni, [System.Text.UTF8Encoding]::new($false))
    Write-Host "  Dosya: $saglikMd" -ForegroundColor DarkGray
    Write-Host "  Sonraki calistirmada otomatik guncel kalir (Manifesto #7 + #8)" -ForegroundColor DarkGray
} elseif ($ToFile) {
    $outPath = Join-Path $env:TEMP "quanfina_saglik_son_rapor.md"
    [System.IO.File]::WriteAllText($outPath, $ciktiMetni, [System.Text.UTF8Encoding]::new($false))
    Write-Host "Rapor TEMP'e yazildi: $outPath" -ForegroundColor Green
    Write-Host "(Repo disinda - commit/ignore gerekmez)" -ForegroundColor DarkGray
} else {
    Write-Host $ciktiMetni
}

exit 0
