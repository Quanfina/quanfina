# Quanfina Pattern Ogrenme + Tescil Onerisi
# Referans: CLAUDE.md Kural #14 (Pattern Tespit + Dogrudan Tescil)
#           notebook/_HATALAR.md (pattern kaynak gunlugu)
#           notebook/_FELSEFE.md (felsefi temel)
#           Manifesto Ozellik #8 (Ogrenen) somut araci
#
# Amac:
# 1. _HATALAR.md parse -> H#X aktif/izlenen ayrim
# 2. git log -> Kural #X tescil tarihi, commit oranlari
# 3. 2+ ortaya cikis pattern'leri tespit (Kural #14 esiti)
# 4. Tescil adayi raporu (Kural / Memory / Script)
#
# Kullanim:
#   .\scripts\pattern_ogren.ps1
#   .\scripts\pattern_ogren.ps1 -ToFile  # TEMP klasore yazar
#
# Versiyon: v0.5 (18 May 2026, Asama 5.5 ilk uretim)
# Kural uyumu: #15 (ASCII-only), #16 (native exe 2>&1 yok)

param(
    [switch]$ToFile
)

$ErrorActionPreference = "Continue"
$repoRoot = (git rev-parse --show-toplevel).Trim()
if (-not $repoRoot) { Write-Host "Git deposu degil." -ForegroundColor Red; exit 1 }

$report = New-Object System.Text.StringBuilder
function Add-Line { param([string]$line) [void]$report.AppendLine($line) }

Add-Line "# Quanfina Pattern Ogrenme Raporu"
Add-Line ""
Add-Line "**Uretim:** $(Get-Date -Format 'dd MMM yyyy HH:mm') (pattern_ogren.ps1 v0.5)"
Add-Line ""
Add-Line "Bu rapor Kural #14 (Pattern Tespit + Dogrudan Tescil) icin destekci."
Add-Line "_HATALAR.md kaynak veri, git log commit'leri tescil zinciri."
Add-Line ""

# ============================================================
# 1. _HATALAR.md analizi
# ============================================================
Add-Line "## 1. _HATALAR.md Analizi"
Add-Line ""
$hatalarMd = Join-Path $repoRoot "notebook\_HATALAR.md"
if (Test-Path $hatalarMd) {
    $hatalarLines = Get-Content -Path $hatalarMd -Encoding UTF8

    $aktifPatterns = $hatalarLines | Where-Object { $_ -match '^### H#\d+' }
    $izlenenPatterns = $hatalarLines | Where-Object { $_ -match '^### P#\d+' }

    Add-Line "| Kategori | Sayi |"
    Add-Line "|---|---:|"
    Add-Line "| Aktif Pattern Hata (H#) | $($aktifPatterns.Count) |"
    Add-Line "| Izlenen Pattern (P#) | $($izlenenPatterns.Count) |"
    Add-Line ""

    Add-Line "### Aktif Pattern Hatalari (tescil edilmis)"
    Add-Line ""
    foreach ($p in $aktifPatterns) {
        $pTrim = $p -replace '^### ', ''
        Add-Line "- $pTrim"
    }
    Add-Line ""

    if ($izlenenPatterns.Count -gt 0) {
        Add-Line "### Izlenen Pattern (henuz tescil yok, 2. ortaya cikis bekliyor)"
        Add-Line ""
        foreach ($p in $izlenenPatterns) {
            $pTrim = $p -replace '^### ', ''
            Add-Line "- $pTrim **(Kural #14 esik adayi)**"
        }
        Add-Line ""
    }
} else {
    Add-Line "[hata] _HATALAR.md bulunamadi"
    Add-Line ""
}

# ============================================================
# 2. Git log tescil zinciri
# ============================================================
Add-Line "## 2. Kural Tescil Zinciri (git log)"
Add-Line ""

# "Kural #X" geçen commit mesajlari (tescil etkinligi)
$tumCommits = git -C $repoRoot log --all --format="%h|%ai|%s"
$kuralCommits = $tumCommits | Where-Object { $_ -match 'Kural #\d+|Asama \d+\.\d+|Manifesto' }

Add-Line "| Konu | Sayi |"
Add-Line "|---|---:|"
Add-Line "| Toplam commit (HEAD) | $((git -C $repoRoot rev-list --count HEAD).Trim()) |"
Add-Line "| Kural/Asama/Manifesto commit | $($kuralCommits.Count) |"
Add-Line ""

if ($kuralCommits.Count -gt 0) {
    Add-Line "### Son 10 tescil commit'i"
    Add-Line ""
    Add-Line "| Hash | Tarih | Mesaj |"
    Add-Line "|---|---|---|"
    foreach ($c in ($kuralCommits | Select-Object -First 10)) {
        $parts = $c -split '\|', 3
        if ($parts.Count -eq 3) {
            $shortDate = ($parts[1] -split ' ')[0]
            Add-Line "| $($parts[0]) | $shortDate | $($parts[2]) |"
        }
    }
    Add-Line ""
}

# ============================================================
# 3. CLAUDE.md Kural sayim
# ============================================================
Add-Line "## 3. CLAUDE.md Kural Envanteri"
Add-Line ""
$claudeMd = Join-Path $repoRoot "CLAUDE.md"
if (Test-Path $claudeMd) {
    $claudeLines = Get-Content -Path $claudeMd -Encoding UTF8
    $kurallar = $claudeLines | Where-Object { $_ -match '^### Kural \d+' }
    Add-Line "**Toplam Operasyonel Kural:** $($kurallar.Count)"
    Add-Line ""
    Add-Line "| # | Kural |"
    Add-Line "|---:|---|"
    foreach ($k in $kurallar) {
        $kTrim = $k -replace '^### Kural ', ''
        $num = ($kTrim -split ' ')[0]
        $baslik = $kTrim -replace '^\d+ - ', '' -replace '^\d+ ', ''
        Add-Line "| $num | $baslik |"
    }
    Add-Line ""
}

# ============================================================
# 4. Memory tescilleri (kullanici duzeyi pattern'ler)
# ============================================================
Add-Line "## 4. Memory Tescilleri (feedback_*.md)"
Add-Line ""
$memoryDir = "$env:USERPROFILE\.claude\projects\C--Projeler-Quanfina\memory"
if (Test-Path $memoryDir) {
    $feedbackFiles = Get-ChildItem -Path $memoryDir -Filter "feedback_*.md"
    Add-Line "**Toplam feedback memory:** $($feedbackFiles.Count)"
    Add-Line ""
    Add-Line "| Dosya | Boyut |"
    Add-Line "|---|---:|"
    foreach ($f in $feedbackFiles | Sort-Object Name) {
        $kb = [math]::Round($f.Length / 1KB, 1)
        Add-Line "| $($f.Name) | $kb KB |"
    }
    Add-Line ""
}

# ============================================================
# 5. Pattern -> Tescil oranlari
# ============================================================
Add-Line "## 5. Pattern -> Tescil Oranlari"
Add-Line ""
Add-Line "Kural #14 dogrudan tescil yetkisinin 3 kanali (Manifesto Ozellik #8):"
Add-Line ""

$kuralSayim = if (Test-Path $claudeMd) { ($claudeLines | Where-Object { $_ -match '^### Kural \d+' }).Count } else { 0 }
$feedbackSayim = if (Test-Path $memoryDir) { (Get-ChildItem -Path $memoryDir -Filter "feedback_*.md").Count } else { 0 }
$userSettings = "$env:USERPROFILE\.claude\settings.json"
$addlDirSayim = 0
if (Test-Path $userSettings) {
    $settings = Get-Content -Path $userSettings -Raw | ConvertFrom-Json
    $addlDirSayim = $settings.permissions.additionalDirectories.Count
}

Add-Line "| Kanal | Sayim | Aciklama |"
Add-Line "|---|---:|---|"
Add-Line "| Anayasa (CLAUDE.md Kural) | $kuralSayim | Pattern -> Kural #X dogrudan tescil |"
Add-Line "| Memory (feedback_*.md) | $feedbackSayim | Kullanici duzeyi tercih tescili |"
Add-Line "| Settings (additionalDirectories) | $addlDirSayim | Sistem duzeyi izin tescili |"
Add-Line ""

# ============================================================
# 6. Oneri (proaktif)
# ============================================================
Add-Line "## 6. Sistem Onerisi"
Add-Line ""
Add-Line "**Manifesto Ozellik #8 (Ogrenen) durum:**"
Add-Line ""

if ($izlenenPatterns.Count -gt 0) {
    Add-Line "- $($izlenenPatterns.Count) izlenen pattern var. 2. ortaya cikislari Kural #14 tetiklemek icin"
    Add-Line "  fursat. Yeni session'da bu pattern'leri yeniden gozlemle."
} else {
    Add-Line "- Izlenen pattern yok. Sistem stabil ve tescil bekleyen yeni pattern yok."
}

if ($aktifPatterns.Count -ge 5) {
    Add-Line "- $($aktifPatterns.Count) aktif pattern tescil edilmis. Yasayan sistem olgun durumda."
}

Add-Line ""
Add-Line "**Kural #14 dogrudan tescil yetkisi:** $kuralSayim Kural + $feedbackSayim memory + $($addlDirSayim) settings"
Add-Line "kanali ile aktif. AI pattern tespit ettiginde onay sormadan tescil eder."
Add-Line ""

# ============================================================
# Cikti
# ============================================================
Add-Line "---"
Add-Line ""
Add-Line "_Bu rapor pattern_ogren.ps1 v0.5 tarafindan uretildi._"
Add-Line "_Aktif pattern dosyalari: notebook/_HATALAR.md (kaynak), CLAUDE.md (tescil),_"
Add-Line "_memory/feedback_*.md (kullanici duzeyi)._"

$ciktiMetni = $report.ToString()

if ($ToFile) {
    $outPath = Join-Path $env:TEMP "quanfina_pattern_ogren_rapor.md"
    [System.IO.File]::WriteAllText($outPath, $ciktiMetni, [System.Text.UTF8Encoding]::new($false))
    Write-Host "Rapor TEMP'e yazildi: $outPath" -ForegroundColor Green
    Write-Host "(Repo disinda - commit/ignore gerekmez)" -ForegroundColor DarkGray
} else {
    Write-Host $ciktiMetni
}

exit 0
