# Quanfina Sızma Kontrolü
# Referans: CLAUDE.md → Operasyonel Kural #10 + GitHub İlke #8
#
# Kullanım:
#   .\scripts\sizma_kontrol.ps1
#
# Çıkış kodu:
#   0 = temiz, push güvenli
#   1 = kirli, push BLOK
#
# Pre-push hook olarak bağlamak için:
#   .git\hooks\pre-push içine: powershell -File scripts/sizma_kontrol.ps1

$ErrorActionPreference = "Continue"
$repoRoot = (git rev-parse --show-toplevel).Trim()
if (-not $repoRoot) { Write-Host "Git deposu degil." -ForegroundColor Red; exit 1 }

$script:failures = @()

function Show-Result {
    param([string]$Name, [bool]$Passed, [string]$Detail = "")
    if ($Passed) {
        Write-Host "  [PASS] $Name" -ForegroundColor Green
    } else {
        Write-Host "  [FAIL] $Name" -ForegroundColor Red
        $script:failures += $Name
    }
    if ($Detail) { Write-Host "         $Detail" -ForegroundColor DarkGray }
}

Write-Host ""
Write-Host "=== Quanfina Sizma Kontrolu (Kural #10 / Ilke #8) ===" -ForegroundColor Cyan
Write-Host "Repo: $repoRoot"
Write-Host ""

# --- Kontrol 1: .gitignore koruması ---
Write-Host "1. .gitignore dogrulama" -ForegroundColor Yellow
$gitignorePath = Join-Path $repoRoot ".gitignore"
$gitignore = if (Test-Path $gitignorePath) { Get-Content $gitignorePath -Raw } else { "" }
$mustHave = @('.env', 'notebook/', '*.backup_*', '/node_modules/', 'test-screenshots/', '*.tmp')
$missing = @()
foreach ($p in $mustHave) {
    if ($gitignore -notmatch [regex]::Escape($p)) { $missing += $p }
}
Show-Result ".gitignore zorunlu pattern'leri" ($missing.Count -eq 0) `
    $(if ($missing) { "Eksik: $($missing -join ', ')" } else { "Hepsi mevcut" })

# --- Kontrol 2: Staged + untracked dosya gözden geçirme ---
Write-Host ""
Write-Host "2. Commit kapsami gozden gecirme" -ForegroundColor Yellow
$staged = @(git diff --cached --name-only)
$unstaged = @(git diff --name-only)
$untracked = @(git ls-files --others --exclude-standard)
Write-Host "   Staged ($($staged.Count)):" -ForegroundColor Cyan
$staged | ForEach-Object { Write-Host "     $_" }
if ($unstaged.Count -gt 0) {
    Write-Host "   Unstaged ($($unstaged.Count)):" -ForegroundColor Cyan
    $unstaged | ForEach-Object { Write-Host "     $_" }
}
if ($untracked.Count -gt 0) {
    Write-Host "   Untracked ($($untracked.Count)):" -ForegroundColor Cyan
    $untracked | ForEach-Object { Write-Host "     $_" }
}
Show-Result "Yetim/surpriz dosya kontrolu (manuel onay)" $true "Listeyi manuel teyit et"

# --- Kontrol 3: Staged içerikte gerçek değer (hardcoded secret) ---
Write-Host ""
Write-Host "3. Staged icerikte hardcoded secret taramasi" -ForegroundColor Yellow
$diff = (git diff --cached) -join "`n"
$hardcodedPatterns = @(
    'password\s*=\s*["''][^"''$\{]{4,}["'']',
    'api[_-]?key\s*=\s*["''][^"''$\{]{8,}["'']',
    'secret\s*=\s*["''][^"''$\{]{8,}["'']',
    'token\s*=\s*["''][a-zA-Z0-9_\-]{20,}["'']'
)
$hardHits = @()
foreach ($pat in $hardcodedPatterns) {
    $m = [regex]::Matches($diff, $pat, 'IgnoreCase')
    foreach ($hit in $m) {
        if ($hit.Value -notmatch 'os\.getenv|env\[|process\.env|\$\{') {
            $hardHits += $hit.Value
        }
    }
}
Show-Result "Hardcoded secret/key staged'de yok" ($hardHits.Count -eq 0) `
    $(if ($hardHits) { "BULUNDU: $($hardHits -join '; ')" } else { "" })

# --- Kontrol 4: Yasaklı isim/marka taraması (tracked dosyalarda) ---
# KARAR #733 alt-paket (Paket 48, 25 May 2026, Kural #14 proaktif): AG Grid
# library std props (valueGetter, valueFormatter, cellRenderer, cellEditor)
# YASAKLI listeden ÇIKARILDI. Sebep: Markets 360 minified internal isim degil,
# AG Grid library normal API kullanim. 1. ortaya cikis Paket 23 (false positive
# BLOK -> amend ile cozuldu). 2. ortaya cikis riski proaktif tescil ile kapatildi.
# CLAUDE.md kendi kural metnini iceriyor — onu disla
Write-Host ""
Write-Host "4. Yasakli isim/marka taramasi" -ForegroundColor Yellow
$forbiddenPatterns = @{
    "Markets 360" = 'Markets 360'
    "Fab 5"       = '\bFab 5\b'
    "SEPA reg"    = 'SEPA®'
    "MonAlert"    = 'MonAlert'
    "MAI"         = '\bMAI\b'
    "aB() M360"   = '\baB\('
    # NOT: valueGetter / valueFormatter / cellRenderer / cellEditor AG Grid std
    # props — yasakli listede YOK. Library normal kullanim, yabanci kod sizmasi
    # degil (Paket 48 Kural #14 tescil).
}
$forbiddenHits = @()
foreach ($label in $forbiddenPatterns.Keys) {
    $pat = $forbiddenPatterns[$label]
    $rawHits = git grep -nE "$pat" -- ':!CLAUDE.md' ':!scripts/sizma_kontrol.ps1' 2>$null
    if ($rawHits) {
        $forbiddenHits += "$label : $($rawHits.Count) yerde"
        $rawHits | Select-Object -First 3 | ForEach-Object { $forbiddenHits += "    $_" }
    }
}
Show-Result "Yasakli isim/marka geçisi yok" ($forbiddenHits.Count -eq 0) `
    $(if ($forbiddenHits) { ($forbiddenHits -join "`n         ") } else { "" })

# --- Kontrol 5: Secret format taraması ---
Write-Host ""
Write-Host "5. Secret format taramasi (AWS/GitHub/OpenAI/PrivateKey/Slack/Google)" -ForegroundColor Yellow
$secretFormats = @{
    "AWS Access Key"  = 'AKIA[0-9A-Z]{16}'
    "GitHub Token"    = 'ghp_[A-Za-z0-9]{36,}'
    "GitHub PAT"      = 'github_pat_[A-Za-z0-9_]{40,}'
    "OpenAI Key"      = 'sk-[A-Za-z0-9]{30,}'
    "Private Key"     = 'BEGIN (RSA |OPENSSH |EC |DSA )?PRIVATE KEY'
    "Slack Bot"       = 'xoxb-[0-9]+-[0-9]+'
    "Google API"      = 'AIza[0-9A-Za-z_-]{35}'
}
$secretHits = @()
foreach ($label in $secretFormats.Keys) {
    $pat = $secretFormats[$label]
    # Bu script kendisi pattern barındırır — disla
    $hits = git grep -nE "$pat" -- ':!scripts/sizma_kontrol.ps1' 2>$null
    if ($hits) { $secretHits += "$label : $($hits -join '; ')" }
}
Show-Result "Secret format eslesmesi yok" ($secretHits.Count -eq 0) `
    $(if ($secretHits) { ($secretHits -join "`n         ") } else { "" })

# --- Kontrol 6: Final liste onayı (manuel) ---
Write-Host ""
Write-Host "6. Final push listesi" -ForegroundColor Yellow
$ahead = (git rev-list --count "@{u}..HEAD" 2>$null)
if (-not $ahead) { $ahead = "?" }
Write-Host "   origin'in $ahead commit ileridesin" -ForegroundColor Cyan
$commitList = git log "@{u}..HEAD" --oneline 2>$null
if ($commitList) {
    Write-Host "   Push edilecek commit'ler:"
    $commitList | ForEach-Object { Write-Host "     $_" }
}
Show-Result "Final liste Sn. Ferit'e gosterildi" $true "Onay sozlu/yazili alinmali"

# --- Kontrol 7: Hardcoded sayisal esik kaynak atifsiz (Kural #26 / KALICI ILKE #4) ---
# ADVISORY (uyari) - push BLOK ETMEZ. Staged .py eklenen satirlarda finansal-gorunumlu
# esik (>=NN, <=NN, *N.NN) var ama kaynak atfi (Kitap/s./TTLC/TLSMW/KARAR/Bundle/
# O'Neil/Weinstein/Tharp/@source/Mark/Stage/VCP) yoksa uyar. Matematik Uydurmama.
Write-Host ""
Write-Host "7. Hardcoded esik kaynak atfi taramasi (Kural #26 - ADVISORY)" -ForegroundColor Yellow
$stagedPy = @(git diff --cached --name-only -- '*.py')
# Sadece GERCEK kaynak atif belirtecleri (domain kelimesi degil — "Mark/RS/Stage"
# gibi icerik sozcukleri HARIC; aksi halde "if rs >= 70" yanlislikla atifli sayilir).
$sourceTokens = "Kitap|s\.\d|p\.\d|page \d|TTLC|TLSMW|KARAR|Bundle|O'Neil|Weinstein|Tharp|@source"
$thresholdWarns = @()
if ($stagedPy.Count -gt 0) {
    $pyDiff = (git diff --cached -- '*.py') -split "`n"
    foreach ($line in $pyDiff) {
        if ($line -notmatch '^\+') { continue }       # sadece eklenen satirlar
        if ($line -match '^\+\+\+') { continue }       # diff header'i atla
        $code = $line.Substring(1)
        # finansal-gorunumlu esik: 2+ haneli karsilastirma veya ondalik carpan
        if (($code -match '(>=|<=|>|<)\s*\d{2,}') -or ($code -match '\*\s*\d+\.\d{1,3}\b')) {
            if ($code -notmatch $sourceTokens) { $thresholdWarns += $code.Trim() }
        }
    }
}
if ($thresholdWarns.Count -gt 0) {
    Write-Host "  [WARN] $($thresholdWarns.Count) satirda kaynak-atifsiz esik (yorumda 'Kitap s.X' ekle):" -ForegroundColor Yellow
    $thresholdWarns | Select-Object -First 8 | ForEach-Object { Write-Host "         $_" -ForegroundColor DarkGray }
} else {
    Write-Host "  [PASS] Staged .py esikleri kaynak atifli (veya esik yok)" -ForegroundColor Green
}
# NOT: ADVISORY - failures'a EKLENMEZ, push BLOK etmez (Kural #26 uyari seviyesi).

# --- Kontrol 8: Faith-neutral (KALICI ILKE #34) ---
# ADVISORY - Carr 2. baski Hristiyan devotional/scripture icerik Quanfina koduna
# sizmasin. NOT: "Christmas" (NYSE tatili) FALSE POSITIVE degil - \bChrist\b ile
# Christmas haric tutulur (Christmas piyasa takvimi fonksiyonel, dini icerik degil).
Write-Host ""
Write-Host "8. Faith-neutral taramasi (KALICI ILKE #34 - ADVISORY)" -ForegroundColor Yellow
$faithPat = "\bGod\b|\bChrist\b|\bJesus\b|\bBible\b|\bHristiyan\b|\bprayer\b|faith in God|kutsal kitap|\bgospel\b|scripture"
$faithHits = git grep -nE "$faithPat" -- '*.py' '*.ts' '*.tsx' ':!scripts/sizma_kontrol.ps1' 2>$null
if ($faithHits) {
    Write-Host "  [WARN] Inanc-icerikli ifade bulundu (Quanfina inanc-notr mimari):" -ForegroundColor Yellow
    $faithHits | Select-Object -First 5 | ForEach-Object { Write-Host "         $_" -ForegroundColor DarkGray }
} else {
    Write-Host "  [PASS] Inanc-icerikli ifade yok (faith-neutral)" -ForegroundColor Green
}
# NOT: ADVISORY - failures'a EKLENMEZ.

# --- Özet ---
Write-Host ""
Write-Host "=== OZET ===" -ForegroundColor Cyan
if ($script:failures.Count -eq 0) {
    Write-Host "TEMIZ - push guvenli" -ForegroundColor Green
    exit 0
} else {
    Write-Host "KIRLI - push BLOK" -ForegroundColor Red
    Write-Host "Basarisiz kontrol(ler): $($script:failures -join ', ')" -ForegroundColor Yellow
    Write-Host "Once temizle, sonra tekrar tara." -ForegroundColor Yellow
    exit 1
}
