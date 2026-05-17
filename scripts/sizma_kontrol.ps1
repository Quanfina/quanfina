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
Write-Host ""
Write-Host "4. Yasakli isim/marka taramasi" -ForegroundColor Yellow
# CLAUDE.md kendi kural metnini iceriyor — onu disla
$forbiddenPatterns = @{
    "Markets 360" = 'Markets 360'
    "Fab 5"       = '\bFab 5\b'
    "SEPA reg"    = 'SEPA®'
    "MonAlert"    = 'MonAlert'
    "MAI"         = '\bMAI\b'
    "valueGetter" = '\bvalueGetter\b'
    "aB() M360"   = '\baB\('
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
