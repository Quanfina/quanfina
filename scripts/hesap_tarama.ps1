# Quanfina Hesap + Ekosistem Tarama
# Referans: CLAUDE.md -> Kural #12 (Once Kesfet, Sonra Sor)
#           notebook/_LINKLER.md (hesap matrisi)
#           memory/project_ekosistem_ayrim.md
#
# Amac: Quanfina'nin bagli oldugu hesap + arac ekosistemini otomatik
# tarar. Sn. Ferit'e tek tek soru sormaya gerek kalmaz. Yasayan sistem
# birikimi (17 May 2026 turundaki manuel kesfin otomasyonu).
#
# NOT: Bu dosya ASCII-only icerik kullanir (Kural #15: PowerShell .ps1
# dosyalarinda Turkce karakter parse hatasi verir, Write tool ile
# yazildiginda BOM yok -> default encoding cp1254 ile uyumsuzluk).
#
# Kullanim:
#   .\scripts\hesap_tarama.ps1
#   .\scripts\hesap_tarama.ps1 -Json

param([switch]$Json)

$ErrorActionPreference = "Continue"
$results = @{}

function Show-Hesap {
    param([string]$Ekosistem, [string]$Email, [string]$Detay)
    if (-not $Json) {
        Write-Host "  $Ekosistem" -ForegroundColor Cyan
        Write-Host "    Email: $Email"
        if ($Detay) { Write-Host "    Detay: $Detay" -ForegroundColor DarkGray }
        Write-Host ""
    }
}

if (-not $Json) {
    Write-Host ""
    Write-Host "=== Quanfina Hesap + Ekosistem Tarama (Kural #12) ===" -ForegroundColor Cyan
    Write-Host ""
}

# --- 1. Anthropic / Claude ---
$claudeConf = "$env:USERPROFILE\.claude.json"
if (Test-Path $claudeConf) {
    $emailMatch = Select-String -Path $claudeConf -Pattern '"emailAddress"\s*:\s*"([^"]+)"' | Select-Object -First 1
    if ($emailMatch) {
        $email = $emailMatch.Matches[0].Groups[1].Value
        $results.anthropic = @{ email = $email; source = ".claude.json L$($emailMatch.LineNumber)" }
        Show-Hesap "Anthropic / Claude" $email "Source: .claude.json"
    }
}

# --- 2. GitHub (git credential + API) ---
$gitEmail = (git config user.email 2>$null).Trim()
$gitName = (git config user.name 2>$null).Trim()
$results.git_author = @{ email = $gitEmail; name = $gitName }

$ghUsername = $null
$ghEmail = $null
try {
    $credOutput = "protocol=https`nhost=github.com`n" | git credential fill 2>$null
    foreach ($line in ($credOutput -split "`n")) {
        if ($line -match '^username=(.+)') { $ghUsername = $matches[1].Trim() }
    }
} catch {}

$ghProfile = $null
try {
    $token = $null
    $credOutput = "protocol=https`nhost=github.com`n" | git credential fill 2>$null
    foreach ($line in ($credOutput -split "`n")) {
        if ($line -match '^password=(.+)') { $token = $matches[1].Trim() }
    }
    if ($token) {
        $ghProfile = Invoke-RestMethod -Uri "https://api.github.com/user" `
            -Headers @{ Authorization = "token $token"; Accept = "application/vnd.github+json" } `
            -ErrorAction Stop
        $ghEmail = $ghProfile.email
    }
} catch {
    if (-not $Json) { Write-Host "    [uyari] GitHub API cagrisi basarisiz: $_" -ForegroundColor Yellow }
}

$results.github = @{
    username = $ghUsername
    email_public = $ghEmail
    author_email = $gitEmail
    name = if ($ghProfile) { $ghProfile.name } else { $null }
    created_at = if ($ghProfile) { $ghProfile.created_at } else { $null }
}
Show-Hesap "GitHub" "$ghEmail (author: $gitEmail)" "Username: $ghUsername | Name: $($ghProfile.name)"

# --- 3. OneDrive Personal ---
$odPersonal = "HKCU:\Software\Microsoft\OneDrive\Accounts\Personal"
if (Test-Path $odPersonal) {
    $p = Get-ItemProperty $odPersonal -ErrorAction SilentlyContinue
    if ($p.UserEmail) {
        $results.onedrive_personal = @{ email = $p.UserEmail; folder = $p.UserFolder }
        Show-Hesap "OneDrive Personal" $p.UserEmail "Folder: $($p.UserFolder)"
    }
}

# --- 4. OneDrive Business (Edu) ---
$odBusiness = "HKCU:\Software\Microsoft\OneDrive\Accounts\Business1"
if (Test-Path $odBusiness) {
    $p = Get-ItemProperty $odBusiness -ErrorAction SilentlyContinue
    if ($p.UserEmail) {
        $results.onedrive_business = @{ email = $p.UserEmail; folder = $p.UserFolder }
        Show-Hesap "OneDrive Business (Edu)" $p.UserEmail "Folder: $($p.UserFolder)"
    }
} else {
    $results.onedrive_business = @{ status = "kurulu_degil" }
    if (-not $Json) {
        Write-Host "  OneDrive Business (Edu)" -ForegroundColor Cyan
        Write-Host "    Durum: lokalde kurulu degil`n" -ForegroundColor DarkGray
    }
}

# --- 5. Office Edu (ClickToRun) ---
$c2r = "HKLM:\SOFTWARE\Microsoft\Office\ClickToRun\Configuration"
if (Test-Path $c2r) {
    $p = Get-ItemProperty $c2r
    $results.office = @{
        product = $p.ProductReleaseIds
        version = $p.VersionToReport
        platform = $p.Platform
    }
    $lisansHint = if ($p.ProductReleaseIds -match "O365ProPlus|EnterpriseRetail") { "Microsoft 365 Apps (Edu/Business uyumlu)" } else { "Bilinmiyor" }
    Show-Hesap "Office 365" "(login Office GUI'de gorunur)" "Product: $($p.ProductReleaseIds) | v$($p.VersionToReport) | $lisansHint"
}

# --- 6. Google Drive Desktop App ---
$driveCandidates = @(
    "$env:USERPROFILE\Google Drive",
    "$env:USERPROFILE\GoogleDrive",
    "G:\My Drive",
    "H:\My Drive"
)
$driveFound = $false
foreach ($path in $driveCandidates) {
    if (Test-Path $path) {
        $results.google_drive_desktop = @{ status = "kurulu"; path = $path }
        Show-Hesap "Google Drive Desktop" "(Drive GUI'de gorunur)" "Path: $path"
        $driveFound = $true
        break
    }
}
if (-not $driveFound) {
    $results.google_drive_desktop = @{ status = "kurulu_degil" }
    if (-not $Json) {
        Write-Host "  Google Drive Desktop" -ForegroundColor Cyan
        Write-Host "    Durum: lokalde kurulu degil (Asama 2.1)`n" -ForegroundColor DarkGray
    }
}

# --- 7. VS Code GitHub Copilot ---
$vsExtDir = "$env:USERPROFILE\.vscode\extensions"
if (Test-Path $vsExtDir) {
    $copilot = Get-ChildItem $vsExtDir -Directory -Filter "github.copilot*" -ErrorAction SilentlyContinue
    if ($copilot) {
        $results.vscode_copilot = @{ extensions = $copilot.Name }
        Show-Hesap "VS Code GitHub Copilot" "(VS Code Account'ta gorunur)" "$($copilot.Count) extension"
    }
}

# --- 8. gh CLI ---
$ghExe = "${env:ProgramFiles}\GitHub CLI\gh.exe"
if (Test-Path $ghExe) {
    $ghVer = & $ghExe --version 2>$null | Select-Object -First 1
    $results.gh_cli = @{ status = "kurulu"; version = $ghVer; path = $ghExe }
    if (-not $Json) {
        Write-Host "  gh CLI" -ForegroundColor Cyan
        Write-Host "    Durum: kurulu - $ghVer`n"
    }
} else {
    $results.gh_cli = @{ status = "kurulu_degil" }
    if (-not $Json) {
        Write-Host "  gh CLI" -ForegroundColor Cyan
        Write-Host "    Durum: kurulu degil`n"
    }
}

# --- Cikti ---
if ($Json) {
    $results | ConvertTo-Json -Depth 10
} else {
    Write-Host "=== OZET ===" -ForegroundColor Cyan
    Write-Host "8 ekosistem komponenti tarandi." -ForegroundColor Green
    Write-Host "Detay: notebook/_LINKLER.md + memory/project_ekosistem_ayrim.md" -ForegroundColor DarkGray
    Write-Host ""
}

exit 0
