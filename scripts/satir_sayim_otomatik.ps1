# Quanfina Satir Sayim Otomatik Refresh
# Referans: CLAUDE.md Kural #8 v2 (Akilli Kapanis) + H#9 pattern
#           build_index.ps1 v3.1 plani (ayri dosya temizligi)
#
# Amac: CLAUDE.md ve Vizyon'un GERCEK satir sayimlari ile dokuman
# literal'leri arasinda H#9 pattern (manuel hijyen yorgunlugu) cozmek.
# Hijyen scripti _BASLAT.md, _INDEX.md, _OZET.md, _SAGLIK_KONTROL.md
# icindeki "X satir" literal'lerini auto-update eder.
#
# Versiyon: v0.5 (19 May 2026, Otonom Hijyen Mod ilk uretim)
# Kural #15 + #16 uyumlu
#
# Kullanim:
#   .\scripts\satir_sayim_otomatik.ps1            # Dry-run rapor
#   .\scripts\satir_sayim_otomatik.ps1 -Uygula    # Gercek update

param(
    [switch]$Uygula
)

$ErrorActionPreference = "Continue"
$scriptPath = $MyInvocation.MyCommand.Path
$repoRoot = Split-Path -Parent (Split-Path -Parent $scriptPath)

# Gercek sayimlari al
$claudeMdSatir = (Get-Content (Join-Path $repoRoot "CLAUDE.md")).Count
$vizyonSatir = (Get-Content (Join-Path $repoRoot "notebook\Notebook_A_Vizyon.md")).Count
$basLatSatir = (Get-Content (Join-Path $repoRoot "notebook\_BASLAT.md")).Count
$indexSatir = (Get-Content (Join-Path $repoRoot "notebook\_INDEX.md")).Count
$ozetSatir = (Get-Content (Join-Path $repoRoot "notebook\_OZET.md")).Count
$devirSatir = (Get-Content (Join-Path $repoRoot "notebook\_DEVIR.md")).Count

Write-Host ""
Write-Host "=== Quanfina Satir Sayim Refresh (v0.5) ===" -ForegroundColor Cyan
Write-Host "Gercek sayimlar:"
Write-Host "  CLAUDE.md      : $claudeMdSatir satir"
Write-Host "  Vizyon         : $vizyonSatir satir"
Write-Host "  _BASLAT.md     : $basLatSatir satir"
Write-Host "  _INDEX.md      : $indexSatir satir"
Write-Host "  _OZET.md       : $ozetSatir satir"
Write-Host "  _DEVIR.md      : $devirSatir satir"
Write-Host ""

# Hedef dosyalar + sayim pattern degisimleri
$hedefler = @(
    Join-Path $repoRoot "notebook\_BASLAT.md",
    Join-Path $repoRoot "notebook\_INDEX.md",
    Join-Path $repoRoot "notebook\_OZET.md"
)

$toplamGuncellenenSatir = 0

foreach ($hedef in $hedefler) {
    if (-not (Test-Path $hedef)) { continue }
    $dosyaAdi = Split-Path $hedef -Leaf
    $icerik = Get-Content $hedef -Raw
    $degisikSayisi = 0

    # CLAUDE.md sayim pattern'leri (en yaygin formlar)
    # "1421 satir", "**1469 satir**", "(1469 satir, ...)", "CLAUDE.md NNNN satir"
    $oldClaudeRegex = 'CLAUDE\.md[^|\n]{0,80}?\*{0,2}\d{3,4}\s*satır\*{0,2}'
    $matches = [regex]::Matches($icerik, $oldClaudeRegex)
    foreach ($m in $matches) {
        $eski = $m.Value
        $yeni = $eski -replace '\d{3,4}\s*satır', "$claudeMdSatir satır"
        if ($eski -ne $yeni) {
            $icerik = $icerik.Replace($eski, $yeni)
            $degisikSayisi++
        }
    }

    if ($degisikSayisi -gt 0) {
        Write-Host "  [$dosyaAdi] $degisikSayisi guncellenebilir" -ForegroundColor Yellow
        if ($Uygula) {
            $icerik | Out-File -FilePath $hedef -Encoding UTF8 -NoNewline -Force
            Write-Host "    [OK] Uygulandi" -ForegroundColor Green
            $toplamGuncellenenSatir += $degisikSayisi
        }
    } else {
        Write-Host "  [$dosyaAdi] Guncel" -ForegroundColor DarkGray
    }
}

Write-Host ""
if ($Uygula) {
    Write-Host "=== OZET ===" -ForegroundColor Cyan
    Write-Host "Toplam guncellenen sayim: $toplamGuncellenenSatir" -ForegroundColor Green
    Write-Host "Sonraki adim: git status (degisiklik var ise commit)" -ForegroundColor DarkGray
} else {
    Write-Host "=== DRY RUN ===" -ForegroundColor Yellow
    Write-Host "Gercek update icin: -Uygula flag" -ForegroundColor Yellow
}
Write-Host ""

exit 0
