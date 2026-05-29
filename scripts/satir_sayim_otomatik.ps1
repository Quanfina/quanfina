# Quanfina Satir Sayim Otomatik Refresh
# Referans: CLAUDE.md Kural #8 v2 (Akilli Kapanis) + H#9 pattern
#           build_index.ps1 v3.1 plani (ayri dosya temizligi)
#
# Amac: CLAUDE.md ve Vizyon'un GERCEK satir sayimlari ile dokuman
# literal'leri arasinda H#9 pattern (manuel hijyen yorgunlugu) cozmek.
# Hijyen scripti _BASLAT.md, _INDEX.md, _OZET.md, _SAGLIK_KONTROL.md
# icindeki "X satir" literal'lerini auto-update eder.
#
# Versiyon: v0.5.3 (21 May 2026, READ tarafi UTF-8 fix - H#11 2. ortaya cikis)
# v0.5  : Ilk surum, Out-File -Encoding UTF8 BOM ekliyordu
# v0.5.1: PowerShell array syntax fix (@((Join-Path...), (Join-Path...)))
# v0.5.2: [System.IO.File]::WriteAllText UTF-8 BOM-less, mojibake yok
# v0.5.3: Get-Content -Raw cp1254 ile okuyordu (Windows PS 5.1 default) ->
#         Turkce karakterler bozuluyor. Fix: [System.IO.File]::ReadAllText
#         + Text.Encoding UTF8 explicit. H#11 pattern 2. ortaya cikis (Kural #14
#         pekistir). _HATALAR.md H#11'e ek not dusulecek.
# Kural #15 + #16 + #19 uyumlu
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
# v0.5.4 (29 May 2026): _INDEX.md + _OZET.md 22 May konsolidasyonunda arsivlendi ->
# read + hedef CIKARILDI (eski hali gone dosyada Get-Content hatasi uretiyordu).
$devirSatir = (Get-Content (Join-Path $repoRoot "notebook\_DEVIR.md")).Count

Write-Host ""
Write-Host "=== Quanfina Satir Sayim Refresh (v0.5) ===" -ForegroundColor Cyan
Write-Host "Gercek sayimlar:"
Write-Host "  CLAUDE.md      : $claudeMdSatir satir"
Write-Host "  Vizyon         : $vizyonSatir satir"
Write-Host "  _BASLAT.md     : $basLatSatir satir"
Write-Host "  _DEVIR.md      : $devirSatir satir"
Write-Host ""

# Hedef dosyalar + sayim pattern degisimleri
# v0.5.4: _INDEX.md + _OZET.md arsivlendi -> hedeflerden CIKARILDI. _BASLAT.md
# canonical "NNNN satir" literal kaynagi (CLAUDE.md sayim referansi).
$hedefler = @(
    (Join-Path $repoRoot "notebook\_BASLAT.md")
)

$toplamGuncellenenSatir = 0

foreach ($hedef in $hedefler) {
    if (-not (Test-Path $hedef)) { continue }
    $dosyaAdi = Split-Path $hedef -Leaf
    # v0.5.3 fix: Get-Content -Raw Windows PS 5.1 cp1254 default kullanir, Turkce
    # karakterleri bozar. [System.IO.File]::ReadAllText + UTF8 explicit kullan.
    $icerik = [System.IO.File]::ReadAllText($hedef, [System.Text.Encoding]::UTF8)
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
            # v0.5.2 fix: Out-File -Encoding UTF8 BOM ekliyor + mojibake. Kullan: [System.IO.File]::WriteAllText UTF-8 BOM-less
            $utf8NoBom = New-Object System.Text.UTF8Encoding $false
            [System.IO.File]::WriteAllText($hedef, $icerik, $utf8NoBom)
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
