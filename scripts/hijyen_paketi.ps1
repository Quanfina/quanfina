# Quanfina Hijyen Paketi - Master Script
# Referans: CLAUDE.md Kural #8 v2 (Akilli Kapanis - Otonom Hijyen Modu B bolumu)
#           Asama 5 hijyen scriptleri tek wrapper'da
#
# Amac: Tum hijyen scriptlerini sirayla calistirir, ozet rapor uretir.
# Sn. Ferit "hijyen yap" / Otonom Mod tetiklemesinde tek komut.
#
# Versiyon: v0.5 (19 May 2026, ilk surum)
# Kural #15 + #16 uyumlu
#
# Kullanim:
#   .\scripts\hijyen_paketi.ps1            # Default - tum hijyen
#   .\scripts\hijyen_paketi.ps1 -KuruCalisma  # Dry-run
#   .\scripts\hijyen_paketi.ps1 -Hizli      # Sadece kritik (saglik + index)

param(
    [switch]$KuruCalisma,
    [switch]$Hizli
)

$ErrorActionPreference = "Continue"
$scriptPath = $MyInvocation.MyCommand.Path
$scriptsDir = Split-Path -Parent $scriptPath
$baslangic = Get-Date

Write-Host ""
Write-Host "=== Quanfina Hijyen Paketi Master ===" -ForegroundColor Cyan
Write-Host "Baslangic: $($baslangic.ToString('yyyy-MM-dd HH:mm:ss'))"
if ($KuruCalisma) { Write-Host "Mod: KURU CALISMA (dry-run)" -ForegroundColor Yellow }
if ($Hizli) { Write-Host "Mod: HIZLI (kritik scripts only)" -ForegroundColor Yellow }
Write-Host ""

# Calistirilacak scriptler (sirali)
$scripts = @(
    @{ Ad = "build_index"; Yol = "build_index.ps1"; Kritik = $true; Acik = "Master indeks dogrulayici" },
    @{ Ad = "satir_sayim_otomatik"; Yol = "satir_sayim_otomatik.ps1"; Kritik = $false; Acik = "Satir sayim refresh (H#9)"; ExtraArg = if ($KuruCalisma) { @() } else { @("-Uygula") } },
    @{ Ad = "saglik_kontrol"; Yol = "saglik_kontrol.ps1"; Kritik = $true; Acik = "Sistem saglik raporu"; ExtraArg = if ($KuruCalisma) { @() } else { @("-Uygula") } },
    @{ Ad = "pattern_ogren"; Yol = "pattern_ogren.ps1"; Kritik = $false; Acik = "Pattern ogrenme + Kural #14 destek" },
    @{ Ad = "proaktif_oneri"; Yol = "proaktif_oneri.ps1"; Kritik = $false; Acik = "Proaktif oneri raporu" },
    @{ Ad = "temp_temizle"; Yol = "temp_temizle.ps1"; Kritik = $false; Acik = "Lokal temp debug clutter temizligi (P581, Kural #18)"; ExtraArg = if ($KuruCalisma) { @() } else { @("-Uygula") } }
)

$basariliSayisi = 0
$hataSayisi = 0
$atlandi = 0
$rapor = @()

foreach ($s in $scripts) {
    $tamYol = Join-Path $scriptsDir $s.Yol
    if (-not (Test-Path $tamYol)) {
        Write-Host "[atla] $($s.Ad) bulunamadi: $tamYol" -ForegroundColor DarkGray
        $atlandi++
        continue
    }

    if ($Hizli -and -not $s.Kritik) {
        Write-Host "[atla] $($s.Ad) (hizli mod, kritik degil)" -ForegroundColor DarkGray
        $atlandi++
        continue
    }

    Write-Host ""
    Write-Host "--- [$($s.Ad)] $($s.Acik) ---" -ForegroundColor Cyan
    $scriptBaslangic = Get-Date

    try {
        if ($s.ExtraArg) {
            & $tamYol @($s.ExtraArg) | Out-Host
        } else {
            & $tamYol | Out-Host
        }
        $scriptSure = ((Get-Date) - $scriptBaslangic).TotalSeconds
        Write-Host "[ok] $($s.Ad) tamamlandi ($([math]::Round($scriptSure, 1)) sn)" -ForegroundColor Green
        $basariliSayisi++
        $rapor += "[OK] $($s.Ad) ($([math]::Round($scriptSure, 1)) sn)"
    } catch {
        Write-Host "[hata] $($s.Ad) basarisiz: $_" -ForegroundColor Red
        $hataSayisi++
        $rapor += "[HATA] $($s.Ad): $_"
    }
}

$toplamSure = ((Get-Date) - $baslangic).TotalSeconds

Write-Host ""
Write-Host "===" -ForegroundColor Cyan
Write-Host "=== HIJYEN PAKETI OZET ===" -ForegroundColor Cyan
Write-Host "===" -ForegroundColor Cyan
Write-Host ""
Write-Host "Calisan script  : $basariliSayisi"
Write-Host "Hatali script   : $hataSayisi"
Write-Host "Atlanan script  : $atlandi"
Write-Host "Toplam sure     : $([math]::Round($toplamSure, 1)) sn"
Write-Host ""
Write-Host "Detay rapor:" -ForegroundColor DarkGray
foreach ($r in $rapor) {
    Write-Host "  $r" -ForegroundColor DarkGray
}
Write-Host ""

if ($hataSayisi -eq 0) {
    Write-Host "[SONUC] TUM HIJYEN BASARILI" -ForegroundColor Green
    exit 0
} else {
    Write-Host "[SONUC] $hataSayisi hata var, kontrol et" -ForegroundColor Red
    exit 1
}
