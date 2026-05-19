# Quanfina Acik Konu Konsolide
# Referans: notebook/_ROADMAP.md Asama 3.5
#           notebook/Notebook_A_Vizyon.md (52 ACIK KONU mevcut)
#
# Amac: Chat sonu acik konularin otomatik tarama + ozet raporu.
# Vizyon dosyasinda "ACIK KONU #X" mention'larini tara, unique sayim,
# baglam ile listele. Sn. Ferit "su an hangi acik konular var?" diye
# soruyu cevaplar.
#
# v0.5 sinir: sadece tarama + liste. v1.0 sonra: status tespiti
# (acik/cozuldu/iptal) + kategori (kod/ux/strateji) + 8+ alt-konu kilidi.
#
# Kullanim:
#   .\scripts\aciklama_konsolide.ps1               # Tum acik konu listesi
#   .\scripts\aciklama_konsolide.ps1 -N 10         # Sadece son 10 acik
#   .\scripts\aciklama_konsolide.ps1 -ID 22        # Spesifik ACIK KONU detay
#   .\scripts\aciklama_konsolide.ps1 -ToFile       # TEMP'e yaz
#
# Versiyon: v0.5 (18 May 2026, Asama 3.5 ilk uretim)
# Kural uyumu: #15 (ASCII-only), #16 (native exe 2>&1 yok)

param(
    [int]$N = 0,
    [int]$ID = 0,
    [switch]$ToFile
)

$ErrorActionPreference = "Continue"
# v0.5.1 fix (19 May 2026 ~04:00): worktree-aware repo root tespiti
$scriptPath = $MyInvocation.MyCommand.Path
$scriptsDir = Split-Path -Parent $scriptPath
$repoRoot = Split-Path -Parent $scriptsDir
if (-not (Test-Path (Join-Path $repoRoot "notebook\Notebook_A_Vizyon.md"))) {
    Write-Host "Vizyon dosyasi bulunamadi: $(Join-Path $repoRoot 'notebook\Notebook_A_Vizyon.md')" -ForegroundColor Red
    exit 1
}

$vizyon = Join-Path $repoRoot "notebook\Notebook_A_Vizyon.md"
if (-not (Test-Path $vizyon)) {
    Write-Host "[hata] Vizyon dosyasi bulunamadi: $vizyon" -ForegroundColor Red
    exit 1
}

$vizyonLines = Get-Content -Path $vizyon -Encoding UTF8

# ============================================================
# Tum ACIK KONU mention'lari ve satir numaralari
# Turkce karakter regex sorunu icin wildcard "A.IK KONU"
# ============================================================
$tumMentions = @()
for ($i = 0; $i -lt $vizyonLines.Count; $i++) {
    if ($vizyonLines[$i] -match 'A.IK KONU #(\d+)') {
        $tumMentions += [PSCustomObject]@{
            Sira = [int]$Matches[1]
            Satir = $i + 1
            Icerik = $vizyonLines[$i].Trim()
        }
    }
}

# Unique ACIK KONU numaralari
$uniqueIDs = $tumMentions | Select-Object -ExpandProperty Sira -Unique | Sort-Object

# ============================================================
# -ID modu: spesifik konu detay
# ============================================================
if ($ID -gt 0) {
    Write-Host ""
    Write-Host "=== ACIK KONU #$ID detay ===" -ForegroundColor Cyan
    Write-Host ""
    $bulgular = $tumMentions | Where-Object { $_.Sira -eq $ID }
    if ($bulgular.Count -eq 0) {
        Write-Host "[bulgu yok] Vizyon'da 'ACIK KONU #$ID' mention bulunamadi" -ForegroundColor Yellow
        exit 1
    }
    Write-Host "$ID numarali konu $($bulgular.Count) kez geciyor:"
    Write-Host ""
    foreach ($b in $bulgular) {
        Write-Host "  Satir $($b.Satir):" -ForegroundColor DarkGray
        # Baglam: 3 satir oncesinden 3 satir sonrasina
        $start = [Math]::Max(0, $b.Satir - 4)
        $end = [Math]::Min($vizyonLines.Count - 1, $b.Satir + 2)
        for ($j = $start; $j -le $end; $j++) {
            $marker = if ($j -eq ($b.Satir - 1)) { ">> " } else { "   " }
            Write-Host "  $marker$($vizyonLines[$j])" -ForegroundColor Gray
        }
        Write-Host ""
    }
    exit 0
}

# ============================================================
# Tum acik konular ozeti (rapor modu)
# ============================================================
$report = New-Object System.Text.StringBuilder
# NOT: function 'R' kullanma - PowerShell 'Invoke-History' alias'i (Kural #16 ailesi: built-in alias cakismasi)
function Out-Rep { param([string]$l) [void]$report.AppendLine($l) }

Out-Rep "# Quanfina Acik Konu Konsolide Raporu"
Out-Rep ""
Out-Rep "**Uretim:** $(Get-Date -Format 'dd MMM yyyy HH:mm') (aciklama_konsolide.ps1 v0.5)"
Out-Rep ""
Out-Rep "## Ozet Sayim"
Out-Rep ""
Out-Rep "| Konu | Sayim |"
Out-Rep "|---|---:|"
Out-Rep "| Toplam ACIK KONU mention | $($tumMentions.Count) |"
Out-Rep "| Unique ACIK KONU sayisi | $($uniqueIDs.Count) |"
Out-Rep "| En kucuk ID | #$($uniqueIDs[0]) |"
Out-Rep "| En buyuk ID | #$($uniqueIDs[-1]) |"
Out-Rep "| Vizyon satir | $($vizyonLines.Count) |"
Out-Rep ""

# Yogunluk - en cok mention edilen acik konular
$yogunluk = $tumMentions | Group-Object Sira | Sort-Object Count -Descending | Select-Object -First 10
if ($yogunluk.Count -gt 0) {
    Out-Rep "## En Yogun 10 Acik Konu (en cok mention)"
    Out-Rep ""
    Out-Rep "| ID | Mention | Aciklama |"
    Out-Rep "|---:|---:|---|"
    foreach ($y in $yogunluk) {
        $ilkMention = ($tumMentions | Where-Object { $_.Sira -eq [int]$y.Name } | Select-Object -First 1)
        $kisaIcerik = if ($ilkMention.Icerik.Length -gt 80) {
            $ilkMention.Icerik.Substring(0, 80) + "..."
        } else {
            $ilkMention.Icerik
        }
        Out-Rep "| #$($y.Name) | $($y.Count) | $kisaIcerik |"
    }
    Out-Rep ""
}

# Tum unique ACIK KONU listesi (numara sirasiyla, ilk mention'i ile)
$gosterilecek = if ($N -gt 0) { $uniqueIDs | Select-Object -Last $N } else { $uniqueIDs }

Out-Rep "## Acik Konu Listesi (numara sirasiyla)"
Out-Rep ""
if ($N -gt 0) {
    Out-Rep "_Son $N acik konu gosteriliyor._"
    Out-Rep ""
}
Out-Rep "| ID | Satir | Ilk mention icerigi |"
Out-Rep "|---:|---:|---|"
foreach ($id in $gosterilecek) {
    $ilkMention = ($tumMentions | Where-Object { $_.Sira -eq $id } | Sort-Object Satir | Select-Object -First 1)
    $kisaIcerik = if ($ilkMention.Icerik.Length -gt 100) {
        $ilkMention.Icerik.Substring(0, 100) + "..."
    } else {
        $ilkMention.Icerik
    }
    Out-Rep "| #$id | $($ilkMention.Satir) | $kisaIcerik |"
}
Out-Rep ""

Out-Rep "---"
Out-Rep ""
Out-Rep "_Detay icin: ``.\scripts\aciklama_konsolide.ps1 -ID <NUMARA>``_"
Out-Rep "_v1.0 (gelecek): status tespiti (acik/cozuldu/iptal) + kategori_"

$ciktiMetni = $report.ToString()

if ($ToFile) {
    $outPath = Join-Path $env:TEMP "quanfina_aciklama_konsolide.md"
    [System.IO.File]::WriteAllText($outPath, $ciktiMetni, [System.Text.UTF8Encoding]::new($false))
    Write-Host "Rapor TEMP'e yazildi: $outPath" -ForegroundColor Green
} else {
    Write-Host $ciktiMetni
}

exit 0
