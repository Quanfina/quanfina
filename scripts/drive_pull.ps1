# Quanfina Notebook Drive Pull (G: -> C:)
# Referans: CLAUDE.md Kural #9 v2 (Akilli Dagilim + Handoff)
#           Otomatik Cift Yonlu Senkron alt-bolum (KARAR #449)
#
# Amac: Web Claude'un Drive Connector ile yazdigi degisiklikleri
# lokal notebook/ klasorune cek. drive_sync.ps1'in TERS yonu.
#
# drive_sync.ps1 : C:\Projeler\Quanfina\notebook  ->  G:\Drive'im\Quanfina_notebook  (PUSH)
# drive_pull.ps1 : G:\Drive'im\Quanfina_notebook  ->  C:\Projeler\Quanfina\notebook  (PULL)
#
# Birlikte: cift yonlu Web Claude <-> Code senkron
#
# Kullanim:
#   .\scripts\drive_pull.ps1                  # Auto-detect kaynak + pull
#   .\scripts\drive_pull.ps1 -Kaynak "G:\..."  # Manuel kaynak
#   .\scripts\drive_pull.ps1 -KuruCalisma     # Dry-run (degisiklik yok)
#   .\scripts\drive_pull.ps1 -ScheduledTask   # Windows task kayit (saatlik, drive_sync ile alternat)
#   .\scripts\drive_pull.ps1 -UnregisterTask  # Task sil
#
# Versiyon: v0.5.1 (19 May 2026 ~04:30, Kural #19 fix — Out-File -> WriteAllText)
# v0.5  : Ilk surum, Out-File -Encoding UTF8 conflict log BOM ekliyordu
# Kural uyumu: #15 (ASCII-only), #16 (native exe 2>&1 yok)
#
# Davranis:
#   - Sadece .md dosyalari pull (txt'ler drive_sync uretimi, lokal'de yok)
#   - Drive newer + lokal yok       -> kopyala (yeni Web Claude dosyasi)
#   - Drive newer + lokal newer     -> CONFLICT, log + skip (Sn. Ferit cozmeli)
#   - Drive newer + lokal yok newer -> overwrite (Drive kanon yeni)
#   - Drive older + lokal newer     -> skip (Code daha yeni)
#   - Hicbir silme YAPMAZ (sadece guncelleme/ekleme, tek yonlu Drive->Code pozitif)
#
# Conflict: _PULL_CONFLICT.md log dosyasi yazar, kullanici manuel cozer
# Kural #4 (yikici eylem onayi): bu script silmez, sadece kopyalar
#
# Iliskili: scripts/drive_sync.ps1, notebook/_OZET.md, CLAUDE.md Kural #9 v2

param(
    [string]$Kaynak = $null,
    [switch]$KuruCalisma,
    [switch]$ScheduledTask,
    [switch]$UnregisterTask
)

$ErrorActionPreference = "Continue"
$TaskName = "Quanfina_Notebook_Drive_Pull"

# Repo kokunu bul
$scriptPath = $MyInvocation.MyCommand.Path
$scriptsDir = Split-Path -Parent $scriptPath
$repoRoot = Split-Path -Parent $scriptsDir
$notebookDir = Join-Path $repoRoot "notebook"

if (-not (Test-Path $notebookDir)) {
    Write-Host "[hata] Lokal notebook/ klasoru bulunamadi: $notebookDir" -ForegroundColor Red
    exit 1
}

# ============================================================
# Drive kaynak auto-detect
# ============================================================
if (-not $Kaynak -and -not $UnregisterTask) {
    $gDrive = Get-PSDrive -Name "G" -ErrorAction SilentlyContinue
    if ($gDrive) {
        $kokKlasorler = Get-ChildItem -Path "G:\" -Directory -ErrorAction SilentlyContinue
        foreach ($k in $kokKlasorler) {
            if ($k.Name -match "(Drive|My Drive)") {
                $Kaynak = Join-Path $k.FullName "Quanfina_notebook"
                break
            }
        }
    }
    if (-not $Kaynak) {
        Write-Host "[hata] Drive kaynak auto-detect basarisiz." -ForegroundColor Red
        Write-Host "  - G:\ surucusu kurulu mu (Drive Stream)?" -ForegroundColor DarkGray
        Write-Host "  - Manuel: -Kaynak 'G:\<DriveAna>\Quanfina_notebook'" -ForegroundColor DarkGray
        exit 1
    }
}

# ============================================================
# ScheduledTask modu
# ============================================================
if ($ScheduledTask -or $UnregisterTask) {
    Write-Host ""
    Write-Host "=== Quanfina Notebook Drive Pull Task ===" -ForegroundColor Cyan
    Write-Host ""

    $mevcut = Get-ScheduledTask -TaskName $TaskName -ErrorAction SilentlyContinue
    if ($mevcut) {
        Write-Host "[bakim] Mevcut task siliniyor: $TaskName" -ForegroundColor Yellow
        Unregister-ScheduledTask -TaskName $TaskName -Confirm:$false
    }

    if ($UnregisterTask) {
        if ($mevcut) {
            Write-Host "[ok] Task silindi: $TaskName" -ForegroundColor Green
        } else {
            Write-Host "[bilgi] Task zaten yoktu: $TaskName" -ForegroundColor DarkGray
        }
        exit 0
    }

    Write-Host "Task adi : $TaskName"
    Write-Host "Tetik    : Saatlik (drive_sync 00, drive_pull 30 alternat)"
    Write-Host "Kaynak   : $Kaynak"
    Write-Host "Script   : $scriptPath"
    Write-Host ""

    $argList = @(
        "-NoProfile",
        "-NonInteractive",
        "-ExecutionPolicy", "Bypass",
        "-File", "`"$scriptPath`"",
        "-Kaynak", "`"$Kaynak`""
    )
    $action = New-ScheduledTaskAction `
        -Execute "powershell.exe" `
        -Argument ($argList -join " ")

    # Saatlik tetik, 09:30 baslangic (drive_sync 09:00, alternat)
    $trigger = New-ScheduledTaskTrigger -Once -At "09:30" `
        -RepetitionInterval (New-TimeSpan -Hours 1) `
        -RepetitionDuration (New-TimeSpan -Hours 14)

    $settings = New-ScheduledTaskSettingsSet `
        -StartWhenAvailable `
        -DontStopOnIdleEnd `
        -ExecutionTimeLimit (New-TimeSpan -Minutes 5)

    $principal = New-ScheduledTaskPrincipal `
        -UserId $env:USERNAME `
        -LogonType Interactive `
        -RunLevel Limited

    Register-ScheduledTask `
        -TaskName $TaskName `
        -Action $action `
        -Trigger $trigger `
        -Settings $settings `
        -Principal $principal `
        -Description "Quanfina Drive -> notebook/ pull (Kural #9 v2 Otomatik Cift Yonlu Senkron, KARAR #449)" | Out-Null

    Write-Host "[ok] ScheduledTask kuruldu: $TaskName" -ForegroundColor Green
    Write-Host ""
    Write-Host "Dogrulama: Get-ScheduledTask -TaskName '$TaskName'" -ForegroundColor DarkGray
    Write-Host "Manuel  : Start-ScheduledTask -TaskName '$TaskName'" -ForegroundColor DarkGray
    Write-Host ""
    exit 0
}

# ============================================================
# Pull modu (default)
# ============================================================
Write-Host ""
Write-Host "=== Quanfina Notebook Drive Pull (G: -> C:) ===" -ForegroundColor Cyan
Write-Host "Kaynak: $Kaynak"
Write-Host "Hedef : $notebookDir"
if ($KuruCalisma) {
    Write-Host "Mod   : KURU CALISMA (dry-run, dosya degismez)" -ForegroundColor Yellow
}
Write-Host ""

if (-not (Test-Path $Kaynak)) {
    Write-Host "[hata] Kaynak bulunamadi: $Kaynak" -ForegroundColor Red
    exit 1
}

# Drive'daki tum .md dosyalarini tara
$driveMdler = Get-ChildItem -Path $Kaynak -Recurse -File -Filter "*.md" -ErrorAction SilentlyContinue

$kopyalanan = 0
$yeniDosya = 0
$atlandi = 0
$conflict = 0
$conflictList = @()

foreach ($driveMd in $driveMdler) {
    # Drive yolundan goreceli yol cikar
    $goreceli = $driveMd.FullName.Substring($Kaynak.Length).TrimStart("\")
    $lokalYol = Join-Path $notebookDir $goreceli

    if (-not (Test-Path $lokalYol)) {
        # Lokal'de yok: yeni Web Claude dosyasi -> kopyala
        if ($KuruCalisma) {
            Write-Host "  [kuru-yeni] $goreceli" -ForegroundColor Yellow
        } else {
            $lokalDir = Split-Path -Parent $lokalYol
            if (-not (Test-Path $lokalDir)) {
                New-Item -ItemType Directory -Path $lokalDir -Force | Out-Null
            }
            Copy-Item -Path $driveMd.FullName -Destination $lokalYol -Force
            Write-Host "  [yeni]      $goreceli" -ForegroundColor Green
        }
        $yeniDosya++
        continue
    }

    # Her ikisi de var: timestamp + hash karsilastir
    $lokalMd = Get-Item $lokalYol
    $driveYeni = $driveMd.LastWriteTime
    $lokalYeni = $lokalMd.LastWriteTime

    # Drive lokal'den daha yeniyse:
    if ($driveYeni -gt $lokalYeni.AddSeconds(2)) {
        # Hash karsilastirmasi (icerik gercekten farkli mi)
        $driveHash = (Get-FileHash -Path $driveMd.FullName -Algorithm SHA256).Hash
        $lokalHash = (Get-FileHash -Path $lokalYol -Algorithm SHA256).Hash

        if ($driveHash -ne $lokalHash) {
            # Conflict tespiti: lokal son 1 saatte degistirilmis mi?
            $lokalSonDegisim = (Get-Date) - $lokalYeni
            $lokalAktif = ($lokalSonDegisim.TotalHours -lt 1)

            if ($lokalAktif) {
                # CONFLICT: hem Drive hem lokal yeni, ikisi de aktif
                Write-Host "  [conflict]  $goreceli (Drive: $($driveYeni.ToString('HH:mm:ss')), Lokal: $($lokalYeni.ToString('HH:mm:ss')))" -ForegroundColor Red
                $conflictList += [pscustomobject]@{
                    Dosya = $goreceli
                    DriveZaman = $driveYeni
                    LokalZaman = $lokalYeni
                    DriveHash = $driveHash.Substring(0,12)
                    LokalHash = $lokalHash.Substring(0,12)
                }
                $conflict++
            } else {
                # Drive newer + lokal eski (1+ saat) -> overwrite
                if ($KuruCalisma) {
                    Write-Host "  [kuru-pull] $goreceli (Drive newer, lokal stale)" -ForegroundColor Yellow
                } else {
                    Copy-Item -Path $driveMd.FullName -Destination $lokalYol -Force
                    Write-Host "  [pull]      $goreceli (Drive newer)" -ForegroundColor Green
                }
                $kopyalanan++
            }
        } else {
            $atlandi++
        }
    } else {
        $atlandi++
    }
}

# ============================================================
# Conflict log
# ============================================================
if ($conflictList.Count -gt 0 -and -not $KuruCalisma) {
    $conflictLog = Join-Path $notebookDir "_PULL_CONFLICT.md"
    $logIcerik = @()
    $logIcerik += "# Quanfina - Drive Pull Conflict Log"
    $logIcerik += ""
    $logIcerik += "**Tarih:** $(Get-Date -Format 'yyyy-MM-dd HH:mm:ss')"
    $logIcerik += "**Drive Stream lokal yolu:** $Kaynak"
    $logIcerik += "**Lokal notebook:** $notebookDir"
    $logIcerik += ""
    $logIcerik += "## Conflicts (Sn. Ferit cozumlemeli)"
    $logIcerik += ""
    $logIcerik += "Her dosya icin Drive ve lokal ikisi de son 1 saatte degistirilmis"
    $logIcerik += "ve icerik farkli. Hangisini kanon kabul edecegini sen sec:"
    $logIcerik += ""
    foreach ($c in $conflictList) {
        $logIcerik += "### $($c.Dosya)"
        $logIcerik += "- Drive  : $($c.DriveZaman) (hash: $($c.DriveHash)...)"
        $logIcerik += "- Lokal  : $($c.LokalZaman) (hash: $($c.LokalHash)...)"
        $logIcerik += "- Cozum  : Manuel diff + birlestir"
        $logIcerik += ""
    }
    # Kural #19 (19 May 2026) — Out-File -Encoding UTF8 BOM + mojibake. WriteAllText kullan.
    $utf8NoBom = New-Object System.Text.UTF8Encoding $false
    [System.IO.File]::WriteAllText($conflictLog, ($logIcerik -join "`n"), $utf8NoBom)
    Write-Host ""
    Write-Host "[uyari] $($conflictList.Count) conflict tespit edildi" -ForegroundColor Red
    Write-Host "[uyari] Log dosyasi: $conflictLog" -ForegroundColor Red
}

# ============================================================
# Ozet
# ============================================================
Write-Host ""
Write-Host "=== OZET ===" -ForegroundColor Cyan
if ($KuruCalisma) {
    Write-Host "Yeni kopyalanacak  : $yeniDosya" -ForegroundColor Yellow
    Write-Host "Pull yapilacak     : $kopyalanan" -ForegroundColor Yellow
} else {
    Write-Host "Yeni kopyalanan    : $yeniDosya" -ForegroundColor Green
    Write-Host "Pull yapilan       : $kopyalanan" -ForegroundColor Green
}
Write-Host "Atlanan (guncel)   : $atlandi" -ForegroundColor DarkGray
if ($conflict -gt 0) {
    Write-Host "Conflict           : $conflict" -ForegroundColor Red
}
Write-Host ""

if ($yeniDosya -gt 0 -or $kopyalanan -gt 0) {
    Write-Host "Web Claude'dan gelen degisiklikler lokal'e cekildi." -ForegroundColor Yellow
    Write-Host "Sonraki adim: git status (eger commit gerekecekse)" -ForegroundColor DarkGray
} else {
    Write-Host "Yeni degisiklik yok." -ForegroundColor DarkGray
}
Write-Host ""

exit 0
