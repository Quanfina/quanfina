# Quanfina Notebook Drive Mirror
# Referans: CLAUDE.md Manifesto Ozellik #9 (Felaket Dayanikiligi)
#           notebook/_ROADMAP.md Asama 2.2 (YENIDEN ACIK, 18 May 2026 v2)
#           notebook/_LINKLER.md Asama 2.3 (NotebookLM Plus kaynak)
#
# Amac: notebook/ klasorunu Google Drive'a DOSYA BAZINDA mirror.
# notebook_yedekle.ps1 ZIP yedek (snapshot, felaket dayanikiligi).
# drive_sync.ps1 CANLI MIRROR (NotebookLM Plus icin Drive linkli kaynak).
# Iki script farkli amac, ayri yasarlar.
#
# Kullanim:
#   .\scripts\drive_sync.ps1                  # Auto-detect Drive hedef + mirror
#   .\scripts\drive_sync.ps1 -Hedef "G:\..."  # Manuel hedef
#   .\scripts\drive_sync.ps1 -KuruCalisma     # Sadece /L dry-run (degisiklik yok)
#   .\scripts\drive_sync.ps1 -ScheduledTask   # Windows task kayit (saatlik)
#   .\scripts\drive_sync.ps1 -UnregisterTask  # Task sil
#
# Versiyon: v2.3 (19 May 2026 ~05:30, .txt'ler ayri _txt/ alt klasore (Sn. Ferit talebi))
# Kural uyumu: #15 (ASCII-only), #16 (native exe 2>&1 yok)
#
# v2.1 degisiklikleri (H#8 pattern - NotebookLM Plus .md gormez):
#   - Robocopy /XF *.txt ile hedefteki .txt'ler extra sayilmaz (silinme korumasi)
#   - Robocopy sonrasi: hedefteki tum .md icin paralel .txt kopya uretilir
#   - Subfolder dahil (kitaplar/ vs.)
#   - Idempotent: ayni icerikte overwrite, fark yoksa is yok
# Kural #14 dogrudan tescil (5. kanal: script otomasyonu)
# Iliskili: notebook/_CLEAN_ROOM.md, notebook/_HATALAR.md H#8

param(
    [string]$Hedef = $null,
    [switch]$KuruCalisma,
    [switch]$ScheduledTask,
    [switch]$UnregisterTask,
    [string]$Saat = "Hourly"
)

$ErrorActionPreference = "Continue"
$TaskName = "Quanfina_Notebook_Drive_Mirror"

# Repo kokunu bul
$scriptPath = $MyInvocation.MyCommand.Path
$scriptsDir = Split-Path -Parent $scriptPath
$repoRoot = Split-Path -Parent $scriptsDir
$notebookDir = Join-Path $repoRoot "notebook"

if (-not (Test-Path $notebookDir)) {
    Write-Host "[hata] notebook/ klasoru bulunamadi: $notebookDir" -ForegroundColor Red
    exit 1
}

# ============================================================
# Drive hedef auto-detect (Kural #15: literal Turkce path yok)
# ============================================================
if (-not $Hedef -and -not $UnregisterTask) {
    $gDrive = Get-PSDrive -Name "G" -ErrorAction SilentlyContinue
    if ($gDrive) {
        $kokKlasorler = Get-ChildItem -Path "G:\" -Directory -ErrorAction SilentlyContinue
        foreach ($k in $kokKlasorler) {
            # Drive ana klasoru ic: Drive'im, Drive'm, My Drive, vs.
            if ($k.Name -match "(Drive|My Drive)") {
                $Hedef = Join-Path $k.FullName "Quanfina_notebook"
                break
            }
        }
    }
    if (-not $Hedef) {
        Write-Host "[hata] Drive yedek hedefi auto-detect basarisiz." -ForegroundColor Red
        Write-Host "  - G:\ surucusu kurulu mu (Drive Stream)?" -ForegroundColor DarkGray
        Write-Host "  - Manuel hedef: -Hedef 'G:\<DriveAna>\Quanfina_notebook'" -ForegroundColor DarkGray
        exit 1
    }
}

# ============================================================
# ScheduledTask modu
# ============================================================
if ($ScheduledTask -or $UnregisterTask) {
    Write-Host ""
    Write-Host "=== Quanfina Notebook Drive Mirror Task ===" -ForegroundColor Cyan
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

    # ScheduledTask kurma
    Write-Host "Task adi : $TaskName"
    Write-Host "Tetik    : Saatlik (her 1 saatte bir, $Saat default)"
    Write-Host "Hedef    : $Hedef"
    Write-Host "Script   : $scriptPath"
    Write-Host ""

    $argList = @(
        "-NoProfile",
        "-NonInteractive",
        "-ExecutionPolicy", "Bypass",
        "-File", "`"$scriptPath`"",
        "-Hedef", "`"$Hedef`""
    )
    $action = New-ScheduledTaskAction `
        -Execute "powershell.exe" `
        -Argument ($argList -join " ")

    # Saatlik tetik (her 1 saat, baslangic 09:00, gunde maksimum 14 kez)
    $trigger = New-ScheduledTaskTrigger -Once -At "09:00" `
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
        -Description "Quanfina notebook/ Drive canli mirror (Asama 2.2 v2, NotebookLM Plus icin)" | Out-Null

    Write-Host "[ok] ScheduledTask kuruldu: $TaskName" -ForegroundColor Green
    Write-Host ""
    Write-Host "Dogrulama: Get-ScheduledTask -TaskName '$TaskName'" -ForegroundColor DarkGray
    Write-Host "Manuel: Start-ScheduledTask -TaskName '$TaskName'" -ForegroundColor DarkGray
    Write-Host ""
    exit 0
}

# ============================================================
# Mirror modu (default)
# ============================================================
Write-Host ""
Write-Host "=== Quanfina Notebook Drive Mirror ===" -ForegroundColor Cyan
Write-Host "Kaynak: $notebookDir"
Write-Host "Hedef : $Hedef"
if ($KuruCalisma) {
    Write-Host "Mod   : KURU CALISMA (dry-run, dosya degismez)" -ForegroundColor Yellow
}
Write-Host ""

# Hedef yoksa olustur
if (-not (Test-Path $Hedef)) {
    Write-Host "[setup] Hedef klasor olusturuluyor" -ForegroundColor Yellow
    New-Item -ItemType Directory -Path $Hedef -Force | Out-Null
}

# Robocopy ile mirror
# /MIR = mirror (silmeleri yansit)
# /R:3 = 3 retry
# /W:5 = 5 saniye bekle
# /NDL = log'ta directory listesi yok
# /NFL = log'ta dosya listesi yok (sadece ozet)
# /L   = dry-run (sadece liste)
# /XO  = sadece daha yeni dosyalari kopyala
$robocopyArgs = @(
    $notebookDir,
    $Hedef,
    "/MIR",
    "/R:3",
    "/W:5",
    "/NDL",
    "/NFL",
    "/XF", "*.txt"  # v2.1: hedef .txt'leri extra sayilmaz - NotebookLM kaynaklari korunur
)
if ($KuruCalisma) {
    $robocopyArgs += "/L"
}

# Robocopy native exe - Kural #16 uyari: 2>&1 yok
$robocopyCikti = & robocopy @robocopyArgs

# Robocopy exit kod ozellikleri:
# 0 = degisiklik yok
# 1 = dosya kopyalandi (basarili)
# 2 = ekstra dosya/dizin
# 3 = 1+2 (kopyalandi + ekstra)
# 4 = uyumsuzluk
# 8 = en az 1 hata
# 16 = ciddi hata
$rc = $LASTEXITCODE

Write-Host ""
if ($rc -le 3) {
    Write-Host "[ok] Mirror tamam (robocopy exit: $rc)" -ForegroundColor Green
    if ($rc -eq 0) {
        Write-Host "  - Degisiklik yok" -ForegroundColor DarkGray
    } elseif ($rc -eq 1) {
        Write-Host "  - Dosyalar kopyalandi" -ForegroundColor DarkGray
    } elseif ($rc -eq 2) {
        Write-Host "  - Ekstra dosya/dizin tespit" -ForegroundColor DarkGray
    } elseif ($rc -eq 3) {
        Write-Host "  - Hem kopyalama hem ekstra" -ForegroundColor DarkGray
    }
} elseif ($rc -ge 8) {
    Write-Host "[hata] Robocopy hata (exit: $rc)" -ForegroundColor Red
    exit 1
} else {
    Write-Host "[uyari] Robocopy uyumsuzluk (exit: $rc)" -ForegroundColor Yellow
}

# ============================================================
# v2.1 NotebookLM .txt paralel kopya uretimi
# ============================================================
# Sebep: NotebookLM Plus Drive picker'da .md uzantisini listelemez (H#8)
# Cozum: hedefteki her .md icin .txt paralel kopya. Lokal kanon .md,
#        Drive ayna hem .md hem .txt (.md insan icin, .txt NotebookLM icin)

Write-Host ""
Write-Host "=== v2.1 .txt paralel kopya uretimi ===" -ForegroundColor Cyan

# v2.3 (19 May 2026 ~05:30): .txt'leri _txt/ alt klasorune DUZ hiyerarsi
# Sn. Ferit talebi: Drive arayuzu temiz, .md insan icin / .txt NotebookLM icin ayri
# NotebookLM Master kurulumu icin tek klasor (_txt/) hedefi
$txtKlasoru = Join-Path $Hedef "_txt"
if (-not (Test-Path $txtKlasoru)) {
    Write-Host "[setup] _txt/ klasoru olusturuluyor" -ForegroundColor Yellow
    New-Item -ItemType Directory -Path $txtKlasoru -Force | Out-Null
}

# v2.4 fix (22 May 2026, H#A2): _archive/ haric tut. Eskiden Recurse
# tum alt klasorleri kapsadigi icin arsivlenen .md'ler orphan logic'inde
# "kaynak var" olarak gozukup karsiligi .txt'ler silinmiyordu (8 orphan
# birikti). Now: kok klasor .md'leri al, _archive/ dahil etme.
$mdDosyalari = Get-ChildItem -Path $Hedef -Recurse -File -Filter "*.md" -ErrorAction SilentlyContinue |
    Where-Object { $_.FullName -notmatch '[\\/]_archive[\\/]' }
$txtUretildi = 0
$txtAtlandi = 0
$txtHata = 0

foreach ($md in $mdDosyalari) {
    # v2.3: hedef _txt/ duz (BaseName, alt klasor hiyerarsisi yok)
    $txtYolu = Join-Path $txtKlasoru "$($md.BaseName).txt"
    try {
        if ($KuruCalisma) {
            # Dry-run: sadece ne uretilecegini raporla
            if (-not (Test-Path $txtYolu) -or `
                (Get-Item $txtYolu).LastWriteTime -lt $md.LastWriteTime) {
                $txtUretildi++
            } else {
                $txtAtlandi++
            }
        } else {
            # Gercek: kaynak .md daha yeniyse veya .txt yoksa kopyala
            $kopyalaGerek = $true
            if (Test-Path $txtYolu) {
                $txtMevcut = Get-Item $txtYolu
                if ($txtMevcut.LastWriteTime -ge $md.LastWriteTime -and `
                    $txtMevcut.Length -eq $md.Length) {
                    $kopyalaGerek = $false
                }
            }

            if ($kopyalaGerek) {
                Copy-Item -Path $md.FullName -Destination $txtYolu -Force
                $txtUretildi++
            } else {
                $txtAtlandi++
            }
        }
    } catch {
        $txtHata++
        Write-Host "  [hata] $($md.Name): $_" -ForegroundColor Red
    }
}

if ($KuruCalisma) {
    Write-Host "[kuru] Uretilecek : $txtUretildi" -ForegroundColor Yellow
    Write-Host "[kuru] Atlanacak  : $txtAtlandi (zaten guncel)" -ForegroundColor DarkGray
} else {
    Write-Host "[ok] Uretildi : $txtUretildi" -ForegroundColor Green
    Write-Host "[ok] Atlandi  : $txtAtlandi (zaten guncel)" -ForegroundColor DarkGray
}
if ($txtHata -gt 0) {
    Write-Host "[uyari] Hata sayisi : $txtHata" -ForegroundColor Red
}

# ============================================================
# v2.3 Orphan .txt temizligi (19 May 2026 ~05:30)
# ============================================================
# Sebep: .md silinince _txt/ icinde orphan .txt kalmasin.
# v2.3 fix: .txt'ler _txt/ altinda DUZ, .md kaynagi BaseName ile bulunur

Write-Host ""
Write-Host "=== v2.3 Orphan .txt temizligi (_txt/ altinda) ===" -ForegroundColor Cyan

$txtDosyalari = Get-ChildItem -Path $txtKlasoru -File -Filter "*.txt" -ErrorAction SilentlyContinue
$orphanSilindi = 0
$orphanTespit = 0

# Tum .md BaseName'leri set olarak topla (kok + alt klasorler)
$mdBaseNames = @{}
foreach ($md in $mdDosyalari) { $mdBaseNames[$md.BaseName] = $true }

foreach ($txt in $txtDosyalari) {
    if (-not $mdBaseNames.ContainsKey($txt.BaseName)) {
        $orphanTespit++
        if ($KuruCalisma) {
            Write-Host "  [kuru-orphan] $($txt.Name)" -ForegroundColor Yellow
        } else {
            Remove-Item $txt.FullName -Force -ErrorAction SilentlyContinue
            Write-Host "  [orphan-sil] $($txt.Name)" -ForegroundColor Green
            $orphanSilindi++
        }
    }
}

if ($orphanTespit -eq 0) {
    Write-Host "[ok] Orphan .txt yok" -ForegroundColor DarkGray
} else {
    if ($KuruCalisma) {
        Write-Host "[kuru] $orphanTespit orphan tespit edilecek" -ForegroundColor Yellow
    } else {
        Write-Host "[ok] $orphanSilindi orphan .txt silindi" -ForegroundColor Green
    }
}

# ============================================================
# Son ozet
# ============================================================
$hedefDosyaSayisi = (Get-ChildItem -Path $Hedef -Recurse -File -ErrorAction SilentlyContinue).Count
$hedefMdSayisi = (Get-ChildItem -Path $Hedef -Recurse -File -Filter "*.md" -ErrorAction SilentlyContinue).Count
$hedefTxtSayisi = (Get-ChildItem -Path $Hedef -Recurse -File -Filter "*.txt" -ErrorAction SilentlyContinue).Count
$hedefBoyutMB = [math]::Round(((Get-ChildItem -Path $Hedef -Recurse -File -ErrorAction SilentlyContinue | Measure-Object Length -Sum).Sum / 1MB), 2)

Write-Host ""
Write-Host "=== OZET ===" -ForegroundColor Cyan
Write-Host "Drive hedef   : $Hedef"
Write-Host "Toplam dosya  : $hedefDosyaSayisi  (.md: $hedefMdSayisi, .txt: $hedefTxtSayisi)"
Write-Host "Toplam boyut  : $hedefBoyutMB MB"
Write-Host ""
Write-Host "NotebookLM Plus: drive.google.com/drive uzerinden klasor goruncu" -ForegroundColor Yellow
Write-Host "  - Quanfina_notebook icindeki *.txt dosyalari notebook'a ekle" -ForegroundColor Yellow
Write-Host "  - (.md uzantisi NotebookLM Plus Drive picker'da listelenmez - H#8)" -ForegroundColor DarkGray
Write-Host "  - Drive degisirse NotebookLM otomatik yeniden indeksler" -ForegroundColor Yellow
Write-Host ""

exit 0
