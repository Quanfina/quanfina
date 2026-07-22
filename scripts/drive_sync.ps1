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
# Versiyon: v2.7 (22 Tem 2026): MD MIRROR AYAGI KALDIRILDI -> script artik SADECE
#            md->txt donusumu + _txt/ yazimi yapar (NotebookLM kaynagi). Sebep: notebook/
#            artik Drive for Desktop "Bilgisayarlar" DOGRUDAN klasor-senkronunda; robocopy
#            ile 2. bir Drive hedefine .md kopyalamak ciftleme idi (Ilke #4 DRY) + notebook/
#            Drive tarafindan izlendigi icin yazma-yarisi yuzeyi yaratiyordu.
#            H#A10 KORUMA: _txt yolu + .txt adlandirmasi BIT DEGISMEDI (kaynak $Hedef ->
#            $notebookDir; ayna 1:1 oldugu icin goreli yollar ayni -> ayni isimler).
# Versiyon: v2.6 (22 Haz 2026, P581 KRITIK: /XD _txt — H#A10 kok neden fix.
#            /MIR her run _txt'yi purge edip 53 .txt'yi yeni file-ID ile yeniden
#            yaratiyordu -> NotebookLM kaynak linkleri her saat kiriliyordu. /XD ile coz.)
# Versiyon: v2.5 (22 May 2026 ~21:00, A: CLAUDE.md de _txt'e yansir + B: alt klasor prefix)
# Kural uyumu: #15 (ASCII-only), #16 (native exe 2>&1 yok)
#
# v2.5 degisiklikleri (Sn. Ferit "Drive senkron kapsam" talebi):
#   A. CLAUDE.md (repo root) -> _txt/CLAUDE.txt (Vizyon Bekcisi NotebookLM
#      anayasa denetci rolu icin Drive linkli kaynak)
#   B. Alt klasor cakisma fix (H#A2 paralel bug):
#      kitaplar/_INDEX.md ve analizler/_INDEX.md ayni BaseName ile
#      _txt/_INDEX.txt'e yaziliyordu (biri kayboluyordu). Fix: alt klasor
#      adi prefix (kitaplar_INDEX.txt, analizler_INDEX.txt).
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
    # P584 (22 Haz 2026) KRITIK FIX: eski -Once trigger gunde BIR KEZ (kayit gunu)
    # calisip duruyordu -> NextRunTime bos -> saatlik sync 18 May'den beri UYKUDAYDI.
    # -Daily gunluk tekrar saglar; repetition gunluk trigger'a graft edilir (her gun
    # 09:00-23:00 saatlik). Boylece notebook -> Drive otomatik akar (felaket dayanikiligi).
    $trigger = New-ScheduledTaskTrigger -Daily -At "09:00"
    $trigger.Repetition = (New-ScheduledTaskTrigger -Once -At "09:00" `
        -RepetitionInterval (New-TimeSpan -Hours 1) `
        -RepetitionDuration (New-TimeSpan -Hours 14)).Repetition

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
Write-Host "=== Quanfina Notebook -> _txt (NotebookLM kaynagi) [v2.7 _txt-only] ===" -ForegroundColor Cyan
Write-Host "Kaynak: $notebookDir  (.md)"
Write-Host "Hedef : $Hedef\_txt  (.txt)"
Write-Host "Not   : .md bulut yedegi = Drive-desktop DOGRUDAN klasor senkronu (bu script degil)" -ForegroundColor DarkGray
if ($KuruCalisma) {
    Write-Host "Mod   : KURU CALISMA (dry-run, dosya degismez)" -ForegroundColor Yellow
}
Write-Host ""

# Hedef yoksa olustur
if (-not (Test-Path $Hedef)) {
    Write-Host "[setup] Hedef klasor olusturuluyor" -ForegroundColor Yellow
    New-Item -ItemType Directory -Path $Hedef -Force | Out-Null
}

# ============================================================
# v2.7 (22 Tem 2026): MD MIRROR AYAGI KALDIRILDI
# ============================================================
# Sebep: notebook/ artik Google Drive for Desktop "Bilgisayarlar" DOGRUDAN
# klasor-senkronunda (Drive Tercihler > Dizustu Bilgisayarim > notebook, 11.6 MB
# canli; 22 Tem 06:39'da _HATALAR.md yuklemesi bulut tarafinda dogrulandi).
# .md dosyalarinin bulut yedegi artik dogrudan senkronun isi -> robocopy /MIR
# ayni .md'leri 2. bir Drive hedefine kopyalamak CIFTLEME idi (Ilke #4 DRY):
#   - gereksiz Drive trafigi + depolama
#   - /MIR silme yayilim riski
#   - notebook/ artik Drive tarafindan izlendigi icin yazma yarisi yuzeyi
# v2.7 sonrasi bu script'in TEK isi: md -> txt donusumu + _txt/ hedefine yazim
# (NotebookLM Plus kaynagi; .md uzantisi Drive picker'da listelenmez - H#8).
#
# H#A10 KORUMASI: _txt/ klasor yolu ve .txt dosya adlandirmasi BIT DEGISMEDI.
# Onceden .md kaynagi $Hedef (ayna) idi; artik $notebookDir (lokal). Ayna 1:1
# kopya oldugu icin goreli yollar AYNI -> Get-TxtFileName ayni isimleri uretir
# -> NotebookLM file-ID baglari korunur (H#A10 tekrarlanmaz).
#
# NOT (bilinen artik): $Hedef altindaki mevcut .md dosyalari artik GUNCELLENMEZ
# (donmus kopya). Silinmediler - temizlik ayri karar (Kural #4).

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
# v2.7: kaynak $Hedef (ayna) DEGIL, lokal $notebookDir. Ayna 1:1 kopya oldugu icin
# goreli yol yapisi ayni -> uretilen .txt isimleri BIT AYNI (H#A10 koruma).
$mdDosyalari = Get-ChildItem -Path $notebookDir -Recurse -File -Filter "*.md" -ErrorAction SilentlyContinue |
    Where-Object { $_.FullName -notmatch '[\\/]_archive[\\/]' }

# v2.5-A (22 May 2026): CLAUDE.md de _txt'e yansir (Vizyon Bekcisi NotebookLM
# anayasa denetci rolu icin Drive linkli kaynak). repoRoot/CLAUDE.md notebook/'da
# degil ama anayasa kanonu olarak NotebookLM kaynagi olmali.
$claudeMdPath = Join-Path $repoRoot "CLAUDE.md"
if (Test-Path $claudeMdPath) {
    $claudeMdItem = Get-Item $claudeMdPath
    $mdDosyalari = @($mdDosyalari) + $claudeMdItem
}

$txtUretildi = 0
$txtAtlandi = 0
$txtHata = 0

# v2.5-B (22 May 2026): Alt klasor cakisma fix. Aynı BaseName farkli klasorde
# (kitaplar/_INDEX vs analizler/_INDEX) cakisma yapmamali.
function Get-TxtFileName {
    param($mdItem, $baseDir)
    # Kok klasor dosyasi mi?
    $relPath = $mdItem.DirectoryName.Substring($baseDir.Length).Trim('\','/')
    if (-not $relPath -or $relPath -eq "") {
        return "$($mdItem.BaseName).txt"
    }
    # Alt klasor: subfolder_BaseName.txt formati
    $subFolder = $relPath -replace '[\\/]', '_'
    # BaseName onunde underscore varsa (_INDEX gibi) cift _ olusur, temizle
    $baseName = $mdItem.BaseName -replace '^_+', ''
    return "${subFolder}_$baseName.txt"
}

foreach ($md in $mdDosyalari) {
    # v2.5-B: alt klasor cakisma onleme + CLAUDE.md ozel durum
    if ($md.FullName -eq $claudeMdPath) {
        # CLAUDE.md repoRoot'tan, ozel isim
        $txtYolu = Join-Path $txtKlasoru "CLAUDE.txt"
    } else {
        # notebook/ icindeki dosyalar
        $txtFileName = Get-TxtFileName -mdItem $md -baseDir $notebookDir
        $txtYolu = Join-Path $txtKlasoru $txtFileName
    }
    try {
        if ($KuruCalisma) {
            # Dry-run: sadece ne uretilecegini raporla
            # v2.7: kuru-calisma da ayni 2sn toleransi kullanir (rapor tutarliligi)
            if (-not (Test-Path $txtYolu) -or `
                (Get-Item $txtYolu).LastWriteTime -lt $md.LastWriteTime.AddSeconds(-2)) {
                $txtUretildi++
            } else {
                $txtAtlandi++
            }
        } else {
            # Gercek: kaynak .md daha yeniyse veya .txt yoksa kopyala
            $kopyalaGerek = $true
            if (Test-Path $txtYolu) {
                $txtMevcut = Get-Item $txtYolu
                # v2.7 KRITIK (22 Tem 2026): 2 saniye TOLERANS.
                # Google Drive sanal FS zaman damgasini KIRPAR (NTFS 100ns tick ->
                # Drive ~ms). Copy-Item kaynak mtime'ini yazar ama hedef mikrosaniye
                # daha ESKI gorunur (orn. .3700171 -> .3700000) -> ham "-ge" HER ZAMAN
                # False -> her kosuda 57 dosya yeniden yazilir -> gereksiz Drive
                # trafigi + NotebookLM saatlik yeniden indeksleme (churn).
                # v2.6'da kaynak da Drive FS'teydi (ayni kirpma) -> sorun gorunmuyordu;
                # v2.7 kaynagi lokal NTFS yapinca ortaya cikti.
                # Tolerans gercek duzenlemeyi maskelemez (gercek edit >> 2sn fark).
                if ($txtMevcut.Length -eq $md.Length -and `
                    $txtMevcut.LastWriteTime -ge $md.LastWriteTime.AddSeconds(-2)) {
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

# v2.5: Beklenen .txt isimleri seti (Get-TxtFileName ile uyumlu)
# Eski sadece BaseName karsilastiryordu, alt klasor cakismasi bug'i vardi.
# Simdi: her .md icin gercek .txt dosya adi hesaplanir.
$beklenenTxtBase = @{}
foreach ($md in $mdDosyalari) {
    if ($md.FullName -eq $claudeMdPath) {
        $beklenenTxtBase["CLAUDE"] = $true
    } else {
        $txtName = Get-TxtFileName -mdItem $md -baseDir $notebookDir
        $beklenenTxtBase[[System.IO.Path]::GetFileNameWithoutExtension($txtName)] = $true
    }
}

foreach ($txt in $txtDosyalari) {
    if (-not $beklenenTxtBase.ContainsKey($txt.BaseName)) {
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
Write-Host "Toplam dosya  : $hedefDosyaSayisi  (.txt CANLI: $hedefTxtSayisi | .md DONMUS: $hedefMdSayisi - v2.7'den beri guncellenmiyor)"
Write-Host "Toplam boyut  : $hedefBoyutMB MB"
Write-Host ""
Write-Host "NotebookLM Plus: drive.google.com/drive uzerinden klasor goruncu" -ForegroundColor Yellow
Write-Host "  - Quanfina_notebook icindeki *.txt dosyalari notebook'a ekle" -ForegroundColor Yellow
Write-Host "  - (.md uzantisi NotebookLM Plus Drive picker'da listelenmez - H#8)" -ForegroundColor DarkGray
Write-Host "  - Drive degisirse NotebookLM otomatik yeniden indeksler" -ForegroundColor Yellow
Write-Host ""

exit 0
