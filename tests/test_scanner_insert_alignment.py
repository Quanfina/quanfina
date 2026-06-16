"""
scanner.py — minervini_scans INSERT kolon/placeholder hizalama testi (Paket 468).

Neden: P465 base-detector entegrasyonu INSERT'e 3 kolon ekledi (36 -> 39).
Bir INSERT'te kolon-listesi sayisi ile VALUES (%s,...) placeholder sayisi
ESIT DEGILSE psycopg2 her satirda patlar VEYA (daha kotusu) degerler yanlis
kolonlara kayar -> sessiz veri bozulmasi. Bu statik test scanner.py kaynagini
parse eder, her "INSERT INTO minervini_scans" icin kolon-sayisi == %s-sayisi
invariantini dogrular. Gelecekte biri kolon ekleyip %s eklemeyi unutursa
(veya tersi) test KIRMIZI olur -> drift erken yakalanir.

Not (kapsam): tuple-deger sayisi statik olarak guvenilir parse edilemez
(ic-ice parantezli Python ifadeleri: row.get("X", "")). Tuple hizasi
canli scan'de psycopg2 tarafindan zaten yuksek sesle dogrulanir; burada
en sik drift kaynagi olan kolon<->placeholder invariantini sabitliyoruz.
"""
import re
import pytest
from pathlib import Path

SCANNER_PATH = Path(__file__).resolve().parents[1] / "scanner.py"

# INSERT INTO minervini_scans ( <cols> ) VALUES ( <placeholders> )
# cols ve vals icinde ')' yok (kolon adlari + %s) -> [^)]* ilk kapanis ')'a kadar.
# ON CONFLICT(...) bu pattern'e girmez (VALUES'tan sonra gelir).
_INSERT_RE = re.compile(
    r"INSERT\s+INTO\s+minervini_scans\s*\((?P<cols>[^)]*)\)\s*VALUES\s*\((?P<vals>[^)]*)\)",
    re.DOTALL | re.IGNORECASE,
)


def _load_inserts():
    src = SCANNER_PATH.read_text(encoding="utf-8")
    return list(_INSERT_RE.finditer(src))


def test_scanner_file_exists():
    assert SCANNER_PATH.exists(), f"scanner.py bulunamadi: {SCANNER_PATH}"


def test_single_consolidated_insert():
    # P469 (15 Haz 2026): save_results + run_scan TEK _upsert_minervini_scan_row
    # helper'inda birlesti (DRY). Tam olarak 1 minervini_scans INSERT olmali.
    # >1 ise bir yazma yolu tekrar ayristi (drift) -> helper'a yonlendirilmeli.
    matches = _load_inserts()
    assert len(matches) == 1, (
        f"minervini_scans INSERT sayisi {len(matches)} (P469 konsolidasyon sonrasi tam 1 "
        "helper bekleniyor). >1 ise DRY ihlali: yeni yazma yolu _upsert_minervini_scan_row kullanmali."
    )


def test_each_insert_column_placeholder_count_matches():
    """Her INSERT: kolon-listesi uzunlugu == VALUES %s sayisi."""
    matches = _load_inserts()
    assert matches, "Hic minervini_scans INSERT bulunamadi (regex/dosya sorunu)."
    for i, m in enumerate(matches):
        cols = [c.strip() for c in m.group("cols").split(",") if c.strip()]
        placeholders = m.group("vals").count("%s")
        assert len(cols) == placeholders, (
            f"INSERT #{i + 1} hizalama bozuk: {len(cols)} kolon vs {placeholders} '%s'. "
            f"Kolonlar: {cols}"
        )


def test_full_insert_has_base_detector_columns():
    """P465 regresyon guard: tam INSERT cup/flat/double base kolonlarini icermeli."""
    matches = _load_inserts()
    base_cols = {"cup_handle_quality", "flat_base_quality", "double_bottom_quality"}
    found = False
    for m in matches:
        cols = {c.strip() for c in m.group("cols").split(",") if c.strip()}
        if base_cols.issubset(cols):
            found = True
            break
    assert found, (
        "Hicbir minervini_scans INSERT'i 3 base-detector kolonunu (cup/flat/double) "
        "birlikte icermiyor — P465 entegrasyonu regrese olmus olabilir."
    )


def test_full_insert_has_carr_stage_columns():
    """Carr entegrasyon regresyon guard: tam INSERT carr_stage kolonlarini icermeli."""
    matches = _load_inserts()
    carr_cols = {"carr_stage", "carr_stage_label"}
    found = any(
        carr_cols.issubset({c.strip() for c in m.group("cols").split(",") if c.strip()})
        for m in matches
    )
    assert found, "carr_stage kolonlari hicbir INSERT'te yok — Carr entegrasyonu regrese olmus olabilir."


def test_both_alter_blocks_add_base_columns():
    """Iki migration blogu da (CREATE + DEVAM path) 3 base kolonu ADD COLUMN IF NOT EXISTS ile eklemeli."""
    src = SCANNER_PATH.read_text(encoding="utf-8")
    for col in ("cup_handle_quality", "flat_base_quality", "double_bottom_quality"):
        n = len(re.findall(rf"ADD COLUMN IF NOT EXISTS {col}\b", src))
        assert n >= 2, (
            f"'{col}' icin ADD COLUMN IF NOT EXISTS {n} kez gecti (>=2 bekleniyor — "
            "her iki migration blogunda olmali, idempotent)."
        )


def _insert_quality_cols():
    """Tum minervini_scans INSERT'lerindeki *_quality (base detector) kolon kumesi."""
    cols = set()
    for m in _load_inserts():
        for c in m.group("cols").split(","):
            c = c.strip()
            if c.endswith("_quality"):  # vcp_quality_SCORE haric (suffix _score)
                cols.add(c)
    return cols


def test_persisted_base_detector_quality_set_is_exactly_three():
    """Saklanan base *_quality seti TAM olarak {cup, flat, double}.

    HTF (P462) BILINCLI olarak high_tight_flag_quality kolonu DEGIL — Power Play =
    High Tight Flag ayni canon (KARAR #467), boolean power_play_pass olarak saklanir
    (quanfina_math.py compute_power_play_pass). Yeni bir *_quality kolonu eklenirse
    hem INSERT hem HER IKI ALTER blogu (scanner.py ~153-155, ~1333-1335) guncellenmeli.
    """
    quality_cols = _insert_quality_cols()
    assert quality_cols == {
        "cup_handle_quality",
        "flat_base_quality",
        "double_bottom_quality",
    }, f"Beklenmedik base *_quality kolon seti: {quality_cols}"
    assert "high_tight_flag_quality" not in quality_cols, (
        "high_tight_flag_quality eklenmis — HTF power_play_pass boolean ile saklanir "
        "(KARAR #467). Kasitli ise bu testi + ALTER bloklarini birlikte guncelle."
    )


def test_insert_base_quality_cols_match_alter_migration():
    """INSERT'teki base *_quality kolonlari ile ALTER migration kolonlari ayni kume.

    Bir kolonu bir yere ekleyip digerini unutmayi yakalar (review onerisi).
    """
    src = SCANNER_PATH.read_text(encoding="utf-8")
    alter_quality = set(re.findall(r"ADD COLUMN IF NOT EXISTS (\w+_quality)\b", src))
    insert_quality = _insert_quality_cols()
    assert insert_quality == alter_quality, (
        f"INSERT base *_quality kolonlari {insert_quality} ile ALTER migration "
        f"kolonlari {alter_quality} ESLESMIYOR — bir yere eklenip digerine eklenmeyen kolon var."
    )


# ---- P481 (#4, #5): fundamental_only + 52w_high INSERT <-> sema kolon kapsami ----
# Mevcut testler SADECE minervini_scans'i kapsiyordu -> #4/#5 (INSERT perf_year/roe
# yaziyor ama tabloda yok) yakalanmamisti. Bu guard her tablo icin INSERT kolonlari
# subset(sema kolonlari) invariantini sabitler: bir kolon INSERT'e eklenip CREATE/ALTER'a
# eklenmezse "column does not exist" -> tum INSERT'ler sessizce fail -> tablo bos kalir.

def _insert_cols(table):
    src = SCANNER_PATH.read_text(encoding="utf-8")
    rx = re.compile(rf"INSERT\s+INTO\s+{table}\s*\((?P<cols>[^)]*)\)\s*VALUES",
                    re.DOTALL | re.IGNORECASE)
    cols = set()
    for m in rx.finditer(src):
        for c in m.group("cols").split(","):
            c = c.strip()
            if c:
                cols.add(c.lower())
    return cols


def _schema_cols(table):
    """CREATE TABLE govdesi (tum kopyalar) + ALTER ADD COLUMN ile taninan kolonlar."""
    src = SCANNER_PATH.read_text(encoding="utf-8")
    cols = set(c.lower() for c in re.findall(
        rf"ALTER\s+TABLE\s+{table}\s+ADD\s+COLUMN\s+IF\s+NOT\s+EXISTS\s+(\w+)",
        src, re.IGNORECASE))
    for m in re.finditer(
        rf"CREATE\s+TABLE\s+IF\s+NOT\s+EXISTS\s+{table}\s*\((?P<body>.*?)UNIQUE",
        src, re.DOTALL | re.IGNORECASE):
        for line in m.group("body").splitlines():
            tok = re.match(r"\s*(\w+)\s+\w", line)
            if tok and tok.group(1).lower() not in ("unique", "primary"):
                cols.add(tok.group(1).lower())
    return cols


@pytest.mark.parametrize("table", [
    "minervini_fundamental_only",
    "minervini_52w_high",
    "minervini_fundamental_scans",
])
def test_insert_columns_exist_in_schema(table):
    """P481 (#4,#5): INSERT yazdigi her kolon CREATE veya ALTER ile taninmali."""
    insert_cols = _insert_cols(table)
    if not insert_cols:
        pytest.skip(f"{table} icin INSERT bulunamadi")
    schema_cols = _schema_cols(table)
    missing = insert_cols - schema_cols
    assert not missing, (
        f"{table}: INSERT su kolonlari yaziyor ama CREATE/ALTER'da YOK: {sorted(missing)} "
        f"-> 'column does not exist' -> tum INSERT'ler sessizce fail -> tablo bos kalir."
    )
