"""
_list_cols.py - Quanfina Mark sayfasi 4 liste yapisi sutun tanimlari

ADIM 4 Sprint 4.1: 4 liste hierarsisi (Notebook v18 hedef sema)
- watch:    7 sutun  - 100+ hisse genel bakis
- on_deck:  10 sutun - 30-40 hisse on-eleme
- focus:    13 sutun - 5-15 hisse detayli analiz
- buy:      17 sutun - 3-5 hisse tam analiz, alim hazirligi

Sutun isimleri minervini_scans/52w_high/fundamental_scans/fundamental_only
tablolari ve mevcut 3_Minervini.py col_rename dict'i ile uyumlu.

Kullanim:
    from _list_cols import COLUMN_LADDER, get_columns_for_list
    cols = get_columns_for_list("focus")  # -> 13 sutunluk liste
"""
from typing import List, Dict


# Kademeli sutun yapisi - her seviye bir oncekini icerir + ekler
COLUMN_LADDER: Dict[str, List[str]] = {
    "watch": [
        "ticker",
        "company",
        "sector",
        "price",
        "change_pct",
        "grade",
        "rs_ibd",
    ],
    "on_deck": [
        # watch sutunlari
        "ticker", "company", "sector", "price", "change_pct", "grade", "rs_ibd",
        # on_deck eklemeleri
        "ma200_slope",
        "pct_from_high",
        "rs_12m",
    ],
    "focus": [
        # on_deck sutunlari
        "ticker", "company", "sector", "price", "change_pct", "grade", "rs_ibd",
        "ma200_slope", "pct_from_high", "rs_12m",
        # focus eklemeleri
        "volume",
        "eps_qoq",
        "sales_qoq",
    ],
    "buy": [
        # focus sutunlari
        "ticker", "company", "sector", "price", "change_pct", "grade", "rs_ibd",
        "ma200_slope", "pct_from_high", "rs_12m",
        "volume", "eps_qoq", "sales_qoq",
        # buy eklemeleri
        "market_cap",
        "days_to_earnings",
        "confirmations",
        "violations",
    ],
}


# Sutun isimlerinin Turkce karsiliklari (3_Minervini.py col_rename ile uyumlu)
COLUMN_LABELS: Dict[str, str] = {
    "ticker": "TICKER",
    "company": "SIRKET",
    "sector": "SEKTOR",
    "industry": "SEKTOR ALTI",
    "price": "FIYAT",
    "change_pct": "DEGISIM",
    "volume": "HACIM",
    "market_cap": "PIYASA DEGERI (M)",
    "ma200_slope": "MA200 SLOPE",
    "pct_from_high": "% YUKSEK MESAFE",
    "days_to_earnings": "BILANCO (gun)",
    "eps_qoq": "EPS Q/Q",
    "sales_qoq": "SALES Q/Q",
    "grade": "GRADE",
    "confirmations": "POZITIF",
    "violations": "NEGATIF",
    "rs_ibd": "RS (IBD)",
    "rs_12m": "RS (12A)",
    "rs_20d": "RS (20G)",
    "rs_50d": "RS (50G)",
    "rs_200d": "RS (200G)",
    "rs_mansfield": "Mansfield",
    "tv_url": "TV",
    "fv_url": "FV",
}


# Liste tipi tanimlari (list_types tablosu ile uyumlu)
LIST_TYPES: Dict[str, Dict] = {
    "watch": {
        "label": "Watch (100+ hisse)",
        "default_column_count": 7,
        "sort_order": 1,
        "description": "Genel bakis, ilk filtre",
    },
    "on_deck": {
        "label": "On Deck (30-40)",
        "default_column_count": 10,
        "sort_order": 2,
        "description": "On-eleme, setup tipi belirlenir",
    },
    "focus": {
        "label": "Focus (5-15)",
        "default_column_count": 13,
        "sort_order": 3,
        "description": "Detayli analiz, pivot fiyat netlesir",
    },
    "buy": {
        "label": "Buy (3-5)",
        "default_column_count": 17,
        "sort_order": 4,
        "description": "Alim hazirligi, alarm kuruldu",
    },
}


def get_columns_for_list(list_code: str) -> List[str]:
    """Liste tipine gore sutun listesini dondurur.

    Args:
        list_code: 'watch', 'on_deck', 'focus', veya 'buy'

    Returns:
        Sutun isimleri listesi (DB kolon adlari)

    Raises:
        ValueError: Gecersiz list_code

    Example:
        >>> get_columns_for_list("watch")
        ['ticker', 'company', 'sector', 'price', 'change_pct', 'grade', 'rs_ibd']
        >>> len(get_columns_for_list("focus"))
        13
    """
    if list_code not in COLUMN_LADDER:
        raise ValueError(
            "Gecersiz list_code: '{}'. Gecerli: {}".format(
                list_code, list(COLUMN_LADDER.keys())
            )
        )
    return list(COLUMN_LADDER[list_code])  # copy


def get_label(col_name: str) -> str:
    """DB kolon adini Turkce display label'a cevirir.

    Args:
        col_name: DB kolon adi (orn. 'rs_ibd')

    Returns:
        Turkce label (orn. 'RS (IBD)') veya kolon adinin upper hali
    """
    return COLUMN_LABELS.get(col_name, col_name.upper())


def get_list_meta(list_code: str) -> Dict:
    """Liste tipi metadata'sini dondurur.

    Args:
        list_code: 'watch', 'on_deck', 'focus', veya 'buy'

    Returns:
        Dict: {label, default_column_count, sort_order, description}
    """
    if list_code not in LIST_TYPES:
        raise ValueError(
            "Gecersiz list_code: '{}'. Gecerli: {}".format(
                list_code, list(LIST_TYPES.keys())
            )
        )
    return dict(LIST_TYPES[list_code])  # copy


__all__ = [
    "COLUMN_LADDER",
    "COLUMN_LABELS",
    "LIST_TYPES",
    "get_columns_for_list",
    "get_label",
    "get_list_meta",
]
