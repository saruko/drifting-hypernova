"""
PubMed 引用数急増アラートシステム — 辞書ファイル
日本語→MeSHクエリ変換辞書 / ジャーナル IF 辞書
"""

import logging

logger = logging.getLogger(__name__)

# =========================================================
# 日本語 → PubMed MeSH クエリ変換辞書
# =========================================================
MESH_DICTIONARY: dict[str, str] = {
    '眼科':         'Ophthalmology[MeSH]',
    '緑内障':       'Glaucoma[MeSH]',
    '網膜':         'Retina[MeSH]',
    '黄斑変性':     'Macular Degeneration[MeSH]',
    '加齢黄斑変性': 'Macular Degeneration[MeSH]',
    '糖尿病網膜症': 'Diabetic Retinopathy[MeSH]',
    '白内障':       'Cataract[MeSH]',
    '近視':         'Myopia[MeSH]',
    '視神経':       'Optic Nerve[MeSH]',
    '角膜':         'Cornea[MeSH]',
    '網膜剥離':     'Retinal Detachment[MeSH]',
    '視野':         'Visual Field Tests[MeSH]',
    '眼圧':         'Intraocular Pressure[MeSH]',
    '人工知能（眼科）': 'Artificial Intelligence[MeSH] AND Ophthalmology[MeSH]',
}

# =========================================================
# ジャーナル名 → インパクトファクター辞書
# =========================================================
IMPACT_FACTOR_DICTIONARY: dict[str, float] = {
    'Ophthalmology':                                          14.0,
    'JAMA Ophthalmology':                                      8.1,
    'Progress in Retinal and Eye Research':                    18.3,
    'American Journal of Ophthalmology':                        5.2,
    'British Journal of Ophthalmology':                         4.1,
    'Investigative Ophthalmology & Visual Science':             4.2,
    'Investigative Ophthalmology and Visual Science':           4.2,
    'IOVS':                                                     4.2,
    'Eye':                                                      3.0,
    'Cornea':                                                   2.8,
    'Journal of Glaucoma':                                      2.4,
    'Retina':                                                   4.5,
    "Graefe's Archive for Clinical and Experimental Ophthalmology": 3.2,
    'Ocular Surface':                                           7.6,
    'The Ocular Surface':                                       7.6,
    'Translational Vision Science & Technology':                3.0,
    'Translational Vision Science and Technology':              3.0,
}


def get_mesh_query(keyword: str) -> str | None:
    """
    日本語キーワードを PubMed MeSH クエリに変換する。

    Args:
        keyword: 日本語の分野キーワード

    Returns:
        MeSH クエリ文字列。辞書に存在しない場合は None を返しログに記録。
    """
    query = MESH_DICTIONARY.get(keyword)
    if query is None:
        logger.error(f"辞書に存在しないキーワード: '{keyword}' — スキップします")
    return query


def get_impact_factor(journal_name: str) -> str | float:
    """
    ジャーナル名からインパクトファクターを取得する。
    完全一致を試み、失敗した場合は部分一致で検索する。

    Args:
        journal_name: ジャーナル名

    Returns:
        IF 値（float）、見つからない場合は 'N/A'
    """
    if not journal_name:
        return 'N/A'

    # 完全一致
    if journal_name in IMPACT_FACTOR_DICTIONARY:
        return IMPACT_FACTOR_DICTIONARY[journal_name]

    # 部分一致（大文字小文字無視）
    journal_lower = journal_name.lower()
    for key, value in IMPACT_FACTOR_DICTIONARY.items():
        if key.lower() in journal_lower or journal_lower in key.lower():
            return value

    return 'N/A'
