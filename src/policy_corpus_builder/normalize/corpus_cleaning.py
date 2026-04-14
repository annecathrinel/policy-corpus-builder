from __future__ import annotations

import re
import numpy as np
import pandas as pd
from celex_lookup import extract_celex_token, parse_celex_to_dict


def normalize_lang(value: object) -> str:
    s = str(value or "").strip().lower()
    if s in {"", "nan", "none"}:
        return "en"
    if s in {"en", "eng", "english"}:
        return "en"
    return s


def clean_text(value: object) -> str:
    return re.sub(r"\s+", " ", str(value or "")).strip()


def harmonize_docs(df: pd.DataFrame, jurisdiction: str | None = None) -> pd.DataFrame:
    out = df.copy()
    if jurisdiction is not None:
        out["jurisdiction"] = jurisdiction
    if "raw_text" not in out.columns:
        out["raw_text"] = out.get("full_text_clean", "")
    out["lang"] = out.get("lang", "en").apply(normalize_lang)
    if "celex_full" in out.columns:
        out["celex_full"] = out["celex_full"].fillna("").astype(str)
    else:
        out["celex_full"] = ""
    if "doc_uid" in out.columns:
        out["doc_uid"] = out["doc_uid"].fillna("").astype(str)
    elif "url" in out.columns:
        out["doc_uid"] = out["url"].fillna("").astype(str)
    else:
        out["doc_uid"] = ""
    out["doc_uid"] = out["doc_uid"].where(out["doc_uid"].str.len().gt(0), out["celex_full"])
    out["celex"] = out.get("celex", out["doc_uid"]).fillna("").astype(str)
    out["celex"] = out["celex"].where(out["celex"].str.len().gt(0), out["doc_uid"].astype(str).apply(extract_celex_token))
    if "doc_id" in out.columns:
        out["doc_id"] = out["doc_id"].fillna("").astype(str)
    else:
        out["doc_id"] = ""
    out["doc_id"] = out["doc_id"].where(out["doc_id"].str.len().gt(0), out["celex_full"])
    out["doc_id"] = out["doc_id"].where(out["doc_id"].str.len().gt(0), out["celex"])
    out["doc_id"] = out["doc_id"].where(out["doc_id"].str.len().gt(0), out["doc_uid"])
    out["full_text_clean"] = out.get("full_text_clean", out["raw_text"]).apply(clean_text)
    out["text_len"] = out["full_text_clean"].str.len()
    out["has_text"] = out["text_len"].gt(0)
    out["text_norm"] = out["full_text_clean"].str.lower()
    if "title" in out.columns:
        out["title"] = out["title"].fillna("").astype(str)
    else:
        out["title"] = ""
    out["retrieval_track"] = np.where(out["jurisdiction"].eq("European Union"), "EU EUR-Lex retrieval", "Non-EU jurisdiction retrieval")
    out["raw_cache_note"] = np.where(out["jurisdiction"].eq("European Union"), "from 01A EUR-Lex retrieval layer", "from 01C non-EU retrieval layer")
    out["text_missing"] = out["full_text_clean"].eq("")
    return out


def construct_corpora(eu_docs: pd.DataFrame, non_eu_docs: pd.DataFrame) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    raw_docs_df = pd.concat([harmonize_docs(eu_docs, "European Union"), harmonize_docs(non_eu_docs)], ignore_index=True, sort=False)
    raw_docs_df = raw_docs_df.drop_duplicates(subset=["doc_id"]).reset_index(drop=True)
    celex_parts = raw_docs_df["celex"].apply(parse_celex_to_dict).apply(pd.Series)
    rename = {
        "sector": "celex_sector",
        "sector_label": "celex_sector_label",
        "year": "celex_year",
        "descriptor": "celex_doc_type_code",
        "descriptor_label": "celex_doc_type_label",
        "document_number": "celex_document_number",
    }
    for src, tgt in rename.items():
        if src in celex_parts.columns:
            raw_docs_df[tgt] = celex_parts[src]
    raw_docs_df["analysis_year"] = raw_docs_df.get("celex_year")
    all_docs_df = raw_docs_df.copy()
    all_en_docs_df = all_docs_df[all_docs_df["lang"].eq("en")].reset_index(drop=True)
    return raw_docs_df, all_docs_df, all_en_docs_df
