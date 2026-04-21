"""Supported EUR-Lex NIM retrieval/full-text helper subset."""

from __future__ import annotations

import hashlib
import os
import re
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Iterable
from urllib.parse import quote

import pandas as pd
import requests
from bs4 import BeautifulSoup

from policy_corpus_builder.utils.celex import (
    extract_celex_token,
    parse_celex,
    parse_celex_to_dict,
)

try:
    from lxml import etree as ET
except Exception:  # pragma: no cover
    import xml.etree.ElementTree as ET  # type: ignore[no-redef]

EURLEX_WS_ENDPOINT = "https://eur-lex.europa.eu/EURLexWebService"

EU_ISO3_TO_NAME = {
    "AUT": "Austria", "BEL": "Belgium", "BGR": "Bulgaria", "HRV": "Croatia", "CYP": "Cyprus", "CZE": "Czechia",
    "DNK": "Denmark", "EST": "Estonia", "FIN": "Finland", "FRA": "France", "DEU": "Germany", "GRC": "Greece",
    "HUN": "Hungary", "IRL": "Ireland", "ITA": "Italy", "LVA": "Latvia", "LTU": "Lithuania", "LUX": "Luxembourg",
    "MLT": "Malta", "NLD": "Netherlands", "POL": "Poland", "PRT": "Portugal", "ROU": "Romania", "SVK": "Slovakia",
    "SVN": "Slovenia", "ESP": "Spain", "SWE": "Sweden", "GBR": "United Kingdom",
}

ISO3_TO_EURLEX_LANG2 = {
    "bul": "BG", "ces": "CS", "dan": "DA", "deu": "DE", "ell": "EL", "eng": "EN",
    "spa": "ES", "est": "ET", "fin": "FI", "fra": "FR", "gle": "GA", "hrv": "HR",
    "hun": "HU", "ita": "IT", "lav": "LV", "lit": "LT", "mlt": "MT", "nld": "NL",
    "pol": "PL", "por": "PT", "ron": "RO", "slk": "SK", "slv": "SL", "swe": "SV",
}

LANG2_TO_LANG3 = {v.lower(): k for k, v in ISO3_TO_EURLEX_LANG2.items()}

MS_ISO3_TO_LANG2S = {
    "AUT": ["DE"], "BEL": ["FR", "NL", "DE"], "BGR": ["BG"], "HRV": ["HR"], "CYP": ["EL", "EN"],
    "CZE": ["CS"], "DNK": ["DA"], "EST": ["ET"], "FIN": ["FI", "SV"], "FRA": ["FR"], "DEU": ["DE"],
    "GRC": ["EL"], "HUN": ["HU"], "IRL": ["EN", "GA"], "ITA": ["IT"], "LVA": ["LV"], "LTU": ["LT"],
    "LUX": ["FR", "DE", "EN"], "MLT": ["MT", "EN"], "NLD": ["NL"], "POL": ["PL"], "PRT": ["PT"],
    "ROU": ["RO"], "SVK": ["SK"], "SVN": ["SL"], "ESP": ["ES"], "SWE": ["SV"],
}

OFFICIAL_EU_LANGS_BY_MS_ISO3 = {
    "AUT": ["de"],
    "BEL": ["nl", "fr", "de"],
    "BGR": ["bg"],
    "HRV": ["hr"],
    "CYP": ["el", "tr", "en"],
    "CZE": ["cs"],
    "DNK": ["da"],
    "EST": ["et"],
    "FIN": ["fi", "sv"],
    "FRA": ["fr"],
    "DEU": ["de"],
    "GRC": ["el"],
    "HUN": ["hu"],
    "IRL": ["ga", "en"],
    "ITA": ["it"],
    "LVA": ["lv"],
    "LTU": ["lt"],
    "LUX": ["fr", "de", "lb", "en"],
    "MLT": ["mt", "en"],
    "NLD": ["nl"],
    "POL": ["pl"],
    "PRT": ["pt"],
    "ROU": ["ro"],
    "SVK": ["sk"],
    "SVN": ["sl"],
    "ESP": ["es"],
    "SWE": ["sv"],
}

NIM_TEXT_ROUTE_ORDER = [
    "direct_text_pdf",
    "direct_text_docx",
    "direct_text_doc",
    "direct_text_file",
    "national_website_eli",
    "national_website",
    "machine_translation",
    "fallback_generic/eurlex_url",
    "fallback_generic/legal-content-html",
    "fallback_generic/legal-content-txt",
    "fallback_generic/legal-content-html-en",
    "fallback_generic/lexuriserv",
]

def _localname(tag: str | None) -> str:
    if not tag:
        return ""
    return tag.rsplit("}", 1)[-1] if "}" in tag else tag


def _text(elem: Any) -> str:
    if elem is None:
        return ""
    return (getattr(elem, "text", "") or "").strip()


def _clean_optional_text(value: Any) -> str:
    if value is None or (isinstance(value, float) and pd.isna(value)):
        return ""
    text = str(value).strip()
    return "" if text.lower() == "nan" else text


def normalize_legal_act_celex(value: object) -> str:
    token = extract_celex_token(str(value or ""))
    if not token:
        return ""
    token = token.upper()
    token = re.sub(r"-\d{8}$", "", token)
    token = re.sub(r"R\(\d{2}\)$", "", token)
    if re.match(r"^0(3\d{4}[A-Z]{1,3}\d+)(?:-\d{8})?$", token):
        token = re.sub(r"^0", "", token, count=1)
    m = re.match(r"^(3\d{4}[A-Z]{1,3}\d+)", token)
    if m:
        token = m.group(1)
    info = parse_celex(token)
    if not info.valid or info.sector != "3":
        return ""
    return info.normalized


def select_eligible_celex_acts(eu_docs: pd.DataFrame) -> pd.DataFrame:
    docs = eu_docs.copy()
    docs["celex"] = docs["celex"].astype(str).apply(normalize_legal_act_celex)
    celex_parts = docs["celex"].apply(parse_celex_to_dict).apply(pd.Series)
    for src, tgt in {"sector": "celex_sector", "descriptor_label": "celex_doc_type_label", "year": "celex_year"}.items():
        if src in celex_parts.columns:
            docs[tgt] = celex_parts[src]
    eligible = (
        docs[docs["celex"].astype(str).str.startswith("3") & docs["celex"].astype(str).str.len().ge(8)][["celex", "title", "celex_doc_type_label", "celex_year"]]
        .drop_duplicates(subset=["celex"])
        .rename(columns={"title": "eu_act_title", "celex_year": "year", "celex_doc_type_label": "eu_act_type"})
        .reset_index(drop=True)
    )
    return eligible


def get_ws_credentials() -> tuple[str, str]:
    user = os.getenv("EURLEX_WS_USER") or os.getenv("EURLEX_USER") or ""
    password = os.getenv("EURLEX_WS_PASS") or os.getenv("EURLEX_WEB_PASS") or ""
    return user, password


def build_mne_expert_query(act_celex: str) -> str:
    celex = normalize_legal_act_celex(act_celex)
    if not celex:
        raise ValueError(f"Invalid legal-act CELEX: {act_celex}")
    return f"DTS_SUBDOM = MNE AND MNE_IMPLEMENTS_DIR = {celex}*"


def _soap_envelope(
    expert_query: str, 
    page: int, 
    page_size: int, 
    search_language: str, 
    user: str, 
    password: str
) -> str:
    """Port of notebook SOAP envelope builder."""
    eurlex_user = user or os.getenv("EURLEX_USER")
    eurlex_web_pass = password or os.getenv("EURLEX_WEB_PASS")
    if not eurlex_user or not eurlex_web_pass:
        raise RuntimeError("Missing EURLEX_USER or EURLEX_WEB_PASS.")

    pw_type = "http://docs.oasis-open.org/wss/2004/01/oasis-200401-wss-username-token-profile-1.0#PasswordText"
    return f'''<?xml version="1.0" encoding="UTF-8"?>
            <soap:Envelope xmlns:soap="http://www.w3.org/2003/05/soap-envelope"
                        xmlns:sear="http://eur-lex.europa.eu/search"
                        xmlns:wsse="http://docs.oasis-open.org/wss/2004/01/oasis-200401-wss-wssecurity-secext-1.0.xsd"
                        xmlns:wsu="http://docs.oasis-open.org/wss/2004/01/oasis-200401-wss-wssecurity-utility-1.0.xsd">
            <soap:Header>
                <wsse:Security soap:mustUnderstand="true">
                <wsse:UsernameToken wsu:Id="UsernameToken-1">
                    <wsse:Username>{eurlex_user}</wsse:Username>
                    <wsse:Password Type="{pw_type}">{eurlex_web_pass}</wsse:Password>
                </wsse:UsernameToken>
                </wsse:Security>
            </soap:Header>
            <soap:Body>
                <sear:searchRequest>
                <sear:expertQuery><![CDATA[{expert_query}]]></sear:expertQuery>
                <sear:page>{page}</sear:page>
                <sear:pageSize>{page_size}</sear:pageSize>
                <sear:searchLanguage>{search_language}</sear:searchLanguage>
                </sear:searchRequest>
            </soap:Body>
            </soap:Envelope>'''


def eurlex_ws_doquery(expert_query: str, *, page: int = 1, page_size: int = 100, search_language: str = "en", timeout_s: int = 90) -> bytes:
    user, password = get_ws_credentials()
    if not user or not password:
        raise RuntimeError("Missing EUR-Lex WebService credentials.")
    xml_body = _soap_envelope(expert_query, page, page_size, search_language, user, password).encode("utf-8")
    headers = {
        "Content-Type": "application/soap+xml; charset=utf-8",
        "Accept": "application/soap+xml, text/xml, */*",
        "User-Agent": "Mozilla/5.0",
    }
    session = requests.Session()
    session.trust_env = False
    response = session.post(EURLEX_WS_ENDPOINT, data=xml_body, headers=headers, timeout=timeout_s)
    response.raise_for_status()
    return response.content


def _lang_uri_to_iso3(uri: str) -> str:
    token = uri.rstrip("/").rsplit("/", 1)[-1].lower() if uri else ""
    mapping = {
        "bul": "bul", "ces": "ces", "dan": "dan", "deu": "deu", "ell": "ell", "eng": "eng",
        "spa": "spa", "est": "est", "fin": "fin", "fra": "fra", "gle": "gle", "hrv": "hrv",
        "hun": "hun", "ita": "ita", "lav": "lav", "lit": "lit", "mlt": "mlt", "nld": "nld",
        "pol": "pol", "por": "por", "ron": "ron", "slk": "slk", "slv": "slv", "swe": "swe",
    }
    return mapping.get(token, "")


def _iso3_from_nim_celex(nim_celex: str) -> str:
    if not isinstance(nim_celex, str) or not nim_celex.strip():
        return ""
    base = nim_celex.strip().upper().split("_", 1)[0]
    code = base[-3:]
    return code if code.isalpha() else ""


def _notice_to_record(notice_elem: Any, act_celex: str = "") -> dict[str, Any]:
    rec: dict[str, Any] = {
        "nim_celex": "", "national_measure_id": "", "nim_date": "", "nim_title": "",
        "nim_title_lang": "", "available_expr_langs3": "", "cellar_uri": "", "nim_resource_uri": "",
    }
    celex_vals: list[str] = []
    date_vals: list[str] = []
    titles: list[str] = []
    title_langs: list[str] = []
    expr_lang_uris: list[str] = []
    member_state_iso3 = ""
    for elem in notice_elem.iter():
        ln = _localname(getattr(elem, "tag", ""))
        if ln == "ID_CELEX":
            for child in list(elem):
                if _localname(getattr(child, "tag", "")) == "VALUE":
                    value = _text(child)
                    if value:
                        celex_vals.append(value)
        elif ln in {"WORK_DATE_DOCUMENT", "DATE_DOCUMENT"}:
            for child in list(elem):
                if _localname(getattr(child, "tag", "")) == "VALUE":
                    value = _text(child)
                    if value:
                        date_vals.append(value)
        elif ln in {"EXPRESSION_TITLE", "TITLE", "WORK_TITLE"}:
            title_lang = ""
            if hasattr(elem, "get"):
                title_lang = (
                    elem.get("{http://www.w3.org/XML/1998/namespace}lang")
                    or elem.get("lang")
                    or ""
                ).strip().lower()
            value_nodes = [child for child in elem.iter() if _localname(getattr(child, "tag", "")) == "VALUE"]
            if value_nodes:
                for child in value_nodes:
                    value = _text(child)
                    if value:
                        titles.append(value)
                        lang = title_lang
                        if hasattr(child, "get"):
                            lang = (
                                child.get("{http://www.w3.org/XML/1998/namespace}lang")
                                or child.get("lang")
                                or title_lang
                                or ""
                            ).strip().lower()
                        title_langs.append(lang)
            else:
                value = _text(elem)
                if value:
                    titles.append(value)
                    title_langs.append(title_lang)
        elif ln in {"EXPRESSION_USES_LANGUAGE", "LANGUAGE"}:
            found_uri = False
            for child in elem.iter():
                child_ln = _localname(getattr(child, "tag", ""))
                if child_ln == "VALUE":
                    uri = _text(child)
                    if "authority/language" in uri.lower():
                        expr_lang_uris.append(uri)
                        found_uri = True
                elif child_ln in {"OP-CODE", "IDENTIFIER"}:
                    code = _text(child).lower()
                    if len(code) == 3 and code in ISO3_TO_EURLEX_LANG2:
                        expr_lang_uris.append(f"http://publications.europa.eu/resource/authority/language/{code}")
                        found_uri = True
            if not found_uri:
                code = _text(elem).lower()
                if len(code) == 3 and code in ISO3_TO_EURLEX_LANG2:
                    expr_lang_uris.append(f"http://publications.europa.eu/resource/authority/language/{code}")
        elif ln == "WORK_CREATED_BY_AGENT":
            for child in elem.iter():
                if _localname(getattr(child, "tag", "")) == "IDENTIFIER":
                    token = _text(child).upper()
                    if token in EU_ISO3_TO_NAME:
                        member_state_iso3 = token
        elif ln == "SAMEAS":
            sameas_type = ""
            sameas_identifier = ""
            sameas_value = ""
            for child in elem.iter():
                child_ln = _localname(getattr(child, "tag", ""))
                if child_ln == "TYPE":
                    sameas_type = _text(child).lower()
                elif child_ln == "IDENTIFIER":
                    sameas_identifier = _text(child).strip()
                elif child_ln == "VALUE":
                    sameas_value = _text(child).strip()
            if sameas_type == "celex" and sameas_identifier:
                celex_vals.append(sameas_identifier)
            elif sameas_type == "nim" and sameas_identifier:
                rec["national_measure_id"] = sameas_identifier
                if sameas_value:
                    rec["nim_resource_uri"] = sameas_value
        elif ln == "URI":
            uri_type = ""
            uri_value = ""
            for child in elem:
                child_ln = _localname(getattr(child, "tag", ""))
                if child_ln == "TYPE":
                    uri_type = _text(child).lower()
                elif child_ln == "VALUE":
                    uri_value = _text(child).strip()
            if uri_type == "cellar" and uri_value:
                rec["cellar_uri"] = uri_value
    normalized_vals = [v.strip().upper() for v in celex_vals if str(v).strip()]
    target_prefix = ""
    if act_celex:
        target = normalize_legal_act_celex(act_celex)
        if target:
            target_prefix = "7" + target[1:]
    chosen = ""
    if target_prefix:
        for value in normalized_vals:
            if value.startswith(target_prefix):
                chosen = value
                break
    if not chosen and normalized_vals:
        chosen = normalized_vals[0]
    rec["nim_celex"] = chosen
    rec["nim_date"] = date_vals[0] if date_vals else ""
    rec["nim_title"] = _clean_optional_text(titles[0] if titles else "")
    rec["nim_title_lang"] = _clean_optional_text(title_langs[0] if title_langs else "")
    expr_langs = sorted({_lang_uri_to_iso3(u) for u in expr_lang_uris if _lang_uri_to_iso3(u)})
    rec["available_expr_langs3"] = ",".join(expr_langs)
    rec["member_state_iso3"] = member_state_iso3 or _iso3_from_nim_celex(rec["nim_celex"])
    rec["member_state_name"] = EU_ISO3_TO_NAME.get(rec["member_state_iso3"], "")
    rec["eurlex_url"] = f"https://eur-lex.europa.eu/legal-content/EN/TXT/?uri=CELEX:{rec['nim_celex']}" if rec["nim_celex"] else ""
    return rec


def parse_ws_nim_results(xml_bytes: bytes, act_celex: str = "") -> pd.DataFrame:
    root = ET.fromstring(xml_bytes)
    notices = [elem for elem in root.iter() if _localname(getattr(elem, "tag", "")) == "NOTICE"]
    rows = [_notice_to_record(notice, act_celex=act_celex) for notice in notices]
    rows = [row for row in rows if row.get("nim_celex")]
    if not rows:
        return pd.DataFrame()
    key = "national_measure_id" if any(row.get("national_measure_id") for row in rows) else "nim_celex"
    return pd.DataFrame(rows).drop_duplicates(subset=[key, "nim_celex"])


def enrich_nim_metadata(
    nim_df: pd.DataFrame,
    *,
    cache_dir: Path | None = None,
) -> pd.DataFrame:
    """Populate stable title/language metadata on nim_long for both live and fallback paths."""
    if nim_df.empty:
        return nim_df.copy()

    out = nim_df.copy()
    required_cols = [
        "nim_title",
        "nim_title_lang",
        "available_expr_langs3",
        "member_state_iso3",
        "eurlex_url",
        "national_measure_id",
        "nim_celex",
    ]
    for col in required_cols:
        if col not in out.columns:
            out[col] = ""

    text_cols = [
        "nim_title",
        "nim_title_lang",
        "available_expr_langs3",
        "member_state_iso3",
        "eurlex_url",
        "national_measure_id",
        "nim_celex",
        "cellar_uri",
        "nim_resource_uri",
    ]
    for col in text_cols:
        if col not in out.columns:
            out[col] = ""
        out[col] = out[col].map(_clean_optional_text)

    out["nim_title_notice"] = out["nim_title"]

    if "nim_resource_uri" not in out.columns:
        out["nim_resource_uri"] = ""
    mask = out["nim_resource_uri"].str.strip().eq("") & out["national_measure_id"].str.strip().ne("")
    out.loc[mask, "nim_resource_uri"] = out.loc[mask, "national_measure_id"].map(
        lambda x: f"http://publications.europa.eu/resource/nim/{x}" if x else ""
    )

    available_langs_series = out["available_expr_langs3"].map(_langs3_to_lang2_list)
    out["available_langs"] = available_langs_series.map(lambda xs: ",".join(xs))

    detect_pairs = out.apply(detect_nim_language, axis=1)
    out["lang_detected"] = detect_pairs.map(lambda x: x[0])
    out["lang_source"] = detect_pairs.map(lambda x: x[1])

    title_lang_norm = out["nim_title_lang"].map(_xml_lang_to_lang2)
    title_lang_norm = title_lang_norm.where(title_lang_norm.str.strip().ne(""), out["lang_detected"])
    out["nim_title_lang"] = title_lang_norm.fillna("")

    if cache_dir is not None:
        try:
            cache_df = load_nim_fulltext_cache_table(cache_dir)
        except Exception:
            cache_df = pd.DataFrame()
        if not cache_df.empty:
            cache_subset = cache_df.copy()
            for col in ["nim_celex", "nim_title", "page_title", "page_title_lang", "lang_detected", "lang_source", "available_languages"]:
                if col not in cache_subset.columns:
                    cache_subset[col] = ""
                cache_subset[col] = cache_subset[col].map(_clean_optional_text)
            cache_subset = (
                cache_subset.sort_values(["nim_celex", "text_len", "timestamp"], ascending=[True, False, False])
                .drop_duplicates(subset=["nim_celex"], keep="first")
            )
            merge_cols = [
                "nim_celex",
                "nim_title",
                "page_title",
                "page_title_lang",
                "lang_detected",
                "lang_source",
                "available_languages",
                "text_len",
            ]
            out = out.merge(
                cache_subset[merge_cols].rename(
                    columns={
                        "nim_title": "cache_nim_title",
                        "page_title": "cache_page_title",
                        "page_title_lang": "cache_page_title_lang",
                        "lang_detected": "cache_lang_detected",
                        "lang_source": "cache_lang_source",
                        "available_languages": "cache_available_languages",
                        "text_len": "cache_text_len",
                    }
                ),
                on="nim_celex",
                how="left",
            )

            title_mask = out["nim_title"].str.strip().eq("")
            out.loc[title_mask, "nim_title"] = out.loc[title_mask, "cache_nim_title"].map(_clean_optional_text)
            title_mask = out["nim_title"].str.strip().eq("")
            out.loc[title_mask, "nim_title"] = out.loc[title_mask, "cache_page_title"].map(_clean_optional_text)

            title_lang_mask = out["nim_title_lang"].str.strip().eq("")
            out.loc[title_lang_mask, "nim_title_lang"] = out.loc[title_lang_mask, "cache_page_title_lang"].map(_clean_optional_text)

            lang_mask = out["lang_detected"].str.strip().eq("")
            out.loc[lang_mask, "lang_detected"] = out.loc[lang_mask, "cache_lang_detected"].map(_clean_optional_text)
            lang_source_mask = out["lang_source"].str.strip().eq("")
            out.loc[lang_source_mask, "lang_source"] = out.loc[lang_source_mask, "cache_lang_source"].map(_clean_optional_text)

            avail_mask = out["available_langs"].str.strip().eq("")
            out.loc[avail_mask, "available_langs"] = out.loc[avail_mask, "cache_available_languages"].map(_clean_optional_text)

            out = out.drop(columns=[c for c in out.columns if c.startswith("cache_")], errors="ignore")

    page_probe_mask = (
        out["nim_resource_uri"].str.strip().ne("")
        & (
            out["nim_title"].str.strip().eq("")
            | out["available_langs"].str.strip().eq("")
            | out["lang_source"].isin(["country_fallback", "default_en"])
        )
    )
    if page_probe_mask.any():
        session = requests.Session()
        session.headers.update({"User-Agent": "Mozilla/5.0"})
        page_rows: list[dict[str, str]] = []
        for idx, row in out.loc[page_probe_mask].iterrows():
            try:
                meta = fetch_nim_page_metadata(row, session=session, timeout_s=30)
            except Exception:
                continue
            langs2 = [str(lang or "").lower() for lang in meta.get("available_languages", []) if str(lang or "").strip()]
            title = _clean_optional_text(meta.get("title", ""))
            title_lang = _clean_optional_text(meta.get("title_lang", ""))
            page_rows.append(
                {
                    "row_index": idx,
                    "page_title": title,
                    "page_title_lang": title_lang,
                    "page_available_langs": ",".join(_uniq_keep_order(langs2)),
                }
            )
        if page_rows:
            page_df = pd.DataFrame(page_rows).drop_duplicates(subset=["row_index"], keep="first").set_index("row_index")
            title_mask = out["nim_title"].str.strip().eq("") & out.index.isin(page_df.index)
            out.loc[title_mask, "nim_title"] = out.loc[title_mask].index.map(lambda idx: page_df.at[idx, "page_title"] if idx in page_df.index else "")

            title_lang_mask = out["nim_title_lang"].str.strip().eq("") & out.index.isin(page_df.index)
            out.loc[title_lang_mask, "nim_title_lang"] = out.loc[title_lang_mask].index.map(lambda idx: page_df.at[idx, "page_title_lang"] if idx in page_df.index else "")

            avail_mask = out["available_langs"].str.strip().eq("") & out.index.isin(page_df.index)
            out.loc[avail_mask, "available_langs"] = out.loc[avail_mask].index.map(lambda idx: page_df.at[idx, "page_available_langs"] if idx in page_df.index else "")

            weak_lang_mask = out["lang_source"].isin(["country_fallback", "default_en"]) & out.index.isin(page_df.index)
            inferred_page_lang = out.loc[weak_lang_mask].index.map(
                lambda idx: (
                    page_df.at[idx, "page_title_lang"]
                    or (page_df.at[idx, "page_available_langs"].split(",")[0] if page_df.at[idx, "page_available_langs"] else "")
                )
                if idx in page_df.index
                else ""
            )
            inferred_page_lang = pd.Series(inferred_page_lang, index=out.loc[weak_lang_mask].index).map(_clean_optional_text)
            replace_lang_mask = weak_lang_mask & inferred_page_lang.astype(str).str.strip().ne("")
            out.loc[replace_lang_mask, "lang_detected"] = inferred_page_lang.loc[replace_lang_mask]
            out.loc[replace_lang_mask, "lang_source"] = "page_metadata"

    empty_expr_mask = out["available_expr_langs3"].str.strip().eq("") & out["available_langs"].str.strip().ne("")
    out.loc[empty_expr_mask, "available_expr_langs3"] = out.loc[empty_expr_mask, "available_langs"].map(
        lambda s: ",".join(_uniq_keep_order(_lang2_to_lang3(part) for part in str(s or "").split(",") if _lang2_to_lang3(part)))
    )

    out["nim_title"] = out["nim_title"].map(_clean_optional_text)
    out["nim_title_lang"] = out["nim_title_lang"].map(_clean_optional_text)
    out["lang_detected"] = out["lang_detected"].map(_clean_optional_text)
    out["lang_source"] = out["lang_source"].map(_clean_optional_text)
    out["available_langs"] = out["available_langs"].map(_clean_optional_text)
    return out


def get_national_transpositions_by_celex_ws(act_celex: str, *, page_size: int = 100, max_pages: int | None = None, search_language: str = "en", sleep_s: float = 0.2) -> pd.DataFrame:
    query = build_mne_expert_query(act_celex)
    all_rows: list[pd.DataFrame] = []
    page = 1
    while True:
        xml_bytes = eurlex_ws_doquery(query, page=page, page_size=page_size, search_language=search_language)
        page_df = parse_ws_nim_results(xml_bytes, act_celex=act_celex)
        if not page_df.empty:
            all_rows.append(page_df)
        if len(page_df) < page_size:
            break
        page += 1
        if max_pages is not None and page > max_pages:
            break
        if sleep_s:
            time.sleep(sleep_s)
    out = (
        pd.concat(all_rows, ignore_index=True).drop_duplicates(subset=["national_measure_id", "nim_celex"])
        if all_rows
        else pd.DataFrame(columns=[
            "nim_celex", "national_measure_id", "nim_date", "nim_title", "nim_title_lang",
            "available_expr_langs3", "member_state_iso3", "member_state_name", "eurlex_url",
        ])
    )
    out.insert(0, "act_celex", normalize_legal_act_celex(act_celex))
    return out


def _uniq_keep_order(xs: Iterable[str]) -> list[str]:
    seen: set[str] = set()
    out: list[str] = []
    for x in xs:
        token = str(x or "").strip()
        if not token or token in seen:
            continue
        seen.add(token)
        out.append(token)
    return out


def _nim_route_priority(route_name: str) -> int:
    token = str(route_name or "").strip()
    try:
        return NIM_TEXT_ROUTE_ORDER.index(token)
    except ValueError:
        return len(NIM_TEXT_ROUTE_ORDER) + 100


def _lang2_from_url(url: str) -> str:
    token = str(url or "").strip()
    if not token:
        return ""
    match = re.search(r"/legal-content/([A-Za-z]{2})/", token)
    if match:
        return match.group(1).lower()
    match = re.search(r"[?&](?:lang|locale)=([A-Za-z]{2})\b", token, flags=re.IGNORECASE)
    if match:
        return match.group(1).lower()
    return ""


def _lang2_to_lang3(lang2: str) -> str:
    token = str(lang2 or "").strip().lower()
    return LANG2_TO_LANG3.get(token, "")


def _safe_log_text(value: Any, max_len: int = 80) -> str:
    text = _clean_optional_text(value)
    if max_len > 0:
        text = text[:max_len]
    return text.encode("ascii", errors="replace").decode("ascii")


def _langs3_to_lang2_list(langs3: Any) -> list[str]:
    if langs3 is None or (isinstance(langs3, float) and pd.isna(langs3)):
        return []
    if isinstance(langs3, str):
        stripped = langs3.strip()
        if not stripped or stripped.lower() == "nan":
            return []
        parts = [part.strip().lower() for part in stripped.split(",") if part.strip()]
    elif isinstance(langs3, (list, tuple, set)):
        parts = [str(part).strip().lower() for part in langs3 if str(part).strip()]
    else:
        return []
    return _uniq_keep_order(ISO3_TO_EURLEX_LANG2.get(part, "") for part in parts)


def _guess_lang2s_from_member_state(ms_iso3: Any) -> list[str]:
    token = str(ms_iso3 or "").strip().upper()
    if not token:
        return []
    return list(MS_ISO3_TO_LANG2S.get(token, []))


def build_nim_try_langs(row: pd.Series, fallback: list[str] | None = None) -> list[str]:
    langs: list[str] = []
    langs.extend(_langs3_to_lang2_list(row.get("available_expr_langs3", "")))
    langs.extend(_guess_lang2s_from_member_state(row.get("member_state_iso3", "")))
    if fallback:
        langs.extend(str(lang or "").upper() for lang in fallback if str(lang or "").strip())
    langs = _uniq_keep_order(langs)
    if "EN" not in langs:
        langs.append("EN")
    return langs


def _lang_to_iso2_upper(lang: str | None) -> str:
    if not lang:
        return "EN"
    token = str(lang).strip().lower()
    if len(token) == 2:
        return token.upper()
    return ISO3_TO_EURLEX_LANG2.get(token, "EN")


def _safe_filename(value: str) -> str:
    text = re.sub(r"[^A-Za-z0-9._-]+", "_", str(value or "").strip())
    return text[:180] if text else "missing_id"


def _ensure_dir(path: Path) -> Path:
    path.mkdir(parents=True, exist_ok=True)
    return path


def _nim_cache_dirs(cache_dir: Path) -> tuple[Path, Path]:
    return _ensure_dir(cache_dir / "text_cache"), _ensure_dir(cache_dir / "html_cache")


def _nim_cache_key(row: pd.Series | dict[str, Any]) -> str:
    national_measure_id = str((row.get("national_measure_id") if isinstance(row, pd.Series) else row.get("national_measure_id")) or "").strip()
    nim_celex = str((row.get("nim_celex") if isinstance(row, pd.Series) else row.get("nim_celex")) or "").strip()
    member_state_iso3 = str((row.get("member_state_iso3") if isinstance(row, pd.Series) else row.get("member_state_iso3")) or "").strip()
    if national_measure_id:
        return _safe_filename(national_measure_id)
    if nim_celex and member_state_iso3:
        return _safe_filename(f"{nim_celex}_{member_state_iso3}")
    if nim_celex:
        return _safe_filename(nim_celex)
    return hashlib.sha1(repr(dict(row)).encode("utf-8")).hexdigest()[:24]


def _nim_text_path(row: pd.Series | dict[str, Any], cache_dir: Path) -> Path:
    text_cache_dir, _ = _nim_cache_dirs(cache_dir)
    return text_cache_dir / f"{_nim_cache_key(row)}.txt"


def _nim_html_path(row: pd.Series | dict[str, Any], cache_dir: Path) -> Path:
    _, html_cache_dir = _nim_cache_dirs(cache_dir)
    return html_cache_dir / f"{_nim_cache_key(row)}.html"


def _nim_cache_csv_path(cache_dir: Path) -> Path:
    return cache_dir / "nim_fulltext_cache.csv"


def _html_to_text_basic(html_text: str) -> str:
    soup = BeautifulSoup(html_text or "", "html.parser")
    for tag in soup(["script", "style", "noscript"]):
        tag.decompose()
    text = soup.get_text(separator=" ", strip=True)
    return re.sub(r"\s+", " ", text).strip()


_NOT_AVAILABLE_PATTERNS = [
    r"not available in (this|the) language",
    r"requested document is not available",
    r"page you requested cannot be found",
    r"document not found",
    r"could not be found",
    r"no html content",
]

def _looks_like_not_available(html_text: str) -> bool:
    lowered = (html_text or "").lower()
    if not lowered:
        return True
    for pat in _NOT_AVAILABLE_PATTERNS:
        if re.search(pat, lowered, flags=re.IGNORECASE):
            return True
    return len(lowered) < 300


def candidate_urls_for_nim_legal(nim_celex: str, *, lang: str = "EN", eurlex_url: str = "") -> list[tuple[str, str]]:
    celex = str(nim_celex or "").strip().upper()
    lang2 = _lang_to_iso2_upper(lang)
    uri_q = quote(f"CELEX:{celex}", safe="")
    candidates: list[tuple[str, str]] = []
    if eurlex_url:
        candidates.append(("eurlex_url", str(eurlex_url).strip()))
    candidates.extend(
        [
            ("legal-content-html", f"https://eur-lex.europa.eu/legal-content/{lang2}/TXT/HTML/?from={lang2}&uri={uri_q}"),
            ("legal-content-txt", f"https://eur-lex.europa.eu/legal-content/{lang2}/TXT/?uri={uri_q}"),
            ("legal-content-html-en", f"https://eur-lex.europa.eu/legal-content/EN/TXT/HTML/?uri={uri_q}"),
            ("lexuriserv", f"https://eur-lex.europa.eu/LexUriServ/LexUriServ.do?uri={quote(f'CELEX:{celex}:{lang2}:HTML', safe='')}"),
        ]
    )
    return candidates


def _nim_headers(route_name: str, lang2: str) -> dict[str, str]:
    headers = {
        "User-Agent": "Mozilla/5.0",
        "Accept-Language": lang2,
    }
    if route_name == "legal-content-txt":
        headers["Accept"] = "text/plain,text/html,application/xhtml+xml"
    else:
        headers["Accept"] = "text/html,application/xhtml+xml"
    return headers


def _looks_like_metadata_response(text: str, final_url: str = "", content_type: str = "") -> str:
    url = str(final_url or "").lower()
    ctype = str(content_type or "").lower()
    body = str(text or "")
    sample = body[:4000].lower()
    if "/rdf/" in url or "application/rdf+xml" in ctype or "<rdf:rdf" in sample:
        return "metadata_rdf"
    if sample.count("http://") + sample.count("https://") > 40 and len(sample) < 5000:
        return "metadata_uri_heavy"
    if "xmlns:rdf" in sample or "dcterms:" in sample:
        return "metadata_xml"
    return ""


def _xml_lang_to_lang2(value: str) -> str:
    token = str(value or "").strip().lower()
    if not token:
        return ""
    if len(token) == 2:
        return token
    return ISO3_TO_EURLEX_LANG2.get(token, "").lower()


def _nim_resource_url(row: pd.Series | dict[str, Any]) -> str:
    value = row if isinstance(row, dict) else row.to_dict()
    if str(value.get("nim_resource_uri", "") or "").strip():
        return str(value.get("nim_resource_uri", "")).strip()
    national_measure_id = str(value.get("national_measure_id", "") or "").strip()
    if national_measure_id:
        return f"http://publications.europa.eu/resource/nim/{national_measure_id}"
    return ""


def _extract_urls_from_text(text: str) -> list[str]:
    return _uniq_keep_order(re.findall(r"https?://[^\\s\"'<>]+", str(text or "")))


def extract_nim_available_languages(rdf_text: str) -> list[str]:
    try:
        root = ET.fromstring((rdf_text or "").encode("utf-8"))
    except Exception:
        try:
            root = ET.fromstring(rdf_text or "")
        except Exception:
            return []
    langs: list[str] = []
    for elem in root.iter():
        lang = elem.attrib.get("{http://www.w3.org/XML/1998/namespace}lang", "") if hasattr(elem, "attrib") else ""
        lang2 = _xml_lang_to_lang2(lang)
        if lang2:
            langs.append(lang2)
    return _uniq_keep_order(langs)


def extract_nim_page_links(rdf_text: str) -> dict[str, Any]:
    """Parse NIM RDF and extract title/language/link metadata for downstream route selection."""
    out: dict[str, Any] = {
        "title": "",
        "title_lang": "",
        "available_languages": [],
        "direct_text_links": [],
        "national_website_links": [],
        "national_website_eli_links": [],
        "machine_translation_links": [],
        "all_links": [],
    }
    try:
        root = ET.fromstring((rdf_text or "").encode("utf-8"))
    except Exception:
        try:
            root = ET.fromstring(rdf_text or "")
        except Exception:
            return out

    title_candidates: list[tuple[str, str]] = []
    links: list[dict[str, str]] = []
    for elem in root.iter():
        ln = _localname(getattr(elem, "tag", ""))
        text_value = _text(elem)
        xml_lang = ""
        if hasattr(elem, "attrib"):
            xml_lang = _xml_lang_to_lang2(elem.attrib.get("{http://www.w3.org/XML/1998/namespace}lang", ""))
        if ln in {"title", "work_title"} and text_value:
            title_candidates.append((text_value, xml_lang))
        urls = []
        if text_value.startswith("http://") or text_value.startswith("https://"):
            urls.append(text_value)
        for attr_value in getattr(elem, "attrib", {}).values():
            if isinstance(attr_value, str) and attr_value.startswith(("http://", "https://")):
                urls.append(attr_value)
        for url in _uniq_keep_order(urls):
            lower_url = url.lower()
            link_type = "other"
            source_format = "html"
            if lower_url.endswith(".pdf"):
                link_type = "direct_text_pdf"
                source_format = "pdf"
            elif lower_url.endswith(".docx"):
                link_type = "direct_text_docx"
                source_format = "docx"
            elif lower_url.endswith(".doc"):
                link_type = "direct_text_doc"
                source_format = "doc"
            elif any(ext in lower_url for ext in [".rtf", ".odt", ".txt"]) or re.search(r"[?&](?:format|download|file)=(pdf|docx|doc|rtf|txt)\b", lower_url):
                link_type = "direct_text_file"
                source_format = "binary"
            elif "machine" in lower_url and "translat" in lower_url:
                link_type = "machine_translation"
            elif "eli" in lower_url:
                link_type = "national_website_eli"
            elif "national_website" in ln.lower() or "website" in ln.lower():
                link_type = "national_website_eli" if "eli" in lower_url else "national_website"
            elif lower_url.startswith(("http://", "https://")) and "publications.europa.eu" not in lower_url and "w3.org" not in lower_url:
                link_type = "national_website_eli" if "eli" in lower_url else "national_website"
            links.append({"url": url, "link_type": link_type, "source_format": source_format, "lang": xml_lang})

    out["available_languages"] = extract_nim_available_languages(rdf_text)
    if title_candidates:
        out["title"], out["title_lang"] = title_candidates[0]
    links = [link for link in links if "publications.europa.eu/resource/authority" not in link["url"]]
    out["all_links"] = _uniq_keep_order(link["url"] for link in links)
    out["direct_text_links"] = sorted(
        [link for link in links if link["link_type"].startswith("direct_text_")],
        key=lambda link: (_nim_route_priority(str(link.get("link_type", ""))), str(link.get("url", ""))),
    )
    out["national_website_links"] = [link for link in links if link["link_type"] == "national_website"]
    out["national_website_eli_links"] = [link for link in links if link["link_type"] == "national_website_eli"]
    out["machine_translation_links"] = [link for link in links if link["link_type"] == "machine_translation"]
    return out


def extract_nim_direct_access_links(html_text: str, base_url: str) -> list[dict[str, str]]:
    soup = BeautifulSoup(html_text or "", "html.parser")
    links: list[dict[str, str]] = []
    from urllib.parse import urljoin

    for a in soup.find_all("a", href=True):
        href = urljoin(base_url, a.get("href"))
        label = a.get_text(" ", strip=True)
        lower = href.lower()
        if lower.endswith(".pdf"):
            link_type = "direct_text_pdf"
            source_format = "pdf"
        elif lower.endswith(".docx"):
            link_type = "direct_text_docx"
            source_format = "docx"
        elif lower.endswith(".doc"):
            link_type = "direct_text_doc"
            source_format = "doc"
        elif any(ext in lower for ext in [".rtf", ".odt", ".txt"]) or "download" in lower or "file=" in lower:
            link_type = "direct_text_file"
            source_format = "binary"
        elif "eli" in lower:
            link_type = "national_website_eli"
            source_format = "html"
        elif ("machine" in lower and "translat" in lower) or "machine translation" in label.lower():
            link_type = "machine_translation"
            source_format = "html"
        else:
            link_type = "national_website"
            source_format = "html"
        links.append(
            {
                "url": href,
                "link_type": link_type,
                "source_format": source_format,
                "label": label,
                "lang": _lang2_from_url(href),
            }
        )
    return sorted(
        links,
        key=lambda link: (_nim_route_priority(str(link.get("link_type", ""))), str(link.get("url", ""))),
    )


def fetch_nim_page_metadata(
    row: pd.Series,
    *,
    session: requests.Session | None = None,
    timeout_s: int = 30,
) -> dict[str, Any]:
    sess = session or requests.Session()
    resource_url = _nim_resource_url(row)
    if not resource_url:
        return {"nim_page_url": "", "nim_rdf_url": "", "title": "", "title_lang": "", "available_languages": [], "page_links": {}, "rdf_text": "", "retrieval_status": 0}
    response = sess.get(resource_url, headers={"User-Agent": "Mozilla/5.0", "Accept": "application/rdf+xml,application/xml,text/html;q=0.9,*/*;q=0.8"}, timeout=timeout_s, allow_redirects=True)
    rdf_text = response.text or ""
    page_links = extract_nim_page_links(rdf_text)
    return {
        "nim_page_url": resource_url,
        "nim_rdf_url": str(response.url),
        "title": str(page_links.get("title", "") or ""),
        "title_lang": str(page_links.get("title_lang", "") or ""),
        "available_languages": list(page_links.get("available_languages", []) or []),
        "page_links": page_links,
        "rdf_text": rdf_text,
        "retrieval_status": int(response.status_code),
    }


def _extract_text_from_pdf_bytes(pdf_bytes: bytes) -> str:
    from io import BytesIO
    from pypdf import PdfReader

    try:
        reader = PdfReader(BytesIO(pdf_bytes))
        parts = [page.extract_text() or "" for page in reader.pages]
        return re.sub(r"\s+", " ", " ".join(parts)).strip()
    except Exception:
        return ""


def _infer_source_format(url: str, content_type: str = "") -> str:
    lower_url = str(url or "").lower()
    lower_ctype = str(content_type or "").lower()
    if lower_url.endswith(".pdf") or "application/pdf" in lower_ctype:
        return "pdf"
    if lower_url.endswith(".docx") or "wordprocessingml" in lower_ctype:
        return "docx"
    if lower_url.endswith(".doc") or "msword" in lower_ctype:
        return "doc"
    if "html" in lower_ctype or lower_url.endswith((".htm", ".html")):
        return "html"
    return "binary"


def _fetch_text_from_candidate(
    candidate: dict[str, str],
    *,
    session: requests.Session,
    timeout: tuple[int, int],
    retries: int,
    min_interval_s: float,
    file_cache_dir: Path,
    verbose: bool,
) -> dict[str, Any]:
    url = candidate["url"]
    route_name = candidate["link_type"]
    last_error = ""
    for attempt in range(retries + 1):
        try:
            if min_interval_s > 0:
                time.sleep(min_interval_s)
            response = session.get(url, headers={"User-Agent": "Mozilla/5.0"}, timeout=timeout, allow_redirects=True)
            status = int(response.status_code)
            if status in (429, 500, 502, 503, 504):
                if verbose:
                    print(f"[NIM TEXT] retry {attempt + 1} route={route_name} after HTTP {status}", flush=True)
                time.sleep(1.7 ** attempt)
                continue
            if status == 202:
                return {"status": status, "error": "HTTP 202", "url": str(response.url), "route": route_name, "text": "", "content_type": response.headers.get("Content-Type", ""), "source_format": _infer_source_format(str(response.url), response.headers.get('Content-Type',''))}
            if status != 200:
                return {"status": status, "error": f"HTTP {status}", "url": str(response.url), "route": route_name, "text": "", "content_type": response.headers.get("Content-Type", ""), "source_format": _infer_source_format(str(response.url), response.headers.get('Content-Type',''))}

            content_type = response.headers.get("Content-Type", "")
            source_format = _infer_source_format(str(response.url), content_type)
            if source_format == "pdf":
                pdf_bytes = response.content
                cache_name = hashlib.sha1(str(response.url).encode("utf-8")).hexdigest()[:20] + ".pdf"
                (file_cache_dir / cache_name).write_bytes(pdf_bytes)
                text = _extract_text_from_pdf_bytes(pdf_bytes)
                return {"status": status, "error": "" if text else "empty_text", "url": str(response.url), "route": route_name, "text": text, "content_type": content_type, "source_format": "pdf"}
            if source_format in {"doc", "docx"}:
                suffix = ".docx" if source_format == "docx" else ".doc"
                cache_name = hashlib.sha1(str(response.url).encode("utf-8")).hexdigest()[:20] + suffix
                (file_cache_dir / cache_name).write_bytes(response.content)
                return {"status": status, "error": f"{source_format}_downloaded_not_extracted", "url": str(response.url), "route": route_name, "text": "", "content_type": content_type, "source_format": source_format}

            html = response.text or ""
            text = _html_to_text_basic(html)
            extra_links = extract_nim_direct_access_links(html, str(response.url))
            return {"status": status, "error": "" if text else "empty_text", "url": str(response.url), "route": route_name, "text": text, "content_type": content_type, "source_format": source_format, "html": html, "extra_links": extra_links}
        except requests.RequestException as exc:
            last_error = str(exc)
            if attempt < retries:
                time.sleep(1.7 ** attempt)
                continue
            break
    return {"status": 0, "error": last_error or "request_failed", "url": url, "route": route_name, "text": "", "content_type": "", "source_format": candidate.get("source_format", "")}


def _build_nim_route_candidates(page_meta: dict[str, Any]) -> list[dict[str, str]]:
    links = page_meta.get("page_links", {}) or {}
    candidates: list[dict[str, str]] = []
    for key in ["direct_text_links", "national_website_eli_links", "national_website_links", "machine_translation_links"]:
        for link in links.get(key, []):
            candidates.append({
                "url": link["url"],
                "link_type": link["link_type"],
                "source_format": link.get("source_format", "html"),
                "lang": link.get("lang", ""),
            })
    candidates = sorted(
        candidates,
        key=lambda candidate: (
            _nim_route_priority(str(candidate.get("link_type", ""))),
            str(candidate.get("url", "")),
        ),
    )
    deduped: list[dict[str, str]] = []
    seen: set[str] = set()
    for candidate in candidates:
        url = str(candidate.get("url", "") or "").strip()
        if not url or url in seen:
            continue
        seen.add(url)
        deduped.append(candidate)
    return deduped


def detect_nim_language(row: pd.Series | dict[str, Any]) -> tuple[str, str]:
    """Detect NIM language with source priority: URL, metadata, member-state fallback, then EN."""
    value = row if isinstance(row, dict) else row.to_dict()
    eurlex_url = str(value.get("eurlex_url", "") or "").strip()
    m = re.search(r"/legal-content/([A-Za-z]{2})/", eurlex_url)
    url_lang = m.group(1).lower() if m else ""
    if url_lang and url_lang != "en":
        return url_lang, "url"

    available_expr_langs3 = value.get("available_expr_langs3", "")
    lang2s = _langs3_to_lang2_list(available_expr_langs3)
    if lang2s:
        return lang2s[0].lower(), "metadata"

    title_lang_raw = value.get("nim_title_lang", "")
    title_lang = "" if title_lang_raw is None or (isinstance(title_lang_raw, float) and pd.isna(title_lang_raw)) else str(title_lang_raw).strip().lower()
    if title_lang == "nan":
        title_lang = ""
    if title_lang:
        if len(title_lang) == 2:
            return title_lang, "metadata"
        if len(title_lang) == 3:
            return ISO3_TO_EURLEX_LANG2.get(title_lang, "EN").lower(), "metadata"

    member_state_iso3 = str(value.get("member_state_iso3", "") or "").strip().upper()
    if member_state_iso3 in OFFICIAL_EU_LANGS_BY_MS_ISO3 and OFFICIAL_EU_LANGS_BY_MS_ISO3[member_state_iso3]:
        return OFFICIAL_EU_LANGS_BY_MS_ISO3[member_state_iso3][0], "country_fallback"

    if url_lang:
        return url_lang, "url"

    return "en", "default_en"


def extract_nim_title_from_html(html_text: str, fallback_title: str = "") -> str:
    """Extract a human-readable NIM title from HTML, preferring body headings over boilerplate."""
    html_text = str(html_text or "")
    fallback_title = str(fallback_title or "").strip()
    if not html_text.strip():
        return fallback_title

    soup = BeautifulSoup(html_text, "html.parser")

    candidates: list[str] = []

    page_title = soup.title.get_text(" ", strip=True) if soup.title else ""
    if page_title:
        cleaned = re.sub(r"\s*[-|]\s*EUR-Lex.*$", "", page_title, flags=re.IGNORECASE).strip()
        cleaned = re.sub(r"^\s*EUR-Lex\s*-\s*", "", cleaned, flags=re.IGNORECASE).strip()
        if cleaned:
            candidates.append(cleaned)

    for selector in ["h1", "h2", ".eli-main-title", ".title", "#title", ".document-title"]:
        for node in soup.select(selector):
            text = node.get_text(" ", strip=True)
            if text and len(text) > 20:
                candidates.append(text)

    for meta_name in ["DC.title", "citation_title", "title"]:
        node = soup.find("meta", attrs={"name": meta_name}) or soup.find("meta", attrs={"property": meta_name})
        if node and node.get("content"):
            text = str(node.get("content") or "").strip()
            if text:
                candidates.append(text)

    candidates = _uniq_keep_order(candidates)
    for candidate in candidates:
        candidate = re.sub(r"\s+", " ", candidate).strip()
        lowered = candidate.lower()
        if len(candidate) < 15:
            continue
        if "eur-lex" in lowered and len(candidate) < 40:
            continue
        if lowered in {"document", "text", "html", "txt"}:
            continue
        return candidate

    return fallback_title


def fetch_nim_document_text(
    nim_celex: str,
    *,
    try_langs2: list[str],
    eurlex_url: str = "",
    timeout: tuple[int, int] = (15, 90),
    retries: int = 3,
    min_interval_s: float = 0.5,
    session: requests.Session | None = None,
    verbose: bool = False,
    trace_routes: bool = False,
    success_min_chars: int = 500,
) -> tuple[str, dict[str, Any]]:
    sess = session or requests.Session()
    last_meta: dict[str, Any] = {
        "fetch_status": 0,
        "url_fetch": "",
        "error": "",
        "lang_used": "",
        "route_used": "",
        "content_type": "",
        "full_text_raw": "",
        "title_extracted": "",
        "source_format": "",
        "available_languages": [],
        "page_links": {},
        "title_from_page": "",
        "title_lang_from_page": "",
        "text_route_used": "",
        "machine_translation_links": [],
        "national_website_links": [],
        "national_website_eli_links": [],
        "direct_text_links": [],
    }
    t0 = time.time()

    page_meta = fetch_nim_page_metadata(
        pd.Series({
            "nim_celex": nim_celex,
            "national_measure_id": re.sub(r"^.*_", "", nim_celex) if "_" in nim_celex else "",
            "eurlex_url": eurlex_url,
        }),
        session=sess,
        timeout_s=timeout[1] if isinstance(timeout, tuple) else int(timeout),
    )
    last_meta.update(
        {
            "available_languages": page_meta.get("available_languages", []),
            "page_links": page_meta.get("page_links", {}),
            "title_from_page": page_meta.get("title", ""),
            "title_lang_from_page": page_meta.get("title_lang", ""),
            "direct_text_links": [link.get("url", "") for link in page_meta.get("page_links", {}).get("direct_text_links", [])],
            "national_website_links": [link.get("url", "") for link in page_meta.get("page_links", {}).get("national_website_links", [])],
            "national_website_eli_links": [link.get("url", "") for link in page_meta.get("page_links", {}).get("national_website_eli_links", [])],
            "machine_translation_links": [link.get("url", "") for link in page_meta.get("page_links", {}).get("machine_translation_links", [])],
        }
    )

    file_cache_dir = _ensure_dir(Path("outputs/nim_fulltext_cache/file_cache"))
    route_candidates = _build_nim_route_candidates(page_meta)
    seen_urls: set[str] = set()
    while route_candidates:
        candidate = route_candidates.pop(0)
        if candidate["url"] in seen_urls:
            continue
        seen_urls.add(candidate["url"])
        if trace_routes:
            print(f"[NIM TEXT] CELEX={nim_celex} route={candidate['link_type']} url={candidate['url']}", flush=True)
        candidate_result = _fetch_text_from_candidate(
            candidate,
            session=sess,
            timeout=timeout,
            retries=retries,
            min_interval_s=min_interval_s,
            file_cache_dir=file_cache_dir,
            verbose=verbose,
        )
        last_meta.update(
            {
                "fetch_status": int(candidate_result.get("status", 0) or 0),
                "url_fetch": str(candidate_result.get("url", "") or candidate["url"]),
                "error": str(candidate_result.get("error", "") or ""),
                "route_used": str(candidate_result.get("route", "") or candidate["link_type"]),
                "text_route_used": str(candidate_result.get("route", "") or candidate["link_type"]),
                "content_type": str(candidate_result.get("content_type", "") or ""),
                "source_format": str(candidate_result.get("source_format", "") or candidate.get("source_format", "")),
                "fetch_seconds": round(time.time() - t0, 2),
            }
        )
        html = str(candidate_result.get("html", "") or "")
        if html:
            last_meta["full_text_raw"] = html
            if page_meta.get("title", ""):
                last_meta["title_extracted"] = page_meta.get("title", "")
            extra_links = candidate_result.get("extra_links", []) or []
            extra_links = sorted(
                extra_links,
                key=lambda link: (_nim_route_priority(str(link.get("link_type", ""))), str(link.get("url", ""))),
            )
            for link in reversed(extra_links):
                if link.get("url") and link["url"] not in seen_urls:
                    route_candidates.insert(
                        0,
                        {
                            k: str(v)
                            for k, v in link.items()
                            if k in {"url", "link_type", "source_format", "lang"}
                        },
                    )
        text = str(candidate_result.get("text", "") or "")
        if len(text) >= success_min_chars:
            final_lang = str((candidate.get("lang", "") or (try_langs2[0] if try_langs2 else ""))).lower()
            last_meta["lang_used"] = final_lang
            return text, last_meta
        if candidate_result.get("status") == 202:
            continue

    for lang2 in try_langs2:
        for route_name, url in candidate_urls_for_nim_legal(nim_celex, lang=lang2, eurlex_url=eurlex_url):
            if trace_routes:
                print(f"[NIM TEXT] CELEX={nim_celex} route=fallback_generic/{route_name} lang={lang2}", flush=True)
            if min_interval_s > 0:
                time.sleep(min_interval_s)
            headers = _nim_headers(route_name, lang2)
            response = None
            error_text = ""
            for attempt in range(retries + 1):
                try:
                    response = sess.get(url, headers=headers, allow_redirects=True, timeout=timeout)
                    status = response.status_code
                    if status in (429, 500, 502, 503, 504):
                        if verbose:
                            print(f"[NIM TEXT] retry {attempt + 1} CELEX={nim_celex} route={route_name} after HTTP {status}", flush=True)
                        time.sleep(1.7 ** attempt)
                        continue
                    break
                except requests.RequestException as exc:
                    error_text = str(exc)
                    if attempt < retries:
                        time.sleep(1.7 ** attempt)
                        continue
                    response = None
                    break
            if response is None:
                route_token = f"fallback_generic/{route_name}"
                last_meta.update({"fetch_status": 0, "url_fetch": url, "error": error_text or "request_failed", "lang_used": lang2.lower(), "route_used": route_token, "text_route_used": route_token, "fetch_seconds": round(time.time() - t0, 2)})
                continue
            status = int(response.status_code)
            final_url = str(response.url)
            content_type = str(response.headers.get("Content-Type", ""))
            html = response.text or ""
            route_token = f"fallback_generic/{route_name}"
            last_meta.update({"fetch_status": status, "url_fetch": final_url, "lang_used": lang2.lower(), "route_used": route_token, "text_route_used": route_token, "content_type": content_type, "fetch_seconds": round(time.time() - t0, 2)})
            if status != 200:
                last_meta["error"] = f"HTTP {status}"
                if status == 202:
                    continue
                continue
            if _looks_like_not_available(html):
                last_meta.update({"error": "not_available", "full_text_raw": html})
                continue
            metadata_reason = _looks_like_metadata_response(html, final_url=final_url, content_type=content_type)
            if metadata_reason:
                last_meta.update({"error": metadata_reason, "full_text_raw": html})
                continue
            text = _html_to_text_basic(html)
            if len(text) < success_min_chars:
                last_meta.update({"error": "empty_text" if not text else "short_text", "full_text_raw": html})
                continue
            last_meta.update({"error": "", "full_text_raw": html})
            return text, last_meta

    last_meta.setdefault("fetch_seconds", round(time.time() - t0, 2))
    return "", last_meta


def fetch_nim_fulltext_for_row(
    row: pd.Series,
    *,
    cache_dir: Path,
    use_cache: bool = True,
    timeout: tuple[int, int] = (15, 90),
    retries: int = 3,
    min_interval_s: float = 0.5,
    session: requests.Session | None = None,
    verbose: bool = True,
    trace_routes: bool = False,
    progress_label: str = "",
    success_min_chars: int = 500,
) -> dict[str, Any]:
    def _clean_optional_text(value: Any) -> str:
        if value is None or (isinstance(value, float) and pd.isna(value)):
            return ""
        text = str(value).strip()
        return "" if text.lower() == "nan" else text

    text_path = _nim_text_path(row, cache_dir)
    html_path = _nim_html_path(row, cache_dir)
    lang_detected, lang_source = detect_nim_language(row)
    fallback_langs = build_nim_try_langs(row)
    try_langs2 = _uniq_keep_order([lang_detected.upper(), *fallback_langs])
    nim_celex = str(row.get("nim_celex", "") or "").strip()
    national_measure_id = str(row.get("national_measure_id", "") or "").strip()
    fallback_title = _clean_optional_text(row.get("nim_title", ""))
    if use_cache and text_path.exists() and text_path.stat().st_size > 0:
        text_clean = text_path.read_text(encoding="utf-8", errors="replace")
        return {
            "celex": str(row.get("celex", "") or ""),
            "nim_celex": nim_celex,
            "national_measure_id": national_measure_id,
            "member_state_iso3": str(row.get("member_state_iso3", "") or ""),
            "member_state_name": str(row.get("member_state_name", "") or ""),
            "nim_title": fallback_title,
            "nim_title_notice": _clean_optional_text(row.get("nim_title_notice", fallback_title)),
            "nim_title_lang": _clean_optional_text(row.get("nim_title_lang", "")),
            "eurlex_url": str(row.get("eurlex_url", "") or ""),
            "available_expr_langs3": _clean_optional_text(row.get("available_expr_langs3", "")),
            "available_langs": _clean_optional_text(row.get("available_langs", "")),
            "nim_resource_uri": _clean_optional_text(row.get("nim_resource_uri", "")),
            "text_source_url": "CACHE",
            "full_text_raw": "",
            "full_text_clean": text_clean,
            "text_len": len(text_clean),
            "retrieval_status": 200,
            "retrieval_error": "",
            "fetch_seconds": 0.0,
            "fetched_from_cache": True,
            "lang": try_langs2[0] if try_langs2 else "EN",
            "lang_detected": lang_detected,
            "lang_source": lang_source,
            "text_path": str(text_path),
            "html_path": "",
            "route_used": "CACHE",
            "text_route_used": "CACHE",
            "content_type": "",
            "source_url": "CACHE",
            "source_format": "",
            "available_languages": _clean_optional_text(row.get("available_langs", "")) or ",".join(try_langs2).lower(),
            "page_title": fallback_title,
            "page_title_lang": lang_detected,
            "cache_key": _nim_cache_key(row),
        }

    text, meta = fetch_nim_document_text(
        nim_celex,
        try_langs2=try_langs2,
        eurlex_url=str(row.get("eurlex_url", "") or ""),
        timeout=timeout,
        retries=retries,
        min_interval_s=min_interval_s,
        session=session,
        verbose=verbose,
        trace_routes=trace_routes,
        success_min_chars=success_min_chars,
    )

    full_text_raw = str(meta.get("full_text_raw", "") or "")
    extracted_title = extract_nim_title_from_html(full_text_raw, fallback_title=fallback_title)
    final_lang = str(meta.get("lang_used", "") or lang_detected or "").lower()
    final_lang_source = lang_source if final_lang == str(lang_detected or "").lower() else "retry_sequence"
    if full_text_raw:
        html_path.write_text(full_text_raw, encoding="utf-8", errors="replace")
    if text:
        text_path.write_text(text, encoding="utf-8", errors="replace")

    result = {
        "celex": str(row.get("celex", "") or ""),
        "nim_celex": nim_celex,
        "national_measure_id": national_measure_id,
        "member_state_iso3": str(row.get("member_state_iso3", "") or ""),
        "member_state_name": str(row.get("member_state_name", "") or ""),
        "nim_title": _clean_optional_text(extracted_title or meta.get("title_from_page", "") or fallback_title),
        "nim_title_notice": _clean_optional_text(row.get("nim_title_notice", fallback_title)),
        "nim_title_lang": _clean_optional_text(row.get("nim_title_lang", "") or meta.get("title_lang_from_page", "")),
        "eurlex_url": str(row.get("eurlex_url", "") or ""),
        "available_expr_langs3": _clean_optional_text(row.get("available_expr_langs3", "")),
        "available_langs": _clean_optional_text(row.get("available_langs", "")),
        "nim_resource_uri": _clean_optional_text(row.get("nim_resource_uri", "")),
        "text_source_url": str(meta.get("url_fetch", "") or ""),
        "source_url": str(meta.get("url_fetch", "") or ""),
        "full_text_raw": full_text_raw,
        "full_text_clean": text,
        "text_len": len(text),
        "retrieval_status": int(meta.get("fetch_status", 0) or 0),
        "retrieval_error": str(meta.get("error", "") or ""),
        "fetch_seconds": float(meta.get("fetch_seconds", 0.0) or 0.0),
        "fetched_from_cache": False,
        "lang": str(meta.get("lang_used", "") or "").lower(),
        "lang_detected": final_lang,
        "lang_source": final_lang_source,
        "text_path": str(text_path) if text else "",
        "html_path": str(html_path) if full_text_raw else "",
        "route_used": str(meta.get("route_used", "") or ""),
        "text_route_used": str(meta.get("text_route_used", "") or meta.get("route_used", "") or ""),
        "content_type": str(meta.get("content_type", "") or ""),
        "source_format": str(meta.get("source_format", "") or ""),
        "available_languages": ",".join(meta.get("available_languages", []) or []),
        "page_title": str(meta.get("title_from_page", "") or ""),
        "page_title_lang": str(meta.get("title_lang_from_page", "") or ""),
        "cache_key": _nim_cache_key(row),
    }
    return result


def _normalize_nim_fulltext_cache_frame(df: pd.DataFrame) -> pd.DataFrame:
    columns = [
        "cache_key", "celex", "nim_celex", "national_measure_id", "member_state_iso3", "member_state_name",
        "nim_title", "nim_title_notice", "nim_title_lang", "eurlex_url", "nim_resource_uri", "available_expr_langs3",
        "lang", "lang_detected", "lang_source", "available_languages",
        "page_title", "page_title_lang", "route_used", "text_route_used", "source_format", "text", "source_url",
        "retrieval_status", "retrieval_error", "text_len", "timestamp",
    ]
    if df.empty:
        return pd.DataFrame(columns=columns)
    out = df.copy()
    for col in columns:
        if col not in out.columns:
            out[col] = ""
    for col in ["retrieval_status", "text_len"]:
        out[col] = pd.to_numeric(out[col], errors="coerce").fillna(0).astype(int)
    for col in [c for c in columns if c not in {"retrieval_status", "text_len"}]:
        out[col] = out[col].fillna("").astype(str)
    if "text_len" in out.columns:
        mask = out["text_len"].le(0) & out["text"].astype(str).str.len().gt(0)
        out.loc[mask, "text_len"] = out.loc[mask, "text"].astype(str).str.len()
    return out[columns]


def load_nim_fulltext_cache_table(cache_dir: Path) -> pd.DataFrame:
    path = _nim_cache_csv_path(cache_dir)
    if not path.exists():
        return _normalize_nim_fulltext_cache_frame(pd.DataFrame())
    return _normalize_nim_fulltext_cache_frame(pd.read_csv(path, low_memory=False))


def _nim_cache_row_from_result(result: dict[str, Any]) -> dict[str, Any]:
    return {
        "cache_key": str(result.get("cache_key", "") or ""),
        "celex": str(result.get("celex", "") or ""),
        "nim_celex": str(result.get("nim_celex", "") or ""),
        "national_measure_id": str(result.get("national_measure_id", "") or ""),
        "member_state_iso3": str(result.get("member_state_iso3", "") or ""),
        "member_state_name": str(result.get("member_state_name", "") or ""),
        "nim_title": str(result.get("nim_title", "") or ""),
        "nim_title_notice": str(result.get("nim_title_notice", "") or ""),
        "nim_title_lang": str(result.get("nim_title_lang", "") or ""),
        "eurlex_url": str(result.get("eurlex_url", "") or ""),
        "nim_resource_uri": str(result.get("nim_resource_uri", "") or ""),
        "available_expr_langs3": str(result.get("available_expr_langs3", "") or ""),
        "lang": str(result.get("lang", "") or "").lower(),
        "lang_detected": str(result.get("lang_detected", "") or "").lower(),
        "lang_source": str(result.get("lang_source", "") or ""),
        "available_languages": str(result.get("available_languages", "") or ""),
        "page_title": str(result.get("page_title", "") or ""),
        "page_title_lang": str(result.get("page_title_lang", "") or ""),
        "route_used": str(result.get("route_used", "") or ""),
        "text_route_used": str(result.get("text_route_used", "") or ""),
        "source_format": str(result.get("source_format", "") or ""),
        "text": str(result.get("full_text_clean", "") or ""),
        "source_url": str(result.get("text_source_url", "") or ""),
        "retrieval_status": int(result.get("retrieval_status", 0) or 0),
        "retrieval_error": str(result.get("retrieval_error", "") or ""),
        "text_len": int(result.get("text_len", 0) or 0),
        "timestamp": datetime.now(timezone.utc).isoformat(),
    }


def merge_and_save_nim_fulltext_cache(cache_dir: Path, results: Iterable[dict[str, Any]]) -> Path:
    path = _nim_cache_csv_path(cache_dir)
    existing = load_nim_fulltext_cache_table(cache_dir)
    new_df = _normalize_nim_fulltext_cache_frame(pd.DataFrame([_nim_cache_row_from_result(r) for r in results]))
    merged = pd.concat([existing, new_df], ignore_index=True)
    merged = (
        merged.sort_values(["cache_key", "timestamp", "text_len"])
        .drop_duplicates(subset=["cache_key", "lang"], keep="last")
        .reset_index(drop=True)
    )
    path.parent.mkdir(parents=True, exist_ok=True)
    merged.to_csv(path, index=False)
    return path


def rebuild_nim_fulltext_cache_state_from_files(cache_dir: Path) -> pd.DataFrame:
    text_cache_dir, _ = _nim_cache_dirs(cache_dir)
    rows: list[dict[str, Any]] = []
    for path in sorted(text_cache_dir.glob("*.txt")):
        try:
            text = path.read_text(encoding="utf-8", errors="replace")
        except Exception:
            text = ""
        rows.append({"cache_key": path.stem, "file_text_len": len(text), "text_path": str(path), "file_exists": True})
    return pd.DataFrame(rows, columns=["cache_key", "file_text_len", "text_path", "file_exists"])


def summarize_nim_fulltext_cache_state(
    cache_dir: Path,
    nim_df: pd.DataFrame,
    *,
    success_min_chars: int = 500,
) -> tuple[pd.DataFrame, pd.DataFrame]:
    cache_df = load_nim_fulltext_cache_table(cache_dir)
    file_df = rebuild_nim_fulltext_cache_state_from_files(cache_dir)
    if nim_df.empty:
        return pd.DataFrame(), cache_df
    doc_df = nim_df.copy()
    doc_df["cache_key"] = doc_df.apply(_nim_cache_key, axis=1)
    if cache_df.empty:
        cache_group = pd.DataFrame(columns=["cache_key", "lang", "retrieval_status", "retrieval_error", "text_len", "source_url", "timestamp"])
    else:
        cache_group = (
            cache_df.sort_values(["cache_key", "timestamp"])
            .groupby("cache_key", as_index=False)
            .agg(
                lang=("lang", "last"),
                retrieval_status=("retrieval_status", "last"),
                retrieval_error=("retrieval_error", "last"),
                text_len=("text_len", "max"),
                source_url=("source_url", "last"),
                timestamp=("timestamp", "last"),
            )
        )
    merged = doc_df[["cache_key", "nim_celex", "national_measure_id", "member_state_iso3", "member_state_name"]].drop_duplicates().merge(cache_group, on="cache_key", how="left").merge(file_df, on="cache_key", how="left")
    merged["file_exists"] = merged.get("file_exists", False).where(merged.get("file_exists", False).notna(), False).astype(bool)
    merged["file_text_len"] = pd.to_numeric(merged.get("file_text_len", 0), errors="coerce").fillna(0).astype(int)
    merged["text_len"] = pd.to_numeric(merged.get("text_len", 0), errors="coerce").fillna(0).astype(int)
    merged["retrieval_status"] = pd.to_numeric(merged.get("retrieval_status", 0), errors="coerce").fillna(0).astype(int)
    merged["retrieval_error"] = merged.get("retrieval_error", "").fillna("").astype(str)
    merged["cache_state"] = "pending"
    merged.loc[merged["file_exists"] & merged["file_text_len"].ge(success_min_chars), "cache_state"] = "successful"
    merged.loc[merged["cache_state"].eq("pending") & merged["text_len"].ge(success_min_chars), "cache_state"] = "successful"
    merged.loc[merged["cache_state"].eq("pending") & (merged["retrieval_status"].gt(0) | merged["retrieval_error"].ne("")), "cache_state"] = "failed"
    return merged, cache_df


def batch_fetch_nim_fulltext(
    nim_df: pd.DataFrame,
    *,
    cache_dir: Path,
    use_cache: bool = True,
    timeout: tuple[int, int] = (15, 90),
    retries: int = 3,
    min_interval_s: float = 0.5,
    max_rows: int | None = None,
    session: requests.Session | None = None,
    verbose: bool = True,
    trace_routes: bool = False,
    resume: bool = True,
    retry_failures: bool = True,
    progress_every: int = 10,
    cache_every: int = 50,
    success_min_chars: int = 500,
) -> pd.DataFrame:
    if nim_df.empty:
        return pd.DataFrame()
    work_df = nim_df.copy()
    work_df["cache_key"] = work_df.apply(_nim_cache_key, axis=1)
    if max_rows is not None:
        work_df = work_df.head(max_rows).copy()

    cache_state_df, _ = summarize_nim_fulltext_cache_state(cache_dir, work_df, success_min_chars=success_min_chars)
    if resume:
        successful_keys = set(cache_state_df.loc[cache_state_df["cache_state"].eq("successful"), "cache_key"].astype(str))
        failed_keys = set(cache_state_df.loc[cache_state_df["cache_state"].eq("failed"), "cache_key"].astype(str))
        keep_mask = ~work_df["cache_key"].astype(str).isin(successful_keys)
        if not retry_failures:
            keep_mask &= ~work_df["cache_key"].astype(str).isin(failed_keys)
        work_df = work_df.loc[keep_mask].copy()

    if verbose:
        print("=== NIM FULLTEXT RESUME STATE ===", flush=True)
        print(f"Total input rows: {len(nim_df)}", flush=True)
        print(f"Already successful: {int(cache_state_df['cache_state'].eq('successful').sum())}", flush=True)
        print(f"Previously failed: {int(cache_state_df['cache_state'].eq('failed').sum())}", flush=True)
        print(f"Still pending: {int(cache_state_df['cache_state'].eq('pending').sum())}", flush=True)
        print(f"Run set size: {len(work_df)}", flush=True)
        print(f"Retry failures: {retry_failures}", flush=True)

    results: list[dict[str, Any]] = []
    n_success = 0
    n_failed = 0
    total = len(work_df)
    failure_counts: dict[str, int] = {}
    route_counts: dict[str, int] = {}
    route_success_counts: dict[str, int] = {}
    route_failure_counts: dict[str, int] = {}
    sess = session or requests.Session()
    for idx, (_, row) in enumerate(work_df.iterrows(), start=1):
        result = fetch_nim_fulltext_for_row(
            row,
            cache_dir=cache_dir,
            use_cache=use_cache,
            timeout=timeout,
            retries=retries,
            min_interval_s=min_interval_s,
            session=sess,
            verbose=verbose,
            trace_routes=trace_routes,
            progress_label=f"{idx}/{total}",
            success_min_chars=success_min_chars,
        )
        results.append(result)
        text_len = int(result.get("text_len", 0) or 0)
        nim_celex = str(result.get("nim_celex", "") or "")
        member_state = str(result.get("member_state_iso3", "") or "")
        route_key = str(result.get("text_route_used", "") or result.get("route_used", "") or "unknown")
        route_counts[route_key] = route_counts.get(route_key, 0) + 1
        if text_len >= success_min_chars:
            n_success += 1
            route_success_counts[route_key] = route_success_counts.get(route_key, 0) + 1
            if verbose:
                print(
                    f"[NIM TEXT] {idx}/{total} NIM={nim_celex} state={member_state} route={route_key} "
                    f"format={str(result.get('source_format', '') or '')} status={int(result.get('retrieval_status', 0) or 0)} "
                    f"title=\"{_safe_log_text(result.get('nim_title', ''))}\" SUCCESS length={text_len}",
                    flush=True,
                )
        else:
            n_failed += 1
            failure = str(result.get("retrieval_error", "") or f"HTTP {result.get('retrieval_status', 0)}").strip() or "unknown"
            failure_counts[failure] = failure_counts.get(failure, 0) + 1
            route_failure_counts[route_key] = route_failure_counts.get(route_key, 0) + 1
            if verbose:
                print(
                    f"[NIM TEXT] {idx}/{total} NIM={nim_celex} state={member_state} route={route_key} "
                    f"format={str(result.get('source_format', '') or '')} status={int(result.get('retrieval_status', 0) or 0)} "
                    f"title=\"{_safe_log_text(result.get('nim_title', ''))}\" FAIL reason={failure} length={text_len}",
                    flush=True,
                )
        if progress_every > 0 and idx % progress_every == 0 and verbose:
            print(f"[NIM TEXT] {idx}/{total} processed | success={n_success} | failed={n_failed}", flush=True)
        if cache_every > 0 and idx % cache_every == 0:
            path = merge_and_save_nim_fulltext_cache(cache_dir, results)
            if verbose:
                print(f"[NIM TEXT] cache saved -> {path.name}", flush=True)

    if results:
        path = merge_and_save_nim_fulltext_cache(cache_dir, results)
        if verbose:
            print(f"[NIM TEXT] cache saved -> {path.name}", flush=True)

    out = pd.DataFrame(results)
    if verbose:
        print("=== NIM FULLTEXT SUMMARY ===", flush=True)
        print(f"Total documents: {total}", flush=True)
        print(f"Successful: {n_success}", flush=True)
        print(f"Failed: {n_failed}", flush=True)
        success_rate = (n_success / total * 100.0) if total else 0.0
        print(f"Success rate: {success_rate:.1f}%", flush=True)
        print("Route counts:", flush=True)
        for route_name, count in sorted(route_counts.items(), key=lambda x: (-x[1], x[0])):
            print(f"{route_name}: {count}", flush=True)
        print("Successful by route:", flush=True)
        for route_name, count in sorted(route_success_counts.items(), key=lambda x: (-x[1], x[0])):
            print(f"{route_name}: {count}", flush=True)
        print("Failed by route:", flush=True)
        for route_name, count in sorted(route_failure_counts.items(), key=lambda x: (-x[1], x[0])):
            print(f"{route_name}: {count}", flush=True)
        for reason, count in sorted(failure_counts.items(), key=lambda x: (-x[1], x[0])):
            print(f"{reason}: {count}", flush=True)
    return out


__all__ = [
    "batch_fetch_nim_fulltext",
    "enrich_nim_metadata",
    "get_national_transpositions_by_celex_ws",
    "normalize_legal_act_celex",
    "select_eligible_celex_acts",
]
