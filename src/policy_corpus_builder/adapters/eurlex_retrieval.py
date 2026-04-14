from __future__ import annotations

"""Standalone EUR-Lex retrieval helpers for the rebuilt pipeline.

Ported and adapted from:
- NID_Retrieval_Pipeline_EURLEX.ipynb

This module now treats the EUR-Lex WebService SOAP workflow as the primary
retrieval path. Cached outputs remain available only as explicit retrieval-layer
fallbacks when live execution is disabled or unavailable.
"""

import json
import math
import os
import re
import time
import xml.etree.ElementTree as ET
from datetime import datetime, timezone
from pathlib import Path
from typing import Iterable
from urllib.parse import quote

import pandas as pd
import requests
from bs4 import BeautifulSoup

from celex_lookup import extract_celex_token, parse_celex_to_dict
from analysis_pipeline.functions.retrieval_queries import (
    SEARCH_TERMS_PRIMARY,
    TRANSLATED_TERMS_PRIMARY,
)

EURLEX_WS_ENDPOINT = "https://eur-lex.europa.eu/EURLexWebService"
EURLEX_ALT_SEARCH_URL = "https://eur-lex.europa.eu/search.html"
EURLEX_TEXT_URL = "https://eur-lex.europa.eu/legal-content/{lang}/TXT/HTML/?uri=CELEX:{celex}"
EURLEX_CELLAR_BASE = "http://publications.europa.eu/resource"

DEFAULT_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0 Safari/537.36"
    ),
    "Accept-Language": "en-US,en;q=0.9",
}

ROUTE_HEADERS = {
    "cellar": {
        "Accept": "application/xhtml+xml",
        "Accept-Max-Cs-Size": "209715200",
    },
    "default_text": {
        "Accept": "text/html, application/xhtml+xml, text/plain, application/xml;q=0.9, text/xml;q=0.8, */*;q=0.2",
    },
}

EU_LANG2_TO_3 = {
    "bg": "bul", "cs": "ces", "da": "dan", "de": "deu", "el": "ell", "en": "eng",
    "es": "spa", "et": "est", "fi": "fin", "fr": "fra", "ga": "gle", "hr": "hrv",
    "hu": "hun", "it": "ita", "lt": "lit", "lv": "lav", "mt": "mlt", "nl": "nld",
    "pl": "pol", "pt": "por", "ro": "ron", "sk": "slk", "sl": "slv", "sv": "swe",
}

SESSION = requests.Session()
SESSION.headers.update(DEFAULT_HEADERS)
try:
    SESSION.trust_env = False
except Exception:
    pass


def build_eurlex_jobs(include_translations: bool = True, include_nim: bool = True) -> list[dict]:
    """Port of notebook `eurlex_jobs(...)`."""
    jobs: list[dict] = [
        {
            "scope": "ALL_ALL",
            "expert_scope": "DTS_SUBDOM = ALL_ALL",
            "lang": "en",
            "terms": list(SEARCH_TERMS_PRIMARY),
        }
    ]

    if include_translations:
        for lang, terms in TRANSLATED_TERMS_PRIMARY.items():
            jobs.append(
                {
                    "scope": "ALL_ALL",
                    "expert_scope": "DTS_SUBDOM = ALL_ALL",
                    "lang": lang,
                    "terms": list(terms),
                }
            )

    if include_nim:
        jobs.append(
            {
                "scope": "NIM",
                "expert_scope": "DTS_DOM = NIM",
                "lang": "en",
                "terms": list(SEARCH_TERMS_PRIMARY),
            }
        )
        if include_translations:
            for lang, terms in TRANSLATED_TERMS_PRIMARY.items():
                jobs.append(
                    {
                        "scope": "NIM",
                        "expert_scope": "DTS_DOM = NIM",
                        "lang": lang,
                        "terms": list(terms),
                    }
                )

    return jobs


def chunk_terms(terms: Iterable[str], size: int) -> list[list[str]]:
    terms = list(terms)
    return [terms[i : i + size] for i in range(0, len(terms), size)]


def build_job_chunks(
    jobs: list[dict],
    *,
    fields: tuple[str, ...] = ("TI", "TE"),
    terms_per_query: int = 12,
) -> pd.DataFrame:
    """Visible job-chunk table for workbook inspection."""
    rows: list[dict] = []
    type_summary_rows: list[dict] = []
    for job_idx, job in enumerate(jobs, start=1):
        for chunk_idx, term_chunk in enumerate(chunk_terms(job["terms"], terms_per_query), start=1):
            expert_query = build_expert_query(job["expert_scope"], term_chunk, fields=fields)
            rows.append(
                {
                    "job_index": job_idx,
                    "chunk_index": chunk_idx,
                    "scope": job["scope"],
                    "lang": job["lang"],
                    "expert_scope": job["expert_scope"],
                    "fields": ", ".join(fields),
                    "term_count": len(term_chunk),
                    "terms": term_chunk,
                    "term_group": " | ".join(term_chunk),
                    "expert_query": expert_query,
                }
            )
    return pd.DataFrame(rows)


def build_expert_query(
    expert_scope: str,
    group_terms: list[str],
    *,
    fields: tuple[str, ...] = ("TI", "TE"),
) -> str:
    """Port of notebook `build_expert_query(...)`."""

    def esc(value: str) -> str:
        return (value or "").replace('"', '\\"')

    per_term: list[str] = []
    for term in group_terms:
        parts = [f'{field} ~ "{esc(term)}"' for field in fields]
        per_term.append("((" + " OR ".join(parts) + "))")
    return f"{expert_scope} AND (" + " OR ".join(per_term) + ")"


def build_soap_envelope(
    expert_query: str,
    *,
    page: int,
    page_size: int,
    lang: str,
    user: str | None = None,
    password: str | None = None,
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
                <sear:searchLanguage>{lang}</sear:searchLanguage>
                </sear:searchRequest>
            </soap:Body>
            </soap:Envelope>'''

def post_eurlex_ws(
    payload: str,
    *,
    session: requests.Session | None = None,
    timeout: int = 45,
    retry_5xx: int = 3,
    min_interval_s: float = 1.6,
    debug: bool = False,
    state: dict | None = None,
) -> tuple[str | None, int, str]:
    """Port of notebook post/retry logic for the EUR-Lex WebService."""
    sess = session or SESSION
    tracker = state if state is not None else {}
    last_call = float(tracker.get("last_call", 0.0))
    wait = min_interval_s - (time.time() - last_call)
    if wait > 0:
        time.sleep(wait)

    headers = {
        "Content-Type": "application/soap+xml; charset=UTF-8",
        "Accept": "application/xml, text/xml;q=0.9, */*;q=0.8",
    }

    for attempt in range(retry_5xx + 1):
        t0 = time.time()
        response = sess.post(
            EURLEX_WS_ENDPOINT,
            data=payload.encode("utf-8"),
            headers=headers,
            timeout=timeout,
        )
        dt = time.time() - t0
        tracker["last_call"] = time.time()

        if debug:
            print(f"[EURLEX] POST {dt:.2f}s status={response.status_code} bytes={len(response.text)}")

        if response.status_code in (500, 502, 503, 504):
            if attempt < retry_5xx:
                time.sleep(1.5 * (2**attempt))
                continue
            return None, response.status_code, response.text

        try:
            response.raise_for_status()
        except requests.HTTPError:
            return None, response.status_code, response.text

        return response.text, response.status_code, response.text

    return None, 0, ""


def _local_name(tag: str) -> str:
    return tag.rsplit("}", 1)[-1]


def _descendants_by_name(node: ET.Element, target_name: str) -> list[ET.Element]:
    return [child for child in node.iter() if _local_name(child.tag) == target_name]


def _descendant_texts(node: ET.Element, target_name: str) -> list[str]:
    values: list[str] = []
    for child in node.iter():
        if _local_name(child.tag) == target_name and child.text and child.text.strip():
            values.append(child.text.strip())
    return values


def _find_first_text(node: ET.Element, target_name: str) -> str:
    values = _descendant_texts(node, target_name)
    return values[0] if values else ""


def _extract_notice_value(node: ET.Element, field_name: str, *, preferred_lang: str | None = None) -> str:
    candidates: list[tuple[list[str], list[str]]] = []
    for field in _descendants_by_name(node, field_name):
        langs = [lang.lower() for lang in _descendant_texts(field, "LANG")]
        values = _descendant_texts(field, "VALUE")
        if values:
            candidates.append((langs, values))

    if preferred_lang:
        target = preferred_lang.lower()
        for langs, values in candidates:
            if target in langs:
                return values[0]

    for _, values in candidates:
        if values:
            return values[0]
    return ""


def _candidate_values_for_field(node: ET.Element, field_name: str) -> list[str]:
    values: list[str] = []
    for field in _descendants_by_name(node, field_name):
        values.extend(_descendant_texts(field, "VALUE"))
    return values


def _extract_celex_fallback(node: ET.Element) -> str:
    """Fallback CELEX recovery from nested text/URI fields when ID_CELEX is absent."""
    candidate_texts: list[str] = []
    for elem in node.iter():
        if elem.text and elem.text.strip():
            candidate_texts.append(elem.text.strip())
        for value in elem.attrib.values():
            if value and str(value).strip():
                candidate_texts.append(str(value).strip())

    for text in candidate_texts:
        lowered = text.lower()
        if "celex:" not in lowered and "/resource/celex/" not in lowered and "/celex/" not in lowered:
            continue
        extracted = extract_celex_token(text)
        if extracted:
            return extracted
    return ""


def _extract_structured_date(node: ET.Element) -> str:
    preferred_fields = [
        "WORK_DATE_DOCUMENT",
        "DATE_DOCUMENT",
        "DATE_PUBLICATION",
        "DATE_ENTRY_INTO_FORCE",
        "RESOURCE_LEGAL_DATE_ENTRY-INTO-FORCE",
    ]

    for field_name in preferred_fields:
        for field in _descendants_by_name(node, field_name):
            values = _descendant_texts(field, "VALUE")
            if values:
                return values[0]

            year = _find_first_text(field, "YEAR")
            month = _find_first_text(field, "MONTH")
            day = _find_first_text(field, "DAY")
            if year and month and day:
                try:
                    return f"{int(year):04d}-{int(month):02d}-{int(day):02d}"
                except ValueError:
                    return f"{year}-{month}-{day}"
            if year and month:
                try:
                    return f"{int(year):04d}-{int(month):02d}"
                except ValueError:
                    return f"{year}-{month}"
            if year:
                return year

    return ""


def get_result_nodes(xml_text: str) -> tuple[ET.Element | None, list[ET.Element]]:
    root = ET.fromstring(xml_text.encode("utf-8", "ignore"))
    search_results: ET.Element | None = None
    for elem in root.iter():
        if _local_name(elem.tag) == "searchResults":
            search_results = elem
            break
    if search_results is None:
        return None, []

    result_nodes = [elem for elem in search_results.iter() if _local_name(elem.tag) == "result"]
    return search_results, result_nodes


def build_result_diagnostics(
    xml_text: str,
    *,
    preferred_lang: str = "en",
    limit: int = 3,
) -> pd.DataFrame:
    """Diagnostic summary for the first few SOAP result records."""
    _, result_nodes = get_result_nodes(xml_text)
    rows: list[dict] = []
    for idx, result in enumerate(result_nodes[:limit], start=1):
        direct_children = [_local_name(child.tag) for child in list(result)]
        unique_tags: list[str] = []
        for child in result.iter():
            name = _local_name(child.tag)
            if name not in unique_tags:
                unique_tags.append(name)

        rows.append(
            {
                "result_index": idx,
                "direct_children": direct_children,
                "celex": _extract_notice_value(result, "ID_CELEX", preferred_lang=preferred_lang),
                "title": _extract_notice_value(result, "EXPRESSION_TITLE", preferred_lang=preferred_lang)
                or _extract_notice_value(result, "WORK_TITLE", preferred_lang=preferred_lang),
                "date": _extract_structured_date(result),
                "id_celex_candidates": _candidate_values_for_field(result, "ID_CELEX")[:5],
                "title_candidates": _candidate_values_for_field(result, "EXPRESSION_TITLE")[:5]
                or _candidate_values_for_field(result, "WORK_TITLE")[:5],
                "unique_tags": unique_tags[:40],
            }
        )
    return pd.DataFrame(rows)


def extract_result_snippets(xml_text: str, *, limit: int = 2, max_chars: int = 4000) -> list[str]:
    """Return compact XML snippets for the first few result nodes."""
    _, result_nodes = get_result_nodes(xml_text)
    snippets: list[str] = []
    for result in result_nodes[:limit]:
        snippet = ET.tostring(result, encoding="unicode")
        snippets.append(snippet[:max_chars])
    return snippets


def parse_searchresults(xml_text: str, *, lang_for_url: str = "EN") -> tuple[int, int, list[dict], int]:
    """Port of notebook XML result parsing without notebook-only dependencies."""
    search_results, result_nodes = get_result_nodes(xml_text)
    if search_results is None:
        return 0, 0, [], 0

    totalhits = 0
    numhits = 0
    results: list[dict] = []
    missing_celex = 0

    for elem in search_results.iter():
        name = _local_name(elem.tag)
        if name == "totalhits" and elem.text:
            try:
                totalhits = int(elem.text.strip())
            except Exception:
                totalhits = 0
        elif name == "numhits" and elem.text:
            try:
                numhits = int(elem.text.strip())
            except Exception:
                numhits = 0

    preferred_lang = lang_for_url.lower()
    for result in result_nodes:
        celex = _extract_notice_value(result, "ID_CELEX", preferred_lang=preferred_lang)
        if not celex:
            celex = _extract_celex_fallback(result)
        title = _extract_notice_value(result, "EXPRESSION_TITLE", preferred_lang=preferred_lang)
        if not title:
            title = _extract_notice_value(result, "WORK_TITLE", preferred_lang=preferred_lang)
        date = _extract_structured_date(result)
        if not celex:
            missing_celex += 1
            continue
        url = f"https://eur-lex.europa.eu/legal-content/{lang_for_url.upper()}/TXT/?uri=CELEX:{celex}"
        results.append({"celex": celex, "title": title, "date": date, "url": url})

    return totalhits, numhits, results, missing_celex


def fetch_eurlex_job(
    job: dict,
    *,
    fields: tuple[str, ...] = ("TI", "TE"),
    terms_per_query: int = 12,
    page_size: int = 100,
    page_size_candidates: tuple[int, ...] = (100, 50, 25),
    max_pages: int = 50,
    min_interval_s: float = 1.6,
    timeout: int = 45,
    retry_5xx: int = 3,
    debug: bool = True,
    session: requests.Session | None = None,
) -> list[dict]:
    """Port of notebook `fetch_eurlex_job(...)`.

    Returns the raw list of EUR-Lex rows for one job.
    """
    scope = job["scope"]
    lang = job["lang"]
    expert_scope = job["expert_scope"]
    terms = list(job["terms"])

    groups = chunk_terms(terms, terms_per_query)
    if debug:
        print(
            f"\n=== JOB scope={scope} lang={lang} "
            f"terms={len(terms)} groups={len(groups)} fields={fields} ==="
        )

    final_out: list[dict] = []
    seen_celex: set[str] = set()
    candidates = [page_size] + [ps for ps in page_size_candidates if ps != page_size and ps > 0]
    post_state: dict = {}

    for group_index, group_terms in enumerate(groups, start=1):
        term_group = " | ".join(group_terms)
        expert_query = build_expert_query(expert_scope, group_terms, fields=fields)
        if debug:
            print(f"\n[EURLEX] group {group_index}/{len(groups)} | terms={len(group_terms)}")

        group_success = False
        for page_size_try in candidates:
            if debug:
                print(f"[EURLEX] trying page_size={page_size_try}")

            payload = build_soap_envelope(
                expert_query,
                page=1,
                page_size=page_size_try,
                lang=lang,
            )
            xml1, status1, raw1 = post_eurlex_ws(
                payload,
                session=session,
                timeout=timeout,
                retry_5xx=retry_5xx,
                min_interval_s=min_interval_s,
                debug=debug,
                state=post_state,
            )
            if xml1 is None:
                if debug:
                    print(
                        f"[EURLEX] page_size={page_size_try} failed on page 1 "
                        f"(status={status1}); trying smaller."
                    )
                continue
            if "Fault" in xml1 or "<soap:Fault" in xml1 or "fault" in xml1.lower():
                if debug:
                    print("[EURLEX] SOAP Fault on page 1; skipping this page_size.")
                    print(raw1[:500])
                continue

            totalhits, numhits, hits, missing_celex = parse_searchresults(xml1, lang_for_url="EN")
            if debug:
                print(
                    f"[EURLEX] page=1 totalhits={totalhits} numhits={numhits} "
                    f"hits_parsed={len(hits)} missing_celex={missing_celex}"
                )
                if totalhits > 0 and not hits:
                    diag_df = build_result_diagnostics(xml1, preferred_lang="en", limit=3)
                    if not diag_df.empty:
                        print("[EURLEX] parser diagnostics for first results:")
                        print(diag_df.to_string(index=False))

            if totalhits == 0 or not hits:
                group_success = True
                break

            pages_needed = max(1, math.ceil(totalhits / page_size_try))
            pages_to_fetch = min(pages_needed, max_pages)

            def _append_hits(page_hits: list[dict]) -> None:
                for hit in page_hits:
                    if hit["celex"] in seen_celex:
                        continue
                    seen_celex.add(hit["celex"])
                    final_out.append(
                        {
                            "source": "EU",
                            "scope": scope,
                            "lang": lang,
                            "term_group": term_group,
                            "title": hit["title"],
                            "celex": hit["celex"],
                            "date": hit["date"],
                            "url": hit["url"],
                        }
                    )

            _append_hits(hits)

            failed = False
            for page in range(2, pages_to_fetch + 1):
                payload = build_soap_envelope(
                    expert_query,
                    page=page,
                    page_size=page_size_try,
                    lang=lang,
                )
                xmlp, statusp, rawp = post_eurlex_ws(
                    payload,
                    session=session,
                    timeout=timeout,
                    retry_5xx=retry_5xx,
                    min_interval_s=min_interval_s,
                    debug=debug,
                    state=post_state,
                )
                if xmlp is None:
                    if debug:
                        print(
                            f"[EURLEX] page={page} failed (status={statusp}) "
                            f"with page_size={page_size_try}; retry with smaller."
                        )
                    failed = True
                    break
                if "Fault" in xmlp or "<soap:Fault" in xmlp or "fault" in xmlp.lower():
                    if debug:
                        print(f"[EURLEX] SOAP Fault at page={page}; stopping this page_size.")
                        print(rawp[:500])
                    failed = True
                    break
                _, numhits2, hits2, missing2 = parse_searchresults(xmlp, lang_for_url="EN")
                if debug:
                    print(
                        f"[EURLEX] page={page} numhits={numhits2} "
                        f"hits_parsed={len(hits2)} missing_celex={missing2}"
                    )
                if not hits2:
                    break
                _append_hits(hits2)

            if not failed:
                group_success = True
                break

        if not group_success and debug:
            print(
                f"[EURLEX] group {group_index} could not complete across "
                f"page_size candidates; continuing."
            )

    return final_out


def run_eurlex_jobs(
    jobs: list[dict],
    *,
    fields: tuple[str, ...] = ("TI", "TE"),
    terms_per_query: int = 12,
    page_size: int = 100,
    page_size_candidates: tuple[int, ...] = (100, 50, 25),
    max_pages: int = 20,
    min_interval_s: float = 1.6,
    timeout: int = 45,
    retry_5xx: int = 3,
    debug: bool = True,
    session: requests.Session | None = None,
) -> list[dict]:
    """Run the full EUR-Lex job list via the WebService workflow."""
    all_rows: list[dict] = []
    for job in jobs:
        rows = fetch_eurlex_job(
            job,
            fields=fields,
            terms_per_query=terms_per_query,
            page_size=page_size,
            page_size_candidates=page_size_candidates,
            max_pages=max_pages,
            min_interval_s=min_interval_s,
            timeout=timeout,
            retry_5xx=retry_5xx,
            debug=debug,
            session=session,
        )
        all_rows.extend(rows)
    return all_rows


def load_cached_eu_rows(cache_path: Path) -> pd.DataFrame:
    """Nearest fallback: previously materialized EUR-Lex retrieval rows."""
    return pd.read_csv(cache_path, low_memory=False)


def reconstruct_eu_rows_from_corpus(canonical_all_docs: Path, term_inventory: list[str]) -> pd.DataFrame:
    """Last-resort fallback if no retrieval-layer cache exists.

    This is intentionally secondary and should only be used after explicit
    retrieval-output caches are checked.
    """
    all_docs = pd.read_csv(canonical_all_docs, low_memory=False)
    eu = all_docs[all_docs["jurisdiction"].eq("European Union")].copy()
    eu["celex"] = eu["doc_uid"].astype(str).apply(extract_celex_token)
    eu["scope"] = "ALL_ALL"
    rows: list[dict] = []
    for row in eu.itertuples(index=False):
        text = f"{getattr(row, 'title', '')} {getattr(row, 'full_text_clean', '')}".lower()
        matched = [term for term in term_inventory if term.lower() in text]
        if not matched:
            matched = [""]
        for term in matched:
            celex_full, celex_base, celex_version = split_celex_identifier(row.celex)
            rows.append(
                {
                    "source": "EU",
                    "scope": getattr(row, "scope", "ALL_ALL"),
                    "lang": getattr(row, "lang", "en"),
                    "term_group": term,
                    "celex": celex_full,
                    "title": getattr(row, "title", ""),
                    "url": getattr(row, "url", ""),
                    "date": getattr(row, "analysis_year", ""),
                    "doc_key": f"EU:{celex_full}" if str(celex_full).strip() else getattr(row, "doc_id", ""),
                }
            )
    return pd.DataFrame(rows)


def build_eu_doc_tables(all_eu_rows_df: pd.DataFrame) -> tuple[pd.DataFrame, pd.DataFrame]:
    """Port of the raw/doc dataframe construction from the notebook."""
    df_raw = all_eu_rows_df.copy()
    df_raw = df_raw[df_raw["celex"].notna() & df_raw["celex"].astype(str).str.len().gt(0)].copy()
    df_raw["celex"] = df_raw["celex"].astype(str)
    celex_parts = df_raw["celex"].apply(split_celex_identifier)
    df_raw["celex_full"] = celex_parts.apply(lambda x: x[0])
    df_raw["celex"] = celex_parts.apply(lambda x: x[1])
    df_raw["celex_version"] = celex_parts.apply(lambda x: x[2])
    df_raw["doc_key"] = "EU:" + df_raw["celex_full"]
    df_raw["url_fix"] = df_raw["celex_full"].apply(
        lambda celex_full: f"https://eur-lex.europa.eu/legal-content/EN/TXT/?uri=CELEX:{celex_full}"
    )

    def _uniq_sorted(series: pd.Series) -> list[str]:
        vals = [str(x).strip() for x in series.dropna().tolist() if str(x).strip()]
        return sorted(set(vals))

    def _pick_best_title(series: pd.Series) -> str:
        vals = [str(x).strip() for x in series.dropna().tolist() if str(x).strip()]
        return max(vals, key=len) if vals else ""

    def _pick_first_nonempty(series: pd.Series) -> str:
        for value in series.dropna().tolist():
            text = str(value).strip()
            if text:
                return text
        return ""

    df_docs = (
        df_raw.groupby("doc_key", as_index=False)
        .agg(
            source=("source", _pick_first_nonempty),
            celex=("celex", _pick_first_nonempty),
            celex_full=("celex_full", _pick_first_nonempty),
            celex_version=("celex_version", _pick_first_nonempty),
            url=("url", _pick_first_nonempty),
            url_fix=("url_fix", _pick_first_nonempty),
            title=("title", _pick_best_title),
            date=("date", _pick_first_nonempty),
            scopes=("scope", _uniq_sorted),
            query_langs=("lang", _uniq_sorted),
            query_term_groups=("term_group", _uniq_sorted),
        )
    )
    for column in ["scopes", "query_langs", "query_term_groups"]:
        df_docs[column] = df_docs[column].apply(lambda value: json.dumps(value, ensure_ascii=False))
    df_raw = annotate_celex_types(df_raw, celex_col="celex_full")
    df_docs = annotate_celex_types(df_docs, celex_col="celex_full")
    return df_raw, df_docs


def build_alt_eurlex_search_url(term: str, *, lang: str = "en", expert_scope: str = "DTS_SUBDOM = ALL_ALL") -> str:
    """Secondary helper only for manual inspection/debugging."""
    query = quote(f'{expert_scope} AND TI ~ "{term}"', safe="")
    return (
        f"{EURLEX_ALT_SEARCH_URL}?name=browse-by:legislation-in-force"
        f"&lang={lang}&text={quote(term, safe='')}&expertQuery={query}"
    )


def fetch_eurlex_document_text(celex: str, *, lang: str = "EN", timeout_s: int = 60) -> requests.Response:
    url = EURLEX_TEXT_URL.format(lang=lang, celex=celex)
    response = SESSION.get(url, timeout=timeout_s)
    response.raise_for_status()
    return response


def _html_to_text(html: str) -> str:
    if not html:
        return ""
    soup = BeautifulSoup(html, "html.parser")
    for tag in soup(["script", "style", "noscript"]):
        tag.decompose()
    text = soup.get_text("\n")
    text = re.sub(r"[ \t\r\f\v]+", " ", text)
    text = re.sub(r"\n{3,}", "\n\n", text)
    return text.strip()


def _response_content_type(response: requests.Response | None) -> str:
    if response is None:
        return ""
    return str(response.headers.get("Content-Type", "") or "").split(";")[0].strip().lower()


def _looks_like_metadata_response(content: str, *, final_url: str = "", content_type: str = "") -> str:
    lowered_url = str(final_url or "").lower()
    lowered_type = str(content_type or "").lower()
    lowered = str(content or "").lower()
    if "/rdf/object/full" in lowered_url:
        return "metadata_response_rdf_url"
    if "application/rdf+xml" in lowered_type or "application/ld+json" in lowered_type:
        return "metadata_response_rdf_type"
    if "<rdf:rdf" in lowered or "xmlns:rdf=" in lowered:
        return "metadata_response_rdf_xml"
    if "<skos:concept" in lowered or "<owl:" in lowered or "<dcterms:" in lowered:
        return "metadata_response_metadata_xml"
    if "<!doctype rdf" in lowered or "<sparql" in lowered:
        return "metadata_response_xml_payload"
    uri_hits = lowered.count("http://") + lowered.count("https://")
    if uri_hits >= 8 and len(_html_to_text(content)) < 2000:
        return "metadata_response_uri_heavy"
    return ""


def _extract_text_candidate(content: str, *, content_type: str = "") -> tuple[str, str]:
    lowered_type = str(content_type or "").lower()
    if not content:
        return "", "empty_text"
    if lowered_type.startswith("text/plain"):
        text = " ".join(str(content).split()).strip()
        return text, "" if text else "empty_text"
    text = _html_to_text(content)
    if not text:
        return "", "empty_text"
    return text, ""


def _looks_like_valid_fulltext(text: str, *, min_chars: int = 300) -> tuple[bool, str]:
    clean = str(text or "").strip()
    if not clean:
        return False, "empty_text"
    if len(clean) < min_chars:
        return False, "short_text"
    lowered = clean.lower()
    namespace_hits = sum(lowered.count(token) for token in ["xmlns:", "rdf:", "skos:", "dcterms:", "owl:"])
    uri_hits = lowered.count("http://") + lowered.count("https://")
    if namespace_hits >= 2:
        return False, "metadata_response_namespace_heavy"
    if uri_hits >= 12 and len(clean) < 3000:
        return False, "metadata_response_uri_heavy"
    return True, ""


def _lang_to_iso639_3(lang: str | None) -> str:
    lang = str(lang or "en").strip().lower()
    if len(lang) == 3:
        return lang
    return EU_LANG2_TO_3.get(lang, "eng")


def _route_headers(route_name: str, *, lang: str = "en") -> dict[str, str]:
    headers = dict(DEFAULT_HEADERS)
    if route_name == "cellar":
        headers.update(ROUTE_HEADERS["cellar"])
        headers["Accept-Language"] = _lang_to_iso639_3(lang)
    else:
        headers.update(ROUTE_HEADERS["default_text"])
        headers["Accept-Language"] = str(lang or "en").upper()
    return headers


def _is_bot_gate(html: str) -> bool:
    lowered = (html or "").lower()
    clean_text = _html_to_text(html)
    warning = "verify that you're not a robot" in lowered or "javascript is disabled" in lowered
    return warning and len(clean_text) < 500


def _parse_lang_list(value) -> list[str]:
    if value is None:
        return []
    if isinstance(value, list):
        return [str(item).strip().lower() for item in value if str(item).strip()]
    if isinstance(value, str):
        value = value.strip()
        if not value:
            return []
        if value.startswith("[") and value.endswith("]"):
            try:
                parsed = json.loads(value)
                return [str(item).strip().lower() for item in parsed if str(item).strip()]
            except Exception:
                return [value.lower()]
        return [value.lower()]
    return [str(value).strip().lower()]


def _lang_candidates_from_doc_row(row: pd.Series) -> list[str]:
    out: list[str] = []
    doc_lang = str(row.get("lang", "") or "").strip().lower()
    preferred = ["en"]
    for lang in preferred + _parse_lang_list(row.get("query_langs")):
        lang = "en" if lang in ("eng", "") else lang
        if lang and lang not in out:
            out.append(lang)
    if doc_lang and doc_lang not in ("", "en", "eng") and doc_lang not in out:
        out.append(doc_lang)
    return out or ["en"]


def lang_candidates_from_row(row: pd.Series) -> list[str]:
    """Port of the old notebook helper: EN first, then query languages."""
    return _lang_candidates_from_doc_row(row)


def cellar_celex_url(celex: str) -> str:
    safe_celex = quote(str(celex or "").strip(), safe="ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789-._()")
    return f"{EURLEX_CELLAR_BASE}/celex/{safe_celex}"


def _finalize_cellar_text(
    *,
    html: str,
    final_url: str,
    content_type: str,
    min_chars: int = 150,
) -> tuple[str, str]:
    metadata_reason = _looks_like_metadata_response(html, final_url=final_url, content_type=content_type)
    if metadata_reason:
        return "", metadata_reason
    text_clean, text_error = _extract_text_candidate(html, content_type=content_type)
    if text_error or not text_clean:
        return "", text_error or "empty_text"
    valid_text, valid_reason = _looks_like_valid_fulltext(text_clean, min_chars=min_chars)
    if not valid_text:
        return "", valid_reason
    return text_clean, ""


def get_eurlex_text(
    celex: str,
    *,
    lang: str = "en",
    session: requests.Session | None = None,
    timeout_s: int = 45,
    retries: int = 4,
    trace_routes: bool = False,
) -> dict:
    """Cellar-based full-text fetch for ordinary EU CELEX documents."""
    url = cellar_celex_url(celex)
    headers = _route_headers("cellar", lang=lang)
    status, html, err, elapsed, final_url, content_type = _fetch_text_with_retries(
        url,
        celex=celex,
        route_name="cellar",
        variant=celex,
        headers=headers,
        timeout_s=timeout_s,
        retries=retries,
        session=session,
        trace_routes=trace_routes,
    )
    if status != 200 or not html:
        return {
            "status": status,
            "error": err or (f"HTTP {status}" if status else "no_response"),
            "full_text_raw": html or "",
            "full_text_clean": "",
            "final_url": final_url,
            "content_type": content_type,
            "fetch_seconds": round(elapsed, 2),
        }
    text_clean, validation_error = _finalize_cellar_text(
        html=html,
        final_url=final_url,
        content_type=content_type,
        min_chars=150,
    )
    if validation_error:
        return {
            "status": status,
            "error": validation_error,
            "full_text_raw": html,
            "full_text_clean": "",
            "final_url": final_url,
            "content_type": content_type,
            "fetch_seconds": round(elapsed, 2),
        }
    return {
        "status": status,
        "error": "",
        "full_text_raw": html,
        "full_text_clean": text_clean,
        "final_url": final_url,
        "content_type": content_type,
        "fetch_seconds": round(elapsed, 2),
    }


def get_eurlex_text_multi(
    row: pd.Series,
    *,
    session: requests.Session | None = None,
    timeout_s: int = 45,
    retries: int = 4,
    trace_routes: bool = False,
) -> dict:
    """Try EN first, then query languages, with CELEX-variant fallback."""
    celex_full = str(row.get("celex_full", "") or row.get("celex", "") or "").strip()
    celex_full, celex, celex_version = split_celex_identifier(celex_full)
    langs = lang_candidates_from_row(row)
    attempt_trace: list[dict] = []
    last_result: dict = {
        "status": 0,
        "error": "no_attempt",
        "full_text_raw": "",
        "full_text_clean": "",
        "final_url": "",
        "content_type": "",
        "fetch_seconds": 0.0,
    }
    saw_202 = False
    for lang in langs:
        for variant in celex_variants(celex_full):
            if trace_routes:
                print(f"[EURLEX TEXT] TRACE CELEX={celex_full} variant={variant} lang={lang.upper()} route=cellar", flush=True)
            result = get_eurlex_text(
                variant,
                lang=lang,
                session=session,
                timeout_s=timeout_s,
                retries=retries,
                trace_routes=trace_routes,
            )
            attempt_trace.append(
                {
                    "celex_full": celex_full,
                    "celex": celex,
                    "celex_variant": variant,
                    "lang": lang,
                    "route_name": "cellar",
                    "final_url": result.get("final_url", ""),
                    "status": result.get("status", 0),
                    "error": result.get("error", ""),
                    "content_type": result.get("content_type", ""),
                    "text_len": len(result.get("full_text_clean", "") or ""),
                }
            )
            last_result = dict(result)
            if int(result.get("status", 0) or 0) == 202:
                saw_202 = True
            if len(result.get("full_text_clean", "") or "") >= 150:
                last_result["lang"] = lang
                last_result["celex_variant_used"] = variant
                last_result["route_used"] = "cellar"
                last_result["attempt_trace"] = attempt_trace
                return last_result
    last_result["lang"] = langs[0] if langs else "en"
    last_result["celex_variant_used"] = attempt_trace[-1]["celex_variant"] if attempt_trace else ""
    last_result["route_used"] = "cellar"
    last_result["attempt_trace"] = attempt_trace
    if saw_202 and str(last_result.get("error", "")).lower() in {"http 202", "", "no_attempt"}:
        last_result["error"] = "route_exhausted_after_202"
    return last_result


def _lang_source_for_doc_row(row: pd.Series, chosen_lang: str) -> str:
    chosen = str(chosen_lang or "").strip().lower()
    doc_lang = str(row.get("lang", "") or "").strip().lower()
    query_langs = _parse_lang_list(row.get("query_langs"))
    if chosen == "en":
        if doc_lang in ("", "en", "eng") and (not query_langs or all(lang in ("en", "eng") for lang in query_langs)):
            return "english_default"
        return "english_fallback"
    if doc_lang == chosen:
        return "job_language"
    if chosen in query_langs:
        return "query_language"
    return "retry_sequence"


def _cache_path_for_celex(celex: str, cache_dir: Path, suffix: str) -> Path:
    cache_dir.mkdir(parents=True, exist_ok=True)
    safe = "".join(ch if ch.isalnum() or ch in "._-()" else "_" for ch in str(celex))
    return cache_dir / f"{safe}.{suffix}"


def split_celex_identifier(celex_value: str) -> tuple[str, str, str]:
    celex_full = str(celex_value or "").strip()
    if not celex_full:
        return "", "", ""
    if "-" in celex_full:
        base, version = celex_full.split("-", 1)
        return celex_full, base, version
    return celex_full, celex_full, ""


def classify_celex_for_fulltext(celex_value: str) -> dict:
    celex_full, celex_base, celex_version = split_celex_identifier(str(celex_value or ""))
    parsed = parse_celex_to_dict(celex_full)
    normalized = str(parsed.get("normalized") or celex_full)
    valid = bool(parsed.get("valid"))
    sector = str(parsed.get("sector") or "")
    descriptor = str(parsed.get("descriptor") or "")
    descriptor_label = str(parsed.get("descriptor_label") or "")
    sector_label = str(parsed.get("sector_label") or "")
    notes = str(parsed.get("notes") or "")

    if not valid:
        alt = parse_celex_to_dict(celex_base) if celex_base and celex_base != celex_full else {}
        if alt and alt.get("valid"):
            parsed = alt
            valid = True
            sector = str(parsed.get("sector") or "")
            descriptor = str(parsed.get("descriptor") or "")
            descriptor_label = str(parsed.get("descriptor_label") or "")
            sector_label = str(parsed.get("sector_label") or "")
            notes = str(parsed.get("notes") or "")

    if not valid and re.match(r"^0\d{4}[A-Z]{1,4}.+\(\d{2}\)$", celex_full):
        sector = "0"
        sector_label = "Consolidated texts"
        descriptor_match = re.match(r"^0\d{4}([A-Z]{1,4})", celex_full)
        descriptor = descriptor_match.group(1) if descriptor_match else ""
        descriptor_label = "Consolidated non-standard form"
        notes = "Sector 0 consolidated text (non-standard suffix form)"
        valid = True

    celex_class = "unknown_or_invalid"
    retrieval_support = "supported"
    if sector == "0":
        celex_class = "sector_0_consolidated"
    elif sector == "1":
        celex_class = "sector_1_treaties"
    elif sector == "2":
        celex_class = "sector_2_international_agreements"
    elif sector == "3":
        celex_class = "sector_3_legal_acts"
    elif sector == "5":
        celex_class = "sector_5_preparatory"
    elif sector == "6":
        celex_class = "sector_6_case_law"
    elif sector == "7":
        celex_class = "sector_7_national_transposition"
        retrieval_support = "unsupported_celex_type_for_fulltext"
    elif parsed.get("is_corrigendum"):
        celex_class = "corrigendum_or_special_form"
    elif sector:
        celex_class = f"sector_{sector}_other"

    if sector in UNSUPPORTED_FULLTEXT_SECTORS and retrieval_support == "supported":
        retrieval_support = "unsupported_celex_type_for_fulltext"

    return {
        "celex_full": celex_full,
        "celex": celex_base,
        "celex_version": celex_version,
        "celex_valid": valid,
        "celex_sector": sector,
        "celex_sector_label": sector_label,
        "celex_descriptor": descriptor,
        "celex_descriptor_label": descriptor_label,
        "celex_notes": notes,
        "celex_is_corrigendum": bool(parsed.get("is_corrigendum")),
        "celex_is_consolidated": bool(parsed.get("is_consolidated")) or sector == "0",
        "celex_class": celex_class,
        "fulltext_support": retrieval_support,
    }


def annotate_celex_types(df: pd.DataFrame, *, celex_col: str = "celex_full") -> pd.DataFrame:
    out = df.copy()
    if celex_col not in out.columns:
        fallback_col = "celex" if "celex" in out.columns else None
        if fallback_col is None:
            return out
        celex_col = fallback_col
    meta_df = out[celex_col].fillna("").astype(str).apply(classify_celex_for_fulltext).apply(pd.Series)
    for col in meta_df.columns:
        out[col] = meta_df[col]
    return out


def filter_celex_types_for_fulltext(
    df: pd.DataFrame,
    *,
    mode: str = "all",
    exclude_descriptors: Iterable[str] | None = None,
) -> pd.DataFrame:
    out = annotate_celex_types(df)
    mode = str(mode or "all").strip().lower()
    if mode == "sector_3_only":
        out = out[out["celex_sector"].eq("3")].copy()
    elif mode == "sector_0_and_3":
        out = out[out["celex_sector"].isin(["0", "3"])].copy()
    elif mode == "supported_only":
        out = out[out["fulltext_support"].eq("supported")].copy()
    exclude = {str(item).strip().upper() for item in (exclude_descriptors or []) if str(item).strip()}
    if exclude:
        out = out[~out["celex_descriptor"].fillna("").astype(str).str.upper().isin(exclude)].copy()
    return out.reset_index(drop=True)


def routes_for_celex_type(celex_meta: dict) -> list[str]:
    support = str(celex_meta.get("fulltext_support") or "supported")
    if support != "supported":
        return []
    celex_class = str(celex_meta.get("celex_class") or "")
    return list(EURLEX_ROUTE_ORDER_BY_CLASS.get(celex_class, EURLEX_ROUTE_ORDER_BY_CLASS["default_supported"]))


def celex_variants(celex: str) -> list[str]:
    """Try consolidated CELEX first, then base CELEX without suffix."""
    celex = str(celex or "").strip()
    if not celex:
        return []
    variants = [celex]
    if "-" in celex:
        variants.append(celex.split("-", 1)[0])
    out: list[str] = []
    for variant in variants:
        if variant and variant not in out:
            out.append(variant)
    return out


EURLEX_TEXT_ROUTE_ORDER = [
    "legal-content TXT EN",
    "legal-content TXT",
    "legal-content HTML EN",
    "legal-content HTML",
    "cellar",
    "LexUriServ",
]

EURLEX_ROUTE_ORDER_BY_CLASS = {
    "sector_3_legal_acts": [
        "legal-content TXT EN",
        "legal-content TXT",
        "legal-content HTML EN",
        "legal-content HTML",
        "cellar",
        "LexUriServ",
    ],
    "sector_0_consolidated": [
        "legal-content TXT EN",
        "legal-content TXT",
        "legal-content HTML EN",
        "legal-content HTML",
        "cellar",
        "LexUriServ",
    ],
    "sector_1_treaties": [
        "legal-content HTML EN",
        "legal-content TXT EN",
        "cellar",
        "LexUriServ",
        "legal-content HTML",
        "legal-content TXT",
    ],
    "sector_2_international_agreements": [
        "legal-content HTML EN",
        "legal-content TXT EN",
        "cellar",
        "LexUriServ",
        "legal-content HTML",
        "legal-content TXT",
    ],
    "sector_5_preparatory": [
        "legal-content HTML EN",
        "legal-content TXT EN",
        "legal-content HTML",
        "legal-content TXT",
        "cellar",
        "LexUriServ",
    ],
    "sector_6_case_law": [
        "legal-content HTML EN",
        "legal-content TXT EN",
        "legal-content HTML",
        "legal-content TXT",
        "cellar",
        "LexUriServ",
    ],
    "default_supported": [
        "legal-content TXT EN",
        "legal-content TXT",
        "legal-content HTML EN",
        "legal-content HTML",
        "cellar",
        "LexUriServ",
    ],
}

UNSUPPORTED_FULLTEXT_SECTORS = {"7", "8"}


def variants_for_route(route_name: str, celex: str) -> list[str]:
    variants = celex_variants(celex)
    if len(variants) <= 1:
        return variants
    original = variants[0]
    base = variants[1]
    if route_name == "cellar":
        ordered = [original, base]
    else:
        ordered = [base, original]
    out: list[str] = []
    for variant in ordered:
        if variant and variant not in out:
            out.append(variant)
    return out


def _route_url(route_name: str, celex: str, lang: str = "EN") -> str:
    uri_val = f"CELEX:{celex}"
    uri_q = quote(uri_val, safe="")
    lex_uri = quote(f"CELEX:{celex}:{lang}:HTML", safe="")
    route_map = {
        "legal-content TXT EN": f"https://eur-lex.europa.eu/legal-content/EN/TXT/?uri={uri_q}",
        "legal-content TXT": f"https://eur-lex.europa.eu/legal-content/{lang}/TXT/?uri={uri_q}",
        "legal-content HTML": f"https://eur-lex.europa.eu/legal-content/{lang}/TXT/HTML/?uri={uri_q}",
        "legal-content HTML EN": f"https://eur-lex.europa.eu/legal-content/EN/TXT/HTML/?uri={uri_q}",
        "LexUriServ": f"https://eur-lex.europa.eu/LexUriServ/LexUriServ.do?uri={lex_uri}",
        "cellar": f"{EURLEX_CELLAR_BASE}/celex/{quote(str(celex), safe='ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789-._')}",
    }
    return route_map[route_name]


def _classify_failure(result: dict) -> str:
    status = int(result.get("retrieval_status", 0) or 0)
    error = str(result.get("retrieval_error", "") or "").lower()
    celex_class = str(result.get("celex_class", "") or "")
    fulltext_support = str(result.get("fulltext_support", "") or "")
    if fulltext_support == "unsupported_celex_type_for_fulltext":
        return "unsupported_celex_type_for_fulltext"
    if "route_exhausted_after_202" in error:
        return "route_exhausted_after_202"
    if "sector0_route_mismatch" in error:
        return "sector0_route_mismatch"
    if "no_text_representation_for_type" in error:
        return "no_text_representation_for_type"
    if error.startswith("metadata_response"):
        return "metadata_rdf_reject"
    if "timeout" in error:
        return "timeout"
    if "parse_error" in error:
        return "parse_error"
    if "bot_gate" in error or "bot/JS gate".lower() in error:
        return "bot-gate detected"
    if status == 202 or "http 202" in error:
        return "http_202"
    if status == 404 or "http 404" in error:
        if celex_class == "sector_0_consolidated":
            return "sector0_route_mismatch"
        return "404_not_found"
    if "empty_text" in error:
        return "empty_text_after_successful_fetch"
    if status >= 400:
        return f"http {status}"
    return "HTML missing"


def _fulltext_cache_csv_path(cache_dir: Path) -> Path:
    cache_dir.mkdir(parents=True, exist_ok=True)
    return cache_dir / "eurlex_cache_fulltext.csv"


def _normalize_fulltext_cache_frame(df: pd.DataFrame) -> pd.DataFrame:
    cols = ["celex_full", "celex", "celex_version", "lang", "text", "source_url", "retrieval_status", "retrieval_error", "text_len", "timestamp"]
    if df is None or df.empty:
        return pd.DataFrame(columns=cols)
    out = df.copy()
    original_cols = set(out.columns)
    if "celex_full" not in out.columns and "celex" in out.columns:
        out["celex_full"] = out["celex"]
    for col in cols:
        if col not in out.columns:
            out[col] = ""
    out["celex_full"] = out["celex_full"].fillna("").astype(str)
    celex_parts = out["celex_full"].apply(split_celex_identifier)
    out["celex"] = celex_parts.apply(lambda x: x[1])
    out["celex_version"] = celex_parts.apply(lambda x: x[2])
    out["lang"] = out["lang"].fillna("").astype(str)
    out["text"] = out["text"].fillna("").astype(str)
    out["source_url"] = out["source_url"].fillna("").astype(str)
    out["retrieval_status"] = pd.to_numeric(out["retrieval_status"], errors="coerce").fillna(0).astype(int)
    out["retrieval_error"] = out["retrieval_error"].fillna("").astype(str)
    raw_text_len = pd.to_numeric(out["text_len"], errors="coerce")
    recomputed_text_len = out["text"].fillna("").astype(str).str.len()
    missing_len_mask = raw_text_len.isna() | raw_text_len.le(0)
    if missing_len_mask.any() and recomputed_text_len.gt(0).any():
        out.loc[missing_len_mask, "text_len"] = recomputed_text_len[missing_len_mask]
    out["text_len"] = pd.to_numeric(out["text_len"], errors="coerce").fillna(0).astype(int)
    out["timestamp"] = out["timestamp"].fillna("").astype(str)
    if ("text_len" not in original_cols or "retrieval_error" not in original_cols) and len(out) > 0:
        print(
            "[EURLEX CACHE] warning: older or incomplete cache CSV detected; reconstructed missing columns from cached text where possible",
            flush=True,
        )
    return out[cols].drop_duplicates(subset=["celex_full", "lang"], keep="last")


def load_fulltext_cache_table(cache_dir: Path) -> pd.DataFrame:
    path = _fulltext_cache_csv_path(cache_dir)
    if not path.exists():
        return _normalize_fulltext_cache_frame(pd.DataFrame())
    return _normalize_fulltext_cache_frame(pd.read_csv(path, low_memory=False))


def _cache_row_from_result(result: dict) -> dict:
    celex_full, celex_base, celex_version = split_celex_identifier(str(result.get("celex_full", "") or result.get("celex", "") or ""))
    return {
        "celex_full": celex_full,
        "celex": celex_base,
        "celex_version": celex_version,
        "lang": str(result.get("lang", "") or ""),
        "text": str(result.get("full_text_clean", "") or ""),
        "source_url": str(result.get("text_source_url", "") or ""),
        "retrieval_status": int(result.get("retrieval_status", 0) or 0),
        "retrieval_error": str(result.get("retrieval_error", "") or ""),
        "text_len": int(result.get("text_len", 0) or 0),
        "timestamp": datetime.now(timezone.utc).isoformat(),
    }


def summarize_fulltext_cache_state(
    cache_dir: Path,
    df_docs: pd.DataFrame,
    *,
    success_min_chars: int = 500,
) -> tuple[pd.DataFrame, pd.DataFrame]:
    cache_df = load_fulltext_cache_table(cache_dir)
    file_df = rebuild_fulltext_cache_state_from_files(cache_dir)
    if df_docs.empty:
        empty_summary = pd.DataFrame(columns=["celex", "cache_state", "lang", "retrieval_status", "retrieval_error", "text_len", "source_url", "timestamp"])
        return empty_summary, cache_df

    doc_df = df_docs.copy()
    if "celex_full" not in doc_df.columns and "celex" in doc_df.columns:
        doc_df["celex_full"] = doc_df["celex"]
    doc_df["celex_full"] = doc_df["celex_full"].fillna("").astype(str)
    celex_parts = doc_df["celex_full"].apply(split_celex_identifier)
    doc_df["celex"] = celex_parts.apply(lambda x: x[1])
    doc_df["celex_version"] = celex_parts.apply(lambda x: x[2])
    cache_df = cache_df.copy()
    cache_df["celex_full"] = cache_df["celex_full"].fillna("").astype(str)

    if cache_df.empty and file_df.empty:
        summary_df = doc_df[["celex_full", "celex", "celex_version"]].drop_duplicates().copy()
        summary_df["cache_state"] = "pending"
        summary_df["lang"] = ""
        summary_df["retrieval_status"] = 0
        summary_df["retrieval_error"] = ""
        summary_df["text_len"] = 0
        summary_df["source_url"] = ""
        summary_df["timestamp"] = ""
        return summary_df, cache_df

    if cache_df.empty:
        cache_group = pd.DataFrame(columns=["celex_full", "celex", "celex_version", "lang", "retrieval_status", "retrieval_error", "text_len", "source_url", "timestamp"])
    else:
        cache_group = (
            cache_df.sort_values(["celex_full", "timestamp"])
            .groupby("celex_full", as_index=False)
            .agg(
                celex=("celex", "last"),
                celex_version=("celex_version", "last"),
                lang=("lang", "last"),
                retrieval_status=("retrieval_status", "last"),
                retrieval_error=("retrieval_error", "last"),
                text_len=("text_len", "max"),
                source_url=("source_url", "last"),
                timestamp=("timestamp", "last"),
            )
        )
    cache_group = cache_group.merge(
        file_df[["celex_full", "celex", "celex_version", "file_text_len", "text_path", "file_exists"]]
        if not file_df.empty
        else pd.DataFrame(columns=["celex_full", "celex", "celex_version", "file_text_len", "text_path", "file_exists"]),
        on=["celex_full", "celex", "celex_version"],
        how="outer",
    )
    if "file_text_len" not in cache_group.columns:
        cache_group["file_text_len"] = 0
    if "file_exists" not in cache_group.columns:
        cache_group["file_exists"] = False
    cache_group["file_text_len"] = pd.to_numeric(cache_group["file_text_len"], errors="coerce").fillna(0).astype(int)
    cache_group["cache_state"] = "failed"
    cache_group.loc[cache_group["file_exists"] & cache_group["file_text_len"].ge(success_min_chars), "cache_state"] = "successful"
    cache_group.loc[
        cache_group["cache_state"].ne("successful") & cache_group["text_len"].ge(success_min_chars),
        "cache_state"
    ] = "successful"

    summary_df = doc_df[["celex_full", "celex", "celex_version"]].drop_duplicates().merge(cache_group, on=["celex_full", "celex", "celex_version"], how="left")
    summary_df["cache_state"] = summary_df["cache_state"].fillna("pending")
    for col in ("lang", "retrieval_error", "source_url", "timestamp", "text_path"):
        if col in summary_df.columns:
            summary_df[col] = summary_df[col].fillna("").astype(str)
    if "file_exists" in summary_df.columns:
        summary_df["file_exists"] = summary_df["file_exists"].where(summary_df["file_exists"].notna(), False).astype(bool)
    summary_df["retrieval_status"] = pd.to_numeric(summary_df.get("retrieval_status", 0), errors="coerce").fillna(0).astype(int)
    summary_df["text_len"] = pd.to_numeric(summary_df.get("text_len", 0), errors="coerce").fillna(0).astype(int)
    summary_df["file_text_len"] = pd.to_numeric(summary_df.get("file_text_len", 0), errors="coerce").fillna(0).astype(int)
    return summary_df, cache_df


def load_failed_fulltext_attempts(
    cache_dir: Path,
    *,
    success_min_chars: int = 500,
) -> pd.DataFrame:
    cache_df = load_fulltext_cache_table(cache_dir)
    if cache_df.empty:
        return cache_df
    return cache_df[cache_df["text_len"].lt(success_min_chars)].copy()


def rebuild_fulltext_cache_state_from_files(cache_dir: Path) -> pd.DataFrame:
    text_cache_dir = cache_dir / "text_cache"
    rows: list[dict] = []
    cols = ["celex_full", "celex", "celex_version", "file_text_len", "text_path", "file_exists"]
    if not text_cache_dir.exists():
        return pd.DataFrame(columns=cols)
    for path in sorted(text_cache_dir.glob("*.txt")):
        try:
            text = path.read_text(encoding="utf-8", errors="replace")
        except Exception:
            text = ""
        celex_full, celex_base, celex_version = split_celex_identifier(path.stem)
        rows.append(
            {
                "celex_full": celex_full,
                "celex": celex_base,
                "celex_version": celex_version,
                "file_text_len": len(text),
                "text_path": str(path),
                "file_exists": True,
            }
        )
    return pd.DataFrame(rows, columns=cols)


def fulltext_cache_validation_diagnostics(
    cache_dir: Path,
    *,
    success_min_chars: int = 500,
) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    cache_df = load_fulltext_cache_table(cache_dir)
    file_df = rebuild_fulltext_cache_state_from_files(cache_dir)

    if cache_df.empty:
        merged = pd.DataFrame(columns=["celex_full", "celex", "celex_version", "lang", "retrieval_status", "retrieval_error", "text_len", "source_url", "timestamp", "file_text_len", "text_path", "file_exists"])
    else:
        cache_group = (
            cache_df.sort_values(["celex_full", "timestamp"])
            .groupby("celex_full", as_index=False)
            .agg(
                celex=("celex", "last"),
                celex_version=("celex_version", "last"),
                lang=("lang", "last"),
                retrieval_status=("retrieval_status", "last"),
                retrieval_error=("retrieval_error", "last"),
                text_len=("text_len", "max"),
                source_url=("source_url", "last"),
                timestamp=("timestamp", "last"),
            )
        )
        merged = cache_group.merge(file_df, on=["celex_full", "celex", "celex_version"], how="outer")

    if "file_text_len" not in merged.columns:
        merged["file_text_len"] = 0
    if "file_exists" not in merged.columns:
        merged["file_exists"] = False
    merged["text_len"] = pd.to_numeric(merged.get("text_len", 0), errors="coerce").fillna(0).astype(int)
    merged["file_text_len"] = pd.to_numeric(merged.get("file_text_len", 0), errors="coerce").fillna(0).astype(int)
    merged["retrieval_status"] = pd.to_numeric(merged.get("retrieval_status", 0), errors="coerce").fillna(0).astype(int)
    merged["retrieval_error"] = merged.get("retrieval_error", "").fillna("").astype(str)
    merged["csv_success"] = merged["text_len"].ge(success_min_chars)
    merged["file_success"] = merged["file_text_len"].ge(success_min_chars)
    merged["inconsistent_csv_vs_files"] = merged["csv_success"] != merged["file_success"]

    diag_df = pd.DataFrame(
        [
            {"metric": "cache_csv_rows", "value": int(len(cache_df))},
            {"metric": "cache_csv_status_200", "value": int(cache_df["retrieval_status"].eq(200).sum()) if not cache_df.empty else 0},
            {"metric": "cache_csv_text_ge_threshold", "value": int(cache_df["text_len"].ge(success_min_chars).sum()) if not cache_df.empty else 0},
            {"metric": "existing_txt_files", "value": int(file_df["file_exists"].sum()) if not file_df.empty else 0},
            {"metric": "txt_files_ge_threshold", "value": int(file_df["file_text_len"].ge(success_min_chars).sum()) if not file_df.empty else 0},
            {"metric": "csv_vs_file_inconsistencies", "value": int(merged["inconsistent_csv_vs_files"].sum()) if not merged.empty else 0},
        ]
    )
    return diag_df, merged, file_df


def merge_and_save_fulltext_cache(cache_dir: Path, results: Iterable[dict]) -> Path:
    path = _fulltext_cache_csv_path(cache_dir)
    existing = load_fulltext_cache_table(cache_dir)
    new_df = _normalize_fulltext_cache_frame(pd.DataFrame([_cache_row_from_result(r) for r in results]))
    merged = pd.concat([existing, new_df], ignore_index=True)
    merged = _normalize_fulltext_cache_frame(merged)
    path.parent.mkdir(parents=True, exist_ok=True)
    merged.to_csv(path, index=False)
    return path


def _fetch_text_with_retries(
    url: str,
    *,
    celex: str = "",
    route_name: str = "",
    variant: str = "",
    headers: dict[str, str] | None = None,
    timeout_s: int = 45,
    retries: int = 4,
    backoff_s: float = 1.2,
    session: requests.Session | None = None,
    trace_routes: bool = False,
) -> tuple[int, str | None, str, float, str, str]:
    sess = session or SESSION
    last_error = ""
    final_url = url
    content_type = ""
    for attempt in range(retries + 1):
        t0 = time.time()
        try:
            response = sess.get(url, timeout=timeout_s, allow_redirects=True, headers=headers or None)
            elapsed = time.time() - t0
            final_url = response.url
            content_type = _response_content_type(response)
            if response.status_code == 200:
                return 200, response.text, "", elapsed, final_url, content_type
            if response.status_code == 202:
                last_error = "HTTP 202"
                return 202, None, last_error, elapsed, final_url, content_type
            if response.status_code in (429, 500, 502, 503, 504):
                last_error = f"HTTP {response.status_code}"
                if trace_routes:
                    print(
                        f"[EURLEX TEXT] retry {attempt + 1} CELEX={celex} variant={variant or celex} route={route_name} after HTTP {response.status_code}",
                        flush=True,
                    )
                time.sleep(backoff_s * (2**attempt))
                continue
            return response.status_code, None, f"HTTP {response.status_code}", elapsed, final_url, content_type
        except requests.RequestException as exc:
            last_error = str(exc)
            if trace_routes:
                print(
                    f"[EURLEX TEXT] retry {attempt + 1} CELEX={celex} variant={variant or celex} route={route_name} after {type(exc).__name__}: {exc}",
                    flush=True,
                )
            time.sleep(backoff_s * (2**attempt))
    return 0, None, last_error, 0.0, final_url, content_type


def fetch_eurlex_fulltext_for_row(
    row: pd.Series,
    *,
    cache_dir: Path,
    use_cache: bool = True,
    timeout_s: int = 45,
    retries: int = 4,
    min_interval_s: float = 2.0,
    session: requests.Session | None = None,
    verbose: bool = False,
    trace_routes: bool = False,
    progress_label: str = "",
) -> dict:
    """Batch-oriented ordinary EU full-text retrieval using the old Cellar-based logic."""
    celex_full = str(row.get("celex_full", "") or row.get("celex", "") or "").strip()
    celex_full, celex, celex_version = split_celex_identifier(celex_full)
    celex_meta = classify_celex_for_fulltext(celex_full)
    title = str(row.get("title", "") or "").strip()
    url = str(row.get("url_fix", "") or row.get("url", "") or "").strip()
    langs = lang_candidates_from_row(row)
    sess = session or SESSION

    html_cache_dir = cache_dir / "html_cache"
    text_cache_dir = cache_dir / "text_cache"
    text_path = _cache_path_for_celex(celex_full, text_cache_dir, "txt")
    html_path = _cache_path_for_celex(celex_full, html_cache_dir, "html")

    if use_cache and text_path.exists() and text_path.stat().st_size > 0:
        text_clean = text_path.read_text(encoding="utf-8", errors="replace")
        if verbose:
            print(
                f"[EURLEX TEXT] {progress_label} CELEX={celex} success length={len(text_clean)} source=CACHE",
                flush=True,
            )
        return {
            "celex": celex,
            "celex_full": celex_full,
            "celex_version": celex_version,
            "title": title,
            "url": url,
            "text_source_url": "CACHE",
            "full_text_raw": "",
            "full_text_clean": text_clean,
            "text_len": len(text_clean),
            "retrieval_status": 200,
            "retrieval_error": "",
            "lang": langs[0] if langs else "en",
            "lang_source_fulltext": _lang_source_for_doc_row(row, langs[0] if langs else "en"),
            "fetch_seconds": 0.0,
            "fetched_from_cache": True,
            "text_path": str(text_path),
            "content_type": "",
            **celex_meta,
        }
    if min_interval_s > 0:
        time.sleep(min_interval_s)
    multi_result = get_eurlex_text_multi(
        row,
        session=sess,
        timeout_s=timeout_s,
        retries=retries,
        trace_routes=trace_routes,
    )
    attempt_trace = list(multi_result.get("attempt_trace", []) or [])
    if multi_result.get("full_text_raw"):
        try:
            html_path.write_text(str(multi_result.get("full_text_raw", "")), encoding="utf-8")
        except Exception:
            pass
    if multi_result.get("full_text_clean"):
        try:
            text_path.write_text(str(multi_result.get("full_text_clean", "")), encoding="utf-8")
        except Exception:
            pass
    return {
        "celex": celex,
        "celex_full": celex_full,
        "celex_version": celex_version,
        "title": title,
        "url": url,
        "text_source_url": str(multi_result.get("final_url", "") or ""),
        "full_text_raw": str(multi_result.get("full_text_raw", "") or ""),
        "full_text_clean": str(multi_result.get("full_text_clean", "") or ""),
        "text_len": len(str(multi_result.get("full_text_clean", "") or "")),
        "retrieval_status": int(multi_result.get("status", 0) or 0),
        "retrieval_error": str(multi_result.get("error", "") or ""),
        "lang": str(multi_result.get("lang", langs[0] if langs else "en") or ""),
        "lang_source_fulltext": _lang_source_for_doc_row(row, str(multi_result.get("lang", langs[0] if langs else "en") or "")),
        "fetch_seconds": float(multi_result.get("fetch_seconds", 0.0) or 0.0),
        "fetched_from_cache": False,
        "text_path": str(text_path) if text_path.exists() else "",
        "celex_variant_used": str(multi_result.get("celex_variant_used", "") or ""),
        "route_used": str(multi_result.get("route_used", "cellar") or "cellar"),
        "content_type": str(multi_result.get("content_type", "") or ""),
        "attempt_trace": attempt_trace,
        **celex_meta,
    }


def batch_fetch_eurlex_fulltext(
    df_docs: pd.DataFrame,
    *,
    cache_dir: Path,
    use_cache: bool = True,
    timeout_s: int = 45,
    retries: int = 4,
    min_interval_s: float = 2.0,
    max_docs: int | None = None,
    session: requests.Session | None = None,
    verbose: bool = True,
    trace_routes: bool = False,
    resume: bool = True,
    retry_failures: bool = True,
    progress_every: int = 50,
    cache_every: int = 50,
    success_min_chars: int = 500,
) -> pd.DataFrame:
    """Retrieve full text for a CELEX document table and return a full-text dataframe."""
    if df_docs.empty:
        return pd.DataFrame(
            columns=[
                "celex_full",
                "celex",
                "celex_version",
                "title",
                "url",
                "text_source_url",
                "full_text_raw",
                "full_text_clean",
                "text_len",
                "retrieval_status",
                "retrieval_error",
                "lang",
                "fetch_seconds",
                "fetched_from_cache",
                "text_path",
                "celex_variant_used",
                "route_used",
                "content_type",
            ]
        )

    work_df = df_docs.copy()
    if max_docs is not None:
        work_df = work_df.head(max_docs).copy()

    cache_state_df, _ = summarize_fulltext_cache_state(
        cache_dir,
        work_df,
        success_min_chars=success_min_chars,
    )
    if resume:
        if "celex_full" not in work_df.columns and "celex" in work_df.columns:
            work_df["celex_full"] = work_df["celex"]
        successful_celex = set(cache_state_df.loc[cache_state_df["cache_state"].eq("successful"), "celex_full"].astype(str))
        failed_celex = set(cache_state_df.loc[cache_state_df["cache_state"].eq("failed"), "celex_full"].astype(str))
        keep_mask = ~work_df["celex_full"].astype(str).isin(successful_celex)
        if not retry_failures:
            keep_mask &= ~work_df["celex_full"].astype(str).isin(failed_celex)
        work_df = work_df.loc[keep_mask].copy()

    if verbose:
        n_input = int(df_docs["celex_full"].astype(str).nunique()) if "celex_full" in df_docs.columns else int(df_docs["celex"].astype(str).nunique()) if "celex" in df_docs.columns else len(df_docs)
        n_success_cached = int(cache_state_df["cache_state"].eq("successful").sum())
        n_failed_cached = int(cache_state_df["cache_state"].eq("failed").sum())
        n_pending = int(cache_state_df["cache_state"].eq("pending").sum())
        print("=== EURLEX FULLTEXT RESUME STATE ===", flush=True)
        print(f"Total input CELEX: {n_input}", flush=True)
        print(f"Already successful: {n_success_cached}", flush=True)
        print(f"Previously failed: {n_failed_cached}", flush=True)
        print(f"Still pending: {n_pending}", flush=True)
        print(f"Retry failures: {retry_failures}", flush=True)
        print(f"Run set size: {len(work_df)}", flush=True)

    rows: list[dict] = []
    type_summary_rows: list[dict] = []
    total = len(work_df)
    n_total = total
    n_processed = 0
    n_success = 0
    n_failed = 0
    failure_counts: dict[str, int] = {}
    for idx, (_, row) in enumerate(work_df.iterrows(), start=1):
        result = fetch_eurlex_fulltext_for_row(
            row,
            cache_dir=cache_dir,
            use_cache=use_cache,
            timeout_s=timeout_s,
            retries=retries,
            min_interval_s=min_interval_s,
            session=session,
            verbose=verbose,
            trace_routes=trace_routes,
            progress_label=f"{idx}/{total}",
        )
        rows.append(result)
        n_processed = idx
        celex = str(result.get("celex", "") or "")
        text_len = int(result.get("text_len", 0) or 0)
        if text_len >= success_min_chars:
            n_success += 1
            if verbose:
                print(
                    f"[EURLEX TEXT] {idx}/{total} CELEX={celex} success length={text_len}",
                    flush=True,
                )
        else:
            n_failed += 1
            category = _classify_failure(result)
            failure_counts[category] = failure_counts.get(category, 0) + 1
            if verbose:
                print(
                    f"[EURLEX TEXT] {idx}/{total} CELEX={celex} failed reason={category} length={text_len}",
                    flush=True,
                )
        type_summary_rows.append(
            {
                "celex_sector": str(result.get("celex_sector", "") or ""),
                "celex_sector_label": str(result.get("celex_sector_label", "") or ""),
                "celex_descriptor": str(result.get("celex_descriptor", "") or ""),
                "celex_descriptor_label": str(result.get("celex_descriptor_label", "") or ""),
                "celex_class": str(result.get("celex_class", "") or ""),
                "success": int(text_len >= success_min_chars),
                "failure_reason": "" if text_len >= success_min_chars else category,
            }
        )

        if cache_every > 0 and idx % cache_every == 0:
            cache_path = merge_and_save_fulltext_cache(cache_dir, rows)
            if verbose:
                print(
                    f"[EURLEX TEXT] {n_processed}/{n_total} processed | success={n_success} | failed={n_failed}",
                    flush=True,
                )
                print(f"[EURLEX TEXT] cache saved -> {cache_path.name}", flush=True)

        if verbose and (idx % max(progress_every, 1) == 0 or idx == total):
            print(
                f"[EURLEX TEXT] {n_processed}/{n_total} processed | success={n_success} | failed={n_failed}",
                flush=True,
            )

    if rows:
        cache_path = merge_and_save_fulltext_cache(cache_dir, rows)
        if verbose and (cache_every <= 0 or n_processed % cache_every != 0):
            print(f"[EURLEX TEXT] cache saved -> {cache_path.name}", flush=True)

    if verbose:
        print("=== EURLEX FULLTEXT SUMMARY ===", flush=True)
        print(f"Total documents: {n_total}", flush=True)
        print(f"Successful: {n_success}", flush=True)
        print(f"Failed: {n_failed}", flush=True)
        success_rate = (n_success / n_total * 100.0) if n_total else 0.0
        print(f"Success rate: {success_rate:.1f}%", flush=True)
        for key in ("metadata_rdf_reject", "http_202", "timeout", "empty_text", "parse_error"):
            print(f"{key}: {failure_counts.get(key, 0)}", flush=True)
        for key, count in sorted(failure_counts.items()):
            if key not in {"metadata_rdf_reject", "http_202", "timeout", "empty_text", "parse_error"}:
                print(f"{key}: {count}", flush=True)
    out_df = pd.DataFrame(rows)
    if not out_df.empty:
        out_df["failure_category"] = out_df.apply(_classify_failure, axis=1)
    if type_summary_rows:
        type_df = pd.DataFrame(type_summary_rows)
        type_summary = (
            type_df.groupby(
                ["celex_sector", "celex_sector_label", "celex_descriptor", "celex_descriptor_label", "celex_class"],
                dropna=False,
            )
            .agg(
                attempted=("success", "size"),
                successful=("success", "sum"),
            )
            .reset_index()
        )
        type_summary["failed"] = type_summary["attempted"] - type_summary["successful"]
        top_failures = (
            type_df[type_df["failure_reason"].astype(str).str.strip().ne("")]
            .groupby(["celex_sector", "celex_descriptor", "celex_class", "failure_reason"])
            .size()
            .reset_index(name="n")
            .sort_values(["celex_sector", "celex_descriptor", "celex_class", "n"], ascending=[True, True, True, False])
        )
        if not top_failures.empty:
            top_failures = top_failures.groupby(["celex_sector", "celex_descriptor", "celex_class"], as_index=False).first()
            type_summary = type_summary.merge(
                top_failures.rename(columns={"failure_reason": "top_failure_reason", "n": "top_failure_count"})[
                    ["celex_sector", "celex_descriptor", "celex_class", "top_failure_reason", "top_failure_count"]
                ],
                on=["celex_sector", "celex_descriptor", "celex_class"],
                how="left",
            )
        out_df.attrs["celex_type_summary"] = type_summary.to_dict(orient="records")
    return out_df


def trace_eurlex_fulltext_decision(
    celex_value: str,
    *,
    title: str = "",
    url: str = "",
    lang: str = "en",
    query_langs: str | list[str] | None = None,
    cache_dir: Path,
    use_cache: bool = False,
    timeout_s: int = 45,
    retries: int = 2,
    min_interval_s: float = 0.0,
    session: requests.Session | None = None,
) -> tuple[pd.DataFrame, dict]:
    """Run one CELEX through the route logic and return the attempt trace plus final result."""
    row = pd.Series(
        {
            "celex_full": celex_value,
            "celex": celex_value,
            "title": title,
            "url": url,
            "lang": lang,
            "query_langs": query_langs if isinstance(query_langs, str) or query_langs is None else ",".join(query_langs),
        }
    )
    result = fetch_eurlex_fulltext_for_row(
        row,
        cache_dir=cache_dir,
        use_cache=use_cache,
        timeout_s=timeout_s,
        retries=retries,
        min_interval_s=min_interval_s,
        session=session,
        verbose=False,
        trace_routes=True,
        progress_label="TRACE",
    )
    trace_df = pd.DataFrame(result.get("attempt_trace", []))
    return trace_df, result


def load_eu_fulltext_docs(canonical_all_docs: Path) -> pd.DataFrame:
    all_docs = pd.read_csv(canonical_all_docs, low_memory=False)
    eu_fulltext_df = all_docs[all_docs["jurisdiction"].eq("European Union")].copy()
    eu_fulltext_df["celex_full"] = eu_fulltext_df["doc_uid"].astype(str).apply(extract_celex_token)
    celex_parts = eu_fulltext_df["celex_full"].apply(split_celex_identifier)
    eu_fulltext_df["celex"] = celex_parts.apply(lambda x: x[1])
    eu_fulltext_df["celex_version"] = celex_parts.apply(lambda x: x[2])
    eu_fulltext_df["full_text_source"] = "fallback from persisted EU full-text export"
    eu_fulltext_df["text_missing"] = eu_fulltext_df["full_text_clean"].fillna("").astype(str).str.strip().eq("")
    if "text_len" not in eu_fulltext_df.columns:
        eu_fulltext_df["text_len"] = eu_fulltext_df["full_text_clean"].fillna("").astype(str).str.len()
    return eu_fulltext_df
