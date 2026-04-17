"""Supported ordinary-EU EUR-Lex helper subset used by the public adapter path."""

from __future__ import annotations

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

from policy_corpus_builder.utils.celex import extract_celex_token, parse_celex_to_dict

EURLEX_WS_ENDPOINT = "https://eur-lex.europa.eu/EURLexWebService"
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

UNSUPPORTED_FULLTEXT_SECTORS = {"7", "8"}

SESSION = requests.Session()
SESSION.headers.update(DEFAULT_HEADERS)
try:
    SESSION.trust_env = False
except Exception:
    pass


def chunk_terms(terms: Iterable[str], size: int) -> list[list[str]]:
    terms = list(terms)
    return [terms[i : i + size] for i in range(0, len(terms), size)]


def build_expert_query(
    expert_scope: str,
    group_terms: list[str],
    *,
    fields: tuple[str, ...] = ("TI", "TE"),
) -> str:
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


def _extract_notice_value(
    node: ET.Element,
    field_name: str,
    *,
    preferred_lang: str | None = None,
) -> str:
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


def _extract_celex_fallback(node: ET.Element) -> str:
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


def parse_searchresults(
    xml_text: str,
    *,
    lang_for_url: str = "EN",
) -> tuple[int, int, list[dict], int]:
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


def build_eu_doc_tables(all_eu_rows_df: pd.DataFrame) -> tuple[pd.DataFrame, pd.DataFrame]:
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


def _normalize_cached_clean_text(text: str) -> str:
    if not text:
        return ""
    normalized = str(text).replace("\r\n", "\n").replace("\r", "\n").replace("\ufeff", "")
    normalized = normalized.replace("\xa0", " ")
    normalized = re.sub(r"[ \t\f\v]+", " ", normalized)
    normalized = re.sub(r" *\n *", "\n", normalized)
    normalized = re.sub(r"\n{3,}", "\n\n", normalized).strip()

    lines = normalized.split("\n")
    if lines:
        first_line = lines[0].strip()
        remaining_text = "\n".join(lines[1:]).strip()
        if (
            re.fullmatch(r"[A-Za-z0-9_.-]+\.(?:xml|html|xhtml|txt)", first_line, flags=re.IGNORECASE)
            and remaining_text
            and "official journal of the european union" in remaining_text.lower()
        ):
            normalized = remaining_text

    return normalized.strip()


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


def _parse_lang_list(value: object) -> list[str]:
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


def get_eurlex_text(
    celex: str,
    *,
    lang: str = "en",
    session: requests.Session | None = None,
    timeout_s: int = 45,
    retries: int = 4,
    trace_routes: bool = False,
) -> dict:
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


def split_celex_identifier(celex_value: str) -> tuple[str, str, str]:
    celex_full = str(celex_value or "").strip()
    if not celex_full:
        return "", "", ""
    if "-" in celex_full:
        base, version = celex_full.split("-", 1)
        return celex_full, base, version
    return celex_full, celex_full, ""


def celex_variants(celex: str) -> list[str]:
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


def get_eurlex_text_multi(
    row: pd.Series,
    *,
    session: requests.Session | None = None,
    timeout_s: int = 45,
    retries: int = 4,
    trace_routes: bool = False,
) -> dict:
    celex_full = str(row.get("celex_full", "") or row.get("celex", "") or "").strip()
    celex_full, celex, _celex_version = split_celex_identifier(celex_full)
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


def classify_celex_for_fulltext(celex_value: str) -> dict:
    celex_full, celex_base, celex_version = split_celex_identifier(str(celex_value or ""))
    parsed = parse_celex_to_dict(celex_full)
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
    celex_full, celex_base, celex_version = split_celex_identifier(
        str(result.get("celex_full", "") or result.get("celex", "") or "")
    )
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


def _read_cached_text_file(path_value: object) -> str:
    text_path = str(path_value or "").strip()
    if not text_path:
        return ""
    try:
        return _normalize_cached_clean_text(
            Path(text_path).read_text(encoding="utf-8", errors="replace")
        )
    except Exception:
        return ""


def _build_cached_resume_rows(
    df_docs: pd.DataFrame,
    cache_state_df: pd.DataFrame,
) -> list[dict]:
    if df_docs.empty or cache_state_df.empty:
        return []

    docs_df = df_docs.copy()
    if "celex_full" not in docs_df.columns and "celex" in docs_df.columns:
        docs_df["celex_full"] = docs_df["celex"]
    docs_df["celex_full"] = docs_df["celex_full"].fillna("").astype(str)
    celex_parts = docs_df["celex_full"].apply(split_celex_identifier)
    docs_df["celex"] = celex_parts.apply(lambda x: x[1])
    docs_df["celex_version"] = celex_parts.apply(lambda x: x[2])

    cached_df = cache_state_df.loc[cache_state_df["cache_state"].eq("successful")].copy()
    if cached_df.empty:
        return []
    cached_df["celex_full"] = cached_df["celex_full"].fillna("").astype(str)
    cached_df = cached_df.sort_values(["celex_full", "timestamp"]).drop_duplicates(
        subset=["celex_full"],
        keep="last",
    )

    merged_df = docs_df.merge(
        cached_df[
            [
                "celex_full",
                "celex",
                "celex_version",
                "lang",
                "retrieval_status",
                "retrieval_error",
                "text_len",
                "source_url",
                "timestamp",
                "text_path",
            ]
        ],
        on=["celex_full", "celex", "celex_version"],
        how="inner",
    )

    rows: list[dict] = []
    for _, row in merged_df.iterrows():
        text_clean = _read_cached_text_file(row.get("text_path"))
        if not text_clean.strip():
            continue
        celex_full = str(row.get("celex_full", "") or "").strip()
        celex_meta = classify_celex_for_fulltext(celex_full)
        lang = str(row.get("lang", "") or "").strip() or (
            lang_candidates_from_row(row)[0] if lang_candidates_from_row(row) else "en"
        )
        rows.append(
            {
                "celex_full": celex_full,
                "celex": str(row.get("celex", "") or "").strip(),
                "celex_version": str(row.get("celex_version", "") or "").strip(),
                "title": str(row.get("title", "") or "").strip(),
                "url": str(row.get("url_fix", "") or row.get("url", "") or "").strip(),
                "text_source_url": str(row.get("source_url", "") or "").strip() or "CACHE",
                "full_text_raw": "",
                "full_text_clean": text_clean,
                "text_len": len(text_clean),
                "retrieval_status": int(row.get("retrieval_status", 0) or 0),
                "retrieval_error": str(row.get("retrieval_error", "") or "").strip(),
                "lang": lang,
                "lang_source_fulltext": _lang_source_for_doc_row(row, lang),
                "fetch_seconds": 0.0,
                "fetched_from_cache": True,
                "text_path": str(row.get("text_path", "") or "").strip(),
                "celex_variant_used": "",
                "route_used": "cache_resume",
                "content_type": "",
                **celex_meta,
            }
        )
    return rows


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


def merge_and_save_fulltext_cache(cache_dir: Path, results: Iterable[dict]) -> Path:
    path = _fulltext_cache_csv_path(cache_dir)
    existing = load_fulltext_cache_table(cache_dir)
    new_df = _normalize_fulltext_cache_frame(pd.DataFrame([_cache_row_from_result(r) for r in results]))
    merged = pd.concat([existing, new_df], ignore_index=True)
    merged = _normalize_fulltext_cache_frame(merged)
    path.parent.mkdir(parents=True, exist_ok=True)
    merged.to_csv(path, index=False)
    return path


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
        text_clean = _normalize_cached_clean_text(
            text_path.read_text(encoding="utf-8", errors="replace")
        )
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
    cached_rows: list[dict] = []
    if resume:
        if "celex_full" not in work_df.columns and "celex" in work_df.columns:
            work_df["celex_full"] = work_df["celex"]
        work_df["celex_full"] = work_df["celex_full"].fillna("").astype(str)
        successful_celex = set(cache_state_df.loc[cache_state_df["cache_state"].eq("successful"), "celex_full"].astype(str))
        failed_celex = set(cache_state_df.loc[cache_state_df["cache_state"].eq("failed"), "celex_full"].astype(str))
        cached_docs_df = work_df.loc[work_df["celex_full"].isin(successful_celex)].copy()
        cached_rows = _build_cached_resume_rows(cached_docs_df, cache_state_df)
        keep_mask = ~work_df["celex_full"].astype(str).isin(successful_celex)
        if not retry_failures:
            keep_mask &= ~work_df["celex_full"].astype(str).isin(failed_celex)
        work_df = work_df.loc[keep_mask].copy()

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
        text_len = int(result.get("text_len", 0) or 0)
        if text_len >= success_min_chars:
            n_success += 1
        else:
            n_failed += 1
            category = _classify_failure(result)
            failure_counts[category] = failure_counts.get(category, 0) + 1
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
            merge_and_save_fulltext_cache(cache_dir, rows)

    if rows:
        merge_and_save_fulltext_cache(cache_dir, rows)

    out_df = pd.DataFrame(cached_rows + rows)
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
        out_df.attrs["celex_type_summary"] = type_summary.to_dict(orient="records")
    return out_df


__all__ = [
    "batch_fetch_eurlex_fulltext",
    "build_eu_doc_tables",
    "fetch_eurlex_job",
    "filter_celex_types_for_fulltext",
]
