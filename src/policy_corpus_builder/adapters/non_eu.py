from __future__ import annotations

"""Live non-EU retrieval and full-text helpers."""

from collections import Counter, defaultdict
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass
from html import unescape
from io import BytesIO
from pathlib import Path
import json
import os
import re
import socket
import threading
import time
import urllib.robotparser as robotparser
import xml.etree.ElementTree as ET
from urllib.parse import parse_qs, quote, unquote, urljoin, urlparse, urlunparse
from urllib.request import Request, urlopen

from bs4 import BeautifulSoup
import certifi
import pandas as pd
from pypdf import PdfReader
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

from policy_corpus_builder.normalize.corpus import harmonize_docs
from policy_corpus_builder.query_sets.nid4ocean import (
    NON_EU_SEARCH_TERMS_PRIMARY,
    SOURCE_TO_COUNTRY,
)

try:
    import truststore

    truststore.inject_into_ssl()
    TRUSTSTORE_OK = True
except Exception:
    TRUSTSTORE_OK = False


UA = os.getenv("POLICY_CORPUS_BUILDER_USER_AGENT", "policy-corpus-builder/0.1")
UK_BROWSER_UA = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
    "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124 Safari/537.36"
)


def _headers_for(user_agent: str | None = None) -> dict[str, str]:
    return {
        "User-Agent": (user_agent or UA).strip(),
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
        "Accept-Language": "en-GB,en;q=0.9",
        "Connection": "keep-alive",
    }


def _uk_content_headers(*, user_agent: str | None = None, accept_xml: bool = False) -> dict[str, str]:
    headers = {
        "User-Agent": (user_agent or UK_BROWSER_UA).strip(),
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
        "Accept-Language": "en-GB,en;q=0.9",
        "Connection": "keep-alive",
    }
    if accept_xml:
        headers["Accept"] = "application/xml,text/xml;q=0.9,*/*;q=0.8"
    return headers


def _us_download_headers(*, detail_url: str, user_agent: str | None = None) -> dict[str, str]:
    return {
        "User-Agent": (user_agent or UK_BROWSER_UA).strip(),
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
        "Accept-Language": "en-US,en;q=0.9",
        "Connection": "keep-alive",
        "Referer": detail_url,
    }


HEADERS = {"User-Agent": UA, "Accept-Language": "en-GB,en;q=0.9"}
DEFAULT_HEADERS = _headers_for()

UK_BASE = "https://www.legislation.gov.uk"
UK_DATASETS = ("ukpga", "uksi", "ukla", "asp", "anaw", "wsi", "ssi", "nisr", "nisi", "ukdsi", "sdsi")
AUS_BASE = "https://www.legislation.gov.au"
CA_BASE = "https://www.publications.gc.ca"
NZ_HOSTS = ["www.legislation.govt.nz", "legislation.govt.nz"]
NZ_API_BASE = "https://api.legislation.govt.nz/v0"
US_BASE = "https://api.regulations.gov/v4"
CANADA_CKAN_PACKAGE_SEARCH = "https://open.canada.ca/data/api/3/action/package_search"

CANADA_SKIP_EXTS = {
    ".zip",
    ".gz",
    ".7z",
    ".rar",
    ".csv",
    ".tsv",
    ".xlsx",
    ".xls",
    ".json",
    ".geojson",
    ".shp",
    ".gpkg",
    ".tif",
    ".tiff",
    ".xml",
}
CANADA_SKIP_PATTERNS = [
    r"/tbl/csv/",
    r"/download/.*\.(csv|zip|xlsx|xls|tsv)\b",
    r"\.(csv|zip|xlsx|xls|tsv)\b",
]
_CANADA_SKIP_RE = re.compile("|".join(CANADA_SKIP_PATTERNS), re.IGNORECASE)

_US_DOCID_RE = re.compile(r"/v4/documents/([^/?#]+)", re.IGNORECASE)
_AU_ID_RE = re.compile(r"/(C\d{4}[A-Z]\d{5,}|F\d{4}[A-Z]\d{5,}|L\d{4}[A-Z]\d{5,})", re.IGNORECASE)
_CELEX_RE = re.compile(r"(?:CELEX:|celex%3A|celex%3a)([0-9A-Z]{4,}[0-9A-Z()./]+)", re.IGNORECASE)
_WS_RE = re.compile(r"\s+")
_thread_local = threading.local()


@dataclass(frozen=True, slots=True)
class NonEUQueryRun:
    """In-memory result for one non-EU query pipeline execution."""

    raw_hits_df: pd.DataFrame
    source_log_df: pd.DataFrame
    fulltext_docs_df: pd.DataFrame
    harmonized_docs_df: pd.DataFrame

    @property
    def source_log(self) -> list[dict[str, object]]:
        return self.source_log_df.to_dict(orient="records")


def _is_missing_text(value: object) -> bool:
    text = str(value or "").strip()
    return text == "" or text.lower() in {"nan", "none", "null", "<na>"}


def build_session(
    *,
    total_retries: int = 6,
    backoff_factor: float = 1.0,
    pool_connections: int = 20,
    pool_maxsize: int = 20,
    user_agent: str | None = None,
) -> requests.Session:
    session = requests.Session()
    session.headers.update(_headers_for(user_agent))
    retries = Retry(
        total=total_retries,
        backoff_factor=backoff_factor,
        status_forcelist=[429, 500, 502, 503, 504],
        allowed_methods=["GET", "HEAD"],
        raise_on_status=False,
    )
    adapter = HTTPAdapter(max_retries=retries, pool_connections=pool_connections, pool_maxsize=pool_maxsize)
    session.mount("http://", adapter)
    session.mount("https://", adapter)
    return session


def dns_check(host: str) -> bool:
    try:
        socket.getaddrinfo(host, 443)
        return True
    except Exception:
        return False


def safe_request(
    method: str,
    url: str,
    *,
    session: requests.Session | None = None,
    timeout: int = 30,
    max_tries: int = 3,
    sleep_s: float = 0.5,
    verify: bool | str = True,
    headers: dict[str, str] | None = None,
    allow_redirects: bool = True,
    verbose_err: bool = True,
    **kwargs,
) -> requests.Response | None:
    sess = session or build_session()
    request_headers = headers or HEADERS
    last_err: Exception | None = None
    for i in range(max_tries):
        try:
            return sess.request(
                method,
                url,
                headers=request_headers,
                timeout=timeout,
                verify=verify,
                allow_redirects=allow_redirects,
                **kwargs,
            )
        except Exception as exc:
            last_err = exc
            time.sleep(sleep_s * (i + 1))
    if verbose_err:
        print(f"[REQUEST ERROR] {method.upper()} {url}\n  -> {type(last_err).__name__}: {last_err}")
    return None


def safe_get(url: str, **kwargs) -> requests.Response | None:
    return safe_request("GET", url, **kwargs)


def query_source(url: str, *, timeout_s: int = 60, session: requests.Session | None = None) -> requests.Response:
    response = (session or build_session()).get(url, timeout=timeout_s)
    response.raise_for_status()
    return response


def canonicalize_uk_doc_url(href: str) -> str:
    resolved = urljoin(UK_BASE, href.split("#", 1)[0])
    parsed = urlparse(resolved)
    parts = [part for part in parsed.path.split("/") if part]
    if len(parts) >= 3:
        normalized_path = "/" + "/".join(parts[:3])
    else:
        normalized_path = parsed.path or "/"
    return urlunparse(("https", parsed.netloc or urlparse(UK_BASE).netloc, normalized_path, "", "", ""))


def uk_contents_url(url: str) -> str:
    canonical_url = canonicalize_uk_doc_url(url)
    parsed = urlparse(canonical_url)
    parts = [part for part in parsed.path.split("/") if part]
    if len(parts) >= 3:
        return urlunparse(("https", parsed.netloc or urlparse(UK_BASE).netloc, "/" + "/".join(parts[:3]) + "/contents", "", "", ""))
    return canonical_url


def build_uk_search_feed_url(term: str, *, page: int = 1) -> str:
    query = f'"{term}"' if " " in term else term
    params: list[tuple[str, str]] = [("text", query)]
    for dataset in UK_DATASETS:
        params.append(("type", dataset))
    if page > 1:
        params.append(("page", str(page)))
    query_string = "&".join(f"{quote(key)}={quote(value)}" for key, value in params)
    return f"{UK_BASE}/search/data.feed?{query_string}"


def build_aus_search_url(term: str) -> str:
    term = term.strip()
    quoted_term = quote(f'"{term}"', safe="")
    return f"{AUS_BASE}/search/text({quoted_term},nameAndText,contains)/pointintime(Latest)"


def nz_search_url(base: str, term: str, page: int = 1) -> str:
    query = "&".join(
        [
            f"search_term={quote(term)}",
            "search_field=content",
            f"page={page}",
            "per_page=20",
        ]
    )
    return f"{base.rstrip('/')}/works?{query}"


def nz_legacy_search_url(base: str, term: str, page: int = 1) -> str:
    q = term.replace(" ", "+")
    return (
        f'https://{base}/items/?search_field=content&search_term="{q}"&as%5Bty%5D%5B%5D=act&as%5Bty%5D%5B%5D=secondary_legislation'
        f'&as%5Bty%5D%5B%5D=bill&as%5Bty%5D%5B%5D=amendment_paper&as%5Bt%5D=&as%5Bc%5D="{q}"&as%5Byear_filter_type%5D=single&as%5B'
        f'y%5D=&as%5Byf%5D=&as%5Byt%5D=&as%5Bno%5D=&as%5Ba%5D%5B%5D=&as%5Bac%5D%5B%5D=principal&as%5Bac%5D%5B%5D=amendment&as%5Bast'
        f'%5D%5B%5D=in_force&as%5Bast%5D%5B%5D=not_yet_in_force&as%5Bast%5D%5B%5D=repealed&as%5Baty%5D%5B%5D=public&as%5Baty%5D%5B%'
        f'5D=imperial&as%5Baty%5D%5B%5D=local&as%5Baty%5D%5B%5D=private&as%5Baty%5D%5B%5D=provincial&as%5Bic%5D%5B%5D=principal&as%'
        f'5Bic%5D%5B%5D=amendment&as%5Bic%5D%5B%5D=no_value&as%5Bist%5D%5B%5D=in_force&as%5Bist%5D%5B%5D=expired&as%5Bist%5D%5B%5D='
        f'not_yet_in_force&as%5Bist%5D%5B%5D=revoked&as%5Bist%5D%5B%5D=superseded&as%5Bist%5D%5B%5D=no_value&as%5Bity%5D%5B%5D=regulations'
        f'&as%5Bity%5D%5B%5D=order&as%5Bity%5D%5B%5D=rules&as%5Bity%5D%5B%5D=code&as%5Bity%5D%5B%5D=bylaws&as%5Bity%5D%5B%5D=determination&'
        f'as%5Bity%5D%5B%5D=exemption&as%5Bity%5D%5B%5D=notice&as%5Bity%5D%5B%5D=instrument&as%5Bity%5D%5B%5D=other_type&as%5Bp%5D%5B%5D=Agency'
        f'&as%5Bp%5D%5B%5D=Parliamentary+Counsel+Office&as%5Bp%5D%5B%5D=no_value&as%5Bbst%5D%5B%5D=current&as%5Bbst%5D%5B%5D=enacted&as%5Bbst%5D'
        f'%5B%5D=terminated&as%5Bbty%5D%5B%5D=government&as%5Bbty%5D%5B%5D=local&as%5Bbty%5D%5B%5D=member&as%5Bbty%5D%5B%5D=private&'
        f'commit=Search&page={page}'
    )


def is_valid_nz_legislation_url(url: str) -> bool:
    return bool(re.search(r"https?://(www\.)?legislation\.govt\.nz/(act|regulation|bill)/", url or "", re.I))


def _nz_api_headers(api_key: str, *, user_agent: str | None = None) -> dict[str, str]:
    headers = _headers_for(user_agent)
    headers["X-Api-Key"] = api_key.strip()
    headers["Accept"] = "application/json"
    return headers


def _nz_pick_format_url(formats: list[dict], preferred_type: str) -> str:
    for item in formats:
        if str(item.get("type") or "").strip().lower() == preferred_type:
            return str(item.get("url") or "").strip()
    return ""


def _extract_nz_api_rows(term: str, payload: dict, *, max_per_term: int) -> list[dict]:
    results = payload.get("results") or []
    if not isinstance(results, list):
        return []

    rows: list[dict] = []
    seen_urls: set[str] = set()
    for result in results:
        if not isinstance(result, dict):
            continue
        version = result.get("latest_matching_version") or {}
        if not isinstance(version, dict):
            continue
        formats = version.get("formats") or []
        if not isinstance(formats, list):
            formats = []

        html_url = _nz_pick_format_url(formats, "html")
        pdf_url = _nz_pick_format_url(formats, "pdf")
        xml_url = _nz_pick_format_url(formats, "xml")
        canonical_doc_url = html_url or pdf_url or xml_url
        if not canonical_doc_url or canonical_doc_url in seen_urls:
            continue
        seen_urls.add(canonical_doc_url)

        rows.append(
            {
                "jurisdiction": "New Zealand",
                "source": "NZ",
                "matched_term": term,
                "term": term,
                "doc_url": canonical_doc_url,
                "url": canonical_doc_url,
                "title": str(version.get("title") or "").strip(),
                "doc_uid": str(version.get("version_id") or result.get("work_id") or "").strip(),
                "text_url": html_url,
                "pdf_url": pdf_url,
                "xml_url": xml_url,
            }
        )
        if len(rows) >= max_per_term:
            break
    return rows


def _fetch_nz_documents_via_legacy_site(
    search_terms: list[str],
    *,
    max_per_term: int,
    session: requests.Session,
    sleep_s: float,
    verify: bool | str,
    max_pages: int,
    verbose: bool,
    return_diagnostics: bool,
) -> pd.DataFrame | tuple[pd.DataFrame, pd.DataFrame]:
    rows: list[dict] = []
    diagnostics: list[dict] = []
    host = next((candidate for candidate in NZ_HOSTS if dns_check(candidate)), None)
    if host is None:
        if verbose:
            print("[NZ] No NZ hosts resolve via DNS on this machine/network right now. Skipping NZ.")
        diag_df = pd.DataFrame(
            [
                {
                    "host": "",
                    "term": "",
                    "page": 0,
                    "status_code": None,
                    "candidates_found": 0,
                    "new_urls_kept": 0,
                    "kept_total": 0,
                    "stop_reason": "no_resolvable_host",
                    "request_url": "",
                    "mode": "scrape",
                }
            ]
        )
        empty = _normalize_raw_rows(rows)
        return (empty, diag_df) if return_diagnostics else empty
    if verbose:
        print(f"[NZ] Using legacy site fallback host: {host}")
    for term in search_terms:
        kept = 0
        seen_urls: set[str] = set()
        if verbose:
            print(f"\n[NZ] term='{term}' START (scrape fallback)")
        for page in range(1, max_pages + 1):
            request_url = nz_legacy_search_url(host, term, page=page)
            response = safe_get(request_url, session=session, verify=verify, verbose_err=False)
            if response is None:
                diagnostics.append(
                    {
                        "host": host,
                        "term": term,
                        "page": page,
                        "status_code": None,
                        "candidates_found": 0,
                        "new_urls_kept": 0,
                        "kept_total": kept,
                        "stop_reason": "request_failed",
                        "request_url": request_url,
                        "mode": "scrape",
                    }
                )
                break
            if response.status_code != 200:
                diagnostics.append(
                    {
                        "host": host,
                        "term": term,
                        "page": page,
                        "status_code": response.status_code,
                        "candidates_found": 0,
                        "new_urls_kept": 0,
                        "kept_total": kept,
                        "stop_reason": f"http_{response.status_code}",
                        "request_url": request_url,
                        "mode": "scrape",
                    }
                )
                break
            soup = BeautifulSoup(response.text, "html.parser")
            candidates = [
                urljoin(f"https://{host}", anchor["href"]).split("#", 1)[0]
                for anchor in soup.find_all("a", href=True)
                if is_valid_nz_legislation_url(urljoin(f"https://{host}", anchor["href"]))
            ]
            candidates = list(dict.fromkeys(candidates))
            if not candidates:
                diagnostics.append(
                    {
                        "host": host,
                        "term": term,
                        "page": page,
                        "status_code": response.status_code,
                        "candidates_found": 0,
                        "new_urls_kept": 0,
                        "kept_total": kept,
                        "stop_reason": "no_candidates",
                        "request_url": request_url,
                        "mode": "scrape",
                    }
                )
                break
            new_urls = [item for item in candidates if item not in seen_urls]
            if not new_urls:
                diagnostics.append(
                    {
                        "host": host,
                        "term": term,
                        "page": page,
                        "status_code": response.status_code,
                        "candidates_found": len(candidates),
                        "new_urls_kept": 0,
                        "kept_total": kept,
                        "stop_reason": "no_new_docs",
                        "request_url": request_url,
                        "mode": "scrape",
                    }
                )
                break
            new_kept = 0
            for doc_url in new_urls:
                if kept >= max_per_term:
                    break
                seen_urls.add(doc_url)
                rows.append(
                    {
                        "jurisdiction": "New Zealand",
                        "source": "NZ",
                        "matched_term": term,
                        "term": term,
                        "doc_url": doc_url,
                        "url": doc_url,
                        "title": "",
                    }
                )
                kept += 1
                new_kept += 1
            diagnostics.append(
                {
                    "host": host,
                    "term": term,
                    "page": page,
                    "status_code": response.status_code,
                    "candidates_found": len(candidates),
                    "new_urls_kept": new_kept,
                    "kept_total": kept,
                    "stop_reason": "continue" if kept < max_per_term else "max_per_term_reached",
                    "request_url": request_url,
                    "mode": "scrape",
                }
            )
            if kept >= max_per_term:
                break
            time.sleep(sleep_s)
    result_df = _normalize_raw_rows(rows)
    diagnostics_df = pd.DataFrame(diagnostics)
    return (result_df, diagnostics_df) if return_diagnostics else result_df


def canonical_url(url: str) -> str:
    if not url:
        return ""
    parsed = urlparse(url.strip())
    path = (parsed.path or "").rstrip("/") or "/"
    return urlunparse(("https", (parsed.netloc or "").lower(), path, "", "", ""))


def extract_celex(url: str) -> str:
    match = _CELEX_RE.search(url or "")
    return match.group(1) if match else ""


def doc_key_country(rec: dict) -> str:
    src = (rec.get("source") or "").strip()
    country = SOURCE_TO_COUNTRY.get(src, src or "UNKNOWN")
    url = rec.get("url", "") or rec.get("doc_url", "") or rec.get("api_self", "") or ""
    canonical = canonical_url(url)
    if country == "EU":
        celex = (rec.get("celex") or extract_celex(url) or "").strip()
        return f"EU:{celex}" if celex else f"EU:{canonical}"
    if country == "US":
        doc_id = (rec.get("document_id") or rec.get("api_id") or "").strip()
        if not doc_id:
            match = _US_DOCID_RE.search(url)
            doc_id = match.group(1) if match else ""
        return doc_id or _clean_path_identifier(urlparse(canonical).path)
    if country == "Australia":
        match = _AU_ID_RE.search(url)
        return match.group(1).upper() if match else _clean_path_identifier(urlparse(canonical).path)
    if country in {"United Kingdom", "UK"}:
        parts = [part for part in urlparse(canonical).path.strip("/").split("/") if part]
        if len(parts) >= 3:
            return f"{parts[0]}_{parts[1]}_{parts[2]}"
        return _clean_path_identifier(urlparse(canonical).path)
    if country == "Canada":
        return clean_canada_doc_id(rec, canonical)
    if country == "New Zealand":
        return clean_nz_doc_id(canonical)
    return f"{country}:{canonical}"


def _clean_path_identifier(path: str) -> str:
    text = str(path or "").strip()
    text = text.split("?", 1)[0].split("#", 1)[0]
    text = text.replace(".html", "").replace(".htm", "")
    text = text.strip("/").replace("/", "_")
    text = re.sub(r"_+", "_", text)
    return text.strip("_")


def clean_nz_doc_id(url: str) -> str:
    parsed = urlparse(str(url or ""))
    parts = [part for part in parsed.path.strip("/").split("/") if part]
    if len(parts) >= 4 and parts[0] in {"act", "regulation", "bill"} and re.fullmatch(r"(18|19|20)\d{2}", parts[2]):
        number = parts[3].lstrip("0") or "0"
        return f"{parts[0]}_{parts[2]}_{number}"
    return _clean_path_identifier(parsed.path)


def clean_canada_doc_id(rec: dict, canonical_url_value: str = "") -> str:
    title = str(rec.get("title", "") or "")
    for pattern in [r"\b(SOR|SI|CRC|TR)[/-](\d{4})[-/](\d+)\b", r"\b(DORS|TR)[/-](\d{4})[-/](\d+)\b"]:
        match = re.search(pattern, title, re.I)
        if match:
            prefix = match.group(1).upper()
            year = match.group(2)
            number = str(int(match.group(3)))
            return f"{prefix}_{year}_{number}"
    parsed = urlparse(canonical_url_value or str(rec.get("url", "") or ""))
    return _clean_path_identifier(parsed.path)


def clean_title_from_fulltext_prefix(text: str) -> str:
    text = str(text or "").strip()
    if not text:
        return ""
    prefix = text.split("Skip to main content", 1)[0].strip()
    prefix = prefix.split("Skip to main", 1)[0].strip()
    prefix = _WS_RE.sub(" ", prefix)
    return prefix[:300].strip(" -|:\n\t")


def clean_uk_title(title: str) -> str:
    text = _WS_RE.sub(" ", str(title or "").strip())
    if not text:
        return ""
    text = re.sub(r"^\s*PDF\s+", "", text, flags=re.IGNORECASE)
    text = re.sub(r"\s*-\s*Legislation\.gov\.uk\s*$", "", text, flags=re.IGNORECASE)
    text = re.sub(r"\s*Legislation\.gov\.uk\s*$", "", text, flags=re.IGNORECASE)
    return text.strip(" -|:\n\t")


def clean_canada_title(title: str) -> str:
    text = _WS_RE.sub(" ", str(title or "").strip())
    if not text:
        return ""
    text = re.sub(r"\s*:\s*[A-Z][A-Za-z0-9-]*/[A-Za-z0-9-]*\d{4}[A-Z-]*\s*$", "", text)
    text = re.sub(r"\s+[A-Z][A-Za-z0-9-]*/[A-Za-z0-9-]*\d{4}[A-Z-]*\s*$", "", text)
    text = re.sub(r"\s*:\s*[A-Z][A-Za-z0-9/-]*PDF\s*$", "", text, flags=re.IGNORECASE)
    text = re.sub(r"\s+[A-Z][A-Za-z0-9/-]*PDF\s*$", "", text, flags=re.IGNORECASE)
    return text.strip(" -|:\n\t")


def clean_canada_full_text(text: str) -> str:
    cleaned = str(text or "")
    replacements = {
        "Passer Ã  Â« Ã€ propos de ce site Â»": "",
        "Passer au contenu principal": "",
        "Passer à « À propos de ce site »": "",
        "Language selection FranÃ§ais fr / Gouvernement du Canada": "",
        "Language selection Français fr / Gouvernement du Canada": "",
        "Government of Canada Publications - Canada.ca": "",
        "Page details Report a problem or mistake on this page": "",
        "About this site Government of Canada All contacts Departments and agencies": "",
        "Government of Canada Corporate Social media Mobile applications About Canada.ca Terms and conditions Privacy": "",
        "FranÃ§ais": "Français",
        "Gouvernement du Canada": "Gouvernement du Canada",
        "Ã€": "À",
        "Ã ": "à",
        "Ã©": "é",
        "Ã¨": "è",
        "Ãª": "ê",
        "Ã«": "ë",
        "Ã¢": "â",
        "Ã®": "î",
        "Ã´": "ô",
        "Ã»": "û",
        "Ã§": "ç",
        "â€™": "'",
        "â€œ": '"',
        "â€": '"',
        "â€“": "-",
        "â€”": "-",
    }
    for old, new in replacements.items():
        cleaned = cleaned.replace(old, new)
    cleaned = re.split(r"\bPage details Report a problem or mistake on this page\b", cleaned, maxsplit=1)[0]
    cleaned = re.split(r"\bAbout this site Government of Canada\b", cleaned, maxsplit=1)[0]
    cleaned = re.split(r"\bAll contacts Departments and agencies About government\b", cleaned, maxsplit=1)[0]
    cleaned = re.sub(
        r"^\s*.*?(Language selection Français fr / Gouvernement du Canada|Language selection FranÃ§ais fr / Gouvernement du Canada|Search Search Canada\.ca Search Menu Main Menu)\s*",
        "",
        cleaned,
        flags=re.IGNORECASE,
    )
    cleaned = _WS_RE.sub(" ", cleaned)
    return cleaned.strip()


def _extract_canada_asset_links(landing_url: str, html: str) -> list[tuple[str, str]]:
    soup = BeautifulSoup(html or "", "html.parser")
    seen: set[str] = set()
    results: list[tuple[str, str]] = []

    def add_candidate(url: str, mode: str) -> None:
        if not url or url in seen:
            return
        seen.add(url)
        results.append((url, mode))

    for anchor in soup.find_all("a", href=True):
        href = str(anchor["href"]).strip()
        anchor_text = _WS_RE.sub(" ", anchor.get_text(" ")).strip().lower()
        if not href:
            continue
        full = urljoin(landing_url, href)
        parsed = urlparse(full)
        host = (parsed.netloc or "").lower()
        lower = full.lower()
        if not (host.endswith("publications.gc.ca") or host.endswith("canada.ca")):
            continue
        if lower.endswith("/publication.html"):
            continue
        if lower.endswith(".pdf"):
            add_candidate(full, "pdf")
        elif lower.endswith((".html", ".htm")):
            if (
                "/marcxml" in lower
                or "/similarsubjects" in lower
                or "/contact/" in lower
                or "/browse/" in lower
            ):
                continue
            if "html" in anchor_text:
                add_candidate(full, "html")

    if not results:
        for match in re.findall(r"https?://[^\s\"'<>]+\.pdf(?:\?[^\s\"'<>]*)?", html or "", flags=re.IGNORECASE):
            add_candidate(match, "pdf")
        for match in re.findall(r"https?://[^\s\"'<>]+\.html?(?:\?[^\s\"'<>]*)?", html or "", flags=re.IGNORECASE):
            parsed = urlparse(match)
            host = (parsed.netloc or "").lower()
            lower = match.lower()
            if not (host.endswith("publications.gc.ca") or host.endswith("canada.ca")):
                continue
            if lower.endswith("/publication.html"):
                continue
            add_candidate(match, "html")

    return results


def _extract_aus_embedded_text_assets(wrapper_url: str, html: str) -> list[str]:
    soup = BeautifulSoup(html or "", "html.parser")
    wrapper_parsed = urlparse(wrapper_url)
    wrapper_parts = [part for part in wrapper_parsed.path.split("/") if part]
    doc_id = wrapper_parts[0] if wrapper_parts else ""
    seen: set[str] = set()
    ranked: list[tuple[int, str]] = []

    def add_candidate(candidate_url: str) -> None:
        parsed = urlparse(candidate_url)
        parts = [part for part in parsed.path.split("/") if part]
        lower = candidate_url.lower()
        if parsed.netloc.lower() != wrapper_parsed.netloc.lower():
            return
        if doc_id and (not parts or parts[0] != doc_id):
            return
        if not ("/text/original/epub/" in lower or "/text/1/epub/" in lower or re.search(r"/text/\d+/epub/", lower)):
            return
        match = re.search(r"/document_(\d+)/document_\1\.html$", parsed.path, re.IGNORECASE)
        if not match:
            return
        normalized = parsed._replace(fragment="", query="").geturl()
        if normalized in seen:
            return
        seen.add(normalized)
        ranked.append((int(match.group(1)), normalized))

    for anchor in soup.find_all("a", href=True):
        href = str(anchor["href"]).strip()
        if not href:
            continue
        add_candidate(urljoin(wrapper_url, href))

    for match in re.findall(
        r"https?://[^\s\"'<>]+/document_\d+/document_\d+\.html(?:#[^\s\"'<>]*)?",
        html or "",
        flags=re.IGNORECASE,
    ):
        add_candidate(match)

    for match in re.findall(
        r"//[^\s\"'<>]+/document_\d+/document_\d+\.html(?:#[^\s\"'<>]*)?",
        html or "",
        flags=re.IGNORECASE,
    ):
        add_candidate(urljoin("https:", match))

    ranked.sort(key=lambda item: item[0])
    return [url for _, url in ranked]


def _canada_ckan_package_url(package: dict) -> str:
    package_id = str(package.get("name") or package.get("id") or "").strip()
    if not package_id:
        return ""
    return f"https://open.canada.ca/data/en/dataset/{package_id}"


def _normalize_canada_ckan_resource_url(url: str) -> str:
    text = str(url or "").strip()
    if not text:
        return ""
    if text.startswith(("http://", "https://")):
        return text
    return urljoin("https://open.canada.ca", text)


def _canada_resource_is_probably_english(resource: dict) -> bool:
    raw_language = resource.get("language")
    if isinstance(raw_language, list):
        language = ",".join(str(item).strip().lower() for item in raw_language if str(item).strip())
    else:
        language = str(raw_language or "").strip().lower()
    if language in {"eng", "en", "english", ""}:
        pass
    elif language in {"fra", "fr", "french"}:
        return False
    url = _normalize_canada_ckan_resource_url(resource.get("url") or "").lower()
    name = str(resource.get("name") or "").strip().lower()
    negative_markers = ("_f.pdf", "-f.pdf", "_fra", "-fra", "/fr/", "francais", "français")
    return not any(marker in url or marker in name for marker in negative_markers)


def _score_canada_ckan_resource(resource: dict) -> int:
    url = _normalize_canada_ckan_resource_url(resource.get("url") or "")
    if not url or should_skip_canada_url(url):
        return -10_000
    fmt = str(resource.get("format") or "").strip().upper()
    if fmt not in {"PDF", "HTML", "HTM"}:
        return -10_000

    score = 0
    if fmt == "PDF":
        score += 50
    else:
        score += 30

    lower = url.lower()
    host = (urlparse(url).netloc or "").lower()
    if "open.canada.ca/data/dataset/" in lower and "/download/" in lower:
        score += 40
    elif host.endswith("publications.gc.ca"):
        score += 35
    elif host.endswith("canada.ca"):
        score += 25

    if _canada_resource_is_probably_english(resource):
        score += 10
    else:
        score -= 100

    return score


def _extract_canada_ckan_rows(term: str, packages: list[dict], *, max_per_term: int) -> list[dict]:
    rows: list[dict] = []
    seen_urls: set[str] = set()
    for package in packages:
        resources = package.get("resources") or []
        scored_resources = sorted(
            (
                (resource, _score_canada_ckan_resource(resource))
                for resource in resources
                if isinstance(resource, dict)
            ),
            key=lambda item: item[1],
            reverse=True,
        )
        best_resource = next((resource for resource, score in scored_resources if score > 0), None)
        if best_resource is None:
            continue
        resource_url = _normalize_canada_ckan_resource_url(best_resource.get("url") or "")
        if not resource_url or resource_url in seen_urls:
            continue
        seen_urls.add(resource_url)
        rows.append(
            {
                "jurisdiction": "Canada",
                "source": "CA",
                "matched_term": term,
                "term": term,
                "doc_url": _canada_ckan_package_url(package),
                "url": resource_url,
                "title": clean_canada_title(str(package.get("title") or best_resource.get("name") or "").strip()),
                "doc_uid": str(best_resource.get("id") or package.get("id") or "").strip(),
            }
        )
        if len(rows) >= max_per_term:
            break
    return rows


def _fetch_canada_documents_via_ckan(
    term: str,
    *,
    max_per_term: int,
    session: requests.Session,
    verify: bool | str,
) -> list[dict]:
    response = safe_get(
        CANADA_CKAN_PACKAGE_SEARCH,
        session=session,
        verify=verify,
        verbose_err=False,
        params={
            "q": term,
            "fq": "type:info",
            "rows": max(max_per_term * 3, 25),
        },
    )
    if response is None or response.status_code != 200:
        return []
    try:
        payload = json.loads(response.text or "{}")
    except json.JSONDecodeError:
        return []
    if not payload.get("success"):
        return []
    packages = ((payload.get("result") or {}).get("results") or [])
    if not isinstance(packages, list):
        return []
    return _extract_canada_ckan_rows(term, packages, max_per_term=max_per_term)


def _fetch_canada_documents_via_publications_search(
    term: str,
    *,
    max_per_term: int,
    session: requests.Session,
    verify: bool | str,
    sleep_s: float,
) -> list[dict]:
    rows: list[dict] = []
    q = quote(f'"{term}"' if " " in term else term, safe="")
    url = f"{CA_BASE}/site/eng/search/search.html?ast={q}&cnst=&adof=on"
    response = safe_get(url, session=session, verify=verify, verbose_err=False)
    if response is None or response.status_code != 200:
        return rows
    soup = BeautifulSoup(response.text, "html.parser")
    candidates: list[tuple[str, str]] = []
    for anchor in soup.find_all("a", href=True):
        href = anchor["href"].strip()
        if "search/search.html" in href:
            continue
        full = urljoin(CA_BASE, href)
        if "publications.gc.ca" not in full:
            continue
        path = urlparse(full).path.lower()
        if path.endswith("/home.html") or path.endswith("/browse/index.html") or "/search/" in path:
            continue
        if full.lower().endswith(".pdf") or "/site/eng/" in full:
            candidates.append((full, anchor.get_text(" ").strip()))
    kept = 0
    for doc_url, title in list(dict.fromkeys(candidates)):
        if kept >= max_per_term:
            break
        rows.append(
            {
                "jurisdiction": "Canada",
                "source": "CA",
                "matched_term": term,
                "term": term,
                "doc_url": doc_url,
                "url": doc_url,
                "title": title,
            }
        )
        kept += 1
    time.sleep(sleep_s)
    return rows


def _extract_pdf_text(pdf_bytes: bytes) -> str:
    try:
        reader = PdfReader(BytesIO(pdf_bytes))
    except Exception:
        return ""
    parts: list[str] = []
    for page in reader.pages:
        try:
            text = page.extract_text() or ""
        except Exception:
            text = ""
        if text.strip():
            parts.append(text.strip())
    return _WS_RE.sub(" ", " ".join(parts)).strip()


def infer_title(row: pd.Series | dict) -> str:
    title = str((row.get("title") if isinstance(row, dict) else row.get("title", "")) or "").strip()
    if not _is_missing_text(title):
        jurisdiction = str((row.get("jurisdiction") if isinstance(row, dict) else row.get("jurisdiction", "")) or "").strip()
        if jurisdiction in {"United Kingdom", "UK"}:
            return clean_uk_title(title)
        if jurisdiction == "Canada":
            return clean_canada_title(title)
        return title
    jurisdiction = str((row.get("jurisdiction") if isinstance(row, dict) else row.get("jurisdiction", "")) or "").strip()
    text = str((row.get("full_text_clean") if isinstance(row, dict) else row.get("full_text_clean", "")) or "").strip()
    if jurisdiction in {"United Kingdom", "New Zealand"} and text:
        inferred = clean_title_from_fulltext_prefix(text)
        if jurisdiction in {"United Kingdom", "UK"}:
            return clean_uk_title(inferred)
        return inferred
    return ""


def infer_year_from_url(url: str) -> str:
    text = str(url or "")
    if not text:
        return ""
    match = re.search(r"/((?:18|19|20)\d{2})/", text)
    return match.group(0).strip("/") if match else ""


def infer_year_from_title(title: str) -> str:
    text = str(title or "")
    matches = re.findall(r"(?<!\d)((?:18|19|20)\d{2})(?!\d)", text)
    return matches[-1] if matches else ""


def normalize_non_eu_identifiers(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty:
        return df.copy()
    out = df.copy()
    if "country" not in out.columns:
        if "jurisdiction" in out.columns:
            out["country"] = out["jurisdiction"]
        else:
            out["country"] = out.get("source", "")
    if "source" not in out.columns:
        out["source"] = out["country"]
    if "jurisdiction" not in out.columns:
        out["jurisdiction"] = out["country"]
    if "url" not in out.columns:
        out["url"] = ""
    if "doc_id" not in out.columns:
        out["doc_id"] = ""
    out["doc_id"] = out.apply(
        lambda row: doc_key_country(
            {
                "source": row.get("source", ""),
                "jurisdiction": row.get("jurisdiction", ""),
                "url": row.get("url", ""),
                "doc_url": row.get("doc_url", ""),
                "api_self": row.get("api_self", ""),
                "title": row.get("title", ""),
                "api_id": row.get("api_id", ""),
                "document_id": row.get("document_id", ""),
            }
        ),
        axis=1,
    )
    return out


def add_date_metadata(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty:
        return df.copy()
    out = normalize_non_eu_identifiers(df)
    out["title"] = out.apply(infer_title, axis=1)
    url_series = out["url"] if "url" in out.columns else pd.Series("", index=out.index)
    title_year = out["title"].fillna("").astype(str).map(infer_year_from_title)
    url_year = url_series.fillna("").astype(str).map(infer_year_from_url)
    source_series = out["source"].fillna("").astype(str)
    prefer_title = source_series.isin(["Australia", "AUS", "AU", "Canada", "CA", "US"])
    out["year"] = url_year
    out.loc[prefer_title, "year"] = title_year.loc[prefer_title]
    out["year"] = out["year"].where(out["year"].astype(str).str.len().gt(0), title_year)
    out["year"] = out["year"].where(out["year"].astype(str).str.len().gt(0), url_year)
    if "full_text_clean" in out.columns:
        text_year = out["full_text_clean"].fillna("").astype(str).map(lambda text: infer_year_from_title(clean_title_from_fulltext_prefix(text)))
        out["year"] = out["year"].where(out["year"].astype(str).str.len().gt(0), text_year)
    out["year"] = pd.to_numeric(out["year"], errors="coerce").astype("Int64")
    out["date"] = out["year"].astype("string")
    return out


def _normalize_raw_rows(rows: list[dict]) -> pd.DataFrame:
    if not rows:
        return pd.DataFrame(
            columns=["source", "jurisdiction", "url", "doc_url", "text_url", "title", "doc_uid", "lang", "date", "year", "term", "matched_term", "doc_id", "api_id", "api_self"]
        )
    df = pd.DataFrame(rows).copy()
    if "matched_term" not in df.columns:
        df["matched_term"] = ""
    if "term" not in df.columns:
        df["term"] = df["matched_term"]
    else:
        df["term"] = df["term"].fillna(df["matched_term"])
    if "url" not in df.columns:
        df["url"] = ""
    for fallback in ["text_url", "doc_url", "api_self"]:
        if fallback in df.columns:
            mask = df["url"].fillna("").astype(str).str.strip().eq("") & df[fallback].fillna("").astype(str).str.strip().ne("")
            df.loc[mask, "url"] = df.loc[mask, fallback]
    if "lang" not in df.columns:
        df["lang"] = "en"
    df["lang"] = df["lang"].fillna("en")
    if "doc_id" not in df.columns:
        df["doc_id"] = df.apply(lambda row: doc_key_country(row.to_dict()), axis=1)
    if "doc_uid" not in df.columns:
        df["doc_uid"] = df["url"].fillna("")
    else:
        df["doc_uid"] = df["doc_uid"].fillna(df["url"].fillna(""))
    return add_date_metadata(df)

def fetch_uk_documents(
    search_terms: list[str],
    *,
    max_per_term: int = 500,
    session: requests.Session | None = None,
    sleep_s: float = 0.25,
    verify: bool | str | None = None,
) -> pd.DataFrame:
    sess = session or build_session()
    verify = certifi.where() if verify is None else verify
    link_re = re.compile(r"^/(" + "|".join(map(re.escape, UK_DATASETS)) + r")/\d{4}/\d+", re.I)
    rows: list[dict] = []
    for term in search_terms:
        kept = 0
        page = 1
        seen_urls: set[str] = set()
        while kept < max_per_term:
            q = f'"{term}"' if " " in term else term
            url = f"{UK_BASE}/all?text={quote(q)}"
            if page > 1:
                url += f"&page={page}"
            response = safe_get(url, session=sess, verify=verify, verbose_err=False)
            if response is None or response.status_code != 200:
                break
            soup = BeautifulSoup(response.text, "html.parser")
            page_urls = [
                canonicalize_uk_doc_url(anchor["href"].strip())
                for anchor in soup.find_all("a", href=True)
                if link_re.match(anchor["href"].strip())
            ]
            page_urls = list(dict.fromkeys(page_urls))
            if not page_urls:
                break
            new_urls = [item for item in page_urls if item not in seen_urls]
            if not new_urls:
                break
            for doc_url in new_urls:
                if kept >= max_per_term:
                    break
                seen_urls.add(doc_url)
                rows.append(
                    {
                        "jurisdiction": "United Kingdom",
                        "source": "UK",
                        "matched_term": term,
                        "term": term,
                        "doc_url": doc_url,
                        "url": doc_url,
                        "title": "",
                    }
                )
                kept += 1
            page += 1
            time.sleep(sleep_s)
    return _normalize_raw_rows(rows)


def fetch_aus_documents(
    search_terms: list[str],
    *,
    max_per_term: int = 500,
    session: requests.Session | None = None,
    sleep_s: float = 0.25,
    verify: bool | str | None = None,
) -> pd.DataFrame:
    sess = session or build_session()
    verify = certifi.where() if verify is None else verify
    href_re = re.compile(r"^/(?:C|F)\d{4}[A-Z]\d{5}(?:/(?:asmade|latest|compilation|made|repealed|superseded))?$", re.I)
    rows: list[dict] = []
    for term in search_terms:
        response = safe_get(build_aus_search_url(term), session=sess, verify=verify, verbose_err=False)
        if response is None or response.status_code != 200:
            continue
        soup = BeautifulSoup(response.text, "html.parser")
        candidates = [
            (anchor["href"].strip().rstrip("/"), anchor.get_text(" ").strip())
            for anchor in soup.find_all("a", href=True)
            if href_re.match(anchor["href"].strip())
        ]
        kept = 0
        for href, title in list(dict.fromkeys(candidates)):
            if kept >= max_per_term:
                break
            doc_url = urljoin(AUS_BASE, href)
            rows.append(
                {
                    "jurisdiction": "Australia",
                    "source": "AUS",
                    "matched_term": term,
                    "term": term,
                    "doc_url": doc_url,
                    "text_url": doc_url.rstrip("/") + "/text",
                    "url": doc_url.rstrip("/") + "/text",
                    "title": title,
                }
            )
            kept += 1
        time.sleep(sleep_s)
    return _normalize_raw_rows(rows)


def fetch_canada_documents(
    search_terms: list[str],
    *,
    max_per_term: int = 500,
    session: requests.Session | None = None,
    sleep_s: float = 0.25,
    verify_ssl_with_certifi: bool = True,
) -> pd.DataFrame:
    sess = session or build_session()
    verify = certifi.where() if verify_ssl_with_certifi else True
    rows: list[dict] = []
    for term in search_terms:
        term_rows = _fetch_canada_documents_via_ckan(
            term,
            max_per_term=max_per_term,
            session=sess,
            verify=verify,
        )
        if len(term_rows) < max_per_term:
            fallback_rows = _fetch_canada_documents_via_publications_search(
                term,
                max_per_term=max_per_term - len(term_rows),
                session=sess,
                verify=verify,
                sleep_s=sleep_s,
            )
            seen_urls = {str(row.get("url") or "").strip() for row in term_rows}
            for row in fallback_rows:
                url = str(row.get("url") or "").strip()
                if not url or url in seen_urls:
                    continue
                seen_urls.add(url)
                term_rows.append(row)
                if len(term_rows) >= max_per_term:
                    break
        rows.extend(term_rows)
    return _normalize_raw_rows(rows)


def fetch_nz_documents(
    search_terms: list[str],
    *,
    api_key: str | None = None,
    mode: str = "auto",
    max_per_term: int = 500,
    session: requests.Session | None = None,
    sleep_s: float = 0.25,
    verify: bool | str | None = None,
    max_pages: int = 5,
    user_agent: str | None = None,
    verbose: bool = True,
    return_diagnostics: bool = False,
) -> pd.DataFrame | tuple[pd.DataFrame, pd.DataFrame]:
    sess = session or build_session()
    verify = certifi.where() if verify is None else verify
    rows: list[dict] = []
    diagnostics: list[dict] = []
    resolved_api_key = (api_key or "").strip()
    resolved_mode = str(mode or "auto").strip().lower()
    if resolved_mode not in {"auto", "api", "scrape"}:
        raise ValueError("NZ retrieval mode must be one of: auto, api, scrape")
    use_api = bool(resolved_api_key) and resolved_mode in {"auto", "api"}
    if verbose:
        print("\n========== NZ retrieval ==========")
        print(f"terms: {len(search_terms)} | max_per_term: {max_per_term} | max_pages: {max_pages}")
    if not use_api:
        if resolved_mode == "api":
            if verbose:
                print("[NZ] API mode requested but no NZ legislation API key is configured; skipping NZ.")
            diag_df = pd.DataFrame(
                [
                    {
                        "host": "api.legislation.govt.nz",
                        "term": "",
                        "page": 0,
                        "status_code": 401,
                        "candidates_found": 0,
                        "new_urls_kept": 0,
                        "kept_total": 0,
                        "stop_reason": "missing_api_key",
                        "request_url": f"{NZ_API_BASE}/works",
                        "mode": "api",
                    }
                ]
            )
            empty = _normalize_raw_rows(rows)
            return (empty, diag_df) if return_diagnostics else empty
        if verbose:
            print("[NZ] No NZ API key configured; using legacy scraper fallback.")
        return _fetch_nz_documents_via_legacy_site(
            search_terms,
            max_per_term=max_per_term,
            session=sess,
            sleep_s=sleep_s,
            verify=verify,
            max_pages=max_pages,
            verbose=verbose,
            return_diagnostics=return_diagnostics,
        )
    if verbose:
        print("[NZ] Using official API: api.legislation.govt.nz/v0/works")
    for term in search_terms:
        kept = 0
        if verbose:
            print(f"\n[NZ] term='{term}' START")
        for page in range(1, max_pages + 1):
            request_url = nz_search_url(NZ_API_BASE, term, page=page)
            if verbose:
                print(f"[NZ] term='{term}' page={page} -> {request_url}")
            response = safe_get(
                request_url,
                session=sess,
                verify=verify,
                verbose_err=False,
                headers=_nz_api_headers(resolved_api_key, user_agent=user_agent),
            )
            if response is None:
                if verbose:
                    print(f"[NZ] term='{term}' page={page} ERROR -> request failed; stopping this term")
                diagnostics.append(
                    {
                        "host": "api.legislation.govt.nz",
                        "term": term,
                        "page": page,
                        "status_code": None,
                        "candidates_found": 0,
                        "new_urls_kept": 0,
                        "kept_total": kept,
                        "stop_reason": "request_failed",
                        "request_url": request_url,
                        "mode": "api",
                    }
                )
                break
            if response.status_code == 403:
                if verbose:
                    print(f"[NZ] term='{term}' page={page} -> HTTP 403 blocked; skipping NZ term")
                diagnostics.append(
                    {
                        "host": "api.legislation.govt.nz",
                        "term": term,
                        "page": page,
                        "status_code": 403,
                        "candidates_found": 0,
                        "new_urls_kept": 0,
                        "kept_total": kept,
                        "stop_reason": "http_403",
                        "request_url": request_url,
                        "mode": "api",
                    }
                )
                break
            if response.status_code != 200:
                if verbose:
                    print(f"[NZ] term='{term}' page={page} ERROR -> HTTP {response.status_code}; stopping this term")
                diagnostics.append(
                    {
                        "host": "api.legislation.govt.nz",
                        "term": term,
                        "page": page,
                        "status_code": response.status_code,
                        "candidates_found": 0,
                        "new_urls_kept": 0,
                        "kept_total": kept,
                        "stop_reason": f"http_{response.status_code}",
                        "request_url": request_url,
                        "mode": "api",
                    }
                )
                break
            try:
                payload = response.json()
            except ValueError:
                payload = {}
            total_results = payload.get("total")
            response_page = payload.get("page", page)
            response_per_page = payload.get("per_page", len(payload.get("results") or []))
            page_rows = _extract_nz_api_rows(term, payload, max_per_term=max_per_term - kept)
            if not page_rows:
                if verbose:
                    print(f"[NZ] term='{term}' page={page} -> no candidates; stopping")
                diagnostics.append(
                    {
                        "host": "api.legislation.govt.nz",
                        "term": term,
                        "page": page,
                        "status_code": response.status_code,
                        "candidates_found": 0,
                        "new_urls_kept": 0,
                        "kept_total": kept,
                        "stop_reason": "no_candidates",
                        "request_url": request_url,
                        "mode": "api",
                    }
                )
                break
            new_kept = 0
            for row in page_rows:
                if kept >= max_per_term:
                    break
                rows.append(row)
                kept += 1
                new_kept += 1
            diagnostics.append(
                {
                    "host": "api.legislation.govt.nz",
                    "term": term,
                    "page": page,
                    "status_code": response.status_code,
                    "candidates_found": len(page_rows),
                    "new_urls_kept": new_kept,
                    "kept_total": kept,
                    "stop_reason": "continue" if kept < max_per_term else "max_per_term_reached",
                    "request_url": request_url,
                    "mode": "api",
                }
            )
            if verbose:
                print(f"[NZ] term='{term}' page={page} -> candidates={len(candidates)} new_kept={new_kept} kept_total={kept}")
            if kept >= max_per_term:
                if verbose:
                    print(f"[NZ] term='{term}' reached max_per_term={max_per_term}; stopping")
                break
            if (
                isinstance(total_results, int)
                and isinstance(response_page, int)
                and isinstance(response_per_page, int)
                and response_per_page > 0
                and response_page * response_per_page >= total_results
            ):
                if verbose:
                    print(f"[NZ] term='{term}' reached final API page; stopping")
                break
            time.sleep(sleep_s)
        if verbose:
            print(f"[NZ] term='{term}' DONE -> kept={kept}")
    if verbose:
        print(f"\n[NZ] total rows kept: {len(rows)}")
    result_df = _normalize_raw_rows(rows)
    diagnostics_df = pd.DataFrame(diagnostics)
    return (result_df, diagnostics_df) if return_diagnostics else result_df


def fetch_us_documents(
    search_terms: list[str],
    *,
    api_key: str | None = None,
    max_per_term: int = 500,
    page_size: int = 250,
    session: requests.Session | None = None,
    sleep_s: float = 0.25,
) -> pd.DataFrame:
    api_key = api_key or os.getenv("REGULATIONS_GOV_API_KEY", "")
    if not api_key:
        raise RuntimeError("US live retrieval requires REGULATIONS_GOV_API_KEY or api_key.")
    sess = session or build_session()
    rows: list[dict] = []
    for term in search_terms:
        kept = 0
        page = 1
        while kept < max_per_term:
            request_page_size = max(5, min(page_size, max_per_term - kept))
            params = {
                "filter[searchTerm]": f'"{term}"' if " " in term else term,
                "page[size]": request_page_size,
                "page[number]": page,
                "api_key": api_key,
            }
            response = safe_get(f"{US_BASE}/documents", session=sess, params=params, verbose_err=False)
            if response is None or response.status_code != 200:
                break
            data = (response.json() or {}).get("data", []) or []
            if not data:
                break
            for item in data:
                if kept >= max_per_term:
                    break
                attrs = item.get("attributes", {}) or {}
                api_self = ((item.get("links") or {}).get("self", "") or "").strip()
                rows.append(
                    {
                        "jurisdiction": "United States",
                        "source": "US",
                        "matched_term": term,
                        "term": term,
                        "api_id": item.get("id", "") or "",
                        "api_self": api_self,
                        "doc_url": api_self,
                        "url": api_self,
                        "title": attrs.get("title") or attrs.get("documentTitle") or "",
                        "document_id": item.get("id", "") or "",
                    }
                )
                kept += 1
            page += 1
            time.sleep(sleep_s)
    return _normalize_raw_rows(rows)


def fetch_non_eu_all(
    search_terms: list[str],
    *,
    sources: tuple[str, ...] = ("UK", "AUS", "NZ", "CA", "US"),
    nz_api_key: str | None = None,
    nz_mode: str = "auto",
    us_api_key: str | None = None,
    max_per_term: int = 500,
    user_agent: str | None = None,
) -> tuple[pd.DataFrame, pd.DataFrame]:
    session = build_session()
    verify_default = certifi.where()
    frames: list[pd.DataFrame] = []
    logs: list[dict] = []
    source_map = {
        "UK": lambda: fetch_uk_documents(search_terms, max_per_term=max_per_term, session=session, verify=verify_default),
        "AUS": lambda: fetch_aus_documents(search_terms, max_per_term=max_per_term, session=session, verify=verify_default),
        "NZ": lambda: fetch_nz_documents(
            search_terms,
            api_key=nz_api_key,
            mode=nz_mode,
            max_per_term=max_per_term,
            session=session,
            verify=verify_default,
            user_agent=user_agent,
        ),
        "CA": lambda: fetch_canada_documents(search_terms, max_per_term=max_per_term, session=session),
        "US": lambda: fetch_us_documents(search_terms, api_key=us_api_key, max_per_term=max_per_term, session=session),
    }
    country_labels = {"UK": "United Kingdom", "AUS": "Australia", "NZ": "New Zealand", "CA": "Canada", "US": "United States"}
    for source in sources:
        try:
            df = source_map[source]()
            frames.append(df)
            logs.append({"source": source, "country": country_labels[source], "ok": True, "rows": len(df), "error": ""})
        except Exception as exc:
            logs.append({"source": source, "country": country_labels[source], "ok": False, "rows": 0, "error": str(exc)})
    combined = pd.concat(frames, ignore_index=True) if frames else _normalize_raw_rows([])
    if not combined.empty:
        combined = combined.drop_duplicates(subset=["doc_id", "term", "source", "url"]).reset_index(drop=True)
    return combined, pd.DataFrame(logs)


def aggregate_one_row_per_doc(records: list[dict]) -> list[dict]:
    def pick_better(a: str, b: str) -> str:
        a = a or ""
        b = b or ""
        if not a.strip():
            return b
        if not b.strip():
            return a
        return b if len(b) > len(a) else a

    aggregated: dict[str, dict] = {}
    for rec in records:
        key = doc_key_country(rec)
        term = rec.get("term") or rec.get("matched_term")
        if key not in aggregated:
            base = dict(rec)
            base["doc_key"] = key
            base["matched_terms"] = set()
            if base.get("url"):
                base["url"] = canonical_url(str(base["url"]))
            aggregated[key] = base
        current = aggregated[key]
        if term:
            current["matched_terms"].add(term)
        current["title"] = pick_better(current.get("title", ""), rec.get("title", ""))
        for col in [
            "lang",
            "celex",
            "date",
            "format",
            "public_timestamp",
            "description",
            "doc_url",
            "text_url",
            "xml_url",
            "pdf_url",
            "api_self",
        ]:
            if not current.get(col) and rec.get(col):
                current[col] = rec[col]
        current.setdefault("sources", set()).add(rec.get("source"))
    out: list[dict] = []
    for value in aggregated.values():
        value["matched_terms"] = sorted(value["matched_terms"])
        value["sources"] = sorted([src for src in value.get("sources", []) if src])
        value.pop("term", None)
        out.append(value)
    return out

def split_by_country(raw_records: list[dict]) -> dict[str, list[dict]]:
    by_country: defaultdict[str, list[dict]] = defaultdict(list)
    for rec in raw_records:
        src = (rec.get("source") or "").strip()
        by_country[SOURCE_TO_COUNTRY.get(src, src or "UNKNOWN")].append(rec)
    return dict(by_country)


def build_and_save_country_dfs(
    raw_records: list[dict] | pd.DataFrame,
    *,
    out_dir: str | Path,
    fmt: str = "csv",
) -> dict[str, str]:
    Path(out_dir).mkdir(parents=True, exist_ok=True)
    records = raw_records.to_dict(orient="records") if isinstance(raw_records, pd.DataFrame) else raw_records
    buckets = split_by_country(records)
    paths: dict[str, str] = {}
    for country, recs in sorted(buckets.items()):
        df = pd.DataFrame(recs)
        df["country"] = country
        safe_name = country.replace(" ", "_").lower()
        path = Path(out_dir) / f"nid_policy_{safe_name}.{fmt}"
        if fmt == "parquet":
            df.to_parquet(path, index=False)
        elif fmt == "csv":
            df.to_csv(path, index=False)
        else:
            raise ValueError("fmt must be 'csv' or 'parquet'")
        paths[country] = str(path)
    return paths


class RobotsCache:
    def __init__(self, user_agent: str = UA, default_allow: bool = True):
        self.user_agent = user_agent
        self.default_allow = default_allow
        self._cache: dict[str, robotparser.RobotFileParser | None] = {}
        self._lock = threading.Lock()

    def allowed(self, url: str) -> bool:
        try:
            parsed = urlparse(url)
            if not parsed.scheme or not parsed.netloc:
                return False
            robots_url = f"{parsed.scheme}://{parsed.netloc}/robots.txt"
        except Exception:
            return False
        with self._lock:
            parser = self._cache.get(robots_url, "MISSING")
        if parser == "MISSING":
            parser = robotparser.RobotFileParser()
            parser.set_url(robots_url)
            try:
                response = requests.get(robots_url, headers=_headers_for(self.user_agent), timeout=20, verify=certifi.where())
                if response.status_code >= 400:
                    parser = None
                else:
                    parser.parse(response.text.splitlines())
            except Exception:
                parser = None
            with self._lock:
                self._cache[robots_url] = parser
        if parser is None:
            return self.default_allow
        try:
            return parser.can_fetch(self.user_agent, url)
        except Exception:
            return False


def _get_thread_session(user_agent: str | None = None) -> requests.Session:
    session = getattr(_thread_local, "session", None)
    session_user_agent = getattr(_thread_local, "session_user_agent", None)
    desired_user_agent = (user_agent or UA).strip()
    if session is None or session_user_agent != desired_user_agent:
        session = build_session(user_agent=desired_user_agent)
        _thread_local.session = session
        _thread_local.session_user_agent = desired_user_agent
    return session


def _get_thread_robots(user_agent: str | None = None) -> RobotsCache:
    robots = getattr(_thread_local, "robots", None)
    robots_user_agent = getattr(_thread_local, "robots_user_agent", None)
    desired_user_agent = (user_agent or UA).strip()
    if robots is None or robots_user_agent != desired_user_agent:
        robots = RobotsCache(user_agent=desired_user_agent)
        _thread_local.robots = robots
        _thread_local.robots_user_agent = desired_user_agent
    return robots


def should_skip_canada_url(url: str) -> bool:
    if not url:
        return False
    path = urlparse(url).path.lower()
    for ext in CANADA_SKIP_EXTS:
        if path.endswith(ext):
            return True
    return bool(_CANADA_SKIP_RE.search(url.lower()))


def html_to_visible_text(html: str) -> str:
    soup = BeautifulSoup(html or "", "html.parser")
    for tag in soup(["script", "style", "noscript", "svg"]):
        tag.decompose()
    return _WS_RE.sub(" ", soup.get_text(" ")).strip()


def uk_xml_to_text(xml: str) -> str:
    try:
        root = ET.fromstring(xml or "")
    except ET.ParseError:
        return ""

    skip_tags = {"Metadata", "Versions", "Contents", "Commentaries"}

    def local_name(tag: str) -> str:
        if "}" in tag:
            return tag.rsplit("}", 1)[-1]
        if ":" in tag:
            return tag.rsplit(":", 1)[-1]
        return tag

    parts: list[str] = []

    def visit(element: ET.Element, *, skip: bool = False) -> None:
        current_skip = skip or local_name(element.tag) in skip_tags
        if not current_skip and element.text and element.text.strip():
            parts.append(element.text.strip())
        for child in element:
            visit(child, skip=current_skip)
            if not current_skip and child.tail and child.tail.strip():
                parts.append(child.tail.strip())

    visit(root)
    text = "\n".join(parts)
    text = re.sub(r"\n{3,}", "\n\n", text)
    text = _WS_RE.sub(" ", text)
    return text.strip()


def canonical_source(src: str) -> str:
    text = (src or "").strip().lower()
    if "aus" in text or "australia" in text:
        return "AUS"
    if "uk" in text or "united kingdom" in text or "legislation.gov.uk" in text:
        return "UK"
    if "publications" in text or "canada" in text or text == "ca":
        return "CA"
    if text.startswith("nz") or "new zealand" in text or "legislation.govt.nz" in text:
        return "NZ"
    if "regulations" in text or "united states" in text or text == "us":
        return "US"
    return src or "UNKNOWN"


def ensure_url_in_record(rec: dict) -> str:
    for key in ("url", "text_url", "doc_url", "api_self", "docUrl", "link", "href"):
        value = rec.get(key)
        if isinstance(value, str) and value.strip():
            return value.strip()
    return ""


def _is_waf_challenge_response(response: requests.Response | None) -> bool:
    if response is None:
        return False
    waf_action = str(response.headers.get("x-amzn-waf-action", "") or "").strip().lower()
    return response.status_code == 202 or waf_action == "challenge"


def get_url_candidates(rec: dict, src: str, us_api_key: str | None) -> list[tuple[str, str]]:
    url = ensure_url_in_record(rec)
    if src == "AUS":
        text_url = (rec.get("text_url") or "").strip()
        doc_url = (rec.get("doc_url") or url or "").strip()
        candidates: list[tuple[str, str]] = []
        if text_url:
            candidates.append((text_url, "aus_text_page"))
        if doc_url:
            if not doc_url.rstrip("/").endswith("/text"):
                candidates.append((doc_url.rstrip("/") + "/text", "aus_text_page"))
            candidates.append((doc_url, "html"))
        return candidates
    if src == "UK":
        if not url:
            return []
        parsed = urlparse(canonicalize_uk_doc_url(url))
        base_parts = [part for part in parsed.path.split("/") if part]
        if len(base_parts) >= 3:
            doc_root = "/" + "/".join(base_parts[:3])
            candidates = [
                (urlunparse(("https", parsed.netloc, f"{doc_root}/data.xml", "", "", "")), "uk_xml"),
                (urlunparse(("https", parsed.netloc, f"{doc_root}/made/data.xml", "", "", "")), "uk_xml"),
                (urlunparse(("https", parsed.netloc, f"{doc_root}/enacted/data.xml", "", "", "")), "uk_xml"),
                (urlunparse(("https", parsed.netloc, f"{doc_root}/data.xht", "", "", "")), "html"),
                (urlunparse(("https", parsed.netloc, f"{doc_root}/made/data.xht", "", "", "")), "html"),
                (urlunparse(("https", parsed.netloc, f"{doc_root}/enacted/data.xht", "", "", "")), "html"),
                (urlunparse(("https", parsed.netloc, f"{doc_root}/contents", "", "", "")), "html"),
                (urlunparse(("https", parsed.netloc, f"{doc_root}/made", "", "", "")), "html"),
                (urlunparse(("https", parsed.netloc, f"{doc_root}/enacted", "", "", "")), "html"),
                (urlunparse(("https", parsed.netloc, f"{doc_root}/contents/made", "", "", "")), "html"),
            ]
            seen: set[str] = set()
            deduped: list[tuple[str, str]] = []
            for candidate in candidates:
                if candidate[0] in seen:
                    continue
                seen.add(candidate[0])
                deduped.append(candidate)
            return deduped
        return [(url, "html")]
    if src == "CA":
        if not url:
            return []
        lower = url.lower()
        if lower.endswith("/publication.html"):
            return [(url, "ca_publication")]
        if lower.endswith(".pdf"):
            return [(url, "pdf")]
        return [(url, "html")]
    if src == "NZ":
        candidates: list[tuple[str, str]] = []
        xml_url = (rec.get("xml_url") or "").strip()
        pdf_url = (rec.get("pdf_url") or "").strip()
        text_url = (rec.get("text_url") or "").strip()
        doc_url = (rec.get("doc_url") or url or "").strip()
        if xml_url:
            candidates.append((xml_url, "nz_xml"))
        if pdf_url:
            candidates.append((pdf_url, "pdf"))
        if text_url:
            candidates.append((text_url, "html"))
        if doc_url and doc_url not in {candidate_url for candidate_url, _ in candidates}:
            candidates.append((doc_url, "html"))
        return candidates
    if src == "US":
        api_self = (rec.get("api_self") or url or "").strip()
        candidates = []
        if api_self:
            candidates.append((api_self, "us_api_json"))
        web_url = (rec.get("doc_url") or rec.get("url") or "").strip()
        if web_url and web_url != api_self:
            candidates.append((web_url, "html"))
        return candidates
    return [(url, "html")] if url else []


def us_json_to_text(js: dict) -> str:
    data = js.get("data") or {}
    attrs = (data.get("attributes") or {}) if isinstance(data, dict) else {}
    parts: list[str] = []
    for key in ("title", "documentType", "agencyId", "docketId", "postedDate", "commentDueDate", "rin"):
        value = attrs.get(key)
        if value:
            parts.append(f"{key}: {value}")
    for key in ("summary", "abstract", "documentAbstract", "additionalRins"):
        value = attrs.get(key)
        if value:
            parts.append(str(value))
    return "\n".join(parts).strip()


def _normalize_us_file_format_entries(
    entries: list[dict] | None,
    *,
    source_kind: str,
    attachment_title: str = "",
    attachment_order: int = 0,
) -> list[dict[str, object]]:
    normalized: list[dict[str, object]] = []
    for entry in entries or []:
        if not isinstance(entry, dict):
            continue
        file_url = str(entry.get("fileUrl") or "").strip()
        fmt = str(entry.get("format") or "").strip().lower()
        if not file_url or not fmt:
            continue
        normalized.append(
            {
                "file_url": file_url,
                "format": fmt,
                "source_kind": source_kind,
                "attachment_title": attachment_title,
                "attachment_order": attachment_order,
            }
        )
    return normalized


def _score_us_download_candidate(candidate: dict[str, object]) -> tuple[int, int, int, int, str]:
    file_url = str(candidate.get("file_url") or "")
    fmt = str(candidate.get("format") or "").lower()
    source_kind = str(candidate.get("source_kind") or "")
    lower_url = file_url.lower()

    source_score = 200 if source_kind == "document" else 100
    format_score = {
        "htm": 50,
        "html": 50,
        "txt": 45,
        "pdf": 40,
        "xml": 20,
        "docx": 10,
        "doc": 5,
    }.get(fmt, -1000)
    content_bonus = 20 if "content." in lower_url else 0
    attachment_order = int(candidate.get("attachment_order") or 0)
    attachment_bias = -attachment_order if source_kind == "attachment" else 0

    return (source_score, format_score, content_bonus, attachment_bias, file_url)


def extract_us_download_candidates(detail_payload: dict) -> list[dict[str, object]]:
    data = detail_payload.get("data") or {}
    if not isinstance(data, dict):
        return []

    candidates: list[dict[str, object]] = []
    attrs = data.get("attributes") or {}
    if isinstance(attrs, dict):
        candidates.extend(
            _normalize_us_file_format_entries(
                attrs.get("fileFormats"),
                source_kind="document",
            )
        )

    included = detail_payload.get("included") or []
    if isinstance(included, list):
        for item in included:
            if not isinstance(item, dict) or item.get("type") != "attachments":
                continue
            attachment_attrs = item.get("attributes") or {}
            if not isinstance(attachment_attrs, dict):
                continue
            candidates.extend(
                _normalize_us_file_format_entries(
                    attachment_attrs.get("fileFormats"),
                    source_kind="attachment",
                    attachment_title=str(attachment_attrs.get("title") or "").strip(),
                    attachment_order=int(attachment_attrs.get("docOrder") or 0),
                )
            )

    deduped: dict[str, dict[str, object]] = {}
    for candidate in sorted(candidates, key=_score_us_download_candidate, reverse=True):
        file_url = str(candidate.get("file_url") or "")
        deduped.setdefault(file_url, candidate)
    return list(deduped.values())


def enrich_one_record_fulltext(
    rec: dict,
    *,
    us_api_key: str | None,
    obey_robots: bool = True,
    timeout: int = 40,
    user_agent: str | None = None,
) -> dict:
    out = dict(rec)
    out.setdefault("full_text", "")
    out.setdefault("full_text_url", "")
    out.setdefault("full_text_error", "")
    out.setdefault("full_text_format", "")
    out.setdefault("full_text_path", "")
    src = canonical_source(out.get("source") or out.get("jurisdiction") or out.get("country") or "")
    out["source_canonical"] = src
    candidates = get_url_candidates(out, src, us_api_key)
    if not candidates:
        out["full_text_error"] = "no_url_candidate"
        return out
    request_headers = _uk_content_headers(user_agent=user_agent) if src == "UK" else _headers_for(user_agent)
    session = _get_thread_session(user_agent)
    robots = _get_thread_robots(user_agent)
    last_err = ""
    for candidate_url, mode in candidates:
        try:
            if obey_robots and not robots.allowed(candidate_url):
                last_err = f"robots_disallow: {candidate_url}"
                continue
            if src == "CA" and should_skip_canada_url(candidate_url):
                last_err = "skipped candidate: data file (zip/csv/xlsx/etc.)"
                continue
            if mode == "aus_text_page":
                response = session.get(candidate_url, timeout=timeout, verify=certifi.where(), headers=request_headers)
                response.raise_for_status()
                asset_urls = _extract_aus_embedded_text_assets(candidate_url, response.text)
                if asset_urls:
                    try:
                        parts: list[str] = []
                        for asset_url in asset_urls:
                            if obey_robots and not robots.allowed(asset_url):
                                last_err = f"robots_disallow: {asset_url}"
                                continue
                            asset_response = session.get(asset_url, timeout=timeout, verify=certifi.where(), headers=request_headers)
                            asset_response.raise_for_status()
                            text = html_to_visible_text(asset_response.text)
                            if text:
                                parts.append(text)
                        combined = "\n\n".join(part for part in parts if part).strip()
                        if combined:
                            out["full_text"] = combined
                            out["full_text_url"] = asset_urls[0] if len(asset_urls) == 1 else json.dumps(asset_urls, ensure_ascii=False)
                            out["full_text_format"] = "html"
                            out["full_text_error"] = ""
                            return out
                        last_err = "aus_embedded_html_empty"
                    except Exception as exc:
                        last_err = f"{type(exc).__name__}: {exc}"
                text = html_to_visible_text(response.text)
                if text:
                    out["full_text"] = text
                    out["full_text_url"] = candidate_url
                    out["full_text_format"] = "html"
                    out["full_text_error"] = ""
                    return out
                last_err = "html_empty"
                continue
            if mode == "ca_publication":
                response = session.get(candidate_url, timeout=timeout, verify=certifi.where(), headers=request_headers)
                response.raise_for_status()
                asset_candidates = _extract_canada_asset_links(candidate_url, response.text)
                for asset_url, asset_mode in asset_candidates:
                    try:
                        if obey_robots and not robots.allowed(asset_url):
                            last_err = f"robots_disallow: {asset_url}"
                            continue
                        if should_skip_canada_url(asset_url):
                            last_err = "skipped candidate: data file (zip/csv/xlsx/etc.)"
                            continue
                        if asset_mode == "pdf":
                            asset_response = session.get(asset_url, timeout=timeout, verify=certifi.where(), headers=request_headers)
                            asset_response.raise_for_status()
                            content_type = str(asset_response.headers.get("content-type", "") or "").lower()
                            if "pdf" not in content_type and asset_response.content[:5].lower() != b"%pdf-":
                                last_err = "canada_pdf_unavailable"
                                continue
                            text = clean_canada_full_text(_extract_pdf_text(asset_response.content))
                            if text:
                                out["full_text"] = text
                                out["full_text_url"] = asset_url
                                out["full_text_format"] = "pdf"
                                out["full_text_error"] = ""
                                return out
                            last_err = "canada_pdf_empty"
                            continue
                        asset_response = session.get(asset_url, timeout=timeout, verify=certifi.where(), headers=request_headers)
                        asset_response.raise_for_status()
                        text = clean_canada_full_text(html_to_visible_text(asset_response.text))
                        if text:
                            out["full_text"] = text
                            out["full_text_url"] = asset_url
                            out["full_text_format"] = "html"
                            out["full_text_error"] = ""
                            return out
                        last_err = "canada_asset_html_empty"
                    except Exception as exc:
                        last_err = f"{type(exc).__name__}: {exc}"
                text = clean_canada_full_text(html_to_visible_text(response.text))
                if text:
                    out["full_text"] = text
                    out["full_text_url"] = candidate_url
                    out["full_text_format"] = "html"
                    out["full_text_error"] = ""
                    return out
                last_err = "canada_landing_page_empty"
                continue
            if mode == "pdf":
                response = session.get(candidate_url, timeout=timeout, verify=certifi.where(), headers=request_headers)
                response.raise_for_status()
                content_type = str(response.headers.get("content-type", "") or "").lower()
                if "pdf" not in content_type and response.content[:5].lower() != b"%pdf-":
                    last_err = "pdf_unavailable"
                    continue
                text = _extract_pdf_text(response.content)
                if src == "CA":
                    text = clean_canada_full_text(text)
                if text:
                    out["full_text"] = text
                    out["full_text_url"] = candidate_url
                    out["full_text_format"] = "pdf"
                    out["full_text_error"] = ""
                    return out
                last_err = "pdf_empty"
                continue
            if mode == "us_api_json":
                headers = dict(request_headers)
                if us_api_key:
                    headers["X-Api-Key"] = us_api_key
                response = session.get(
                    candidate_url,
                    headers=headers,
                    params={"include": "attachments"},
                    timeout=timeout,
                    verify=certifi.where(),
                )
                response.raise_for_status()
                detail_payload = response.json()
                file_candidates = extract_us_download_candidates(detail_payload)
                for file_candidate in file_candidates:
                    file_url = str(file_candidate.get("file_url") or "")
                    file_format = str(file_candidate.get("format") or "").lower()
                    if not file_url:
                        continue
                    try:
                        if obey_robots and not robots.allowed(file_url):
                            last_err = f"robots_disallow: {file_url}"
                            continue
                        download_response = session.get(
                            file_url,
                            headers=_us_download_headers(detail_url=candidate_url, user_agent=user_agent),
                            timeout=timeout,
                            verify=certifi.where(),
                        )
                        download_response.raise_for_status()
                        if file_format in {"pdf"}:
                            content_type = str(download_response.headers.get("content-type", "") or "").lower()
                            if "pdf" not in content_type and download_response.content[:5].lower() != b"%pdf-":
                                last_err = "us_pdf_unavailable"
                                continue
                            text = _extract_pdf_text(download_response.content)
                            if text:
                                out["full_text"] = text
                                out["full_text_url"] = file_url
                                out["full_text_format"] = "pdf"
                                out["full_text_error"] = ""
                                return out
                            last_err = "us_pdf_empty"
                            continue
                        if file_format in {"htm", "html", "xml"}:
                            text = html_to_visible_text(download_response.text)
                            if text:
                                out["full_text"] = text
                                out["full_text_url"] = file_url
                                out["full_text_format"] = "html"
                                out["full_text_error"] = ""
                                return out
                            last_err = "us_html_empty"
                            continue
                        if file_format in {"txt"}:
                            text = str(download_response.text or "").strip()
                            if text:
                                out["full_text"] = text
                                out["full_text_url"] = file_url
                                out["full_text_format"] = "txt"
                                out["full_text_error"] = ""
                                return out
                            last_err = "us_txt_empty"
                            continue
                    except Exception as exc:
                        last_err = f"{type(exc).__name__}: {exc}"
                text = us_json_to_text(detail_payload)
                if text:
                    out["full_text"] = text
                    out["full_text_url"] = candidate_url
                    out["full_text_format"] = "json"
                    out["full_text_error"] = ""
                    return out
                last_err = "us_api_json_empty"
                continue
            if mode == "uk_xml":
                response = session.get(
                    candidate_url,
                    timeout=timeout,
                    verify=certifi.where(),
                    headers=_uk_content_headers(user_agent=user_agent, accept_xml=True),
                )
                if _is_waf_challenge_response(response):
                    last_err = "waf_challenge"
                    continue
                response.raise_for_status()
                text = uk_xml_to_text(response.text)
                if text:
                    out["full_text"] = text
                    out["full_text_url"] = candidate_url
                    out["full_text_format"] = "uk_xml"
                    out["full_text_error"] = ""
                    return out
                last_err = "uk_xml_empty"
                continue
            if mode == "nz_xml":
                response = session.get(
                    candidate_url,
                    timeout=timeout,
                    verify=certifi.where(),
                    headers=_headers_for(user_agent),
                )
                if _is_waf_challenge_response(response):
                    last_err = "waf_challenge"
                    continue
                response.raise_for_status()
                text = uk_xml_to_text(response.text)
                if text:
                    out["full_text"] = text
                    out["full_text_url"] = candidate_url
                    out["full_text_format"] = "nz_xml"
                    out["full_text_error"] = ""
                    return out
                last_err = "nz_xml_empty"
                continue
            response = session.get(candidate_url, timeout=timeout, verify=certifi.where(), headers=request_headers)
            if _is_waf_challenge_response(response):
                last_err = "waf_challenge"
                continue
            response.raise_for_status()
            text = html_to_visible_text(response.text)
            if src == "CA":
                text = clean_canada_full_text(text)
            if text:
                out["full_text"] = text
                out["full_text_url"] = candidate_url
                out["full_text_format"] = "html"
                out["full_text_error"] = ""
                return out
            last_err = "html_empty"
        except Exception as exc:
            last_err = f"{type(exc).__name__}: {exc}"
    out["full_text_error"] = last_err or "unknown_error"
    return out


def add_full_texts_parallel(
    records: list[dict],
    *,
    us_api_key: str | None,
    max_workers: int = 12,
    progress_every: int = 25,
    obey_robots: bool = True,
    user_agent: str | None = None,
) -> list[dict]:
    if not records:
        return []
    out: list[dict] = []
    errors = 0
    ok = 0
    counter: Counter[str] = Counter()
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        futures = [
            executor.submit(
                enrich_one_record_fulltext,
                rec,
                us_api_key=us_api_key,
                obey_robots=obey_robots,
                user_agent=user_agent,
            )
            for rec in records
        ]
        for idx, future in enumerate(as_completed(futures), start=1):
            try:
                enriched = future.result()
            except Exception as exc:
                errors += 1
                counter[type(exc).__name__] += 1
                continue
            out.append(enriched)
            if enriched.get("full_text"):
                ok += 1
            else:
                errors += 1
                counter[str(enriched.get("full_text_error", "error"))[:120]] += 1
            if progress_every and (idx % progress_every == 0 or idx == len(futures)):
                print(f"[PROGRESS] {idx}/{len(futures)} | ok={ok} | errors={errors}")
    if counter:
        print("[ERROR SUMMARY]")
        for label, count in counter.most_common(15):
            print(f"{count}x {label}")
    return out


def build_non_eu_doc_tables(all_non_eu_rows_df: pd.DataFrame) -> tuple[pd.DataFrame, pd.DataFrame]:
    raw_hits = add_date_metadata(_normalize_raw_rows(all_non_eu_rows_df.to_dict(orient="records")))

    def uniq_sorted(series: pd.Series) -> str:
        vals = sorted({str(item).strip() for item in series.dropna().tolist() if str(item).strip()})
        return json.dumps(vals, ensure_ascii=False)

    def pick_best_title(series: pd.Series) -> str:
        vals = [str(item).strip() for item in series.dropna().tolist() if str(item).strip()]
        return max(vals, key=len) if vals else ""

    def pick_first_nonempty(series: pd.Series) -> str:
        for value in series.dropna().tolist():
            text = str(value).strip()
            if text:
                return text
        return ""

    docs = (
        raw_hits.groupby(["doc_id", "jurisdiction"], as_index=False)
        .agg(
            country=("country", pick_first_nonempty),
            doc_uid=("doc_uid", pick_first_nonempty),
            title=("title", pick_best_title),
            url=("url", pick_first_nonempty),
            lang=("lang", pick_first_nonempty),
            source=("source", pick_first_nonempty),
            date=("date", pick_first_nonempty),
            year=("year", pick_first_nonempty),
            matched_terms=("term", uniq_sorted),
        )
    )
    return raw_hits, docs


def build_non_eu_fulltext_docs(
    raw_hits_df: pd.DataFrame,
    *,
    us_api_key: str | None = None,
    max_workers: int = 12,
    progress_every: int = 25,
    obey_robots: bool = True,
    user_agent: str | None = None,
) -> pd.DataFrame:
    resolved_us_api_key = us_api_key or os.getenv("REGULATIONS_GOV_API_KEY", "")
    if raw_hits_df.empty:
        return pd.DataFrame(
            columns=["doc_id", "country", "jurisdiction", "doc_uid", "title", "url", "lang", "date", "year", "source_file", "full_text_clean", "text_len", "has_text", "retrieval_status", "full_text_url", "full_text_error", "full_text_format", "source"]
        )
    grouped_docs = aggregate_one_row_per_doc(raw_hits_df.to_dict(orient="records"))
    enriched = add_full_texts_parallel(
        grouped_docs,
        us_api_key=resolved_us_api_key,
        max_workers=max_workers,
        progress_every=progress_every,
        obey_robots=obey_robots,
        user_agent=user_agent,
    )
    df = pd.DataFrame(enriched)
    if df.empty:
        return pd.DataFrame()
    df["full_text_clean"] = df["full_text"].fillna("").astype(str)
    df = add_date_metadata(df)
    df["country"] = df["jurisdiction"]
    df["text_len"] = df["full_text_clean"].str.len()
    df["has_text"] = df["text_len"].gt(0)
    df["retrieval_status"] = "missing_text"
    df.loc[df["has_text"], "retrieval_status"] = "ok"
    df.loc[df["full_text_error"].fillna("").astype(str).str.len().gt(0), "retrieval_status"] = "error"
    df.loc[df["full_text_error"].eq("waf_challenge"), "retrieval_status"] = "upstream_blocked"
    df["source_file"] = df["full_text_url"].fillna("")
    df["doc_uid"] = df["doc_id"]
    if "doc_url" in df.columns:
        df["url"] = df["url"].fillna(df["doc_url"]).fillna("")
    else:
        df["url"] = df["url"].fillna("")
    for column in ["jurisdiction", "title", "lang", "source"]:
        if column not in df.columns:
            df[column] = ""
    ordered = ["doc_id", "country", "jurisdiction", "doc_uid", "title", "url", "lang", "date", "year", "source_file", "full_text_clean", "text_len", "has_text", "retrieval_status", "full_text_url", "full_text_error", "full_text_format", "source"]
    for column in ordered:
        if column not in df.columns:
            df[column] = ""
    return df[ordered].sort_values(["jurisdiction", "doc_id"]).reset_index(drop=True)


def reconstruct_non_eu_hits_from_cache(
    canonical_all_docs: Path,
    term_inventory: list[str] | None = None,
    *,
    jurisdiction: str | None = None,
) -> tuple[pd.DataFrame, pd.DataFrame]:
    term_inventory = term_inventory or NON_EU_SEARCH_TERMS_PRIMARY
    all_docs = pd.read_csv(canonical_all_docs, low_memory=False)
    subset = all_docs[all_docs["jurisdiction"].ne("European Union")].copy()
    if jurisdiction:
        subset = subset[subset["jurisdiction"].eq(jurisdiction)].copy()
    raw_records: list[dict] = []
    for row in subset.itertuples(index=False):
        text = f"{getattr(row, 'title', '')} {getattr(row, 'full_text_clean', '')}".lower()
        matched = [term for term in term_inventory if term.lower() in text]
        if not matched:
            matched = [""]
        for term in matched:
            jurisdiction_value = getattr(row, "jurisdiction", "")
            url_value = getattr(row, "url", "")
            fallback_doc_id = doc_key_country({"source": jurisdiction_value, "jurisdiction": jurisdiction_value, "url": url_value})
            raw_records.append(
                {
                    "source": jurisdiction_value,
                    "jurisdiction": jurisdiction_value,
                    "url": url_value,
                    "title": getattr(row, "title", ""),
                    "doc_uid": getattr(row, "doc_uid", ""),
                    "lang": getattr(row, "lang", "en"),
                    "term": term,
                    "matched_term": term,
                    "doc_id": fallback_doc_id,
                }
            )
    raw_hits = _normalize_raw_rows(raw_records)
    raw_hits = add_date_metadata(raw_hits)
    if "date" not in subset.columns:
        subset["date"] = ""
    if "year" not in subset.columns:
        subset["year"] = ""
    subset["country"] = subset["jurisdiction"]
    subset["doc_id"] = subset.apply(
        lambda row: doc_key_country({"source": row.get("jurisdiction", ""), "jurisdiction": row.get("jurisdiction", ""), "url": row.get("url", "")}),
        axis=1,
    )
    subset["retrieval_status"] = "missing_text"
    subset.loc[subset["has_text"].fillna(False), "retrieval_status"] = "ok"
    fulltext_docs = subset[["doc_id", "country", "jurisdiction", "doc_uid", "title", "url", "lang", "date", "year", "source_file", "full_text_clean", "text_len", "has_text", "retrieval_status"]].drop_duplicates(subset=["doc_id"]).reset_index(drop=True)
    fulltext_docs = add_date_metadata(fulltext_docs)
    return raw_hits, fulltext_docs


def summarize_non_eu_docs(non_eu_raw_hits: pd.DataFrame) -> pd.DataFrame:
    _, docs = build_non_eu_doc_tables(non_eu_raw_hits)
    return docs[["doc_id", "country", "jurisdiction", "doc_uid", "title", "url", "lang", "date", "year", "matched_terms"]].copy()


def run_non_eu_query_pipeline(
    query_text: str,
    *,
    countries: tuple[str, ...] = ("UK",),
    nz_api_key: str | None = None,
    nz_mode: str = "auto",
    us_api_key: str | None = None,
    max_per_term: int = 100,
    max_workers: int = 4,
    progress_every: int = 0,
    obey_robots: bool = True,
    user_agent: str | None = None,
) -> NonEUQueryRun:
    """Run one real non-EU retrieval query through retrieval, full text, and harmonization."""

    resolved_us_api_key = us_api_key or os.getenv("REGULATIONS_GOV_API_KEY", "")

    raw_hits_df, source_log_df = fetch_non_eu_all(
        [query_text],
        sources=countries,
        nz_api_key=nz_api_key,
        nz_mode=nz_mode,
        us_api_key=resolved_us_api_key,
        max_per_term=max_per_term,
        user_agent=user_agent,
    )
    fulltext_docs_df = build_non_eu_fulltext_docs(
        raw_hits_df,
        us_api_key=resolved_us_api_key,
        max_workers=max_workers,
        progress_every=progress_every,
        obey_robots=obey_robots,
        user_agent=user_agent,
    )

    if not fulltext_docs_df.empty:
        doc_summary_df = summarize_non_eu_docs(raw_hits_df)
        if not doc_summary_df.empty:
            fulltext_docs_df = fulltext_docs_df.merge(
                doc_summary_df[["doc_id", "matched_terms"]],
                on="doc_id",
                how="left",
            )
        harmonized_docs_df = harmonize_docs(fulltext_docs_df)
    else:
        harmonized_docs_df = fulltext_docs_df.copy()

    return NonEUQueryRun(
        raw_hits_df=raw_hits_df,
        source_log_df=source_log_df,
        fulltext_docs_df=fulltext_docs_df,
        harmonized_docs_df=harmonized_docs_df,
    )
