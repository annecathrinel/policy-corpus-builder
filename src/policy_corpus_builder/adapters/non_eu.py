from __future__ import annotations

"""Live non-EU retrieval and full-text helpers."""

from collections import Counter, defaultdict
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass
from html import unescape
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


def _headers_for(user_agent: str | None = None) -> dict[str, str]:
    return {
        "User-Agent": (user_agent or UA).strip(),
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
        "Accept-Language": "en-GB,en;q=0.9",
        "Connection": "keep-alive",
    }


HEADERS = {"User-Agent": UA, "Accept-Language": "en-GB,en;q=0.9"}
DEFAULT_HEADERS = _headers_for()

UK_BASE = "https://www.legislation.gov.uk"
UK_DATASETS = ("ukpga", "uksi", "ukla", "asp", "anaw", "wsi", "ssi", "nisr", "nisi", "ukdsi", "sdsi")
AUS_BASE = "https://www.legislation.gov.au"
CA_BASE = "https://www.publications.gc.ca"
NZ_HOSTS = ["www.legislation.govt.nz", "legislation.govt.nz"]
US_BASE = "https://api.regulations.gov/v4"

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


def _is_uk_search_challenge_response(response: requests.Response | None) -> bool:
    if response is None:
        return False
    waf_action = str(response.headers.get("x-amzn-waf-action", "") or "").strip().lower()
    return response.status_code == 202 or waf_action == "challenge"


def _looks_like_uk_document_href(href: str) -> bool:
    parsed = urlparse(urljoin(UK_BASE, href))
    if parsed.netloc and "legislation.gov.uk" not in parsed.netloc.lower():
        return False
    parts = [part for part in parsed.path.split("/") if part]
    if len(parts) < 3:
        return False
    if parts[0].lower() not in {dataset.lower() for dataset in UK_DATASETS}:
        return False
    if not re.fullmatch(r"\d{4}", parts[1]):
        return False
    if not re.fullmatch(r"\d+", parts[2]):
        return False
    return True


def _extract_uk_search_result_links(html: str) -> list[tuple[str, str]]:
    soup = BeautifulSoup(html, "html.parser")
    results: list[tuple[str, str]] = []
    seen: set[str] = set()

    for anchor in soup.find_all("a", href=True):
        href = str(anchor["href"]).strip()
        if not href:
            continue
        if not _looks_like_uk_document_href(href):
            continue
        doc_url = canonicalize_uk_doc_url(urljoin(UK_BASE, href))
        if doc_url in seen:
            continue
        seen.add(doc_url)
        title = anchor.get_text(" ").strip()
        results.append((doc_url, title))

    return results


def _decode_duckduckgo_result_url(href: str) -> str:
    absolute = urljoin("https://duckduckgo.com", href)
    parsed = urlparse(absolute)
    query = parse_qs(parsed.query)
    raw_target = query.get("uddg", [""])[0]
    if not raw_target:
        return ""
    return unquote(raw_target).strip()


def _extract_uk_duckduckgo_links(html: str) -> list[tuple[str, str]]:
    soup = BeautifulSoup(html, "html.parser")
    results: list[tuple[str, str]] = []
    seen: set[str] = set()

    for anchor in soup.select("a.result__a"):
        href = str(anchor.get("href", "")).strip()
        target_url = _decode_duckduckgo_result_url(href)
        if not target_url or not _looks_like_uk_document_href(target_url):
            continue
        doc_url = canonicalize_uk_doc_url(target_url)
        if doc_url in seen:
            continue
        seen.add(doc_url)
        title = unescape(anchor.get_text(" ").strip())
        results.append((doc_url, title))

    return results


def _fetch_uk_search_results_via_duckduckgo(term: str) -> list[tuple[str, str]]:
    query = f"site:legislation.gov.uk {term} legislation"
    url = f"https://html.duckduckgo.com/html/?q={quote(query)}"
    request = Request(
        url,
        headers={
            "User-Agent": UA,
            "Accept-Language": "en-GB,en;q=0.9",
        },
    )
    with urlopen(request, timeout=30) as response:
        html = response.read().decode("utf-8", errors="replace")
    return _extract_uk_duckduckgo_links(html)


def build_aus_search_url(term: str) -> str:
    term = term.strip()
    if " " in term:
        term = f'""{term}""'
    q = quote(term, safe="")
    return f"{AUS_BASE}/search/text({q},nameandtext,contains)/pointintime(latest)/sort(searchcontexts%2Ftext%2Frelevance%20desc)"


def nz_search_url(base: str, term: str, page: int = 1) -> str:
    # keep the manual pattern you were using
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


def infer_title(row: pd.Series | dict) -> str:
    title = str((row.get("title") if isinstance(row, dict) else row.get("title", "")) or "").strip()
    if not _is_missing_text(title):
        jurisdiction = str((row.get("jurisdiction") if isinstance(row, dict) else row.get("jurisdiction", "")) or "").strip()
        if jurisdiction in {"United Kingdom", "UK"}:
            return clean_uk_title(title)
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
    rows: list[dict] = []
    for term in search_terms:
        kept = 0
        page = 1
        seen_urls: set[str] = set()
        used_search_fallback = False
        while kept < max_per_term:
            q = f'"{term}"' if " " in term else term
            url = f"{UK_BASE}/all?text={quote(q)}"
            if page > 1:
                url += f"&page={page}"
            response = safe_get(url, session=sess, verify=verify, verbose_err=False)
            if response is None:
                break
            page_results = []
            if response.status_code == 200 and not _is_uk_search_challenge_response(response):
                page_results = _extract_uk_search_result_links(response.text)
            elif not used_search_fallback:
                page_results = _fetch_uk_search_results_via_duckduckgo(term)
                used_search_fallback = True
            else:
                break
            page_urls = [doc_url for doc_url, _ in page_results]
            if not page_urls:
                break
            new_urls = [item for item in page_urls if item not in seen_urls]
            if not new_urls:
                break
            for doc_url in new_urls:
                if kept >= max_per_term:
                    break
                seen_urls.add(doc_url)
                title_lookup = dict(page_results)
                contents_url = uk_contents_url(doc_url)
                rows.append(
                    {
                        "jurisdiction": "United Kingdom",
                        "source": "UK",
                        "matched_term": term,
                        "term": term,
                        "doc_url": doc_url,
                        "url": doc_url,
                        "contents_url": contents_url,
                        "title": clean_uk_title(title_lookup.get(doc_url, "")),
                    }
                )
                kept += 1
            if used_search_fallback:
                break
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
        q = quote(f'"{term}"' if " " in term else term, safe="")
        url = f"{CA_BASE}/site/eng/search/search.html?ast={q}&cnst=&adof=on"
        response = safe_get(url, session=sess, verify=verify, verbose_err=False)
        if response is None or response.status_code != 200:
            continue
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
    return _normalize_raw_rows(rows)


def fetch_nz_documents(
    search_terms: list[str],
    *,
    max_per_term: int = 500,
    session: requests.Session | None = None,
    sleep_s: float = 0.25,
    verify: bool | str | None = None,
    max_pages: int = 5,
    verbose: bool = True,
    return_diagnostics: bool = False,
) -> pd.DataFrame | tuple[pd.DataFrame, pd.DataFrame]:
    sess = session or build_session()
    verify = certifi.where() if verify is None else verify
    rows: list[dict] = []
    diagnostics: list[dict] = []
    if verbose:
        print("\n========== NZ retrieval (v2) ==========")
        print(f"terms: {len(search_terms)} | max_per_term: {max_per_term} | max_pages: {max_pages}")
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
                }
            ]
        )
        empty = _normalize_raw_rows(rows)
        return (empty, diag_df) if return_diagnostics else empty
    if verbose:
        print(f"[NZ] Using host: {host}")
    for term in search_terms:
        kept = 0
        seen_urls: set[str] = set()
        if verbose:
            print(f"\n[NZ] term='{term}' START")
        for page in range(1, max_pages + 1):
            request_url = nz_search_url(host, term, page=page)
            if verbose:
                print(f"[NZ] term='{term}' page={page} -> {request_url}")
            response = safe_get(request_url, session=sess, verify=verify, verbose_err=False)
            if response is None:
                if verbose:
                    print(f"[NZ] term='{term}' page={page} ERROR -> request failed; stopping this term")
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
                    }
                )
                break
            if response.status_code == 403:
                if verbose:
                    print(f"[NZ] term='{term}' page={page} -> HTTP 403 blocked; skipping NZ term")
                diagnostics.append(
                    {
                        "host": host,
                        "term": term,
                        "page": page,
                        "status_code": 403,
                        "candidates_found": 0,
                        "new_urls_kept": 0,
                        "kept_total": kept,
                        "stop_reason": "http_403",
                        "request_url": request_url,
                    }
                )
                break
            if response.status_code != 200:
                if verbose:
                    print(f"[NZ] term='{term}' page={page} ERROR -> HTTP {response.status_code}; stopping this term")
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
                if verbose:
                    print(f"[NZ] term='{term}' page={page} -> no candidates; stopping")
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
                    }
                )
                break
            new_urls = [item for item in candidates if item not in seen_urls]
            if not new_urls:
                if verbose:
                    print(f"[NZ] term='{term}' page={page} -> 0 new docs; stopping")
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
                }
            )
            if verbose:
                print(f"[NZ] term='{term}' page={page} -> candidates={len(candidates)} new_kept={new_kept} kept_total={kept}")
            if kept >= max_per_term:
                if verbose:
                    print(f"[NZ] term='{term}' reached max_per_term={max_per_term}; stopping")
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
            params = {
                "filter[searchTerm]": f'"{term}"' if " " in term else term,
                "page[size]": min(page_size, max_per_term - kept),
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
    us_api_key: str | None = None,
    max_per_term: int = 500,
) -> tuple[pd.DataFrame, pd.DataFrame]:
    session = build_session()
    verify_default = certifi.where()
    frames: list[pd.DataFrame] = []
    logs: list[dict] = []
    source_map = {
        "UK": lambda: fetch_uk_documents(search_terms, max_per_term=max_per_term, session=session, verify=verify_default),
        "AUS": lambda: fetch_aus_documents(search_terms, max_per_term=max_per_term, session=session, verify=verify_default),
        "NZ": lambda: fetch_nz_documents(search_terms, max_per_term=max_per_term, session=session, verify=verify_default),
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
        for col in ["lang", "celex", "date", "format", "public_timestamp", "description", "doc_url", "text_url", "api_self"]:
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
            candidates.append((text_url, "html"))
        if doc_url:
            if not doc_url.rstrip("/").endswith("/text"):
                candidates.append((doc_url.rstrip("/") + "/text", "html"))
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
    if src in ("NZ", "CA"):
        return [(url, "html")] if url else []
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
    request_headers = _headers_for(user_agent)
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
            if mode == "us_api_json":
                headers = dict(request_headers)
                if us_api_key:
                    headers["X-Api-Key"] = us_api_key
                response = session.get(candidate_url, headers=headers, timeout=timeout, verify=certifi.where())
                response.raise_for_status()
                text = us_json_to_text(response.json())
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
                    headers={**request_headers, "Accept": "application/xml,text/xml;q=0.9,*/*;q=0.8"},
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
            response = session.get(candidate_url, timeout=timeout, verify=certifi.where(), headers=request_headers)
            if _is_waf_challenge_response(response):
                last_err = "waf_challenge"
                continue
            response.raise_for_status()
            text = html_to_visible_text(response.text)
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
    if raw_hits_df.empty:
        return pd.DataFrame(
            columns=["doc_id", "country", "jurisdiction", "doc_uid", "title", "url", "lang", "date", "year", "source_file", "full_text_clean", "text_len", "has_text", "retrieval_status", "full_text_url", "full_text_error", "full_text_format", "source"]
        )
    grouped_docs = aggregate_one_row_per_doc(raw_hits_df.to_dict(orient="records"))
    enriched = add_full_texts_parallel(
        grouped_docs,
        us_api_key=us_api_key,
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
    us_api_key: str | None = None,
    max_per_term: int = 100,
    max_workers: int = 4,
    progress_every: int = 0,
    obey_robots: bool = True,
    user_agent: str | None = None,
) -> NonEUQueryRun:
    """Run one real non-EU retrieval query through retrieval, full text, and harmonization."""

    raw_hits_df, source_log_df = fetch_non_eu_all(
        [query_text],
        sources=countries,
        us_api_key=us_api_key,
        max_per_term=max_per_term,
    )
    fulltext_docs_df = build_non_eu_fulltext_docs(
        raw_hits_df,
        us_api_key=us_api_key,
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
