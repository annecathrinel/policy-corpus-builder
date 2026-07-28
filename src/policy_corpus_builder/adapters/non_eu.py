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
from urllib.parse import  quote, urljoin, urlparse, urlunparse #parse_qs, unquote, 
#from urllib.request import Request, urlopen

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

try:
    from curl_cffi import requests as curl_cffi_requests
except ImportError:  # pragma: no cover - exercised only when the optional
    # curl_cffi dependency isn't installed; callers fall back to plain
    # requests in that case. See _get_thread_impersonated_session.
    curl_cffi_requests = None

try:
    from playwright.sync_api import sync_playwright
except ImportError:  # pragma: no cover - exercised only when the optional
    # playwright dependency isn't installed (or `playwright install
    # chromium` hasn't been run). Callers skip the browser-solve fallback
    # in that case - see _solve_waf_challenge_via_browser.
    sync_playwright = None


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


def _nz_content_headers(*, user_agent: str | None = None, accept_xml: bool = False) -> dict[str, str]:
    # A real NZ smoke test (2026-07-27) found every single full-text request
    # to www.legislation.govt.nz getting a WAF challenge. Sending a real
    # browser User-Agent here (this function) was the first attempted fix,
    # on the theory that NZ was blocked on the tool's self-identifying
    # default User-Agent the same way UK once was. A follow-up smoke test
    # with this fix deployed got the *exact same* 92/97 challenge count,
    # which rules that theory out - see _get_thread_impersonated_session
    # for the real culprit (TLS/JA3 fingerprinting) and the actual fix.
    # This UA is kept because it's harmless and still correct browser-like
    # behavior, just not what actually resolves the block.
    headers = {
        "User-Agent": (user_agent or UK_BROWSER_UA).strip(),
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
        "Accept-Language": "en-NZ,en;q=0.9",
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
# CA search/discovery targets publications.gc.ca's own search page directly
# (Government of Canada Publications). An earlier version of this module
# briefly searched laws-lois.justice.gc.ca (Justice Laws, acts/regulations
# only) instead; that was reverted after a 2026-07-27 smoke test found every
# search term - including nonsense terms with no plausible real hits -
# returning the exact same 7 URLs, all of them laws-lois's own standing
# navigation/help chrome rather than real results. publications.gc.ca is
# the originally-verified working base, per a preserved earlier copy of
# this module.
CA_BASE = "https://www.publications.gc.ca"
NZ_API_BASE = "https://api.legislation.govt.nz/v0"
US_BASE = "https://api.regulations.gov/v4"

# publications.gc.ca's own search results page always sits under /site/eng/,
# and the search page itself, its home page, and its browse/help pages also
# happen to sit under /site/eng/ - so unlike AUS/UK/NZ, "is this URL under
# the right path prefix" isn't enough on its own to tell a real result apart
# from site chrome. _CA_PUBLICATIONS_SKIP_PATH_RE excludes the known
# non-result page shapes (home, browse index, the search page itself, and
# its French-language equivalent "recherche/recherche.html" - a 2026-07-27
# live run found this exact URL, the site's own language-switcher link back
# to the French search page, showing up as a "result" on every single
# search term, real or empty, since it's present as boilerplate on every
# results page regardless of hit count); anything else under /site/eng/ or
# ending in .pdf is treated as a candidate result, matching the shape
# confirmed working in an earlier version of this module.
_CA_PUBLICATIONS_SKIP_PATH_RE = re.compile(
    r"/(home\.html|browse/index\.html|search/|recherche/recherche\.html)", re.IGNORECASE
)

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
    # legislation.govt.nz's search (both the website box and this API,
    # which the developer docs say has "functionality equivalent to the
    # search function on this website") treats unquoted multi-word input
    # as a fuzzy/OR-style match over the individual words, not a phrase.
    # `"..."` is the documented operator for an exact word/phrase match
    # (confirmed on /advanced_search/'s "Search operators and examples").
    # Without it, a term like "marine biodiversity" was being searched as
    # "marine" OR "biodiversity" rather than the actual phrase - the same
    # quoting fetch_uk_documents and fetch_us_documents already do for
    # their multi-word terms.
    term = term.strip()
    query_term = f'"{term}"' if " " in term else term
    query = "&".join(
        [
            f"search_term={quote(query_term)}",
            "search_field=content",
            f"page={page}",
            "per_page=20",
        ]
    )
    return f"{base.rstrip('/')}/works?{query}"


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


def _derive_nz_pdf_url(url: str) -> str:
    """Best-effort derivation of a PDF rendition URL from an NZ legislation
    html/landing-page URL, e.g. .../whole.html -> .../whole.pdf, or
    .../latest -> .../latest.pdf if there's no recognized extension at all.

    Returns "" if url is empty, already ends in .pdf, or ends in some
    other file extension this doesn't know how to handle (better to omit
    a candidate than guess wrong and mask the real error).
    """
    url = (url or "").strip()
    if not url:
        return ""
    parsed = urlparse(url)
    path = parsed.path
    last_segment = path.rsplit("/", 1)[-1]
    lower_path = path.lower()
    if lower_path.endswith(".pdf"):
        return ""
    if lower_path.endswith(".html") or lower_path.endswith(".htm"):
        new_path = re.sub(r"\.html?$", ".pdf", path, flags=re.IGNORECASE)
    elif "." in last_segment:
        # Some other file extension (e.g. .xml) - not a shape this has
        # evidence for, so don't guess at a transformation.
        return ""
    else:
        new_path = path.rstrip("/") + ".pdf"
    return urlunparse((parsed.scheme, parsed.netloc, new_path, "", "", ""))


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


def _extract_canada_publication_pdf_url(landing_url: str, html: str) -> str:
    """Finds the actual downloadable-PDF link embedded in a
    publications.gc.ca /publication.html catalogue-record landing page.

    That landing page is a metadata/catalogue record (title, department,
    a one-paragraph abstract, "Permanent link to this Catalogue record",
    links to "MARC XML"/"MARC HTML" *metadata* formats) - not the document
    itself. A 2026-07-27 live run found every CA full_text was just that
    catalogue-record boilerplate, because enrich_one_record_fulltext had
    no dedicated handling for "ca_publication" candidates and fell through
    to the generic HTML-page handler, which took the landing page's own
    visible text as if it were the document. The real content lives at a
    separate PDF the landing page links to (same shape as the direct .pdf
    hits publications.gc.ca's search results page returns directly, e.g.
    /collections/collection_2024/eccc/En1-45-2024-eng.pdf) - this pulls
    that link out rather than guessing a filename from the catalogue
    number, since the catalogue-number-to-filename mapping isn't reliably
    derivable and a wrong guess would silently point at nothing.

    A 2026-07-28 live rerun (after the fix above shipped) found the
    catalogue page's "Electronic document" row is still not being
    followed: its href isn't always a literal *.pdf* URL (some are
    server-side redirect/collection links, e.g.
    publications.gc.ca/collections/Collection/FA1-2-2005-3E.pdf reached
    via a wrapper), so the old strict ``endswith(".pdf")`` filter missed
    it and every record fell through to the landing-page-only fallback.
    This now also accepts an anchor whose link *text* mentions "pdf"
    (e.g. "FA1-2-2005-3E.pdf (PDF, 547 KB)") or whose path is under
    publications.gc.ca's /collections/ document-hosting prefix, even
    without a literal .pdf suffix on the href itself. A false positive
    here just costs one wasted fetch - enrich_one_record_fulltext still
    verifies the actual response content-type/magic-bytes before
    trusting it as a PDF, so this can't silently mislabel non-PDF
    content as the document.
    """
    soup = BeautifulSoup(html or "", "html.parser")
    for anchor in soup.find_all("a", href=True):
        href = str(anchor["href"]).strip()
        if not href:
            continue
        full = urljoin(landing_url or CA_BASE, href).split("#", 1)[0]
        if "publications.gc.ca" not in full.lower():
            continue
        lower_full = full.lower()
        looks_like_pdf = lower_full.endswith(".pdf")
        if not looks_like_pdf:
            link_text = anchor.get_text(" ", strip=True).lower()
            looks_like_pdf = "pdf" in link_text or "/collections/" in lower_full
        if not looks_like_pdf:
            continue
        if should_skip_canada_url(full):
            continue
        return full
    return ""


def _canada_marc_xml_url(publication_url: str) -> str:
    """Derives a publications.gc.ca catalogue record's MARC XML metadata
    URL from its /publication.html landing-page URL, e.g.
    .../site/eng/9.698872/publication.html ->
    .../site/eng/9.698872/marcXml.html - same catalogue ID, sibling page.
    """
    if not publication_url:
        return ""
    lower = publication_url.lower()
    suffix = "/publication.html"
    if not lower.endswith(suffix):
        return ""
    return publication_url[: -len(suffix)] + "/marcXml.html"


def _extract_canada_marc_pdf_url(xml_text: str) -> str:
    """Extracts the real document URL from a publications.gc.ca MARC XML
    metadata record (the page linked as "MARC XML format" from a
    catalogue-record landing page).

    Confirmed live (2026-07-28) that this is a more reliable source for
    the actual document link than scraping the landing page's own HTML
    (see _extract_canada_publication_pdf_url): the landing page's
    "Electronic document" link isn't always a literal *.pdf*-suffixed
    href, but the MARC record's standard 856 "location and access"
    field's $u subfield always has the direct URL, e.g.:
        <marc:datafield tag="856" ind1="4" ind2="0">
          <marc:subfield code="u">https://publications.gc.ca/collections/collection_2007/ic/Iu91-4-8-2004E.pdf</marc:subfield>
        </marc:datafield>
    """
    try:
        root = ET.fromstring(xml_text or "")
    except ET.ParseError:
        return ""

    def local_name(tag: str) -> str:
        return tag.rsplit("}", 1)[-1] if "}" in tag else tag

    for datafield in root.iter():
        if local_name(datafield.tag) != "datafield" or datafield.get("tag") != "856":
            continue
        for subfield in datafield:
            if local_name(subfield.tag) != "subfield" or subfield.get("code") != "u":
                continue
            url = (subfield.text or "").strip()
            if url:
                return url
    return ""


def _is_canada_archived_notice_response(response: requests.Response) -> bool:
    """Detects publications.gc.ca's "Information Archived on the Web"
    interstitial - a real, live-confirmed (2026-07-28) reason a genuine
    PDF link still doesn't serve the PDF: for older catalogue entries,
    the direct .pdf URL 302-redirects to
    site/archivee-archived.html?url=<original PDF URL>, an ordinary
    (HTTP 200, text/html) archival-compliance notice with a "Continue to
    publication" link back to the very same URL - not an AWS WAF action,
    so _classify_waf_response never flags it, and not something requests
    treats as an error, so it looks exactly like "the PDF link just
    doesn't have a PDF at it" without this dedicated check.
    """
    final_url = str(getattr(response, "url", "") or "")
    if "archivee-archived.html" in final_url.lower():
        return True
    text_head = (getattr(response, "text", "") or "")[:2000]
    return "Information Archived on the Web" in text_head


def _fetch_and_extract_canada_pdf(
    session: requests.Session,
    pdf_url: str,
    *,
    headers: dict[str, str],
    timeout: int,
    obey_robots: bool,
    robots: "RobotsCache",
) -> tuple[str, str]:
    """Fetches pdf_url and, if it's really a PDF, extracts+cleans its
    text. Returns (text, "") on success or ("", error_label) on any
    failure. Shared by enrich_one_record_fulltext's two CA-publication
    PDF-discovery paths (the MARC XML 856 $u field, and the HTML-scrape
    fallback) so a bad/blocked/non-PDF link is handled identically
    either way.
    """
    if obey_robots and not robots.allowed(pdf_url):
        return "", f"robots_disallow: {pdf_url}"
    try:
        pdf_response = _get_with_waf_retry(session, pdf_url, headers=headers, timeout=timeout)
    except Exception as exc:
        return "", f"{type(exc).__name__}: {exc}"
    waf_label = _classify_waf_response(pdf_response)
    if waf_label:
        return "", waf_label
    try:
        pdf_response.raise_for_status()
    except Exception as exc:
        return "", f"{type(exc).__name__}: {exc}"
    if _is_canada_archived_notice_response(pdf_response):
        # A live 2026-07-28 run found this notice on every single CA
        # "Electronic document" link it followed (both the MARC XML 856
        # $u path and the HTML-scrape fallback found the right URL, but
        # every one of them hit this page instead of the PDF). The
        # notice page's own "Continue to publication" link points right
        # back at the same URL, which suggests the notice may only show
        # once per session (a cookie-gated "you've been warned"
        # pattern) - so retry the identical URL once more on the same
        # session, which will carry forward whatever cookies the first
        # response set. This is a reasonable, live-motivated guess, not
        # a confirmed mechanism - it may simply not work, in which case
        # this second attempt will hit the same notice and fail the
        # same way, which is still safe (just one extra request).
        try:
            pdf_response = _get_with_waf_retry(session, pdf_url, headers=headers, timeout=timeout)
            pdf_response.raise_for_status()
        except Exception as exc:
            return "", f"canada_publication_archived_notice retry_failed: {type(exc).__name__}: {exc}"
        if _is_canada_archived_notice_response(pdf_response):
            return "", "canada_publication_archived_notice"
    content_type = str(pdf_response.headers.get("content-type", "") or "").lower()
    if "pdf" not in content_type and pdf_response.content[:5].lower() != b"%pdf-":
        return "", "canada_publication_pdf_unavailable"
    text = clean_canada_full_text(_extract_pdf_text(pdf_response.content))
    if not text:
        return "", "canada_publication_pdf_empty"
    return text, ""


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
    verbose: bool = True,
) -> pd.DataFrame:
    sess = session or build_session()
    verify = certifi.where() if verify is None else verify
    link_re = re.compile(r"^/(" + "|".join(map(re.escape, UK_DATASETS)) + r")/\d{4}/\d+", re.I)
    rows: list[dict] = []
    if verbose:
        print("\n========== UK retrieval ==========")
        print(f"terms: {len(search_terms)} | max_per_term: {max_per_term}")
    for term in search_terms:
        kept = 0
        page = 1
        seen_urls: set[str] = set()
        if verbose:
            print(f"\n[UK] term='{term}' START")
        while kept < max_per_term:
            q = f'"{term}"' if " " in term else term
            url = f"{UK_BASE}/all?text={quote(q)}"
            if page > 1:
                url += f"&page={page}"
            if verbose:
                print(f"[UK] term='{term}' page={page} -> {url}")
            response = safe_get(url, session=sess, verify=verify, verbose_err=False)
            if response is None:
                if verbose:
                    print(f"[UK] term='{term}' page={page} ERROR -> request failed; stopping this term")
                break
            if response.status_code != 200:
                if verbose:
                    print(f"[UK] term='{term}' page={page} ERROR -> HTTP {response.status_code}; stopping this term")
                break
            soup = BeautifulSoup(response.text, "html.parser")
            page_urls = [
                canonicalize_uk_doc_url(anchor["href"].strip())
                for anchor in soup.find_all("a", href=True)
                if link_re.match(anchor["href"].strip())
            ]
            page_urls = list(dict.fromkeys(page_urls))
            if not page_urls:
                if verbose:
                    print(f"[UK] term='{term}' page={page} -> no candidates; stopping")
                break
            new_urls = [item for item in page_urls if item not in seen_urls]
            if not new_urls:
                if verbose:
                    print(f"[UK] term='{term}' page={page} -> no new urls (all duplicates); stopping")
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
            if verbose:
                print(f"[UK] term='{term}' page={page} -> candidates={len(page_urls)} new_kept={len(new_urls)} kept_total={kept}")
            page += 1
            time.sleep(sleep_s)
        if verbose:
            print(f"[UK] term='{term}' DONE -> kept={kept}")
    if verbose:
        print(f"\n[UK] total rows kept: {len(rows)}")
    return _normalize_raw_rows(rows)


def fetch_aus_documents(
    search_terms: list[str],
    *,
    max_per_term: int = 500,
    session: requests.Session | None = None,
    sleep_s: float = 0.25,
    verify: bool | str | None = None,
    verbose: bool = True,
) -> pd.DataFrame:
    sess = session or build_session()
    # verify is accepted for signature/call-site compatibility with the
    # other fetch_* functions (fetch_non_eu_all always passes it), but the
    # search request below now goes through _get_with_waf_retry, which
    # always uses certifi.where() internally - see the same tradeoff in
    # every other _get_with_waf_retry caller in this module.
    del verify
    href_re = re.compile(r"^/(?:C|F)\d{4}[A-Z]\d{5}(?:/(?:asmade|latest|compilation|made|repealed|superseded))?$", re.I)
    rows: list[dict] = []
    if verbose:
        print("\n========== AUS retrieval ==========")
        print(f"terms: {len(search_terms)} | max_per_term: {max_per_term}")
        print("[AUS] note: search results are a single unpaginated page per term; every")
        print("[AUS] term below is queried exactly once as an exact-phrase match.")
    for term in search_terms:
        request_url = build_aus_search_url(term)
        if verbose:
            print(f"\n[AUS] term='{term}' -> {request_url}")
        # www.legislation.gov.au is in _WAF_PRONE_HOST_MIN_INTERVAL_S (see
        # _is_waf_block_response's docstring for the smoke-test evidence:
        # ~12 clean search requests, then HTTP 403 on every remaining term
        # for the rest of the run). _get_with_waf_retry throttles requests
        # to this host, prefers a curl_cffi browser-TLS-impersonated
        # session when available, and retries with backoff on a
        # challenge/block response - safe_get alone only retries on
        # network-level exceptions, never on a "successful" HTTP response
        # carrying a WAF status code. Wrapped in try/except since, unlike
        # safe_get, _get_with_waf_retry doesn't catch connection-level
        # exceptions itself.
        try:
            response = _get_with_waf_retry(
                sess, request_url, headers=_headers_for(), timeout=30,
            )
        except Exception as exc:
            if verbose:
                print(f"[AUS] term='{term}' ERROR -> request failed ({type(exc).__name__}: {exc}); skipping this term")
            continue
        waf_label = _classify_waf_response(response)
        if waf_label:
            if verbose:
                print(f"[AUS] term='{term}' ERROR -> {waf_label} (HTTP {response.status_code}); skipping this term")
            continue
        if response.status_code != 200:
            if verbose:
                print(f"[AUS] term='{term}' ERROR -> HTTP {response.status_code}; skipping this term")
            continue
        soup = BeautifulSoup(response.text, "html.parser")
        candidates = [
            (anchor["href"].strip().rstrip("/"), anchor.get_text(" ").strip())
            for anchor in soup.find_all("a", href=True)
            if href_re.match(anchor["href"].strip())
        ]
        deduped = list(dict.fromkeys(candidates))
        if verbose:
            print(f"[AUS] term='{term}' status={response.status_code} -> candidates={len(deduped)}")
        kept = 0
        for href, title in deduped:
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
        if verbose:
            print(f"[AUS] term='{term}' DONE -> kept={kept}")
        time.sleep(sleep_s)
    if verbose:
        print(f"\n[AUS] total rows kept: {len(rows)}")
    return _normalize_raw_rows(rows)


def build_canada_publications_search_url(term: str) -> str:
    term = term.strip()
    q = quote(f'"{term}"', safe="")
    return f"{CA_BASE}/site/eng/search/search.html?sLF=eng&text={q}&cnst=&adof=on"


def _extract_canada_publications_result_links(html: str) -> list[tuple[str, str]]:
    """
    Returns (doc_url, title) tuples for real publications.gc.ca search
    results, filtering out the search page's own furniture (a link back to
    itself and its French-language equivalent, the site's home page, its
    browse index) - everything else under /site/eng/, plus any direct .pdf
    link, is treated as a candidate. This is the same filter shape
    confirmed working in an earlier version of this module, before CA
    search briefly (and incorrectly) moved to laws-lois.justice.gc.ca.

    /site/fra/ links are excluded from candidates entirely, not just the
    search page's own self-link: this search is restricted to English
    results (sLF=eng), and a 2026-07-27 live run found the only /site/fra/
    link ever produced was the language-switcher's link back to the French
    version of the *same* search page - present as boilerplate on every
    results page, including ones with zero real hits, so a genuinely-empty
    search was silently returning 1 fake "result" every time instead of 0.
    """
    soup = BeautifulSoup(html or "", "html.parser")
    seen: set[str] = set()
    results: list[tuple[str, str]] = []

    for anchor in soup.find_all("a", href=True):
        href = str(anchor["href"]).strip()
        if not href or "search/search.html" in href:
            continue

        full = urljoin(CA_BASE, href).split("#", 1)[0]
        if "publications.gc.ca" not in full.lower():
            continue

        path = urlparse(full).path
        if _CA_PUBLICATIONS_SKIP_PATH_RE.search(path):
            continue

        lower = full.lower()
        if not (lower.endswith(".pdf") or "/site/eng/" in lower):
            continue

        if full in seen:
            continue
        seen.add(full)

        title = clean_canada_title(anchor.get_text(" ", strip=True))
        results.append((full, title))

    return results


def fetch_canada_documents(
    search_terms: list[str],
    *,
    max_per_term: int = 500,
    session: requests.Session | None = None,
    sleep_s: float = 0.25,
    verify_ssl_with_certifi: bool = True,
    verbose: bool = True,
) -> pd.DataFrame:
    sess = session or build_session()
    verify = certifi.where() if verify_ssl_with_certifi else True
    rows: list[dict] = []

    if verbose:
        print("\n========== CA retrieval ==========")
        print(f"terms: {len(search_terms)} | max_per_term: {max_per_term}")
        print("[CA] note: publications.gc.ca's search results are a single")
        print("[CA] unpaginated page per term, same as AUS.")

    for term in search_terms:
        request_url = build_canada_publications_search_url(term)
        if verbose:
            print(f"\n[CA] term='{term}' -> {request_url}")
        response = safe_get(request_url, session=sess, verify=verify, verbose_err=False)
        if response is None:
            if verbose:
                print(f"[CA] term='{term}' ERROR -> request failed; skipping this term")
            continue
        if response.status_code != 200:
            if verbose:
                print(f"[CA] term='{term}' ERROR -> HTTP {response.status_code}; skipping this term")
            continue

        candidates = _extract_canada_publications_result_links(response.text)
        if verbose:
            print(f"[CA] term='{term}' status={response.status_code} -> candidates={len(candidates)}")

        kept = 0
        for doc_url, title in candidates:
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
        if verbose:
            print(f"[CA] term='{term}' DONE -> kept={kept}")
        time.sleep(sleep_s)

    if verbose:
        print(f"\n[CA] total rows kept: {len(rows)}")

    return _normalize_raw_rows(rows)

def fetch_nz_documents(
    search_terms: list[str],
    *,
    api_key: str | None = None,
    max_per_term: int = 500,
    session: requests.Session | None = None,
    sleep_s: float = 0.25,
    verify: bool | str | None = None,
    user_agent: str | None = None,
    verbose: bool = True,
    return_diagnostics: bool = False,
) -> pd.DataFrame | tuple[pd.DataFrame, pd.DataFrame]:
    # NZ retrieval only supports the official api.legislation.govt.nz
    # search API - there used to be an unauthenticated "auto"/"scrape"
    # fallback against the public website, but that route doesn't work (see
    # the WAF-challenge investigation this cycle) and isn't worth
    # maintaining. A key is required, same as fetch_us_documents.
    api_key = api_key or os.getenv("NZ_LEGISLATION_API_KEY") or os.getenv("NZ_API_KEY", "")
    if not api_key:
        raise RuntimeError("NZ live retrieval requires NZ_LEGISLATION_API_KEY or api_key.")
    resolved_api_key = api_key.strip()

    sess = session or build_session()
    verify = certifi.where() if verify is None else verify
    rows: list[dict] = []
    diagnostics: list[dict] = []
    if verbose:
        print("\n========== NZ retrieval ==========")
        print(f"terms: {len(search_terms)} | max_per_term: {max_per_term}")
        print("[NZ] Using official API: api.legislation.govt.nz/v0/works")
    for term in search_terms:
        kept = 0
        if verbose:
            print(f"\n[NZ] term='{term}' START")
        # No page-count ceiling here, to match UK/AUS/US: pagination keeps
        # going until max_per_term is reached or the API runs out of
        # results (either an empty page or the total-results check below).
        # A fixed max_pages (previously 5, then 20) was silently capping
        # NZ well below max_per_term=500 - at 20 pages * 20 results/page
        # that's at most 400 documents per term, always short of the
        # per-term budget every other jurisdiction gets to use in full.
        page = 1
        while kept < max_per_term:
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
                print(f"[NZ] term='{term}' page={page} -> candidates={len(page_rows)} new_kept={new_kept} kept_total={kept}")
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
            page += 1
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
    verbose: bool = True,
) -> pd.DataFrame:
    api_key = api_key or os.getenv("REGULATIONS_GOV_API_KEY", "")
    if not api_key:
        raise RuntimeError("US live retrieval requires REGULATIONS_GOV_API_KEY or api_key.")
    # regulations.gov's API is well known for tight rate limits, and this
    # loop below is fully sequential (one term, one page, at a time) rather
    # than parallelized like the full-text fetch stage - so every single
    # rate-limited (429) request pays whatever retry cost the session's
    # transport adapter incurs, one after another, with nothing else able
    # to make progress in the meantime. build_session()'s default retry
    # policy (total_retries=6, backoff_factor=1.0, 429 included in
    # status_forcelist) is tuned as a generic "be persistent" default, but
    # applied here it lets urllib3's *own* internal retry/backoff run
    # silently inside a single sess.get() call - each fully-exhausted retry
    # cycle can take upwards of a minute (roughly 1+2+4+8+16+32s of sleep
    # between the 6 attempts) before this loop even sees a response back,
    # with no diagnostic output explaining the delay since it never
    # surfaces above the transport layer. Across many search terms this
    # compounds into a run that looks hung rather than just slow. A much
    # lighter, bounded retry budget here trades a bit of resilience to
    # truly transient errors for keeping each blocked request's cost small
    # enough that this loop - and the diagnostic prints below, which DO
    # distinguish a 429 - stays informative rather than silent for minutes
    # at a time.
    sess = session or build_session(total_retries=2, backoff_factor=0.5)
    rows: list[dict] = []
    if verbose:
        print("\n========== US retrieval ==========")
        print(f"terms: {len(search_terms)} | max_per_term: {max_per_term} | page_size: {page_size}")
    for term in search_terms:
        kept = 0
        page = 1
        if verbose:
            print(f"\n[US] term='{term}' START")
        while kept < max_per_term:
            request_page_size = max(5, min(page_size, max_per_term - kept))
            search_term = f'"{term}"' if " " in term else term
            params = {
                "filter[searchTerm]": search_term,
                "page[size]": request_page_size,
                "page[number]": page,
                "api_key": api_key,
            }
            if verbose:
                print(f"[US] term='{term}' page={page} -> filter[searchTerm]={search_term!r} page[size]={request_page_size}")
            response = safe_get(f"{US_BASE}/documents", session=sess, params=params, verbose_err=False)
            if response is None:
                if verbose:
                    print(f"[US] term='{term}' page={page} ERROR -> request failed; stopping this term")
                break
            if response.status_code == 429:
                if verbose:
                    print(
                        f"[US] term='{term}' page={page} ERROR -> HTTP 429 (rate limited by "
                        "regulations.gov); stopping this term"
                    )
                break
            if response.status_code != 200:
                if verbose:
                    print(f"[US] term='{term}' page={page} ERROR -> HTTP {response.status_code}; stopping this term")
                break
            data = (response.json() or {}).get("data", []) or []
            if not data:
                if verbose:
                    print(f"[US] term='{term}' page={page} -> no candidates; stopping")
                break
            page_kept = 0
            for item in data:
                if kept >= max_per_term:
                    break
                attrs = item.get("attributes", {}) or {}
                doc_id = item.get("id", "") or ""
                api_self = ((item.get("links") or {}).get("self", "") or "").strip()
                if not api_self and doc_id:
                    # A production run found regulations.gov's search response
                    # omitting links.self for every single result (0/2023 full
                    # text retrieved), even though the same response's "id"
                    # field was always populated. The v4 documents-detail
                    # endpoint URL is deterministic from that id, so fall back
                    # to constructing it directly rather than depending
                    # entirely on an upstream-controlled links field.
                    api_self = f"{US_BASE}/documents/{doc_id}"
                rows.append(
                    {
                        "jurisdiction": "United States",
                        "source": "US",
                        "matched_term": term,
                        "term": term,
                        "api_id": doc_id,
                        "api_self": api_self,
                        "doc_url": api_self,
                        "url": api_self,
                        "title": attrs.get("title") or attrs.get("documentTitle") or "",
                        "document_id": item.get("id", "") or "",
                    }
                )
                kept += 1
                page_kept += 1
            if verbose:
                print(f"[US] term='{term}' page={page} -> candidates={len(data)} new_kept={page_kept} kept_total={kept}")
            page += 1
            time.sleep(sleep_s)
        if verbose:
            print(f"[US] term='{term}' DONE -> kept={kept}")
    if verbose:
        print(f"\n[US] total rows kept: {len(rows)}")
    return _normalize_raw_rows(rows)


def fetch_non_eu_all(
    search_terms: list[str],
    *,
    sources: tuple[str, ...] = ("UK", "AUS", "NZ", "CA", "US"),
    nz_api_key: str | None = None,
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


def _get_thread_impersonated_session():
    """A thread-local curl_cffi session impersonating a real Chrome browser's
    TLS/JA3 fingerprint, used for hosts in _WAF_PRONE_HOST_MIN_INTERVAL_S.

    Why this exists: a 2026-07-27 NZ smoke test with a real browser-like
    User-Agent header (_nz_content_headers) got the *exact same* 92/97 WAF
    challenge count as before that header change - clear evidence the
    block isn't header-based. A follow-up check fetching the same
    www.legislation.govt.nz URL through an actual Chrome browser (not
    Python requests) succeeded immediately, with zero challenges, on the
    first try, no retries or special headers needed. Headers alone doing
    nothing while a real browser works instantly points at TLS-handshake
    fingerprinting (e.g. AWS WAF Bot Control's non-browser-TLS signal):
    Python's requests/urllib3 has a distinctive TLS ClientHello that
    differs from any real browser, and that handshake completes before a
    single HTTP header is ever sent, so no header change could ever have
    fixed this. curl_cffi presents a genuine browser TLS fingerprint,
    which is the standard mitigation for this class of block.

    Returns None if curl_cffi isn't installed, so callers fall back to the
    plain requests session.

    Update (2026-07-27): this was a well-evidenced hypothesis, not a
    confirmed fix, when written - and a live NZ run with it deployed came
    back at 16/17 full-text requests still waf_challenged, statistically
    indistinguishable from the 92/97 (94.8%) rate *before* this fix. TLS
    fingerprint spoofing alone isn't resolving the block. The most likely
    explanation: this is an AWS WAF *Challenge* action (status 202 +
    x-amzn-waf-action: challenge, exactly what's observed), which serves an
    interactive JavaScript challenge that must actually be executed to
    obtain a valid session token/cookie - a capability no plain HTTP
    client has, TLS impersonation or not, since curl_cffi still just sends
    a static HTTP request. The earlier "a real Chrome browser succeeded
    immediately" observation is consistent with this too: a real browser
    executes that JS automatically and gets a valid cookie, which a
    TLS-spoofed non-browser client never can. See
    _solve_waf_challenge_via_browser for the follow-up fix this points to.
    Kept in the retry chain regardless, since it's still a plausible
    partial mitigation for the separate Bot Control non-browser-TLS
    scoring signal and costs nothing when curl_cffi is available.
    """
    if curl_cffi_requests is None:
        return None
    session = getattr(_thread_local, "impersonated_session", None)
    if session is None:
        session = curl_cffi_requests.Session(impersonate="chrome124")
        _thread_local.impersonated_session = session
    return session


def _solve_waf_challenge_via_browser(url: str, *, user_agent: str | None = None, timeout_ms: int = 45000) -> dict[str, str] | None:
    """Loads url in a real headless Chromium browser (via Playwright) so its
    JavaScript actually runs, letting it solve an AWS WAF Challenge the way
    a real browser does - something no plain or TLS-impersonated HTTP
    client can do (see _get_thread_impersonated_session's 2026-07-27
    update for why that approach alone wasn't enough). Returns the
    resulting cookies as a plain dict for reuse in ordinary HTTP requests
    against the same host, or None if playwright isn't installed/configured
    or the load didn't produce any cookies (browser launch failure,
    navigation timeout, or a genuinely unresolved challenge).

    This is deliberately a one-shot, fairly expensive operation (a full
    browser launch + page load) - callers are expected to cache the result
    per host rather than call this per document. Not verified against the
    live site (no network access to www.legislation.govt.nz from the
    environment this was written in); the wait_for_load_state("networkidle")
    heuristic is a best-effort guess at "the challenge has finished
    resolving and any redirect has settled", not a confirmed signal for
    this specific site's challenge page.
    """
    if sync_playwright is None:
        return None
    try:
        with sync_playwright() as pw:
            browser = pw.chromium.launch(headless=True, args=["--no-sandbox"])
            try:
                context = browser.new_context(user_agent=(user_agent or UK_BROWSER_UA).strip())
                page = context.new_page()
                page.goto(url, timeout=timeout_ms, wait_until="domcontentloaded")
                page.wait_for_load_state("networkidle", timeout=timeout_ms)
                cookies = context.cookies()
            finally:
                browser.close()
    except Exception:
        return None
    if not cookies:
        return None
    return {str(c.get("name")): str(c.get("value")) for c in cookies if c.get("name")}


def _get_thread_browser_waf_cookies(url: str, *, user_agent: str | None = None) -> dict[str, str] | None:
    """Thread-local, per-host cache around _solve_waf_challenge_via_browser.

    A browser launch is expensive enough (seconds, not milliseconds) that
    trying it again for every single subsequently-blocked document in the
    same thread would be prohibitively slow, so both a successful solve
    and a failed one (None) are cached - a failure this run (e.g.
    playwright not installed, or a genuinely unsolvable challenge) is
    assumed to still fail on the next document in the same thread rather
    than retried.
    """
    host = (urlparse(url).netloc or "").lower()
    cache = getattr(_thread_local, "browser_waf_cookies", None)
    if cache is None:
        cache = {}
        _thread_local.browser_waf_cookies = cache
    if host not in cache:
        cache[host] = _solve_waf_challenge_via_browser(url, user_agent=user_agent)
    return cache[host]


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


def _is_waf_block_response(response: requests.Response | None) -> bool:
    """A harder block variant of the same class of problem as
    _is_waf_challenge_response, distinguished because it warrants a
    different diagnostic label (waf_block vs waf_challenge) even though
    both get the same throttle/impersonation/retry treatment.

    Evidence: a 2026-07-27 AUS smoke test (fetch_aus_documents against
    www.legislation.gov.au) got a clean HTTP 200 for its first ~12 search
    terms, then HTTP 403 for every single term after that for the rest of
    the run, with no recovery. That's the signature of a rate-based bot
    rule tripping partway through a burst of requests and then blocking
    the session/IP outright, rather than a per-term "forbidden" (which
    would be inconsistent with success on the very same endpoint moments
    earlier for unrelated terms). x-amzn-waf-action: block is the header
    AWS WAF Bot Control sets for a hard block action (as opposed to
    "challenge" for the interactive 202 case _is_waf_challenge_response
    handles) - checked here too, but the bare status_code == 403 check is
    kept as the primary signal since that header wasn't confirmed present
    on the actual blocked responses (only the status code was logged).
    """
    if response is None:
        return False
    waf_action = str(response.headers.get("x-amzn-waf-action", "") or "").strip().lower()
    return response.status_code == 403 or waf_action == "block"


def _classify_waf_response(response: requests.Response | None) -> str | None:
    """Returns "waf_challenge", "waf_block", or None (not WAF-related)."""
    if _is_waf_challenge_response(response):
        return "waf_challenge"
    if _is_waf_block_response(response):
        return "waf_block"
    return None


# Hosts that run rate-based bot/WAF detection aggressive enough to challenge
# or block a meaningful fraction of requests (observed: a 2026-07 NZ smoke
# test got waf_challenge on 92/97 full-text documents; a 2026-07-27 AUS
# smoke test got a clean run for ~12 search requests and then a hard
# waf_block - HTTP 403 - on every one of the remaining ~15). A minimum
# interval between requests to the same host - enforced across all
# concurrent worker threads, not just within one - reduces the chance that
# a burst of requests trips a rate-based rule in the first place.
_WAF_PRONE_HOST_MIN_INTERVAL_S = {
    "www.legislation.govt.nz": 1.5,
    "www.legislation.gov.uk": 1.5,
    "www.legislation.gov.au": 1.5,
}
_host_throttle_locks_guard = threading.Lock()
_host_throttle_locks: dict[str, threading.Lock] = {}
_host_last_request_monotonic: dict[str, float] = {}


def _get_host_throttle_lock(host: str) -> threading.Lock:
    with _host_throttle_locks_guard:
        lock = _host_throttle_locks.get(host)
        if lock is None:
            lock = threading.Lock()
            _host_throttle_locks[host] = lock
        return lock


def _throttle_host_request(url: str) -> None:
    """Pace requests to WAF-prone hosts so concurrent workers don't burst them.

    A no-op for any host not listed in _WAF_PRONE_HOST_MIN_INTERVAL_S.
    """
    host = (urlparse(url).netloc or "").lower()
    min_interval = _WAF_PRONE_HOST_MIN_INTERVAL_S.get(host)
    if not min_interval:
        return
    lock = _get_host_throttle_lock(host)
    with lock:
        now = time.monotonic()
        elapsed = now - _host_last_request_monotonic.get(host, 0.0)
        remaining = min_interval - elapsed
        if remaining > 0:
            time.sleep(remaining)
        _host_last_request_monotonic[host] = time.monotonic()


def _get_with_waf_retry(
    session: requests.Session,
    url: str,
    *,
    headers: dict[str, str],
    timeout: int,
    max_retries: int = 2,
    backoff_factor: float = 4.0,
    use_browser_impersonation: bool = True,
    use_browser_challenge_solver: bool = True,
) -> requests.Response:
    """GET url, retrying with backoff if the response is a WAF challenge or
    block (see _classify_waf_response).

    For hosts in _WAF_PRONE_HOST_MIN_INTERVAL_S, prefers a curl_cffi
    session that impersonates a real browser's TLS fingerprint over the
    plain requests session passed in - see
    _get_thread_impersonated_session for why. Falls back to the plain
    session if curl_cffi isn't installed, or if
    use_browser_impersonation=False (tests use this to exercise the
    retry/backoff logic against an injected fake session without it being
    swapped out for a real curl_cffi client).

    Also mirrors the fix already applied to EUR-Lex NIM full-text fetches:
    an HTTP 202 there meant "still generating, try again shortly" and a
    bounded retry resolved it. If TLS impersonation alone doesn't clear
    the challenge/block, a bounded retry with backoff is a cheap additional
    mitigation. Always paces requests to WAF-prone hosts first via
    _throttle_host_request, including before the first attempt.

    If every plain/impersonated retry above is still classified as a WAF
    challenge/block, makes one last attempt using cookies obtained by
    actually solving the challenge in a real headless browser (see
    _get_thread_browser_waf_cookies) - this is the only one of these
    mitigations confirmed to matter against an AWS WAF *Challenge* action,
    which requires executing JavaScript no HTTP client, impersonated or
    not, can run. use_browser_challenge_solver=False skips this (tests use
    this so they don't attempt a real Playwright browser launch).
    """
    client = session
    if use_browser_impersonation:
        host = (urlparse(url).netloc or "").lower()
        if host in _WAF_PRONE_HOST_MIN_INTERVAL_S:
            impersonated = _get_thread_impersonated_session()
            if impersonated is not None:
                client = impersonated
    response: requests.Response | None = None
    for attempt in range(max_retries + 1):
        _throttle_host_request(url)
        response = client.get(url, timeout=timeout, verify=certifi.where(), headers=headers)
        if _classify_waf_response(response) is None:
            return response
        if attempt < max_retries:
            time.sleep(backoff_factor * (attempt + 1))
    if use_browser_challenge_solver:
        host = (urlparse(url).netloc or "").lower()
        if host in _WAF_PRONE_HOST_MIN_INTERVAL_S:
            cookies = _get_thread_browser_waf_cookies(url, user_agent=headers.get("User-Agent"))
            if cookies:
                _throttle_host_request(url)
                browser_solved_response = client.get(
                    url, timeout=timeout, verify=certifi.where(), headers=headers, cookies=cookies,
                )
                if _classify_waf_response(browser_solved_response) is None:
                    return browser_solved_response
                response = browser_solved_response
    return response


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
        # Manual inspection of individual NZ full-text failures (not just
        # the aggregate waf_challenge/404 counts) found that a meaningful
        # share of the "real" failures were the html_url/doc_url candidate
        # below getting blocked, on documents that had a working .pdf
        # rendition at the same path once the extension was swapped/added -
        # confirmed by opening that .pdf URL directly. The API's own
        # "formats" list doesn't always include a pdf entry (older acts in
        # particular), so pdf_url above is often just empty rather than
        # wrong - this derives a best-effort candidate instead of relying
        # on the API to have listed one. Tried before the html candidates
        # since those are the ones observed failing; skipped entirely if
        # it would just duplicate pdf_url above.
        derived_pdf_url = _derive_nz_pdf_url(text_url or doc_url)
        if derived_pdf_url and derived_pdf_url != pdf_url:
            candidates.append((derived_pdf_url, "pdf"))
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

def _fix_common_mojibake(text: str) -> str:
    replacements = {
        "â": "’",
        "â": "‘",
        "â": "“",
        "â": "”",
        "â": "–",
        "â": "—",
        "â¢": "•",
        "â¦": "…",
        "Â ": " ",
        "Â": "",
    }
    out = text
    for bad, good in replacements.items():
        out = out.replace(bad, good)
    return out

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
    out.setdefault("full_text_pdf_lookup_status", "")
    src = canonical_source(out.get("source") or out.get("jurisdiction") or out.get("country") or "")
    out["source_canonical"] = src
    candidates = get_url_candidates(out, src, us_api_key)
    if not candidates:
        out["full_text_error"] = "no_url_candidate"
        return out
    if src == "UK":
        request_headers = _uk_content_headers(user_agent=user_agent)
    elif src == "NZ":
        request_headers = _nz_content_headers(user_agent=user_agent)
    else:
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
            if mode == "aus_text_page":
                response = _get_with_waf_retry(
                    session, candidate_url, headers=request_headers, timeout=timeout,
                )
                waf_label = _classify_waf_response(response)
                if waf_label:
                    last_err = waf_label
                    continue
                response.raise_for_status()
                asset_urls = _extract_aus_embedded_text_assets(candidate_url, response.text)
                if asset_urls:
                    try:
                        parts: list[str] = []
                        for asset_url in asset_urls:
                            if obey_robots and not robots.allowed(asset_url):
                                last_err = f"robots_disallow: {asset_url}"
                                continue
                            asset_response = _get_with_waf_retry(
                                session, asset_url, headers=request_headers, timeout=timeout,
                            )
                            asset_waf_label = _classify_waf_response(asset_response)
                            if asset_waf_label:
                                last_err = asset_waf_label
                                continue
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
                response = _get_with_waf_retry(
                    session, candidate_url, headers=request_headers, timeout=timeout,
                )
                waf_label = _classify_waf_response(response)
                if waf_label:
                    last_err = waf_label
                    continue
                response.raise_for_status()

                # Primary: the record's MARC XML metadata page (same
                # catalogue ID, sibling of the .../publication.html
                # landing page) has a standard 856 field whose $u
                # subfield is the real document URL - confirmed live
                # 2026-07-28 as the reliable source for this, since the
                # landing page's own "Electronic document" link isn't
                # always a literal *.pdf*-suffixed href (see
                # _extract_canada_publication_pdf_url's docstring for
                # the HTML-scrape fallback this supersedes as the first
                # choice).
                marc_url = _canada_marc_xml_url(candidate_url)
                if marc_url and not should_skip_canada_url(marc_url):
                    try:
                        marc_response = _get_with_waf_retry(
                            session, marc_url, headers=request_headers, timeout=timeout,
                        )
                        marc_waf_label = _classify_waf_response(marc_response)
                        if marc_waf_label:
                            last_err = marc_waf_label
                        else:
                            marc_response.raise_for_status()
                            marc_pdf_url = _extract_canada_marc_pdf_url(marc_response.text)
                            if marc_pdf_url and not should_skip_canada_url(marc_pdf_url):
                                text, err = _fetch_and_extract_canada_pdf(
                                    session, marc_pdf_url, headers=request_headers, timeout=timeout,
                                    obey_robots=obey_robots, robots=robots,
                                )
                                if text:
                                    out["full_text"] = text
                                    out["full_text_url"] = marc_pdf_url
                                    out["full_text_format"] = "pdf"
                                    out["full_text_error"] = ""
                                    return out
                                last_err = err or "canada_publication_marc_pdf_failed"
                            else:
                                last_err = "canada_publication_marc_no_856_url"
                    except Exception as exc:
                        last_err = f"{type(exc).__name__}: {exc}"

                # Fallback: scrape the landing page's own HTML for
                # something that looks like the document link, in case
                # the MARC XML page is unavailable or lacks an 856 $u.
                pdf_url = _extract_canada_publication_pdf_url(candidate_url, response.text)
                if pdf_url and not should_skip_canada_url(pdf_url):
                    text, err = _fetch_and_extract_canada_pdf(
                        session, pdf_url, headers=request_headers, timeout=timeout,
                        obey_robots=obey_robots, robots=robots,
                    )
                    if text:
                        out["full_text"] = text
                        out["full_text_url"] = pdf_url
                        out["full_text_format"] = "pdf"
                        out["full_text_error"] = ""
                        return out
                    last_err = err or last_err

                # Last resort: the landing page's own visible text.
                # That's catalogue metadata rather than the real
                # document body, but it's better than nothing for the
                # (apparently rare) publication with no PDF link at all.
                #
                # A 2026-07-28 live run found this "last resort" was
                # STILL being hit for every single record even with the
                # MARC XML lookup above in place, with no way to tell
                # from the output alone whether that's because the MARC
                # fetch failed, no 856 $u was present, the HTML-scrape
                # fallback also came up empty, or the PDF it found
                # wasn't actually a PDF - full_text_error stays "" on
                # this branch since some text was still retrieved, so
                # that field can't carry the reason. full_text_pdf_lookup_status
                # records it instead, specifically for diagnosing this.
                out["full_text_pdf_lookup_status"] = last_err or "no_pdf_link_found"
                text = clean_canada_full_text(html_to_visible_text(response.text))
                if text:
                    out["full_text"] = text
                    out["full_text_url"] = candidate_url
                    out["full_text_format"] = "html"
                    out["full_text_error"] = ""
                    return out
                last_err = "canada_publication_landing_empty"
                continue
            if mode == "pdf":
                response = _get_with_waf_retry(
                    session, candidate_url, headers=request_headers, timeout=timeout,
                )
                waf_label = _classify_waf_response(response)
                if waf_label:
                    last_err = waf_label
                    continue
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
                response = _get_with_waf_retry(
                    session,
                    candidate_url,
                    headers=_uk_content_headers(user_agent=user_agent, accept_xml=True),
                    timeout=timeout,
                )
                waf_label = _classify_waf_response(response)
                if waf_label:
                    last_err = waf_label
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
                response = _get_with_waf_retry(
                    session,
                    candidate_url,
                    headers=_nz_content_headers(user_agent=user_agent, accept_xml=True),
                    timeout=timeout,
                )
                waf_label = _classify_waf_response(response)
                if waf_label:
                    last_err = waf_label
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
            response = _get_with_waf_retry(
                session, candidate_url, headers=request_headers, timeout=timeout,
            )
            waf_label = _classify_waf_response(response)
            if waf_label:
                last_err = waf_label
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

def _matched_terms_found_in_text(matched_terms: object, title: str, full_text: str) -> bool | None:
    """Checks whether every word of at least one of a record's matched
    query terms literally appears (case-insensitively) in its own title
    or full_text.

    Returns None when there's nothing to check against (no matched terms
    recorded, or full-text retrieval never produced any text) - a fetch
    failure shouldn't be reported as an irrelevant match, since there's
    no text to judge relevance from.

    A 2026-07-28 live run's US results included several fetched
    documents that share no words at all with their matched search term
    (e.g. a "blue economy" hit whose title/text was an unrelated raw
    climate-data CSV export, and a "nature repair" hit that was an EPA
    spill-response data-export stub). regulations.gov's
    filter[searchTerm] appears to match at the docket/submission level
    rather than the individual attached document's own text, so the
    API returning a hit doesn't guarantee the fetched document is
    actually about the term. AUS showed a milder version of the same
    thing (large omnibus Acts like the Income Tax Assessment Act 1997
    matching "offshore renewable" via a full-text-contains search with
    no relevance ranking). This flags the mismatch (term_verified=False
    in the output) rather than silently trusting every upstream hit -
    it doesn't drop the record outright, since a false negative here (a
    genuinely relevant document that just phrases things differently)
    would otherwise disappear from the corpus with no way to notice or
    recover it; callers can filter on term_verified downstream instead.
    """
    if isinstance(matched_terms, (list, tuple, set)):
        terms = [str(term) for term in matched_terms if str(term).strip()]
    elif matched_terms:
        terms = [str(matched_terms)]
    else:
        terms = []
    if not terms:
        return None
    haystack = f"{title or ''} {full_text or ''}".lower()
    if not haystack.strip():
        return None
    for term in terms:
        words = [word for word in re.split(r"\s+", term.lower().strip()) if word]
        if words and all(word in haystack for word in words):
            return True
    return False


def add_full_texts_parallel(
    records: list[dict],
    *,
    us_api_key: str | None,
    max_workers: int = 12,
    progress_every: int = 25,
    obey_robots: bool = True,
    user_agent: str | None = None,
    fulltext_cache: dict[str, dict] | None = None,
) -> list[dict]:
    if not records:
        return []
    # fulltext_cache lets a caller (NonEUAdapter, in practice) share one
    # dict across every query term's separate call into this pipeline
    # within the same jurisdiction run. Each query term is otherwise a
    # fully independent, stateless pass (see run_non_eu_query_pipeline),
    # so the same document turning up under two different search terms
    # was being fetched and parsed from scratch twice - confirmed on a
    # 2026-07-27 live run (EU: one 5.4MB Commission working document was
    # independently fetched 5 separate times, once per matching term;
    # US/EU overall had 14%/30% of their full-text fetches be exact
    # duplicate URLs already seen under a different term in the same
    # run). Only successful fetches are cached: a failure (WAF
    # challenge, timeout, etc.) is left to retry fresh under the next
    # term rather than being permanently remembered as failed, since
    # this run's per-term throttling/backoff timing could easily let a
    # later attempt succeed where an earlier one didn't.
    cache = fulltext_cache if fulltext_cache is not None else {}
    cache_hits = 0
    to_fetch: list[dict] = []
    cached_results: dict[int, dict] = {}
    for index, rec in enumerate(records):
        key = doc_key_country(rec)
        cached = cache.get(key) if key else None
        if cached:
            enriched = dict(rec)
            enriched.update(cached)
            cached_results[index] = enriched
            cache_hits += 1
        else:
            to_fetch.append((index, rec))
    out: list[dict] = []
    errors = 0
    ok = 0
    counter: Counter[str] = Counter()
    term_mismatches = 0
    term_checked = 0
    # curl_cffi supplies the browser-TLS-fingerprint impersonation
    # _get_with_waf_retry uses for WAF-prone hosts (see
    # _get_thread_impersonated_session's docstring for the full story on
    # why that's needed). If it isn't actually importable in this
    # environment - not installed, or a `pip install --upgrade` on an
    # existing HPC environment didn't pick up the dependency added after
    # curl_cffi was introduced - every request to those hosts silently
    # falls back to the plain requests session and keeps hitting the same
    # waf_challenge this was meant to fix, with nothing in the log
    # explaining why. This makes that observable instead of something to
    # guess at from the waf_challenge count in [ERROR SUMMARY] alone.
    print(
        "[FULLTEXT] curl_cffi browser-TLS impersonation: "
        + (
            "available"
            if curl_cffi_requests is not None
            else "NOT available (pip install curl_cffi) - WAF-prone hosts will "
            "use the plain requests session and may see more waf_challenge errors"
        )
    )
    # playwright drives a real headless Chromium browser to actually solve
    # an AWS WAF Challenge (see _solve_waf_challenge_via_browser) when
    # curl_cffi's TLS impersonation alone doesn't clear it - confirmed
    # necessary for NZ (2026-07-27: 16/17 full-text requests still
    # waf_challenged with curl_cffi alone, unchanged from before it).
    # Requires both `pip install playwright` and a one-time
    # `playwright install chromium` to download the browser binary; if
    # either is missing this silently falls back to whatever curl_cffi
    # alone achieves, so surface it here rather than leaving it to be
    # inferred from an unchanged waf_challenge count.
    print(
        "[FULLTEXT] Playwright headless-browser WAF-challenge solver: "
        + (
            "available"
            if sync_playwright is not None
            else "NOT available (pip install playwright && playwright install "
            "chromium) - WAF-prone hosts will rely on curl_cffi TLS "
            "impersonation alone, which is not sufficient for hosts using an "
            "AWS WAF Challenge action (e.g. www.legislation.govt.nz)"
        )
    )
    def _apply_term_check(enriched: dict) -> None:
        nonlocal term_checked, term_mismatches
        verified = _matched_terms_found_in_text(
            enriched.get("matched_terms"), enriched.get("title", ""), enriched.get("full_text", ""),
        )
        enriched["term_verified"] = verified
        if verified is not None:
            term_checked += 1
            if not verified:
                term_mismatches += 1

    for enriched in cached_results.values():
        _apply_term_check(enriched)
        out.append(enriched)
        if enriched.get("full_text"):
            ok += 1
        else:
            errors += 1
            counter[str(enriched.get("full_text_error", "error"))[:120]] += 1

    if cache_hits:
        print(f"[FULLTEXT] cross-term cache: reused {cache_hits} already-fetched document(s), skipping their fetch")

    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        futures = [
            executor.submit(
                enrich_one_record_fulltext,
                rec,
                us_api_key=us_api_key,
                obey_robots=obey_robots,
                user_agent=user_agent,
            )
            for _, rec in to_fetch
        ]
        for idx, future in enumerate(as_completed(futures), start=1):
            try:
                enriched = future.result()
            except Exception as exc:
                errors += 1
                counter[type(exc).__name__] += 1
                continue
            _apply_term_check(enriched)
            out.append(enriched)
            if enriched.get("full_text"):
                ok += 1
                key = doc_key_country(enriched)
                if key:
                    cache[key] = {
                        "full_text": enriched.get("full_text", ""),
                        "full_text_url": enriched.get("full_text_url", ""),
                        "full_text_format": enriched.get("full_text_format", ""),
                        "full_text_error": "",
                    }
            else:
                errors += 1
                counter[str(enriched.get("full_text_error", "error"))[:120]] += 1
            if progress_every and (idx % progress_every == 0 or idx == len(futures)):
                print(f"[PROGRESS] {idx}/{len(futures)} | ok={ok} | errors={errors}")
    if counter:
        print("[ERROR SUMMARY]")
        for label, count in counter.most_common(15):
            print(f"{count}x {label}")
    if term_checked:
        print(
            f"[RELEVANCE] {term_mismatches}/{term_checked} fetched document(s) don't contain any of "
            "their matched query term's words in title/full_text (term_verified=False) - likely a "
            "docket-level or loose upstream search match rather than a wrong fetch"
        )
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
    fulltext_cache: dict[str, dict] | None = None,
) -> pd.DataFrame:
    resolved_us_api_key = us_api_key or os.getenv("REGULATIONS_GOV_API_KEY", "")
    if raw_hits_df.empty:
        return pd.DataFrame(
            columns=["doc_id", "country", "jurisdiction", "doc_uid", "title", "url", "lang", "date", "year", "source_file", "full_text_clean", "text_len", "has_text", "retrieval_status", "full_text_url", "full_text_error", "full_text_format", "source", "term_verified", "full_text_pdf_lookup_status"]
        )
    grouped_docs = aggregate_one_row_per_doc(raw_hits_df.to_dict(orient="records"))
    enriched = add_full_texts_parallel(
        grouped_docs,
        us_api_key=resolved_us_api_key,
        max_workers=max_workers,
        progress_every=progress_every,
        obey_robots=obey_robots,
        user_agent=user_agent,
        fulltext_cache=fulltext_cache,
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
    ordered = ["doc_id", "country", "jurisdiction", "doc_uid", "title", "url", "lang", "date", "year", "source_file", "full_text_clean", "text_len", "has_text", "retrieval_status", "full_text_url", "full_text_error", "full_text_format", "source", "term_verified", "full_text_pdf_lookup_status"]
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
    us_api_key: str | None = None,
    max_per_term: int = 100,
    max_workers: int = 4,
    progress_every: int = 0,
    obey_robots: bool = True,
    user_agent: str | None = None,
    fulltext_cache: dict[str, dict] | None = None,
) -> NonEUQueryRun:
    """Run one real non-EU retrieval query through retrieval, full text, and harmonization.

    fulltext_cache, if given, is a plain dict the caller owns and reuses
    across multiple calls to this function for the same jurisdiction (one
    call per query term - see NonEUAdapter.collect) so a document that
    matches more than one search term only gets its full text fetched
    once. Leaving it as None (the default) preserves the old
    fetch-every-time behaviour for direct callers/tests that don't care
    about cross-call caching.
    """

    resolved_us_api_key = us_api_key or os.getenv("REGULATIONS_GOV_API_KEY", "")

    raw_hits_df, source_log_df = fetch_non_eu_all(
        [query_text],
        sources=countries,
        nz_api_key=nz_api_key,
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
        fulltext_cache=fulltext_cache,
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
