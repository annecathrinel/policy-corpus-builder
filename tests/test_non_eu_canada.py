from __future__ import annotations

import unittest
from contextlib import redirect_stdout
from io import StringIO
from unittest.mock import patch

from policy_corpus_builder.adapters import non_eu
from policy_corpus_builder.adapters.non_eu_adapter import NonEUAdapter
from policy_corpus_builder.schemas import SourceConfig


# A representative publications.gc.ca search-results page: the search page's
# own furniture (a link back to itself, the site's home page, its browse
# index, and its French-language equivalent) mixed in with real result rows
# and a direct .pdf link, matching the shape confirmed working in an earlier
# version of this module before CA search briefly (and incorrectly) moved
# to laws-lois.justice.gc.ca.
CANADA_PUBLICATIONS_SEARCH_HTML = """
<html>
  <body>
    <a href="/site/eng/search/search.html?sLF=eng&text=biodiversity&cnst=&adof=on">New search</a>
    <a href="/site/eng/home.html">Home</a>
    <a href="/site/eng/browse/index.html">Browse all publications</a>
    <a href="/site/eng/9.876543/publication.html">Biodiversity Plan 2024</a>
    <a href="/collections/collection_2024/eccc/En1-45-2024-eng.pdf">Species at Risk Report (PDF)</a>
    <a href="/site/fra/recherche/recherche.html">Français</a>
  </body>
</html>
"""

# Regression fixture: a 2026-07-27 live run found the site's own
# language-switcher link back to the French search page
# (/site/fra/recherche/recherche.html) present as boilerplate on every
# single search-results page, real hits or none - so a genuinely-empty
# search was silently coming back as 1 fake "result" instead of 0 on every
# term. This page has zero real results, only that one furniture link.
CANADA_PUBLICATIONS_ZERO_HITS_HTML = """
<html>
  <body>
    <a href="/site/eng/search/search.html?sLF=eng&text=nature%20restoration&cnst=&adof=on">New search</a>
    <a href="/site/fra/recherche/recherche.html">Français</a>
  </body>
</html>
"""


class _FakeResponse:
    def __init__(
        self,
        status_code: int,
        text: str = "",
        *,
        content: bytes | None = None,
        headers: dict[str, str] | None = None,
    ):
        self.status_code = status_code
        self.text = text
        self.content = content if content is not None else text.encode("utf-8")
        self.headers = headers or {}

    def raise_for_status(self) -> None:
        if self.status_code >= 400:
            raise RuntimeError(f"HTTP {self.status_code}")


class _FakeRobots:
    def allowed(self, url: str) -> bool:
        return True


class _FakeSession:
    def __init__(self, responses: dict[str, _FakeResponse]):
        self._responses = responses

    def get(self, url: str, **kwargs) -> _FakeResponse:
        try:
            return self._responses[url]
        except KeyError as exc:
            raise AssertionError(f"unexpected URL fetched: {url}") from exc


class NonEUCanadaTests(unittest.TestCase):
    def test_build_canada_publications_search_url_matches_current_live_route_shape(self) -> None:
        self.assertEqual(
            non_eu.build_canada_publications_search_url("biodiversity"),
            "https://www.publications.gc.ca/site/eng/search/search.html?sLF=eng&text=%22biodiversity%22&cnst=&adof=on",
        )
        self.assertEqual(
            non_eu.build_canada_publications_search_url("soil biodiversity"),
            "https://www.publications.gc.ca/site/eng/search/search.html?sLF=eng&text=%22soil%20biodiversity%22&cnst=&adof=on",
        )

    def test_extract_canada_publications_result_links_filters_search_furniture(self) -> None:
        results = non_eu._extract_canada_publications_result_links(CANADA_PUBLICATIONS_SEARCH_HTML)

        # Only the two real result rows - the self-referential search link,
        # the home page, the browse index, and the French language-switcher
        # link are all excluded.
        self.assertEqual(
            results,
            [
                (
                    "https://www.publications.gc.ca/site/eng/9.876543/publication.html",
                    "Biodiversity Plan 2024",
                ),
                (
                    "https://www.publications.gc.ca/collections/collection_2024/eccc/En1-45-2024-eng.pdf",
                    "Species at Risk Report (PDF)",
                ),
            ],
        )

    def test_extract_canada_publications_result_links_excludes_french_search_self_link(self) -> None:
        # Regression test: a 2026-07-27 live run found every single search
        # term - including genuinely zero-hit terms like "nature
        # restoration" - returning exactly one "result": a link back to the
        # French version of the search page itself
        # (/site/fra/recherche/recherche.html), present as boilerplate on
        # every results page. A real zero-hit search must come back as 0
        # candidates, not 1.
        results = non_eu._extract_canada_publications_result_links(CANADA_PUBLICATIONS_ZERO_HITS_HTML)

        self.assertEqual(results, [])

    def test_fetch_canada_documents_extracts_results_from_search_page(self) -> None:
        with patch.object(non_eu, "safe_get", return_value=_FakeResponse(200, CANADA_PUBLICATIONS_SEARCH_HTML)):
            df = non_eu.fetch_canada_documents(["biodiversity"], max_per_term=10)

        self.assertEqual(len(df), 2)
        self.assertTrue((df["jurisdiction"] == "Canada").all())
        self.assertTrue((df["source"] == "CA").all())
        self.assertEqual(
            df["doc_url"].tolist(),
            [
                "https://www.publications.gc.ca/site/eng/9.876543/publication.html",
                "https://www.publications.gc.ca/collections/collection_2024/eccc/En1-45-2024-eng.pdf",
            ],
        )

    def test_fetch_canada_documents_reports_zero_kept_for_genuinely_empty_search(self) -> None:
        with patch.object(non_eu, "safe_get", return_value=_FakeResponse(200, CANADA_PUBLICATIONS_ZERO_HITS_HTML)):
            df = non_eu.fetch_canada_documents(["nature restoration"], max_per_term=10)

        self.assertEqual(len(df), 0)

    def test_fetch_canada_documents_stops_when_max_per_term_reached(self) -> None:
        with patch.object(non_eu, "safe_get", return_value=_FakeResponse(200, CANADA_PUBLICATIONS_SEARCH_HTML)):
            df = non_eu.fetch_canada_documents(["biodiversity"], max_per_term=1)

        self.assertEqual(len(df), 1)

    def test_fetch_canada_documents_prints_progress_diagnostics_by_default(self) -> None:
        stdout = StringIO()
        with (
            patch.object(non_eu, "safe_get", return_value=_FakeResponse(200, CANADA_PUBLICATIONS_SEARCH_HTML)),
            patch.object(non_eu.time, "sleep"),
            redirect_stdout(stdout),
        ):
            non_eu.fetch_canada_documents(["biodiversity"], max_per_term=10)

        output = stdout.getvalue()
        self.assertIn("[CA] term='biodiversity'", output)
        self.assertIn("candidates=2", output)
        self.assertIn("DONE -> kept=2", output)
        self.assertIn("[CA] total rows kept: 2", output)

    def test_fetch_canada_documents_verbose_false_suppresses_output(self) -> None:
        stdout = StringIO()
        with (
            patch.object(non_eu, "safe_get", return_value=_FakeResponse(200, CANADA_PUBLICATIONS_SEARCH_HTML)),
            patch.object(non_eu.time, "sleep"),
            redirect_stdout(stdout),
        ):
            non_eu.fetch_canada_documents(["biodiversity"], max_per_term=1, verbose=False)

        self.assertEqual(stdout.getvalue(), "")

    def test_fetch_canada_documents_logs_non_200_status_and_continues_to_next_term(self) -> None:
        def fake_safe_get(url: str, **kwargs) -> _FakeResponse:
            if "soil" in url:
                return _FakeResponse(503, "")
            return _FakeResponse(200, CANADA_PUBLICATIONS_SEARCH_HTML)

        stdout = StringIO()
        with (
            patch.object(non_eu, "safe_get", side_effect=fake_safe_get),
            patch.object(non_eu.time, "sleep"),
            redirect_stdout(stdout),
        ):
            df = non_eu.fetch_canada_documents(["soil biodiversity", "biodiversity"], max_per_term=10)

        output = stdout.getvalue()
        self.assertIn("term='soil biodiversity' ERROR -> HTTP 503", output)
        self.assertEqual(len(df), 2)

    def test_clean_canada_doc_id_extracts_regulation_key_from_title(self) -> None:
        self.assertEqual(
            non_eu.clean_canada_doc_id({"title": "Fishery (General) Regulations SOR/2021-118", "url": ""}),
            "SOR_2021_118",
        )

    def test_clean_canada_doc_id_falls_back_to_url_path(self) -> None:
        self.assertEqual(
            non_eu.clean_canada_doc_id(
                {"title": "Biodiversity Plan 2024", "url": "https://www.publications.gc.ca/site/eng/9.876543/publication.html"}
            ),
            "site_eng_9.876543_publication",
        )

    def test_clean_canada_title_removes_trailing_catalogue_identifier(self) -> None:
        self.assertEqual(
            non_eu.clean_canada_title(
                "Soil biodiversity : what's most important? : A59-82/2021E-PDF"
            ),
            "Soil biodiversity : what's most important?",
        )
        self.assertEqual(
            non_eu.clean_canada_title(
                "Compendium of Canada's engagement in international environmental agreements and instruments. Intergovernmental Platform on Biodiversity and Ecosystem Services (IPBES). En4-381/4-5-2018E-PDF"
            ),
            "Compendium of Canada's engagement in international environmental agreements and instruments. Intergovernmental Platform on Biodiversity and Ecosystem Services (IPBES).",
        )

    def test_clean_canada_full_text_trims_boilerplate_and_common_encoding_noise(self) -> None:
        cleaned = non_eu.clean_canada_full_text(
            "Title - Government of Canada Publications - Canada.ca "
            "Passer au contenu principal "
            "Passer à « À propos de ce site » "
            "Language selection Français fr / Gouvernement du Canada "
            "Search Search Canada.ca Search Menu Main Menu "
            "Useful body text with biodiversitÃ© and authorâ€™s note. "
            "Page details Report a problem or mistake on this page "
            "About this site Government of Canada All contacts Departments and agencies"
        )

        self.assertIn("Useful body text", cleaned)
        self.assertIn("biodiversité", cleaned)
        self.assertIn("author's note", cleaned)
        self.assertNotIn("Passer au contenu principal", cleaned)
        self.assertNotIn("Government of Canada Publications - Canada.ca", cleaned)

    def test_should_skip_canada_url_flags_data_files(self) -> None:
        self.assertTrue(non_eu.should_skip_canada_url("https://www.publications.gc.ca/tbl/csv/example.csv"))
        self.assertTrue(non_eu.should_skip_canada_url("https://www.publications.gc.ca/download/example.zip"))
        self.assertFalse(non_eu.should_skip_canada_url("https://www.publications.gc.ca/collections/example-eng.pdf"))

    def test_get_url_candidates_for_canada_publication_and_pdf_urls(self) -> None:
        self.assertEqual(
            non_eu.get_url_candidates(
                {"url": "https://www.publications.gc.ca/site/eng/9.876543/publication.html"},
                "CA",
                None,
            ),
            [("https://www.publications.gc.ca/site/eng/9.876543/publication.html", "ca_publication")],
        )
        self.assertEqual(
            non_eu.get_url_candidates(
                {"url": "https://www.publications.gc.ca/collections/collection_2024/eccc/En1-45-2024-eng.pdf"},
                "CA",
                None,
            ),
            [("https://www.publications.gc.ca/collections/collection_2024/eccc/En1-45-2024-eng.pdf", "pdf")],
        )

    def test_enrich_canada_publication_follows_embedded_pdf_link_for_real_full_text(self) -> None:
        # Regression test: a 2026-07-27 live run found every CA full_text
        # was just the /publication.html catalogue-record page's own
        # boilerplate (title, department, "Permanent link to this
        # Catalogue record", "MARC XML format MARC HTML format", nav menu
        # chrome) rather than the actual document - because there was no
        # dedicated handling for "ca_publication" candidates, so it fell
        # through to the generic HTML-page handler, which just took the
        # landing page's own visible text. The real content lives at a PDF
        # the landing page links to.
        #
        # The marcXml.html response here has no 856 field, so this
        # exercises the HTML-scrape fallback path specifically (see
        # test_enrich_canada_publication_prefers_marc_xml_856_link_when_available
        # for the primary MARC XML path).
        landing_url = "https://www.publications.gc.ca/site/eng/9.576782/publication.html"
        marc_url = "https://www.publications.gc.ca/site/eng/9.576782/marcXml.html"
        pdf_url = "https://www.publications.gc.ca/collections/collection_2005/environ/FA1-2-2005-3E.pdf"
        session = _FakeSession(
            {
                landing_url: _FakeResponse(
                    200,
                    f"""
                    <html><body>
                      You are here: Canada.ca About government Government communications
                      Government of Canada Publications
                      Report of the Commissioner ... : FA1-2/2005-3E-PDF
                      <a href="{pdf_url}">PDF Version</a>
                      Permanent link to this Catalogue record MARC XML format MARC HTML format
                    </body></html>
                    """,
                ),
                marc_url: _FakeResponse(200, "<record><leader>00000</leader></record>"),
                pdf_url: _FakeResponse(
                    200,
                    "",
                    content=b"%PDF-1.4 fake pdf bytes",
                    headers={"content-type": "application/pdf"},
                ),
            }
        )

        with (
            patch.object(non_eu, "_get_thread_session", return_value=session),
            patch.object(non_eu, "_get_thread_robots", return_value=_FakeRobots()),
            patch.object(non_eu, "_extract_pdf_text", return_value="The actual audit report full text."),
        ):
            enriched = non_eu.enrich_one_record_fulltext(
                {
                    "source": "CA",
                    "jurisdiction": "Canada",
                    "url": landing_url,
                },
                us_api_key=None,
                obey_robots=True,
            )

        self.assertEqual(enriched["full_text"], "The actual audit report full text.")
        self.assertEqual(enriched["full_text_url"], pdf_url)
        self.assertEqual(enriched["full_text_format"], "pdf")
        self.assertEqual(enriched["full_text_error"], "")

    def test_enrich_canada_publication_prefers_marc_xml_856_link_when_available(self) -> None:
        # A 2026-07-28 live rerun (after the HTML-scrape PDF-follow fix
        # above shipped) found every CA full_text was STILL landing-page
        # boilerplate: the "Electronic document" link on these catalogue
        # pages isn't always a literal *.pdf*-suffixed href, so the
        # HTML-scrape extractor missed it. Cat identified the reliable
        # source: the record's MARC XML metadata page has a standard 856
        # field whose $u subfield is the real document URL. This is tried
        # first, before the HTML-scrape fallback.
        landing_url = "https://www.publications.gc.ca/site/eng/9.698872/publication.html"
        marc_url = "https://www.publications.gc.ca/site/eng/9.698872/marcXml.html"
        pdf_url = "https://publications.gc.ca/collections/collection_2007/ic/Iu91-4-8-2004E.pdf"
        marc_xml = f"""<?xml version="1.0"?>
        <marc:record xmlns:marc="http://www.loc.gov/MARC21/slim">
          <marc:datafield tag="856" ind1="4" ind2="0">
            <marc:subfield code="a">http://publications.gc.ca</marc:subfield>
            <marc:subfield code="q">PDF</marc:subfield>
            <marc:subfield code="s">5640 KB</marc:subfield>
            <marc:subfield code="u">{pdf_url}</marc:subfield>
          </marc:datafield>
        </marc:record>
        """
        session = _FakeSession(
            {
                landing_url: _FakeResponse(
                    200,
                    "<html><body>Government of Canada Publications landing page chrome.</body></html>",
                ),
                marc_url: _FakeResponse(200, marc_xml),
                pdf_url: _FakeResponse(
                    200,
                    "",
                    content=b"%PDF-1.4 fake pdf bytes",
                    headers={"content-type": "application/pdf"},
                ),
            }
        )

        with (
            patch.object(non_eu, "_get_thread_session", return_value=session),
            patch.object(non_eu, "_get_thread_robots", return_value=_FakeRobots()),
            patch.object(non_eu, "_extract_pdf_text", return_value="The real document text from the MARC-linked PDF."),
        ):
            enriched = non_eu.enrich_one_record_fulltext(
                {
                    "source": "CA",
                    "jurisdiction": "Canada",
                    "url": landing_url,
                },
                us_api_key=None,
                obey_robots=True,
            )

        self.assertEqual(enriched["full_text"], "The real document text from the MARC-linked PDF.")
        self.assertEqual(enriched["full_text_url"], pdf_url)
        self.assertEqual(enriched["full_text_format"], "pdf")
        self.assertEqual(enriched["full_text_error"], "")

    def test_canada_marc_xml_url_derives_sibling_page_from_publication_html(self) -> None:
        self.assertEqual(
            non_eu._canada_marc_xml_url("https://www.publications.gc.ca/site/eng/9.698872/publication.html"),
            "https://www.publications.gc.ca/site/eng/9.698872/marcXml.html",
        )
        self.assertEqual(non_eu._canada_marc_xml_url(""), "")
        self.assertEqual(
            non_eu._canada_marc_xml_url("https://www.publications.gc.ca/collections/example.pdf"),
            "",
        )

    def test_extract_canada_marc_pdf_url_finds_856_subfield_u(self) -> None:
        xml = """<?xml version="1.0"?>
        <marc:record xmlns:marc="http://www.loc.gov/MARC21/slim">
          <marc:datafield tag="856" ind1="4" ind2="0">
            <marc:subfield code="a">http://publications.gc.ca</marc:subfield>
            <marc:subfield code="u">https://publications.gc.ca/collections/collection_2007/ic/Iu91-4-8-2004E.pdf</marc:subfield>
          </marc:datafield>
        </marc:record>
        """
        self.assertEqual(
            non_eu._extract_canada_marc_pdf_url(xml),
            "https://publications.gc.ca/collections/collection_2007/ic/Iu91-4-8-2004E.pdf",
        )

    def test_extract_canada_marc_pdf_url_returns_empty_without_856_field(self) -> None:
        self.assertEqual(non_eu._extract_canada_marc_pdf_url("<record><leader>00000</leader></record>"), "")
        self.assertEqual(non_eu._extract_canada_marc_pdf_url(""), "")
        self.assertEqual(non_eu._extract_canada_marc_pdf_url("not xml at all <<<"), "")

    def test_extract_canada_publication_pdf_url_matches_link_text_mentioning_pdf_without_pdf_suffix(self) -> None:
        # Broadened 2026-07-28: the catalogue landing page's "Electronic
        # document" link isn't always a literal *.pdf*-ending href (some
        # are reached through a redirect/collections link, e.g.
        # /pub?id=...&sl=1), so this now also accepts a link whose
        # visible text mentions "pdf" even without a .pdf-suffixed href.
        landing_url = "https://www.publications.gc.ca/site/eng/9.576782/publication.html"
        html = """
        <html><body>
          <a href="/pub?id=9.576782&sl=0">Permanent link</a>
          <a href="/pub?id=9.576782&sl=1">FA1-2-2005-3E.pdf (PDF, 547 KB).</a>
        </body></html>
        """
        self.assertEqual(
            non_eu._extract_canada_publication_pdf_url(landing_url, html),
            "https://www.publications.gc.ca/pub?id=9.576782&sl=1",
        )

    def test_extract_canada_publication_pdf_url_matches_collections_path_without_pdf_suffix(self) -> None:
        # Also accepts a publications.gc.ca /collections/ path even
        # without a literal .pdf suffix or "pdf" in the link text -
        # that's the document-hosting path prefix regardless of
        # extension. The actual content-type is verified after
        # fetching, so a false positive here just costs one wasted
        # fetch rather than mislabeling the wrong thing as the document.
        landing_url = "https://www.publications.gc.ca/site/eng/9.576782/publication.html"
        html = """
        <html><body>
          <a href="/pub?id=9.576782&sl=0">Permanent link</a>
          <a href="https://publications.gc.ca/collections/Collection/FA1-2-2005-3E">View document</a>
        </body></html>
        """
        self.assertEqual(
            non_eu._extract_canada_publication_pdf_url(landing_url, html),
            "https://publications.gc.ca/collections/Collection/FA1-2-2005-3E",
        )

    def test_extract_canada_publication_pdf_url_finds_embedded_pdf_link(self) -> None:
        landing_url = "https://www.publications.gc.ca/site/eng/9.576782/publication.html"
        html = """
        <html><body>
          <a href="/pub?id=9.576782&sl=0">Permanent link</a>
          <a href="/collections/collection_2005/environ/FA1-2-2005-3E.pdf">PDF Version</a>
        </body></html>
        """

        self.assertEqual(
            non_eu._extract_canada_publication_pdf_url(landing_url, html),
            "https://www.publications.gc.ca/collections/collection_2005/environ/FA1-2-2005-3E.pdf",
        )

    def test_extract_canada_publication_pdf_url_returns_empty_when_no_pdf_link_present(self) -> None:
        landing_url = "https://www.publications.gc.ca/site/eng/9.123456/publication.html"
        html = """
        <html><body>
          <a href="/pub?id=9.123456&sl=0">Permanent link</a>
          <a href="/site/eng/9.123456/marc.xml">MARC XML format</a>
        </body></html>
        """

        self.assertEqual(non_eu._extract_canada_publication_pdf_url(landing_url, html), "")

    def test_enrich_canada_publication_falls_back_to_landing_page_when_no_asset_is_available(self) -> None:
        landing_url = "https://www.publications.gc.ca/site/eng/9.123456/publication.html"
        marc_url = "https://www.publications.gc.ca/site/eng/9.123456/marcXml.html"
        session = _FakeSession(
            {
                landing_url: _FakeResponse(
                    200,
                    """
                    <html><body>
                      Government of Canada Publications - Canada.ca
                      Useful landing page content for fallback.
                    </body></html>
                    """,
                ),
                marc_url: _FakeResponse(200, "<record><leader>00000</leader></record>"),
            }
        )

        with (
            patch.object(non_eu, "_get_thread_session", return_value=session),
            patch.object(non_eu, "_get_thread_robots", return_value=_FakeRobots()),
        ):
            enriched = non_eu.enrich_one_record_fulltext(
                {
                    "source": "CA",
                    "jurisdiction": "Canada",
                    "url": landing_url,
                },
                us_api_key=None,
                obey_robots=True,
            )

        self.assertIn("Useful landing page content for fallback.", enriched["full_text"])
        self.assertEqual(enriched["full_text_url"], landing_url)
        self.assertEqual(enriched["full_text_format"], "html")
        self.assertEqual(enriched["full_text_error"], "")
        # Regression test for a 2026-07-28 live run where every single CA
        # record fell back to this landing-page text even after the MARC
        # XML fix shipped, with no way to tell from the output alone
        # whether that's because the MARC page had no 856 $u, the
        # HTML-scrape fallback also failed, or something else entirely -
        # full_text_error stays "" here (some text WAS retrieved), so this
        # dedicated field carries the reason instead.
        self.assertEqual(enriched["full_text_pdf_lookup_status"], "canada_publication_marc_no_856_url")

    def test_full_text_pdf_lookup_status_is_empty_when_the_pdf_is_actually_found(self) -> None:
        # The flip side of the regression above: once a real PDF is found
        # (via either the MARC XML path or the HTML-scrape fallback), the
        # function returns immediately and never touches
        # full_text_pdf_lookup_status - it should stay unset/empty rather
        # than carrying a stale value.
        landing_url = "https://www.publications.gc.ca/site/eng/9.576782/publication.html"
        marc_url = "https://www.publications.gc.ca/site/eng/9.576782/marcXml.html"
        pdf_url = "https://publications.gc.ca/collections/collection_2007/ic/Iu91-4-8-2004E.pdf"
        marc_xml = f"""<?xml version="1.0"?>
        <marc:record xmlns:marc="http://www.loc.gov/MARC21/slim">
          <marc:datafield tag="856" ind1="4" ind2="0">
            <marc:subfield code="u">{pdf_url}</marc:subfield>
          </marc:datafield>
        </marc:record>
        """
        session = _FakeSession(
            {
                landing_url: _FakeResponse(200, "<html><body>Landing chrome.</body></html>"),
                marc_url: _FakeResponse(200, marc_xml),
                pdf_url: _FakeResponse(
                    200, "", content=b"%PDF-1.4 fake pdf bytes", headers={"content-type": "application/pdf"},
                ),
            }
        )

        with (
            patch.object(non_eu, "_get_thread_session", return_value=session),
            patch.object(non_eu, "_get_thread_robots", return_value=_FakeRobots()),
            patch.object(non_eu, "_extract_pdf_text", return_value="Real PDF text."),
        ):
            enriched = non_eu.enrich_one_record_fulltext(
                {"source": "CA", "jurisdiction": "Canada", "url": landing_url},
                us_api_key=None,
                obey_robots=True,
            )

        self.assertEqual(enriched["full_text_format"], "pdf")
        self.assertEqual(enriched.get("full_text_pdf_lookup_status", ""), "")

    def test_canada_row_to_result_preserves_working_output_shape(self) -> None:
        adapter = NonEUAdapter()
        source = SourceConfig(name="canada-publications", adapter="non-eu", settings={"countries": ["CA"]})
        row = {
            "country": "Canada",
            "date": "2024",
            "doc_id": "SOR_2024_12",
            "doc_uid": "SOR_2024_12",
            "full_text_clean": "Canadian policy text.",
            "full_text_error": "",
            "full_text_format": "html",
            "full_text_url": "https://www.publications.gc.ca/collections/example-eng.pdf",
            "has_text": "True",
            "jurisdiction": "Canada",
            "lang": "en",
            "matched_terms": "[\"biodiversity\"]",
            "retrieval_status": "ok",
            "source": "CA",
            "source_file": "",
            "text_len": "22",
            "title": "Biodiversity Plan 2024",
            "url": "https://www.publications.gc.ca/collections/example-eng.pdf",
            "year": "2024",
        }

        result = adapter._row_to_result(row, source=source, source_log=[{"source": "CA", "ok": True}])

        self.assertEqual(result.payload["document_id"], "canada-publications:SOR_2024_12")
        self.assertEqual(result.payload["jurisdiction"], "Canada")
        self.assertEqual(result.payload["full_text"], "Canadian policy text.")
        self.assertEqual(
            result.payload["raw_record"]["full_text_url"],
            "https://www.publications.gc.ca/collections/example-eng.pdf",
        )


if __name__ == "__main__":
    unittest.main()
