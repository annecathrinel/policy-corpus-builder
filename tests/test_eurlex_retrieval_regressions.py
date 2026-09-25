import tempfile
import unittest
from pathlib import Path
from unittest.mock import Mock, patch
from urllib.parse import parse_qs, urlparse

import pandas as pd
import requests
from policy_corpus_builder.adapters import eurlex_supported as eu
from policy_corpus_builder.adapters.eurlex_nim_supported import surface as nim
from policy_corpus_builder.adapters.eurlex_nim_supported.workflow import _resolve_full_text
from policy_corpus_builder.utils.celex import extract_celex_token, parse_celex


def response(url, status=200, body=b"", content_type="text/html"):
    r = requests.Response()
    r.status_code, r.url, r._content = status, url, body
    r.headers["Content-Type"] = content_type
    return r


class RetrievalRegressions(unittest.TestCase):
    def test_parenthesized_celex_is_preserved(self):
        for value in ("32023H0901(22)", "https://eur-lex.europa.eu/?uri=CELEX%3A32023H0901%2822%29&qid=123"):
            self.assertEqual(extract_celex_token(value), "32023H0901(22)")
        self.assertTrue(parse_celex("32023H0901(22)").valid)

    def test_html_fallback_uses_complete_encoded_identifier(self):
        session = Mock()
        def get(url, **kwargs):
            if "publications.europa.eu" in url:
                return response(url, 404)
            self.assertIn("%2822%29", url)
            self.assertEqual(parse_qs(urlparse(url).query)["uri"], ["CELEX:32023H0901(22)"])
            return response(url, body=b"<html><body>" + b"Council recommendation. " * 30 + b"</body></html>")
        session.get.side_effect = get
        result = eu.get_eurlex_text_multi(pd.Series({"celex": "32023H0901(22)"}), session=session, retries=0)
        self.assertEqual(result["route_used"], "eurlex_html")
        self.assertEqual(len(result["attempt_trace"]), 4)
        self.assertGreater(len(result["full_text_clean"]), 150)

    def test_cellar_multiple_choice_concatenates_items(self):
        base = "https://publications.europa.eu/resource/cellar/abc.0001.03"
        listing = (
            '<html><body><ul><li title="manifestation">cellar:abc.0001.03<ul>'
            f'<li title="item"><a href="{base}/DOC_3">x</a><ul><li title="stream_name">1_EN_annexe.html</li>'
            '<li title="stream_order">3</li></ul></li>'
            f'<li title="item"><a href="{base}/DOC_1">x</a><ul><li title="stream_name">10_EN_ACT.html</li>'
            '<li title="stream_order">1</li></ul></li>'
            "</ul></li></ul></body></html>"
        ).encode()
        session = Mock()
        def get(url, **kwargs):
            if url.endswith("/celex/52020DC0381"):
                return response(url, 300, listing, "application/xhtml+xml")
            if url.endswith("/DOC_1"):
                return response(url, body=b"<html><body>" + b"Farm to Fork main act. " * 20 + b"</body></html>")
            if url.endswith("/DOC_3"):
                return response(url, body=b"<html><body>Annex action plan.</body></html>")
            self.fail(f"unexpected request {url}")
        session.get.side_effect = get
        result = eu.get_eurlex_text_multi(pd.Series({"celex": "52020DC0381"}), session=session, retries=0)
        self.assertEqual(result["route_used"], "cellar")
        self.assertEqual(result["cellar_items_used"], 2)
        self.assertLess(result["full_text_clean"].index("main act"), result["full_text_clean"].index("Annex action plan"))

    def test_cellar_html_recovers_acts_without_xhtml(self):
        session = Mock()
        def get(url, **kwargs):
            self.assertTrue(url.startswith("https://publications.europa.eu/"))
            if kwargs["headers"]["Accept"] == "text/html":
                return response(url, body=b"<html><body>" + b"Habitats Directive article. " * 20 + b"</body></html>")
            return response(url, 404)
        session.get.side_effect = get
        result = eu.get_eurlex_text_multi(pd.Series({"celex": "31992L0043"}), session=session, retries=0)
        self.assertEqual(result["route_used"], "cellar_html")
        self.assertIn("Habitats Directive", result["full_text_clean"])

    def test_erdf_mentions_are_not_rdf_metadata(self):
        text = "Support from the ERDF: investment priorities. " * 40
        self.assertEqual(eu._looks_like_valid_fulltext(text, min_chars=150), (True, ""))
        rdf = "rdf:type skos:Concept rdf:about dcterms:title " * 5
        self.assertFalse(eu._looks_like_valid_fulltext(rdf, min_chars=10)[0])

    def test_soap_read_timeout_is_retried_not_raised(self):
        session = Mock()
        ok = response(eu.EURLEX_WS_ENDPOINT, body=b"<xml/>", content_type="application/xml")
        session.post.side_effect = [requests.exceptions.ReadTimeout("slow"), ok]
        with patch.object(eu.time, "sleep"):
            xml, status, _ = eu.post_eurlex_ws("<x/>", session=session, retry_5xx=1, min_interval_s=0)
        self.assertEqual((xml, status), ("<xml/>", 200))
        session.post.side_effect = requests.exceptions.ReadTimeout("slow")
        with patch.object(eu.time, "sleep"):
            xml, status, raw = eu.post_eurlex_ws("<x/>", session=session, retry_5xx=1, min_interval_s=0)
        self.assertIsNone(xml)
        self.assertEqual(status, 0)
        self.assertIn("ReadTimeout", raw)

    def test_waf_challenge_skips_remaining_eurlex_routes(self):
        session = Mock()
        def get(url, **kwargs):
            if "publications.europa.eu" in url:
                return response(url, 404)
            r = response(url, 202)
            r.headers["x-amzn-waf-action"] = "challenge"
            return r
        session.get.side_effect = get
        result = eu.get_eurlex_text_multi(pd.Series({"celex": "52025JC0130"}), session=session, retries=0)
        self.assertEqual([a["route_name"] for a in result["attempt_trace"]], ["cellar", "cellar_html", "cellar_pdf", "eurlex_html"])
        self.assertEqual(eu._classify_failure({"retrieval_error": result["error"]}), "waf_challenge")

    def test_pdf_fallback_extracts_bytes(self):
        session = Mock()
        session.get.side_effect = lambda url, **kw: response(url, body=b"%PDF-fixture", content_type="application/pdf") if "/PDF/" in url else response(url, 404)
        with patch("pypdf.PdfReader") as reader:
            reader.return_value.pages = [Mock(extract_text=Mock(return_value="Commission decision. " * 30))]
            result = eu.get_eurlex_text_multi(pd.Series({"celex": "32024M11475"}), session=session, retries=0)
        self.assertEqual(result["route_used"], "eurlex_pdf")
        self.assertIn("Commission decision", result["full_text_clean"])
        self.assertEqual(len(result["attempt_trace"]), 5)

    def test_cellar_pdf_recovers_without_visiting_eurlex(self):
        session = Mock()
        def get(url, **kwargs):
            self.assertTrue(url.startswith("https://publications.europa.eu/"))
            self.assertIn("%2822%29", url)
            if kwargs["headers"]["Accept"] == "application/pdf":
                return response(url, body=b"%PDF-fixture", content_type="application/pdf")
            return response(url, 404)
        session.get.side_effect = get
        with patch("pypdf.PdfReader") as reader:
            reader.return_value.pages = [Mock(extract_text=Mock(return_value="Council recommendation. " * 30))]
            result = eu.get_eurlex_text_multi(pd.Series({"celex": "32023H0901(22)"}), session=session, retries=0)
        self.assertEqual(result["route_used"], "cellar_pdf")
        self.assertEqual(session.get.call_count, 3)
        self.assertGreater(len(result["full_text_clean"]), 150)

    def test_pdf_parse_error_and_challenge_are_not_fulltext(self):
        for body, ctype in ((b"%PDF-broken", "application/pdf"), (b"<html>JavaScript is disabled. " * 30, "text/html")):
            session = Mock()
            session.get.side_effect = lambda url, **kw: response(url, body=body, content_type=ctype)
            result = eu.get_eurlex_text_multi(pd.Series({"celex": "32024M11475"}), session=session, retries=0)
            self.assertEqual(result["full_text_clean"], "")
            self.assertTrue(result["error"])

    def test_rdf_datatypes_and_subjects_are_not_document_links(self):
        rdf = """<rdf:RDF xmlns:rdf="http://www.w3.org/1999/02/22-rdf-syntax-ns#" xmlns:c="http://publications.europa.eu/ontology/cdm#">
        <rdf:Description rdf:about="https://unrelated.test/subject">
        <c:national_website_link rdf:datatype="http://www.w3.org/2001/XMLSchema#anyURI">https://law.test/act/123</c:national_website_link>
        <c:eli rdf:datatype="http://www.w3.org/2001/XMLSchema#anyURI">https://law.test/eli/123</c:eli>
        <c:work_title rdf:datatype="http://www.w3.org/2001/XMLSchema#string">Act title</c:work_title>
        </rdf:Description></rdf:RDF>"""
        links = nim.extract_nim_page_links(rdf)
        self.assertEqual(links["all_links"], ["https://law.test/act/123", "https://law.test/eli/123"])

    def test_navigation_links_are_not_crawled(self):
        html = '<a href="https://userway.org/">Accessibility</a><a href="/privacy">Privacy</a><a href="/act.pdf">PDF</a>'
        links = nim.extract_nim_direct_access_links(html, "https://law.test/act")
        self.assertEqual([x["url"] for x in links], ["https://law.test/act.pdf"])

    def test_metadata_and_schema_pages_are_rejected(self):
        for url, text in (("https://eur-lex.europa.eu/legal-content/EN/TXT/", "National measure metadata " * 50), ("https://www.w3.org/", "Related Resources for XML Schema " * 50)):
            session = Mock()
            session.get.return_value = response(url, body=text.encode())
            with tempfile.TemporaryDirectory() as tmp:
                result = nim._fetch_text_from_candidate({"url": url, "link_type": "national_website"}, session=session, timeout=(1, 1), retries=0, min_interval_s=0, file_cache_dir=Path(tmp), verbose=False)
            self.assertEqual(result["text"], "")
            self.assertTrue(result["error"])
        self.assertIsNone(_resolve_full_text({"full_text_clean": "", "full_text_raw": "Metadata page"}))

    def test_old_nim_cache_is_refetched_and_validated_cache_resumes(self):
        row = pd.Series({"celex": "32023L0958", "nim_celex": "72023L0958EST_202505431", "national_measure_id": "202505431", "member_state_iso3": "EST", "member_state_name": "Estonia"})
        with tempfile.TemporaryDirectory() as tmp:
            cache = Path(tmp)
            path = nim._nim_text_path(row, cache)
            path.write_text("Stale metadata " * 100)
            with patch.object(nim, "fetch_nim_document_text", return_value=("Actual legislation " * 100, {"fetch_status": 200, "url_fetch": "https://law.test/act"})) as fetch:
                result = nim.batch_fetch_nim_fulltext(pd.DataFrame([row]), cache_dir=cache, verbose=False)
                self.assertEqual(fetch.call_count, 1)
            with patch.object(nim, "fetch_nim_document_text", side_effect=AssertionError("No network on validated cache hit")):
                resumed = nim.batch_fetch_nim_fulltext(pd.DataFrame([row]), cache_dir=cache, verbose=False)
            self.assertEqual(len(resumed), 1)
            self.assertEqual(resumed.iloc[0]["full_text_clean"], result.iloc[0]["full_text_clean"])


if __name__ == "__main__":
    unittest.main()
