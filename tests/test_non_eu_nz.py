from __future__ import annotations

import json
import unittest
from unittest.mock import patch

from policy_corpus_builder.adapters import non_eu


NZ_API_PAYLOAD = {
    "results": [
        {
            "work_id": "act_public_2024_12",
            "legislation_type": "act",
            "latest_matching_version": {
                "title": "Biodiversity Restoration Act 2024",
                "version_id": "act_public_2024_12_en_latest",
                "is_latest_version": True,
                "formats": [
                    {"type": "xml", "url": "https://www.legislation.govt.nz/act/public/2024/12/en/latest.xml"},
                    {"type": "pdf", "url": "https://www.legislation.govt.nz/act/public/2024/12/en/latest.pdf"},
                    {"type": "html", "url": "https://www.legislation.govt.nz/act/public/2024/12/en/latest/"},
                ],
            },
        }
    ],
    "page": 1,
    "per_page": 20,
    "total": 1,
}


NZ_XML = """
<document>
  <title>Biodiversity Restoration Act 2024</title>
  <body>
    <section>Restore biodiversity values across public lands.</section>
  </body>
</document>
"""


class _FakeResponse:
    def __init__(self, status_code: int, text: str = "", *, headers: dict[str, str] | None = None):
        self.status_code = status_code
        self.text = text
        self.headers = headers or {}
        self.content = text.encode("utf-8")

    def json(self):
        return json.loads(self.text)

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


class NonEUNewZealandTests(unittest.TestCase):
    def test_nz_search_url_uses_official_api_contract(self) -> None:
        self.assertEqual(
            non_eu.nz_search_url(non_eu.NZ_API_BASE, "biodiversity", page=2),
            "https://api.legislation.govt.nz/v0/works?search_term=biodiversity&search_field=content&page=2&per_page=20",
        )

    def test_extract_nz_api_rows_prefers_version_formats(self) -> None:
        rows = non_eu._extract_nz_api_rows("biodiversity", NZ_API_PAYLOAD, max_per_term=10)

        self.assertEqual(len(rows), 1)
        self.assertEqual(
            rows[0]["url"],
            "https://www.legislation.govt.nz/act/public/2024/12/en/latest/",
        )
        self.assertEqual(
            rows[0]["xml_url"],
            "https://www.legislation.govt.nz/act/public/2024/12/en/latest.xml",
        )
        self.assertEqual(
            rows[0]["pdf_url"],
            "https://www.legislation.govt.nz/act/public/2024/12/en/latest.pdf",
        )
        self.assertEqual(rows[0]["title"], "Biodiversity Restoration Act 2024")

    def test_fetch_nz_documents_uses_api_discovery(self) -> None:
        def fake_safe_get(url: str, **kwargs) -> _FakeResponse:
            self.assertEqual(
                kwargs["headers"]["X-Api-Key"],
                "nz-test-key",
            )
            return _FakeResponse(200, json.dumps(NZ_API_PAYLOAD))

        with patch.object(non_eu, "safe_get", side_effect=fake_safe_get):
            df = non_eu.fetch_nz_documents(
                ["biodiversity"],
                api_key="nz-test-key",
                max_per_term=5,
                verbose=False,
            )

        self.assertEqual(len(df), 1)
        self.assertEqual(df["source"].iloc[0], "NZ")
        self.assertEqual(df["jurisdiction"].iloc[0], "New Zealand")
        self.assertEqual(
            df["url"].iloc[0],
            "https://www.legislation.govt.nz/act/public/2024/12/en/latest/",
        )

    def test_fetch_nz_documents_uses_legacy_scraper_fallback_when_no_api_key_in_auto_mode(self) -> None:
        legacy_html = """
        <html><body>
          <a href="/act/public/2024/12/en/latest/">Biodiversity Restoration Act 2024</a>
          <a href="/regulation/public/2023/45/en/latest/">Marine Biodiversity Regulations 2023</a>
        </body></html>
        """

        with (
            patch.object(non_eu, "dns_check", return_value=True),
            patch.object(non_eu, "safe_get", return_value=_FakeResponse(200, legacy_html)),
            patch.object(non_eu.time, "sleep"),
        ):
            df = non_eu.fetch_nz_documents(
                ["biodiversity"],
                api_key=None,
                mode="auto",
                max_per_term=10,
                verbose=False,
            )

        self.assertEqual(len(df), 2)
        self.assertEqual(df["source"].tolist(), ["NZ", "NZ"])
        self.assertIn("https://www.legislation.govt.nz/act/public/2024/12/en/latest/", df["url"].tolist())

    def test_fetch_nz_documents_in_api_mode_without_key_fails_cleanly(self) -> None:
        df, diagnostics = non_eu.fetch_nz_documents(
            ["biodiversity"],
            api_key=None,
            mode="api",
            max_per_term=10,
            verbose=False,
            return_diagnostics=True,
        )

        self.assertEqual(len(df), 0)
        self.assertEqual(diagnostics.iloc[0]["stop_reason"], "missing_api_key")
        self.assertEqual(diagnostics.iloc[0]["mode"], "api")

    def test_get_url_candidates_for_nz_prefers_xml_then_pdf_then_html(self) -> None:
        candidates = non_eu.get_url_candidates(
            {
                "source": "NZ",
                "url": "https://www.legislation.govt.nz/act/public/2024/12/en/latest/",
                "xml_url": "https://www.legislation.govt.nz/act/public/2024/12/en/latest.xml",
                "pdf_url": "https://www.legislation.govt.nz/act/public/2024/12/en/latest.pdf",
                "text_url": "https://www.legislation.govt.nz/act/public/2024/12/en/latest/",
            },
            "NZ",
            None,
        )

        self.assertEqual(
            candidates,
            [
                ("https://www.legislation.govt.nz/act/public/2024/12/en/latest.xml", "nz_xml"),
                ("https://www.legislation.govt.nz/act/public/2024/12/en/latest.pdf", "pdf"),
                ("https://www.legislation.govt.nz/act/public/2024/12/en/latest/", "html"),
            ],
        )

    def test_enrich_nz_record_uses_xml_when_available(self) -> None:
        xml_url = "https://www.legislation.govt.nz/act/public/2024/12/en/latest.xml"
        session = _FakeSession({xml_url: _FakeResponse(200, NZ_XML)})

        with (
            patch.object(non_eu, "_get_thread_session", return_value=session),
            patch.object(non_eu, "_get_thread_robots", return_value=_FakeRobots()),
        ):
            enriched = non_eu.enrich_one_record_fulltext(
                {
                    "source": "NZ",
                    "jurisdiction": "New Zealand",
                    "url": "https://www.legislation.govt.nz/act/public/2024/12/en/latest/",
                    "xml_url": xml_url,
                },
                us_api_key=None,
                obey_robots=False,
            )

        self.assertEqual(enriched["full_text_format"], "nz_xml")
        self.assertEqual(enriched["full_text_url"], xml_url)
        self.assertIn("Restore biodiversity values", enriched["full_text"])


if __name__ == "__main__":
    unittest.main()
