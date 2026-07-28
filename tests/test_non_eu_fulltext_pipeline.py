from __future__ import annotations

import unittest
from contextlib import redirect_stdout
from io import StringIO
from unittest.mock import patch

from policy_corpus_builder.adapters import non_eu


class MatchedTermsFoundInTextTests(unittest.TestCase):
    # Regression tests for a 2026-07-28 live run finding several US hits
    # (regulations.gov's filter[searchTerm] appears to match at the
    # docket/submission level, not the individual attached document) and
    # AUS hits (a full-text-contains search over huge omnibus Acts with
    # no relevance ranking) where the fetched document shares no words
    # at all with the search term it supposedly matched.

    def test_returns_true_when_all_words_of_a_matched_term_are_present(self) -> None:
        self.assertTrue(
            non_eu._matched_terms_found_in_text(
                ["offshore renewable"],
                "Discussion paper on offshore renewable energy",
                "",
            )
        )

    def test_returns_true_when_words_are_split_across_title_and_text(self) -> None:
        self.assertTrue(
            non_eu._matched_terms_found_in_text(
                ["offshore renewable"],
                "Offshore energy discussion paper",
                "This covers renewable energy sources.",
            )
        )

    def test_returns_false_when_no_matched_term_words_appear_anywhere(self) -> None:
        # Modeled on a real 2026-07-28 US hit: a "blue economy" search
        # term matched a raw climate-data CSV export whose title/text
        # never mentions either word.
        self.assertFalse(
            non_eu._matched_terms_found_in_text(
                ["blue economy"],
                "WRI_2016_CAIT_Paris_Contributions_Map_All_raw_data_0224",
                "documentType: Supporting & Related Material agencyId: NHTSA",
            )
        )

    def test_returns_true_if_any_one_of_several_matched_terms_is_found(self) -> None:
        self.assertTrue(
            non_eu._matched_terms_found_in_text(
                ["blue economy", "offshore wind"],
                "Offshore Wind Farm Environmental Review",
                "",
            )
        )

    def test_returns_none_when_there_are_no_matched_terms(self) -> None:
        self.assertIsNone(non_eu._matched_terms_found_in_text([], "Some title", "Some text"))
        self.assertIsNone(non_eu._matched_terms_found_in_text(None, "Some title", "Some text"))

    def test_returns_none_when_there_is_no_text_to_check(self) -> None:
        # A fetch failure (empty title and full_text) shouldn't be
        # reported as an irrelevant match - there's nothing to judge.
        self.assertIsNone(non_eu._matched_terms_found_in_text(["offshore wind"], "", ""))

    def test_accepts_a_single_string_matched_term_as_well_as_a_list(self) -> None:
        self.assertTrue(
            non_eu._matched_terms_found_in_text("nature repair", "Nature Repair (Committee) Rules 2024", "")
        )


def _make_record(source: str, url: str, matched_terms: list[str], title: str = "") -> dict:
    return {
        "source": source,
        "jurisdiction": "Canada" if source == "CA" else source,
        "url": url,
        "title": title,
        "matched_terms": matched_terms,
    }


class AddFullTextsParallelTermVerifiedTests(unittest.TestCase):
    def test_flags_term_verified_false_when_fetched_text_lacks_the_matched_term(self) -> None:
        record = _make_record(
            "US",
            "https://api.regulations.gov/v4/documents/EPA-HQ-OLEM-2018-0024-0011",
            ["nature repair"],
            title="National Response Center Data 2013",
        )

        def fake_enrich(rec, **kwargs):
            out = dict(rec)
            out["full_text"] = "documentType: Supporting & Related Material agencyId: EPA"
            out["full_text_url"] = rec["url"]
            out["full_text_format"] = "html"
            out["full_text_error"] = ""
            return out

        with patch.object(non_eu, "enrich_one_record_fulltext", side_effect=fake_enrich):
            [enriched] = non_eu.add_full_texts_parallel([record], us_api_key=None, progress_every=0)

        self.assertFalse(enriched["term_verified"])

    def test_flags_term_verified_true_when_fetched_text_contains_the_matched_term(self) -> None:
        record = _make_record(
            "AUS",
            "https://www.legislation.gov.au/F2024L00848/asmade/text",
            ["nature repair"],
            title="Nature Repair (Committee) Rules 2024",
        )

        def fake_enrich(rec, **kwargs):
            out = dict(rec)
            out["full_text"] = "Nature Repair (Committee) Rules 2024, made under the Nature Repair Act."
            out["full_text_url"] = rec["url"]
            out["full_text_format"] = "html"
            out["full_text_error"] = ""
            return out

        with patch.object(non_eu, "enrich_one_record_fulltext", side_effect=fake_enrich):
            [enriched] = non_eu.add_full_texts_parallel([record], us_api_key=None, progress_every=0)

        self.assertTrue(enriched["term_verified"])

    def test_leaves_term_verified_none_when_full_text_fetch_failed(self) -> None:
        record = _make_record("NZ", "https://www.legislation.govt.nz/act/public/2024/56/en/latest", ["offshore wind"])

        def fake_enrich(rec, **kwargs):
            out = dict(rec)
            out["full_text"] = ""
            out["full_text_error"] = "waf_challenge"
            return out

        with patch.object(non_eu, "enrich_one_record_fulltext", side_effect=fake_enrich):
            [enriched] = non_eu.add_full_texts_parallel([record], us_api_key=None, progress_every=0)

        self.assertIsNone(enriched["term_verified"])

    def test_prints_a_relevance_summary_line_when_any_records_were_checked(self) -> None:
        record = _make_record(
            "US", "https://api.regulations.gov/v4/documents/x-1", ["blue economy"], title="Unrelated CSV export",
        )

        def fake_enrich(rec, **kwargs):
            out = dict(rec)
            out["full_text"] = "Nothing to do with the term."
            out["full_text_url"] = rec["url"]
            out["full_text_format"] = "html"
            out["full_text_error"] = ""
            return out

        stdout = StringIO()
        with patch.object(non_eu, "enrich_one_record_fulltext", side_effect=fake_enrich), redirect_stdout(stdout):
            non_eu.add_full_texts_parallel([record], us_api_key=None, progress_every=0)

        self.assertIn("[RELEVANCE] 1/1 fetched document(s)", stdout.getvalue())


class AddFullTextsParallelCrossTermCacheTests(unittest.TestCase):
    # Regression tests for a 2026-07-27 live run finding the same document
    # independently fetched (and PDF-parsed) once per matching search term
    # within a single jurisdiction run - one 5.4MB EU document was fetched
    # 5 separate times. Each query term is otherwise a fully independent,
    # stateless pipeline call (see run_non_eu_query_pipeline), so a cache
    # shared across those calls (by the caller, in practice NonEUAdapter)
    # is the only way to notice "we already have this one."

    def test_second_call_reuses_the_cached_result_instead_of_refetching(self) -> None:
        url = "https://eur-lex.europa.eu/legal-content/EN/TXT/?uri=CELEX:52025SC0001"
        record_term_a = _make_record("EU", url, ["offshore wind"], title="Commission Staff Working Document")
        record_term_b = _make_record("EU", url, ["blue economy"], title="Commission Staff Working Document")

        call_count = {"n": 0}

        def fake_enrich(rec, **kwargs):
            call_count["n"] += 1
            out = dict(rec)
            out["full_text"] = "The real, large Commission working document body text."
            out["full_text_url"] = rec["url"]
            out["full_text_format"] = "html"
            out["full_text_error"] = ""
            return out

        cache: dict[str, dict] = {}
        with patch.object(non_eu, "enrich_one_record_fulltext", side_effect=fake_enrich):
            [first] = non_eu.add_full_texts_parallel(
                [record_term_a], us_api_key=None, progress_every=0, fulltext_cache=cache,
            )
            [second] = non_eu.add_full_texts_parallel(
                [record_term_b], us_api_key=None, progress_every=0, fulltext_cache=cache,
            )

        self.assertEqual(call_count["n"], 1, "the second term's call should reuse the cache, not refetch")
        self.assertEqual(second["full_text"], first["full_text"])
        self.assertEqual(second["full_text_format"], "html")
        # The reused record keeps its own term/title, not the first call's.
        self.assertEqual(second["matched_terms"], ["blue economy"])

    def test_failed_fetches_are_not_cached_so_a_later_term_can_retry(self) -> None:
        url = "https://www.legislation.govt.nz/act/public/2024/56/en/latest"
        record_term_a = _make_record("NZ", url, ["offshore wind"])
        record_term_b = _make_record("NZ", url, ["offshore renewable"])

        call_count = {"n": 0}

        def fake_enrich(rec, **kwargs):
            call_count["n"] += 1
            out = dict(rec)
            if call_count["n"] == 1:
                out["full_text"] = ""
                out["full_text_error"] = "waf_challenge"
            else:
                out["full_text"] = "Second attempt succeeded."
                out["full_text_url"] = rec["url"]
                out["full_text_format"] = "nz_xml"
                out["full_text_error"] = ""
            return out

        cache: dict[str, dict] = {}
        with patch.object(non_eu, "enrich_one_record_fulltext", side_effect=fake_enrich):
            [first] = non_eu.add_full_texts_parallel(
                [record_term_a], us_api_key=None, progress_every=0, fulltext_cache=cache,
            )
            [second] = non_eu.add_full_texts_parallel(
                [record_term_b], us_api_key=None, progress_every=0, fulltext_cache=cache,
            )

        self.assertEqual(call_count["n"], 2, "a failed fetch must not be cached against a later retry")
        self.assertEqual(first["full_text"], "")
        self.assertEqual(second["full_text"], "Second attempt succeeded.")

    def test_no_cache_given_behaves_exactly_like_before(self) -> None:
        url = "https://api.regulations.gov/v4/documents/x-1"
        record = _make_record("US", url, ["offshore wind"])

        with patch.object(non_eu, "enrich_one_record_fulltext", side_effect=lambda rec, **kw: {**rec, "full_text": "ok", "full_text_error": ""}):
            [enriched] = non_eu.add_full_texts_parallel([record], us_api_key=None, progress_every=0)

        self.assertEqual(enriched["full_text"], "ok")


class NonEUAdapterFulltextCacheTests(unittest.TestCase):
    def test_adapter_instance_owns_one_cache_dict_reused_across_collect_calls(self) -> None:
        from policy_corpus_builder.adapters.non_eu_adapter import NonEUAdapter

        adapter = NonEUAdapter()
        self.assertEqual(adapter._fulltext_cache, {})

        # A fresh adapter instance must not share state with another one -
        # get_adapter() creates a new instance per source, so caching
        # would only be safe/correct if each instance's cache starts empty
        # and is never shared across different NonEUAdapter() objects.
        other = NonEUAdapter()
        adapter._fulltext_cache["EU:CELEX:1"] = {"full_text": "x"}
        self.assertEqual(other._fulltext_cache, {})


if __name__ == "__main__":
    unittest.main()
