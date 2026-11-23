import unittest
from urllib.parse import parse_qs, urlparse, unquote
from policy_corpus_builder.adapters import non_eu as n
from policy_corpus_builder.adapters.eurlex_supported import build_expert_query

class PhraseQuotingTests(unittest.TestCase):
    def test_quoted_and_unquoted_inventory_produce_same_requests(self):
        for builder in [n.build_uk_search_feed_url, n.build_aus_search_url,
                        n.build_canada_publications_search_url,
                        lambda t: n.nz_search_url('https://api.legislation.govt.nz/v0', t)]:
            with self.subTest(builder=builder):
                self.assertEqual(builder('nature credit'), builder('"nature credit"'))
                self.assertNotIn('""', unquote(builder('"nature credit"')))

    def test_eu_inventory_quotes_are_not_literal_search_characters(self):
        self.assertEqual(build_expert_query('DTS_SUBDOM = ALL_ALL', ['nature credit']),
                         build_expert_query('DTS_SUBDOM = ALL_ALL', ['"nature credit"']))

    def test_quoted_phrase_verification(self):
        self.assertTrue(n._matched_terms_found_in_text(['"nature credit"'], 'Nature credit programme', ''))
        self.assertFalse(n._matched_terms_found_in_text(['"nature credit"'], 'Marine planning', ''))

    def test_wildcard_verification_is_unknown(self):
        self.assertIsNone(n._matched_terms_found_in_text(['"nature credit*"'], 'Nature credits', ''))

if __name__ == '__main__':
    unittest.main()
