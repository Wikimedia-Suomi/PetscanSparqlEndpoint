from django.test import SimpleTestCase

from petscan import normalization


class NormalizationTests(SimpleTestCase):
    def test_normalize_page_title_handles_empty_values(self):
        self.assertEqual(normalization.normalize_page_title(None), "")
        self.assertEqual(normalization.normalize_page_title(""), "")
        self.assertEqual(normalization.normalize_page_title("   "), "")

    def test_normalize_page_title_strips_leading_colon_and_spaces(self):
        self.assertEqual(normalization.normalize_page_title(":Albert Einstein"), "Albert_Einstein")
        self.assertEqual(normalization.normalize_page_title("  Turku post card  "), "Turku_post_card")

    def test_normalize_qid_accepts_common_formats(self):
        self.assertEqual(normalization.normalize_qid("Q42"), "Q42")
        self.assertEqual(normalization.normalize_qid("q937"), "Q937")
        self.assertEqual(normalization.normalize_qid("http://www.wikidata.org/entity/Q1"), "Q1")

    def test_normalize_qid_rejects_invalid_values(self):
        self.assertIsNone(normalization.normalize_qid(None))
        self.assertIsNone(normalization.normalize_qid(""))
        self.assertIsNone(normalization.normalize_qid("Q0"))
        self.assertIsNone(normalization.normalize_qid("Q01"))
        self.assertIsNone(normalization.normalize_qid("not-a-qid"))

    def test_normalize_qid_keeps_existing_search_semantics(self):
        self.assertEqual(normalization.normalize_qid("prefix Q123 suffix"), "Q123")
