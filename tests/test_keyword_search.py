import unittest
from pathlib import Path

import yaml

from tools.keyword_search import JsonKeywordSearcher


class TestJsonKeywordSearcher(unittest.TestCase):
    @staticmethod
    def _load_preprocess_config() -> dict:
        project_root = Path(__file__).resolve().parents[1]
        config_path = project_root / "config" / "preprocess.yml"
        with open(config_path, "r", encoding="utf-8") as f:
            return yaml.safe_load(f) or {}

    def test_keyword_search_from_preprocess_config_paths(self) -> None:
        cfg = self._load_preprocess_config()
        kw_cfg = cfg.get("keyword_search", {}) or {}

        project_root = Path(__file__).resolve().parents[1]
        output_path = project_root / str(kw_cfg.get("output_path", "data/indexes/keyword/keyword_index.json"))
        min_token_len = int(kw_cfg.get("min_token_length", 2))

        self.assertTrue(output_path.exists(), f"Keyword index not found: {output_path}")

        searcher = JsonKeywordSearcher.load(index_path=output_path, min_token_length=min_token_len)
        self.assertGreater(searcher.doc_count, 0)
        self.assertGreater(searcher.token_count, 0)

        query_text = "debugging address standardizer"
        results = searcher.search(query_text, top_k=5)
        self.assertGreater(len(results), 0)
        first = results[0]
        self.assertIn("doc_id", first)
        self.assertIn("score", first)
        self.assertIn("metadata", first)
        self.assertIn("document", first)
        self.assertIsInstance(first["document"], str)
        self.assertGreater(len(first["document"]), 0)
        self.assertIsInstance(first["metadata"], dict)
        self.assertIn("function_id", first["metadata"])
        self.assertTrue(
            any(
                "debug_standardize_address" in str(item.get("metadata", {}).get("function_id", ""))
                for item in results
            ),
            f"Expected keyword results for '{query_text}' to include debug_standardize_address",
        )


if __name__ == "__main__":
    unittest.main()
