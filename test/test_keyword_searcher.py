import sys
import types
import unittest
import importlib.util
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

try:
    import sklearn  # type: ignore  # noqa: F401
except ModuleNotFoundError:
    sklearn_module = types.ModuleType("sklearn")
    feature_extraction = types.ModuleType("sklearn.feature_extraction")
    text_module = types.ModuleType("sklearn.feature_extraction.text")
    metrics_module = types.ModuleType("sklearn.metrics")
    pairwise_module = types.ModuleType("sklearn.metrics.pairwise")

    class _DummyTfidfVectorizer:
        def __init__(self, *args, **kwargs):
            pass

    def _dummy_cosine_similarity(*args, **kwargs):
        return []

    text_module.TfidfVectorizer = _DummyTfidfVectorizer
    pairwise_module.cosine_similarity = _dummy_cosine_similarity

    sys.modules["sklearn"] = sklearn_module
    sys.modules["sklearn.feature_extraction"] = feature_extraction
    sys.modules["sklearn.feature_extraction.text"] = text_module
    sys.modules["sklearn.metrics"] = metrics_module
    sys.modules["sklearn.metrics.pairwise"] = pairwise_module

try:
    import numpy  # type: ignore  # noqa: F401
except ModuleNotFoundError:
    sys.modules["numpy"] = types.ModuleType("numpy")


def _load_module(module_name: str, file_path: Path):
    spec = importlib.util.spec_from_file_location(module_name, file_path)
    module = importlib.util.module_from_spec(spec)
    sys.modules[module_name] = module
    assert spec.loader is not None
    spec.loader.exec_module(module)
    return module


keyword_searcher_module = _load_module(
    "keyword_searcher_for_test",
    ROOT / "src" / "retrieval" / "keyword_searcher.py",
)
KeywordSearcher = keyword_searcher_module.KeywordSearcher


class KeywordSearcherFormatResultTests(unittest.TestCase):
    def setUp(self):
        self.searcher = KeywordSearcher({})

    def test_format_search_result_skips_none_sql_example(self):
        doc = {
            "function_id": "RT_ST_TRI",
            "description": "Example function without runnable SQL.",
            "examples": [
                {
                    "steps": [
                        {
                            "sql": None,
                        }
                    ]
                }
            ],
        }

        result = self.searcher._format_search_result(doc)

        self.assertIn("Function: RT_ST_TRI", result)
        self.assertIn("Description: Example function without runnable SQL.", result)
        self.assertNotIn("Example SQL:", result)


if __name__ == "__main__":
    unittest.main()
