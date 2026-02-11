import unittest
from pathlib import Path

import chromadb
import yaml

class TestVectorRetrieval(unittest.TestCase):
    @staticmethod
    def _load_preprocess_config() -> dict:
        project_root = Path(__file__).resolve().parents[1]
        config_path = project_root / "config" / "preprocess.yml"
        with open(config_path, "r", encoding="utf-8") as f:
            return yaml.safe_load(f) or {}

    def test_vector_retrieval_for_debugging_address_standardizer(self) -> None:
        cfg = self._load_preprocess_config()
        vec_cfg = cfg.get("vectorize", {}) or {}

        project_root = Path(__file__).resolve().parents[1]
        chroma_path = project_root / str(vec_cfg.get("chroma_path", "data/indexes/vector/chroma"))
        collection_name = str(vec_cfg.get("collection_name", "postgis_extracted"))

        self.assertTrue(chroma_path.exists(), f"Chroma path not found: {chroma_path}")

        client = chromadb.PersistentClient(path=str(chroma_path))
        collection = client.get_or_create_collection(name=collection_name)
        self.assertGreater(collection.count(), 0)

        query_text = "debugging address standardizer"
        seed = collection.get(
            where_document={"$contains": query_text},
            include=["documents", "metadatas", "embeddings"],
        )
        seed_ids = seed.get("ids") or []
        seed_embeddings = seed.get("embeddings")
        if seed_embeddings is None:
            seed_embeddings = []
        self.assertGreater(
            len(seed_ids),
            0,
            f"No document found in collection for query seed text: {query_text}",
        )
        self.assertGreater(len(seed_embeddings), 0)

        top_k = 5
        result = collection.query(query_embeddings=[seed_embeddings[0]], n_results=top_k)
        self.assertIn("ids", result)
        self.assertIn("documents", result)
        self.assertIn("metadatas", result)
        self.assertIn("distances", result)

        self.assertGreater(len(result["ids"][0]), 0)
        self.assertLessEqual(len(result["ids"][0]), top_k)
        self.assertIn(seed_ids[0], result["ids"][0])


if __name__ == "__main__":
    unittest.main()
