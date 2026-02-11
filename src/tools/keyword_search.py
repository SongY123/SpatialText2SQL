from __future__ import annotations

import json
import re
from pathlib import Path
from typing import Dict, List, Optional


class JsonKeywordSearcher:
    """Keyword index/search utility persisted as JSON."""

    def __init__(self, min_token_length: int = 2, index_path: str | None = None) -> None:
        self.min_token_length = max(1, int(min_token_length))
        self.index_path = Path(index_path) if index_path else None
        self.token_to_docs: Dict[str, List[int]] = {}
        self.docs: List[Dict] = []

    def clear(self) -> None:
        self.token_to_docs = {}
        self.docs = []

    def _tokenize(self, text: str) -> List[str]:
        tokens = re.findall(r"[a-zA-Z_][a-zA-Z0-9_]*", str(text).lower())
        return [t for t in tokens if len(t) >= self.min_token_length]

    @property
    def token_count(self) -> int:
        return len(self.token_to_docs)

    @property
    def doc_count(self) -> int:
        return len(self.docs)

    def insert_documents(
        self,
        documents: List[str],
        ids: Optional[List[str]] = None,
        metadatas: Optional[List[Dict]] = None,
    ) -> int:
        if not documents:
            return 0

        if ids is not None and len(ids) != len(documents):
            raise ValueError("Length of ids must match documents.")
        if metadatas is not None and len(metadatas) != len(documents):
            raise ValueError("Length of metadatas must match documents.")

        base = len(self.docs)
        for i, text in enumerate(documents):
            doc_id = base + i
            meta = dict(metadatas[i]) if metadatas is not None else {}
            if ids is not None and "function_id" not in meta:
                meta["function_id"] = ids[i]

            entry = {"doc_id": doc_id}
            entry.update(meta)
            self.docs.append(entry)

            for token in sorted(set(self._tokenize(text))):
                self.token_to_docs.setdefault(token, []).append(doc_id)

        return len(documents)

    def search(self, query: str, top_k: int = 10) -> List[Dict]:
        n = max(1, int(top_k))
        scores: Dict[int, int] = {}
        for token in self._tokenize(query):
            for doc_id in self.token_to_docs.get(token, []):
                scores[doc_id] = scores.get(doc_id, 0) + 1

        ranked = sorted(scores.items(), key=lambda x: (-x[1], x[0]))[:n]
        rows: List[Dict] = []
        for doc_id, score in ranked:
            meta = self.docs[doc_id] if 0 <= doc_id < len(self.docs) else {"doc_id": doc_id}
            rows.append({"doc_id": doc_id, "score": score, "metadata": meta})
        return rows

    def to_payload(self, doc_source: str) -> Dict:
        return {
            "doc_source": doc_source,
            "doc_count": self.doc_count,
            "token_count": self.token_count,
            "token_to_docs": self.token_to_docs,
            "docs": self.docs,
        }

    def save(self, doc_source: str, output_path: str | Path | None = None) -> Path:
        path = Path(output_path) if output_path else self.index_path
        if path is None:
            raise ValueError("output_path is required when index_path is not set.")
        path.parent.mkdir(parents=True, exist_ok=True)
        with open(path, "w", encoding="utf-8") as f:
            json.dump(self.to_payload(doc_source), f, ensure_ascii=False)
        self.index_path = path
        return path

    @classmethod
    def load(cls, index_path: str | Path, min_token_length: int = 2) -> "JsonKeywordSearcher":
        path = Path(index_path)
        with open(path, "r", encoding="utf-8") as f:
            payload = json.load(f)

        searcher = cls(min_token_length=min_token_length, index_path=str(path))
        searcher.token_to_docs = payload.get("token_to_docs", {}) or {}
        searcher.docs = payload.get("docs", []) or []
        return searcher
