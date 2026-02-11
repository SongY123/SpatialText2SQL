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
        self.documents: List[str] = []

    def clear(self) -> None:
        self.token_to_docs = {}
        self.docs = []
        self.documents = []

    def _tokenize(self, text: str) -> List[str]:
        tokens = re.findall(r"[a-zA-Z_][a-zA-Z0-9_]*", str(text).lower())
        return [t for t in tokens if len(t) >= self.min_token_length]

    @staticmethod
    def _build_doc_text(doc: Dict) -> str:
        parts: List[str] = []
        function_id = doc.get("function_id", "")
        if function_id:
            parts.append(str(function_id))
        chapter_info = doc.get("chapter_info", "")
        if chapter_info:
            parts.append(str(chapter_info))
        description = doc.get("description", "")
        if description:
            parts.append(str(description))

        for fd in doc.get("function_definitions", []):
            if isinstance(fd, dict):
                fn = fd.get("function_name", "")
                sig = fd.get("signature_str", "")
                if fn:
                    parts.append(str(fn))
                if sig:
                    parts.append(str(sig))

        for ex in doc.get("examples", []):
            if not isinstance(ex, dict):
                continue
            name = ex.get("name", "")
            if name:
                parts.append(str(name))
            for step in ex.get("steps", []):
                if not isinstance(step, dict):
                    continue
                q = step.get("question", "")
                sql = step.get("sql", "")
                if q:
                    parts.append(str(q))
                if sql:
                    parts.append(str(sql))
        return "\n".join(parts)

    @staticmethod
    def _resolve_doc_source(doc_source: str, index_path: Path) -> Path:
        p = Path(doc_source)
        if p.is_absolute():
            return p
        candidates = [
            index_path.parent / p,
            index_path.parents[1] / p if len(index_path.parents) > 1 else None,
            Path.cwd() / p,
        ]
        for c in candidates:
            if c and c.exists():
                return c
        return p

    @classmethod
    def _recover_documents_from_source(cls, payload: Dict, index_path: Path, target_count: int) -> List[str]:
        doc_source = payload.get("doc_source")
        if not doc_source:
            return []
        source_path = cls._resolve_doc_source(str(doc_source), index_path)
        if not source_path.exists():
            return []
        try:
            with open(source_path, "r", encoding="utf-8") as f:
                data = json.load(f)
        except Exception:
            return []
        if not isinstance(data, list):
            return []

        recovered: List[str] = []
        for item in data[:target_count]:
            if isinstance(item, dict):
                recovered.append(cls._build_doc_text(item))
            else:
                recovered.append(str(item))
        return recovered

    @property
    def token_count(self) -> int:
        return len(self.token_to_docs)

    @property
    def doc_count(self) -> int:
        return len(self.documents) if self.documents else len(self.docs)

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
            self.documents.append(str(text))

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
            document = None
            if 0 <= doc_id < len(self.documents):
                document = self.documents[doc_id]
            elif isinstance(meta, dict):
                document = meta.get("document")
            rows.append(
                {
                    "doc_id": doc_id,
                    "score": score,
                    "metadata": meta,
                    "document": document,
                }
            )
        return rows

    def to_payload(self, doc_source: str) -> Dict:
        return {
            "doc_source": doc_source,
            "doc_count": self.doc_count,
            "token_count": self.token_count,
            "token_to_docs": self.token_to_docs,
            "docs": self.docs,
            "documents": self.documents,
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
        documents = payload.get("documents")
        if documents is None:
            documents = []
            for item in searcher.docs:
                if isinstance(item, dict) and "document" in item:
                    documents.append(str(item.get("document", "")))
        if not documents:
            documents = cls._recover_documents_from_source(
                payload=payload,
                index_path=path,
                target_count=len(searcher.docs),
            )
        searcher.documents = documents
        return searcher
