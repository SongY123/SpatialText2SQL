from __future__ import annotations

from abc import ABC, abstractmethod
import logging
from pathlib import Path
from typing import Any, Dict, List, Optional
import uuid

logger = logging.getLogger(__name__)


class VectorStore(ABC):
    """Standard interface for vector DB backends."""

    @abstractmethod
    def insert_documents(
        self,
        documents: List[str],
        ids: Optional[List[str]] = None,
        metadatas: Optional[List[Dict]] = None,
    ) -> int:
        raise NotImplementedError

    @abstractmethod
    def search(self, query: str, top_k: int = 5) -> List[Dict]:
        raise NotImplementedError


class SentenceTransformersEmbeddingFunction:
    """Embedding function for Chroma using sentence-transformers."""

    def __init__(
        self,
        model_name: str,
        batch_size: int = 8,
        normalize_embeddings: bool = True,
        model_kwargs: Optional[Dict[str, Any]] = None,
        tokenizer_kwargs: Optional[Dict[str, Any]] = None,
    ) -> None:
        try:
            from sentence_transformers import SentenceTransformer
        except ImportError as exc:
            raise RuntimeError(
                "Chroma vector store needs `sentence-transformers` for embeddings."
            ) from exc

        self.batch_size = batch_size
        self.normalize_embeddings = normalize_embeddings
        self.model_name = model_name
        self.model_kwargs = model_kwargs or {}
        self.tokenizer_kwargs = tokenizer_kwargs or {}
        self.model = SentenceTransformer(
            model_name_or_path=model_name,
            model_kwargs=self.model_kwargs,
            tokenizer_kwargs=self.tokenizer_kwargs,
        )

    @staticmethod
    def name() -> str:
        return "sentence_transformer"

    @staticmethod
    def build_from_config(config: Dict[str, Any]) -> "SentenceTransformersEmbeddingFunction":
        return SentenceTransformersEmbeddingFunction(
            model_name=config.get("model_name", "Qwen/Qwen3-Embedding-0.6B"),
            batch_size=int(config.get("batch_size", 8)),
            normalize_embeddings=bool(config.get("normalize_embeddings", True)),
            model_kwargs=config.get("model_kwargs") or {},
            tokenizer_kwargs=config.get("tokenizer_kwargs") or {},
        )

    def get_config(self) -> Dict[str, Any]:
        return {
            "model_name": self.model_name,
            "batch_size": self.batch_size,
            "normalize_embeddings": self.normalize_embeddings,
            "model_kwargs": self.model_kwargs,
            "tokenizer_kwargs": self.tokenizer_kwargs,
        }

    def __call__(self, input: List[str]) -> List[List[float]]:
        texts = list(input or [])
        if not texts:
            return []
        embeddings = self.model.encode(
            texts,
            batch_size=self.batch_size,
            normalize_embeddings=self.normalize_embeddings,
            convert_to_numpy=True,
            show_progress_bar=False,
        )
        return embeddings.tolist()

    def embed_query(self, query: str, prompt_name: Optional[str] = "query") -> List[float]:
        kwargs = {
            "batch_size": self.batch_size,
            "normalize_embeddings": self.normalize_embeddings,
            "convert_to_numpy": True,
            "show_progress_bar": False,
        }
        if prompt_name:
            try:
                query_emb = self.model.encode([query], prompt_name=prompt_name, **kwargs)
            except TypeError:
                logger.warning("Current sentence-transformers does not support prompt_name, fallback without prompt.")
                query_emb = self.model.encode([query], **kwargs)
        else:
            query_emb = self.model.encode([query], **kwargs)
        return query_emb[0].tolist()


class ChromaVectorStore(VectorStore):
    """Standard vector DB interface backed by Chroma."""

    def __init__(
        self,
        chroma_path: str,
        collection_name: str,
        model_name: str = "Qwen/Qwen3-Embedding-0.6B",
        batch_size: int = 8,
        normalize_embeddings: bool = True,
        use_query_prompt: bool = True,
        query_prompt_name: str = "query",
        model_kwargs: Optional[Dict[str, Any]] = None,
        tokenizer_kwargs: Optional[Dict[str, Any]] = None,
        **_: Any,
    ) -> None:
        try:
            import chromadb
        except ImportError as exc:
            raise RuntimeError("Chroma vector store needs `chromadb` installed.") from exc

        self.path = Path(chroma_path)
        self.path.mkdir(parents=True, exist_ok=True)
        self.use_query_prompt = use_query_prompt
        self.query_prompt_name = query_prompt_name

        self.embedding_function = SentenceTransformersEmbeddingFunction(
            model_name=model_name,
            batch_size=batch_size,
            normalize_embeddings=normalize_embeddings,
            model_kwargs=model_kwargs,
            tokenizer_kwargs=tokenizer_kwargs,
        )

        self.client = chromadb.PersistentClient(path=str(self.path))
        self.collection = self.client.get_or_create_collection(name=collection_name)

    def insert_documents(
        self,
        documents: List[str],
        ids: Optional[List[str]] = None,
        metadatas: Optional[List[Dict]] = None,
    ) -> int:
        if not documents:
            return 0

        doc_count = len(documents)

        if ids is None:
            ids = [str(uuid.uuid4()) for _ in range(doc_count)]
        if len(ids) != doc_count:
            raise ValueError("Length of ids must match documents.")

        if metadatas is not None and len(metadatas) != doc_count:
            raise ValueError("Length of metadatas must match documents.")

        batch_size = self.embedding_function.batch_size
        for i in range(0, doc_count, batch_size):
            batch_docs = documents[i : i + batch_size]
            batch_ids = ids[i : i + batch_size]
            batch_meta = metadatas[i : i + batch_size] if metadatas is not None else None
            batch_embeddings = self.embedding_function(batch_docs)

            if batch_meta is not None:
                self.collection.add(
                    documents=batch_docs,
                    ids=batch_ids,
                    metadatas=batch_meta,
                    embeddings=batch_embeddings,
                )
            else:
                self.collection.add(documents=batch_docs, ids=batch_ids, embeddings=batch_embeddings)

        return doc_count

    def search(self, query: str, top_k: int = 5) -> List[Dict]:
        n_results = max(1, int(top_k))
        query_embedding = self.embedding_function.embed_query(
            query=query,
            prompt_name=self.query_prompt_name if self.use_query_prompt else None,
        )
        result = self.collection.query(query_embeddings=[query_embedding], n_results=n_results)

        ids = (result.get("ids") or [[]])[0]
        docs = (result.get("documents") or [[]])[0]
        metas = (result.get("metadatas") or [[]])[0]
        dists = (result.get("distances") or [[]])[0]

        rows: List[Dict] = []
        for idx, doc_id in enumerate(ids):
            rows.append(
                {
                    "id": doc_id,
                    "document": docs[idx] if idx < len(docs) else None,
                    "metadata": metas[idx] if idx < len(metas) else None,
                    "distance": dists[idx] if idx < len(dists) else None,
                }
            )
        return rows

    def count(self) -> int:
        return self.collection.count()
