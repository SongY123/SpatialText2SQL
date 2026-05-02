"""High-level synthesis pipeline for multi-table spatial databases."""

from __future__ import annotations

import logging
import math
import re
from dataclasses import dataclass, field
from typing import Sequence

import numpy as np

from .embeddings import DEFAULT_EMBEDDING_MODEL, EmbeddingProvider
from .graph import RelationGraphBuilder
from .models import CanonicalSpatialTable, SynthesizedSpatialDatabase
from .sampler import RelationAwareDatabaseSampler

LOGGER = logging.getLogger(__name__)


def _slugify(value: str) -> str:
    slug = re.sub(r"[^a-z0-9]+", "_", value.strip().lower())
    slug = re.sub(r"_+", "_", slug).strip("_")
    return slug or "database"


@dataclass
class SpatialDatabaseSynthesizer:
    """Orchestrates graph construction and relation-aware sampling."""

    embedding_provider: EmbeddingProvider
    target_avg_degree: float = 4.0
    exploration_prob: float = 0.1
    size_mean: float = 8.0
    size_std: float = 2.0
    min_tables: int = 2
    max_tables: int = 12
    max_sampling_steps: int = 100
    random_seed: int = 42
    embedding_model: str = DEFAULT_EMBEDDING_MODEL
    rng: np.random.Generator = field(init=False, repr=False)

    def __post_init__(self) -> None:
        if not 0.0 <= float(self.exploration_prob) <= 1.0:
            raise ValueError("exploration_prob must be within [0, 1].")
        if int(self.min_tables) < 1:
            raise ValueError("min_tables must be at least 1.")
        if int(self.max_tables) < int(self.min_tables):
            raise ValueError("max_tables must be greater than or equal to min_tables.")
        if int(self.max_sampling_steps) < 1:
            raise ValueError("max_sampling_steps must be at least 1.")
        self.rng = np.random.default_rng(self.random_seed)

    def sample_target_table_count(self, available_tables: int) -> int:
        if available_tables <= 0:
            return 0
        if available_tables < self.min_tables:
            LOGGER.warning(
                "City only has %s table(s), below min_tables=%s. Falling back to available count.",
                available_tables,
                self.min_tables,
            )
            return available_tables
        sampled = int(round(float(self.rng.normal(loc=self.size_mean, scale=self.size_std))))
        clipped = max(self.min_tables, min(self.max_tables, sampled))
        return min(clipped, available_tables)

    def estimate_num_databases_for_city(self, available_tables: int) -> int:
        if available_tables <= 0:
            return 0
        return max(1, int(math.ceil(available_tables / 10.0)))

    def synthesize(
        self,
        tables: Sequence[CanonicalSpatialTable],
        *,
        selected_city_ids: Sequence[str] | None = None,
        num_databases_per_city: int | None = None,
    ) -> list[SynthesizedSpatialDatabase]:
        if num_databases_per_city is not None and num_databases_per_city < 1:
            raise ValueError("num_databases_per_city must be at least 1.")
        if not tables:
            LOGGER.warning("No canonical spatial tables were provided for synthesis.")
            return []

        normalized_selected_city_ids = {
            str(city_id).strip().lower()
            for city_id in (selected_city_ids or [])
            if str(city_id).strip()
        }
        by_city: dict[str, list[CanonicalSpatialTable]] = {}
        for table in tables:
            if normalized_selected_city_ids and table.city.strip().lower() not in normalized_selected_city_ids:
                continue
            by_city.setdefault(table.city, []).append(table)

        graph_builder = RelationGraphBuilder(
            embedding_provider=self.embedding_provider,
            target_avg_degree=self.target_avg_degree,
        )
        synthesized: list[SynthesizedSpatialDatabase] = []
        for city in sorted(by_city):
            city_tables = sorted(by_city[city], key=lambda table: table.table_id)
            if not city_tables:
                continue
            city_num_databases = (
                num_databases_per_city
                if num_databases_per_city is not None
                else self.estimate_num_databases_for_city(len(city_tables))
            )
            graph, graph_stats = graph_builder.build_city_graph(city_tables)
            sampler = RelationAwareDatabaseSampler(
                rng=self.rng,
                exploration_prob=self.exploration_prob,
                max_sampling_steps=self.max_sampling_steps,
            )
            for index in range(city_num_databases):
                target_table_count = self.sample_target_table_count(len(city_tables))
                if target_table_count == 0:
                    continue
                selected_tables, sampling_trace = sampler.sample_tables(
                    graph,
                    target_num_tables=target_table_count,
                )
                database_id = f"{_slugify(city)}_{index + 1:04d}"
                synthesized.append(
                    SynthesizedSpatialDatabase.from_selected_tables(
                        database_id=database_id,
                        city=city,
                        selected_tables=selected_tables,
                        sampling_trace=sampling_trace,
                        graph_stats=graph_stats,
                        synthesize_config={
                            "num_databases_for_city": city_num_databases,
                            "target_avg_degree": self.target_avg_degree,
                            "exploration_prob": self.exploration_prob,
                            "size_mean": self.size_mean,
                            "size_std": self.size_std,
                            "min_tables": self.min_tables,
                            "max_tables": self.max_tables,
                            "max_sampling_steps": self.max_sampling_steps,
                            "embedding_model": self.embedding_model,
                            "random_seed": self.random_seed,
                        },
                    )
                )
        return synthesized
