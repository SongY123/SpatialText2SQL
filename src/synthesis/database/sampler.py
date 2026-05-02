"""Relation-aware sampling over table relation graphs."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any

import numpy as np

from .models import CanonicalSpatialTable


@dataclass
class RelationAwareDatabaseSampler:
    """Sample a set of related tables through weighted random walk."""

    rng: np.random.Generator
    exploration_prob: float = 0.1
    max_sampling_steps: int = 100

    def __post_init__(self) -> None:
        if not 0.0 <= float(self.exploration_prob) <= 1.0:
            raise ValueError("exploration_prob must be within [0, 1].")
        if int(self.max_sampling_steps) < 1:
            raise ValueError("max_sampling_steps must be at least 1.")

    def sample_tables(
        self,
        graph: Any,
        target_num_tables: int,
    ) -> tuple[list[CanonicalSpatialTable], list[dict[str, Any]]]:
        node_ids = list(graph.nodes())
        if not node_ids or target_num_tables <= 0:
            return [], []

        current_table_id = str(self.rng.choice(node_ids))
        selected_ids = [current_table_id]
        selected_set = {current_table_id}
        sampling_trace: list[dict[str, Any]] = []

        for step in range(1, self.max_sampling_steps + 1):
            if len(selected_ids) >= target_num_tables:
                break

            neighbors = sorted(graph.neighbors(current_table_id))
            action = "weighted_walk"
            edge_weight: float | None = None

            if not neighbors:
                candidates = [node_id for node_id in node_ids if node_id != current_table_id]
                if not candidates:
                    break
                next_table_id = str(self.rng.choice(candidates))
                action = "fallback_jump"
            elif self.rng.random() < self.exploration_prob:
                candidates = [node_id for node_id in node_ids if node_id != current_table_id]
                if not candidates:
                    break
                next_table_id = str(self.rng.choice(candidates))
                action = "exploration_jump"
            else:
                weights = np.asarray(
                    [float(graph[current_table_id][neighbor].get("weight", 0.0)) for neighbor in neighbors],
                    dtype=float,
                )
                if weights.sum() > 0 and np.any(weights > 0):
                    probabilities = weights / weights.sum()
                    next_index = int(self.rng.choice(len(neighbors), p=probabilities))
                else:
                    next_index = int(self.rng.integers(0, len(neighbors)))
                next_table_id = neighbors[next_index]
                edge_weight = float(graph[current_table_id][next_table_id].get("weight", 0.0))

            added_new_table = next_table_id not in selected_set
            if added_new_table:
                selected_set.add(next_table_id)
                selected_ids.append(next_table_id)
            sampling_trace.append(
                {
                    "step": step,
                    "current_table_id": current_table_id,
                    "next_table_id": next_table_id,
                    "action": action,
                    "edge_weight": edge_weight,
                    "added_new_table": added_new_table,
                }
            )
            current_table_id = next_table_id

        if len(selected_ids) < target_num_tables:
            remaining = [node_id for node_id in node_ids if node_id not in selected_set]
            if remaining:
                permuted = list(self.rng.permutation(remaining))
                for next_table_id in permuted[: max(0, target_num_tables - len(selected_ids))]:
                    next_table_id = str(next_table_id)
                    sampling_trace.append(
                        {
                            "step": len(sampling_trace) + 1,
                            "current_table_id": current_table_id,
                            "next_table_id": next_table_id,
                            "action": "fallback_jump",
                            "edge_weight": None,
                            "added_new_table": True,
                        }
                    )
                    selected_set.add(next_table_id)
                    selected_ids.append(next_table_id)
                    current_table_id = next_table_id

        selected_tables = [graph.nodes[node_id]["table"] for node_id in selected_ids]
        return selected_tables, sampling_trace
