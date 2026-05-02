import json
import tempfile
import unittest
from pathlib import Path

from src.synthesis.database import (
    MockEmbeddingProvider,
    RelationAwareDatabaseSampler,
    RelationGraphBuilder,
    SpatialDatabaseSynthesizer,
    build_table_text,
    load_canonical_tables,
    write_synthesized_databases,
)
from src.synthesis.database.models import CanonicalSpatialTable


def make_table(
    table_id: str,
    city: str,
    table_name: str,
    *,
    semantic_summary: str = "",
    themes: list[str] | None = None,
    path: str | None = None,
) -> CanonicalSpatialTable:
    payload = {
        "table_id": table_id,
        "city": city,
        "table_name": table_name,
        "semantic_summary": semantic_summary or table_name,
        "normalized_schema": [
            {
                "name": "id",
                "canonical_name": "id",
                "canonical_type": "integer",
            },
            {
                "name": "name",
                "canonical_name": "name",
                "canonical_type": "text",
            },
        ],
        "representative_values": {},
        "themes": themes or ["theme_a"],
        "spatial_fields": [{"canonical_name": "the_geom", "crs": "EPSG:4326"}],
    }
    if path:
        payload["path"] = path
    return CanonicalSpatialTable.from_dict(payload)


class SpatialDatabaseSynthesisTests(unittest.TestCase):
    def _city_tables(self) -> list[CanonicalSpatialTable]:
        return [
            make_table("t1", "nyc", "hydrants", semantic_summary="hydrant water infrastructure"),
            make_table("t2", "nyc", "water_mains", semantic_summary="water pipe infrastructure"),
            make_table("t3", "nyc", "subway_stations", semantic_summary="rail station transit"),
        ]

    def _embedding_provider_for_city_tables(self) -> MockEmbeddingProvider:
        tables = self._city_tables()
        vectors_by_text = {
            build_table_text(tables[0]): [1.0, 0.0],
            build_table_text(tables[1]): [0.9, 0.1],
            build_table_text(tables[2]): [0.0, 1.0],
        }
        return MockEmbeddingProvider(vectors_by_text=vectors_by_text, dimension=2)

    def test_relation_graph_builds_for_same_city(self):
        builder = RelationGraphBuilder(
            embedding_provider=self._embedding_provider_for_city_tables(),
            target_avg_degree=1.0,
        )
        graph, stats = builder.build_city_graph(self._city_tables())
        self.assertEqual(graph.number_of_nodes(), 3)
        self.assertEqual(stats["num_nodes"], 3)
        self.assertGreaterEqual(stats["avg_degree"], 1.0)
        self.assertTrue(graph.has_edge("t1", "t2"))

    def test_target_avg_degree_is_clipped_to_city_maximum(self):
        builder = RelationGraphBuilder(
            embedding_provider=self._embedding_provider_for_city_tables(),
            target_avg_degree=10.0,
        )
        _, stats = builder.build_city_graph(self._city_tables())
        self.assertEqual(stats["effective_target_avg_degree"], 2.0)

    def test_sampling_only_contains_same_city_tables(self):
        synthesizer = SpatialDatabaseSynthesizer(
            embedding_provider=self._embedding_provider_for_city_tables(),
            target_avg_degree=1.0,
            exploration_prob=0.0,
            size_mean=3,
            size_std=0,
            min_tables=2,
            max_tables=3,
            random_seed=7,
            embedding_model="mock-embedding",
        )
        databases = synthesizer.synthesize(self._city_tables())
        self.assertEqual(len(databases), 1)
        self.assertTrue(all(table.city == "nyc" for table in databases[0].selected_tables))
        self.assertTrue(2 <= len(databases[0].table_ids) <= 3)

    def test_exploration_prob_one_uses_random_jumps(self):
        builder = RelationGraphBuilder(
            embedding_provider=self._embedding_provider_for_city_tables(),
            target_avg_degree=2.0,
        )
        graph, _ = builder.build_city_graph(self._city_tables())
        sampler = RelationAwareDatabaseSampler(
            rng=__import__("numpy").random.default_rng(3),
            exploration_prob=1.0,
            max_sampling_steps=10,
        )
        _, trace = sampler.sample_tables(graph, target_num_tables=2)
        self.assertTrue(trace)
        self.assertTrue(all(step["action"] != "weighted_walk" for step in trace))
        self.assertTrue(any(step["action"] == "exploration_jump" for step in trace))

    def test_exploration_prob_zero_prefers_graph_edges(self):
        builder = RelationGraphBuilder(
            embedding_provider=self._embedding_provider_for_city_tables(),
            target_avg_degree=2.0,
        )
        graph, _ = builder.build_city_graph(self._city_tables())
        sampler = RelationAwareDatabaseSampler(
            rng=__import__("numpy").random.default_rng(4),
            exploration_prob=0.0,
            max_sampling_steps=10,
        )
        _, trace = sampler.sample_tables(graph, target_num_tables=2)
        self.assertTrue(trace)
        self.assertEqual(trace[0]["action"], "weighted_walk")

    def test_isolated_graph_uses_fallback_jump(self):
        builder = RelationGraphBuilder(
            embedding_provider=self._embedding_provider_for_city_tables(),
            target_avg_degree=0.0,
        )
        graph, _ = builder.build_city_graph(self._city_tables())
        sampler = RelationAwareDatabaseSampler(
            rng=__import__("numpy").random.default_rng(9),
            exploration_prob=0.0,
            max_sampling_steps=10,
        )
        _, trace = sampler.sample_tables(graph, target_num_tables=2)
        self.assertTrue(trace)
        self.assertEqual(trace[0]["action"], "fallback_jump")

    def test_random_seed_is_reproducible(self):
        tables = [
            make_table(f"t{i}", "nyc", f"table_{i}", semantic_summary=f"table {i}")
            for i in range(1, 21)
        ]
        provider = self._embedding_provider_for_city_tables()
        left = SpatialDatabaseSynthesizer(
            embedding_provider=provider,
            target_avg_degree=1.0,
            exploration_prob=0.2,
            size_mean=3,
            size_std=0,
            min_tables=2,
            max_tables=3,
            random_seed=11,
            embedding_model="mock-embedding",
        ).synthesize(tables)
        right = SpatialDatabaseSynthesizer(
            embedding_provider=provider,
            target_avg_degree=1.0,
            exploration_prob=0.2,
            size_mean=3,
            size_std=0,
            min_tables=2,
            max_tables=3,
            random_seed=11,
            embedding_model="mock-embedding",
        ).synthesize(tables)
        self.assertEqual(len(left), 2)
        self.assertEqual(len(right), 2)
        self.assertEqual([item.to_dict() for item in left], [item.to_dict() for item in right])

    def test_target_table_count_is_clipped_to_default_bounds(self):
        provider = MockEmbeddingProvider()
        low = SpatialDatabaseSynthesizer(
            embedding_provider=provider,
            size_mean=-100,
            size_std=0,
            min_tables=2,
            max_tables=12,
        )
        high = SpatialDatabaseSynthesizer(
            embedding_provider=provider,
            size_mean=999,
            size_std=0,
            min_tables=2,
            max_tables=12,
        )
        self.assertEqual(low.sample_target_table_count(available_tables=20), 2)
        self.assertEqual(high.sample_target_table_count(available_tables=20), 12)

    def test_city_with_single_table_degrades_safely(self):
        table = make_table("solo", "la", "single_table", semantic_summary="single table")
        synthesizer = SpatialDatabaseSynthesizer(
            embedding_provider=MockEmbeddingProvider(),
            min_tables=2,
            max_tables=12,
            size_mean=8,
            size_std=2,
            random_seed=5,
            embedding_model="mock-embedding",
        )
        databases = synthesizer.synthesize([table])
        self.assertEqual(len(databases), 1)
        self.assertEqual(databases[0].table_ids, ["solo"])

    def test_jsonl_load_and_write_round_trip(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            tmp_path = Path(tmpdir)
            input_path = tmp_path / "tables.jsonl"
            input_path.write_text(
                json.dumps(
                    {
                        "table_id": "t1",
                        "city": "nyc",
                        "table_name": "hydrants",
                        "semantic_summary": "hydrant data",
                        "columns": [
                            {"name": "id", "canonical_name": "id", "canonical_type": "integer"},
                            {"name": "name", "canonical_name": "name", "canonical_type": "text"},
                            {"name": "the_geom", "canonical_name": "the_geom", "canonical_type": "spatial"},
                        ],
                        "representative_values": {"id": [1, 2], "name": ["alpha", "beta"]},
                        "themes": ["Physical Infrastructure"],
                        "spatial_fields": [{"canonical_name": "the_geom", "crs": "EPSG:4326"}],
                    },
                    ensure_ascii=False,
                )
                + "\n",
                encoding="utf-8",
            )

            tables = load_canonical_tables(str(input_path))
            self.assertEqual(len(tables), 1)
            self.assertEqual(tables[0].representative_values, {"id": [1, 2], "name": ["alpha", "beta"]})
            self.assertTrue(all("nullable" not in column for column in tables[0].normalized_schema))

            synthesizer = SpatialDatabaseSynthesizer(
                embedding_provider=MockEmbeddingProvider(),
                size_mean=1,
                size_std=0,
                min_tables=1,
                max_tables=2,
                random_seed=1,
                embedding_model="mock-embedding",
            )
            databases = synthesizer.synthesize(tables)
            output_path = tmp_path / "output.jsonl"
            write_synthesized_databases(str(output_path), databases)
            lines = [json.loads(line) for line in output_path.read_text(encoding="utf-8").splitlines() if line.strip()]
            self.assertEqual(len(lines), 1)
            self.assertEqual(lines[0]["city"], "nyc")
            self.assertTrue(
                all("nullable" not in column for column in lines[0]["selected_tables"][0]["normalized_schema"])
            )

    def test_metadata_canonicalized_json_can_be_loaded(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            tmp_path = Path(tmpdir)
            metadata_path = tmp_path / "metadata_canonicalized.json"
            metadata_path.write_text(
                json.dumps(
                    [
                        {
                            "City": "New York City",
                            "city_id": "nyc",
                            "datasets": [
                                {
                                    "id": "hydrants",
                                    "name": "NYC Hydrants",
                                    "canonical_name": "nyc_hydrants",
                                    "semantic_summary": "hydrant data",
                                    "representative_values": {"id": [1], "status": ["active"]},
                                    "themes": ["Physical Infrastructure"],
                                    "columns": [
                                        {
                                            "name": "id",
                                            "canonical_name": "id",
                                            "canonical_type": "integer",
                                        },
                                        {
                                            "name": "status",
                                            "canonical_name": "status",
                                            "canonical_type": "text",
                                        },
                                        {
                                            "name": "the_geom",
                                            "canonical_name": "the_geom",
                                            "canonical_type": "spatial",
                                        },
                                    ],
                                    "spatial_fields": [{"canonical_name": "the_geom", "crs": "EPSG:4326"}],
                                }
                            ],
                        }
                    ],
                    ensure_ascii=False,
                ),
                encoding="utf-8",
            )

            tables = load_canonical_tables(str(metadata_path))
            self.assertEqual(len(tables), 1)
            self.assertEqual(tables[0].table_id, "hydrants")
            self.assertEqual(tables[0].city, "nyc")
            self.assertEqual(tables[0].table_name, "nyc_hydrants")
            self.assertEqual(tables[0].representative_values, {"id": [1], "status": ["active"]})
            self.assertTrue(all("nullable" not in column for column in tables[0].normalized_schema))

    def test_num_databases_per_city_is_derived_from_table_count(self):
        synthesizer = SpatialDatabaseSynthesizer(embedding_provider=MockEmbeddingProvider())
        self.assertEqual(synthesizer.estimate_num_databases_for_city(0), 0)
        self.assertEqual(synthesizer.estimate_num_databases_for_city(1), 1)
        self.assertEqual(synthesizer.estimate_num_databases_for_city(10), 1)
        self.assertEqual(synthesizer.estimate_num_databases_for_city(11), 2)

    def test_synthesize_can_filter_selected_cities(self):
        provider = self._embedding_provider_for_city_tables()
        tables = self._city_tables() + [
            make_table("sf1", "sf", "streets", semantic_summary="street network"),
            make_table("sf2", "sf", "parcels", semantic_summary="land parcel"),
        ]
        synthesizer = SpatialDatabaseSynthesizer(
            embedding_provider=provider,
            size_mean=2,
            size_std=0,
            min_tables=1,
            max_tables=3,
            random_seed=13,
            embedding_model="mock-embedding",
        )
        databases = synthesizer.synthesize(tables, selected_city_ids=["sf"])
        self.assertEqual(len(databases), 1)
        self.assertEqual(databases[0].city, "sf")

    def test_invalid_json_line_reports_line_number(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            input_path = Path(tmpdir) / "bad.jsonl"
            input_path.write_text(
                '{"table_id": "t1", "city": "nyc", "table_name": "hydrants"}\nnot-json\n',
                encoding="utf-8",
            )
            with self.assertRaisesRegex(ValueError, "line 2"):
                load_canonical_tables(str(input_path))


if __name__ == "__main__":
    unittest.main()
