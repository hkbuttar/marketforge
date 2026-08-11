import json
import tempfile
import unittest
from pathlib import Path

from warehouse.lineage import ancestors, build_lineage, write_lineage


class LineageTests(unittest.TestCase):
    def test_manifest_dependencies_and_dataset_metadata_are_joined(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            manifest = {
                "metadata": {"invocation_id": "dbt-run"},
                "sources": {"source.x.raw.prices": {
                    "name": "prices", "resource_type": "source", "original_file_path": "sources.yml",
                }},
                "nodes": {
                    "model.x.stg_prices": {"name": "stg_prices", "resource_type": "model",
                        "depends_on": {"nodes": ["source.x.raw.prices"]}},
                    "model.x.mart_prices": {"name": "mart_prices", "resource_type": "model",
                        "depends_on": {"nodes": ["model.x.stg_prices"]}},
                },
            }
            manifest_path = root / "manifest.json"
            manifest_path.write_text(json.dumps(manifest))
            builds = root / "builds"
            builds.mkdir()
            (builds / "one.json").write_text(json.dumps({
                "build_id": "build-1", "datasets": ["prices"],
                "source_partitions": [{"pipeline_run_id": "ingest-1"}],
            }))
            graph = build_lineage(manifest_path, raw_root=root / "raw", builds_root=builds)
            trace = ancestors(graph, "mart_prices")
            self.assertEqual(len(trace["nodes"]), 3)
            self.assertEqual(len(trace["edges"]), 2)
            source = next(node for node in trace["nodes"] if node["type"] == "source")
            self.assertEqual(source["dataset_build_id"], "build-1")
            self.assertEqual(source["pipeline_run_ids"], ["ingest-1"])
            target = root / "metadata" / "lineage.json"
            write_lineage(graph, target)
            self.assertEqual(json.loads(target.read_text())["dbt_invocation_id"], "dbt-run")

    def test_unknown_target_fails_visibly(self):
        with self.assertRaises(ValueError):
            ancestors({"nodes": [], "edges": []}, "missing")


if __name__ == "__main__":
    unittest.main()
