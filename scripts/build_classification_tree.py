#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
from collections import defaultdict
from pathlib import Path
from typing import Any


def build_tree(records: list[dict[str, Any]]) -> dict[str, Any]:
    root: dict[str, Any] = {"name": "All Cities", "value": len(records), "children": []}
    city_group: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for row in records:
        city_group[str(row.get("city_label") or row.get("city") or "Unknown")].append(row)

    for city_name in sorted(city_group):
        city_rows = city_group[city_name]
        ggim_group: dict[str, list[dict[str, Any]]] = defaultdict(list)
        for row in city_rows:
            ggim_group[str(row.get("ggim_iso_label") or row.get("ggim_iso_code") or "Unknown")].append(row)

        city_node = {"name": city_name, "value": len(city_rows), "children": []}
        for ggim_name in sorted(ggim_group):
            ggim_rows = ggim_group[ggim_name]
            scenario_group: dict[str, list[dict[str, Any]]] = defaultdict(list)
            for row in ggim_rows:
                scenario_group[str(row.get("scenario_label") or row.get("scenario_id") or "Unknown")].append(row)

            ggim_node = {"name": ggim_name, "value": len(ggim_rows), "children": []}
            for scenario_name in sorted(scenario_group):
                ggim_node["children"].append(
                    {
                        "name": scenario_name,
                        "value": len(scenario_group[scenario_name]),
                    }
                )
            city_node["children"].append(ggim_node)
        root["children"].append(city_node)
    return root


def build_html(tree_data: dict[str, Any]) -> str:
    payload = json.dumps(tree_data, ensure_ascii=False)
    return f"""<!doctype html>
<html>
<head>
  <meta charset="utf-8" />
  <title>Classification Tree</title>
  <script src="https://cdn.jsdelivr.net/npm/echarts@5/dist/echarts.min.js"></script>
  <style>
    body {{ margin: 0; font-family: Arial, sans-serif; }}
    #tree, #sunburst {{ width: 100vw; height: 50vh; }}
  </style>
</head>
<body>
  <div id="tree"></div>
  <div id="sunburst"></div>
  <script>
    const data = {payload};

    const treeChart = echarts.init(document.getElementById('tree'));
    treeChart.setOption({{
      title: {{ text: 'Dataset Classification Tree (City -> GGIM -> Scenario)' }},
      tooltip: {{
        trigger: 'item',
        formatter: (p) => `${{p.name}}<br/>Datasets: ${{p.value || 0}}`
      }},
      series: [{{
        type: 'tree',
        data: [data],
        left: '2%',
        right: '20%',
        top: '10%',
        bottom: '5%',
        orient: 'LR',
        symbolSize: 8,
        label: {{ position: 'left', verticalAlign: 'middle', align: 'right' }},
        leaves: {{ label: {{ position: 'right', verticalAlign: 'middle', align: 'left' }} }},
        expandAndCollapse: true,
        initialTreeDepth: 2
      }}]
    }});

    const sunburstChart = echarts.init(document.getElementById('sunburst'));
    sunburstChart.setOption({{
      title: {{ text: 'Sunburst View (Node value = dataset count)' }},
      tooltip: {{
        trigger: 'item',
        formatter: (p) => `${{p.name}}<br/>Datasets: ${{p.value || 0}}`
      }},
      series: [{{
        type: 'sunburst',
        data: data.children || [],
        radius: [0, '90%'],
        sort: null,
        label: {{ rotate: 'radial' }}
      }}]
    }});
  </script>
</body>
</html>
"""


def main() -> None:
    parser = argparse.ArgumentParser(description="Build classification tree JSON + HTML visualization.")
    parser.add_argument(
        "--input-json",
        type=Path,
        default=Path("scripts/artifacts/dataset_hierarchy_map.json"),
        help="Input dataset hierarchy map JSON.",
    )
    parser.add_argument(
        "--out-dir",
        type=Path,
        default=Path("scripts/artifacts"),
        help="Output directory.",
    )
    args = parser.parse_args()

    records = json.loads(args.input_json.read_text(encoding="utf-8"))
    tree = build_tree(records)
    args.out_dir.mkdir(parents=True, exist_ok=True)

    (args.out_dir / "classification_tree.json").write_text(
        json.dumps(tree, ensure_ascii=False, indent=2),
        encoding="utf-8",
    )
    (args.out_dir / "classification_tree.html").write_text(
        build_html(tree),
        encoding="utf-8",
    )
    print(f"records={len(records)}")
    print(f"tree_json={ (args.out_dir / 'classification_tree.json').resolve() }")
    print(f"tree_html={ (args.out_dir / 'classification_tree.html').resolve() }")


if __name__ == "__main__":
    main()
