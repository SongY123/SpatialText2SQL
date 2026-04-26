#!/usr/bin/env python3
"""Export interactive HTML for sampled database graph examples by city."""

from __future__ import annotations

import argparse
import json
from collections import defaultdict
from pathlib import Path
from typing import Any


def load_jsonl(path: Path) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for line in path.read_text(encoding="utf-8").splitlines():
        line = line.strip()
        if not line:
            continue
        rows.append(json.loads(line))
    return rows


def pick_examples_by_city(
    samples: list[dict[str, Any]],
    *,
    per_city: int,
) -> dict[str, list[dict[str, Any]]]:
    by_city: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for s in samples:
        by_city[str(s.get("city") or "unknown")].append(s)

    selected: dict[str, list[dict[str, Any]]] = {}
    for city, rows in sorted(by_city.items()):
        # Choose diversified examples: high similarity, high jump, and first items.
        by_sim = sorted(rows, key=lambda x: float((x.get("stats") or {}).get("avg_similarity") or 0.0), reverse=True)
        by_jump = sorted(rows, key=lambda x: int((x.get("stats") or {}).get("jump_count") or 0), reverse=True)

        seen: set[str] = set()
        out: list[dict[str, Any]] = []
        for cand in (by_sim[: max(1, per_city // 2)] + by_jump[: max(1, per_city // 2)] + rows):
            sid = str(cand.get("sample_id") or "")
            if not sid or sid in seen:
                continue
            out.append(cand)
            seen.add(sid)
            if len(out) >= per_city:
                break
        selected[city] = out
    return selected


def build_payload(
    canonical_rows: list[dict[str, Any]],
    selected_samples: dict[str, list[dict[str, Any]]],
) -> dict[str, Any]:
    canonical_map = {str(r.get("dataset_uid") or ""): r for r in canonical_rows}
    payload: dict[str, Any] = {"cities": {}, "meta": {"n_cities": len(selected_samples)}}

    for city, samples in sorted(selected_samples.items()):
        city_samples: list[dict[str, Any]] = []
        for s in samples:
            sample_tables = [str(x) for x in (s.get("tables") or [])]
            node_list = []
            for uid in sample_tables:
                row = canonical_map.get(uid, {})
                node_list.append(
                    {
                        "id": uid,
                        "name": row.get("dataset_name") or uid,
                        "dataset_uid": uid,
                        "geometry_type": ((row.get("spatial_meta") or {}).get("geometry_type") if isinstance(row.get("spatial_meta"), dict) else None) or row.get("geometry_type") or "UNKNOWN",
                        "n_columns": len(row.get("columns") or []),
                        "n_spatial_columns": len(row.get("spatial_columns") or []),
                        "labels": row.get("thematic_labels") or [],
                        "summary": row.get("summary") or "",
                    }
                )

            edge_list = []
            for e in s.get("edges") or []:
                edge_list.append(
                    {
                        "source": str(e.get("src") or ""),
                        "target": str(e.get("dst") or ""),
                        "score": float(e.get("score") or 0.0),
                    }
                )

            city_samples.append(
                {
                    "sample_id": s.get("sample_id"),
                    "seed_table": s.get("seed_table"),
                    "stats": s.get("stats") or {},
                    "walk_trace": s.get("walk_trace") or [],
                    "nodes": node_list,
                    "edges": edge_list,
                }
            )
        payload["cities"][city] = city_samples
    return payload


def build_html(payload: dict[str, Any]) -> str:
    data = json.dumps(payload, ensure_ascii=False)
    return f"""<!doctype html>
<html>
<head>
  <meta charset="utf-8" />
  <title>Sampled Database Graph Explorer</title>
  <script src="https://cdn.jsdelivr.net/npm/echarts@5/dist/echarts.min.js"></script>
  <style>
    body {{ font-family: Arial, sans-serif; margin: 0; background: #f8fafc; color: #0f172a; }}
    .topbar {{ display: flex; gap: 12px; padding: 12px 16px; background: #e2e8f0; align-items: center; }}
    .topbar select {{ padding: 6px; font-size: 14px; }}
    .layout {{ display: flex; height: calc(100vh - 56px); }}
    #graph {{ flex: 1; min-width: 60%; }}
    .side {{ width: 420px; border-left: 1px solid #cbd5e1; padding: 10px 12px; overflow: auto; background: #ffffff; }}
    .box {{ margin-bottom: 12px; border: 1px solid #dbe3ef; border-radius: 8px; padding: 10px; }}
    .k {{ color: #475569; font-size: 12px; }}
    .v {{ font-size: 13px; margin-top: 2px; }}
    code {{ background: #f1f5f9; padding: 1px 4px; border-radius: 4px; }}
    h3 {{ margin: 6px 0 8px 0; font-size: 16px; }}
  </style>
</head>
<body>
  <div class="topbar">
    <label>City</label>
    <select id="citySel"></select>
    <label>Sample</label>
    <select id="sampleSel"></select>
    <span id="sampleStats"></span>
  </div>
  <div class="layout">
    <div id="graph"></div>
    <div class="side">
      <div class="box">
        <h3>Node Info</h3>
        <div id="nodeInfo" class="v">Click a node to inspect details.</div>
      </div>
      <div class="box">
        <h3>Edge Info</h3>
        <div id="edgeInfo" class="v">Click an edge to inspect score and endpoints.</div>
      </div>
      <div class="box">
        <h3>Walk Trace</h3>
        <div id="traceInfo" class="v"></div>
      </div>
    </div>
  </div>

  <script>
    const payload = {data};
    const citySel = document.getElementById('citySel');
    const sampleSel = document.getElementById('sampleSel');
    const sampleStats = document.getElementById('sampleStats');
    const nodeInfo = document.getElementById('nodeInfo');
    const edgeInfo = document.getElementById('edgeInfo');
    const traceInfo = document.getElementById('traceInfo');
    const chart = echarts.init(document.getElementById('graph'));

    const cityNames = Object.keys(payload.cities).sort();
    cityNames.forEach(c => {{
      const opt = document.createElement('option');
      opt.value = c; opt.textContent = c;
      citySel.appendChild(opt);
    }});

    function formatTrace(trace) {{
      if (!trace || !trace.length) return 'N/A';
      return trace.map(t => `step ${{t.step}}: ${{t.node}} (${{t.mode}})`).join('<br/>');
    }}

    function refreshSampleOptions() {{
      const city = citySel.value;
      const samples = payload.cities[city] || [];
      sampleSel.innerHTML = '';
      samples.forEach((s, idx) => {{
        const opt = document.createElement('option');
        opt.value = String(idx);
        opt.textContent = s.sample_id;
        sampleSel.appendChild(opt);
      }});
      sampleSel.value = '0';
      render();
    }}

    function render() {{
      const city = citySel.value;
      const idx = Number(sampleSel.value || 0);
      const sample = (payload.cities[city] || [])[idx];
      if (!sample) return;

      sampleStats.textContent =
        `tables=${{sample.stats.n_tables}}, edges=${{sample.stats.n_edges}}, avg_similarity=${{Number(sample.stats.avg_similarity || 0).toFixed(3)}}, jumps=${{sample.stats.jump_count}}`;
      traceInfo.innerHTML = formatTrace(sample.walk_trace);

      const nodes = sample.nodes.map(n => ({{
        id: n.id,
        name: n.name,
        symbolSize: n.id === sample.seed_table ? 52 : 36,
        category: n.geometry_type || 'UNKNOWN',
        value: n,
        itemStyle: n.id === sample.seed_table ? {{ borderWidth: 3, borderColor: '#111827' }} : undefined,
      }}));
      const links = sample.edges.map(e => ({{
        source: e.source, target: e.target, value: e.score, lineStyle: {{ width: 1 + 5 * e.score, opacity: 0.5 + 0.4 * e.score }},
      }}));

      const categories = [...new Set(nodes.map(n => n.category))].map(c => ({{ name: c }}));

      chart.setOption({{
        title: {{ text: `Sampled DB Graph: ${{sample.sample_id}}`, left: 'center' }},
        tooltip: {{
          trigger: 'item',
          formatter: (p) => {{
            if (p.dataType === 'edge') {{
              return `${{p.data.source}} -> ${{p.data.target}}<br/>score=${{Number(p.data.value).toFixed(4)}}`;
            }}
            const v = p.data.value || {{}};
            return `<b>${{v.name || p.name}}</b><br/>uid=${{v.dataset_uid || p.name}}<br/>geom=${{v.geometry_type}}<br/>columns=${{v.n_columns}}, spatial=${{v.n_spatial_columns}}`;
          }},
        }},
        legend: {{ data: categories.map(c => c.name), top: 28 }},
        series: [{{
          type: 'graph',
          layout: 'force',
          roam: true,
          draggable: true,
          force: {{ repulsion: 220, edgeLength: [80, 180], gravity: 0.08 }},
          label: {{ show: true, formatter: (p) => p.data.value.name, fontSize: 11 }},
          edgeLabel: {{ show: true, formatter: (p) => Number(p.data.value).toFixed(2), fontSize: 10 }},
          categories,
          data: nodes,
          links,
        }}],
      }});

      nodeInfo.textContent = 'Click a node to inspect details.';
      edgeInfo.textContent = 'Click an edge to inspect score and endpoints.';
    }}

    chart.on('click', (params) => {{
      if (params.dataType === 'node') {{
        const n = params.data.value || {{}};
        nodeInfo.innerHTML =
          `<div class='k'>name</div><div class='v'><b>${{n.name || ''}}</b></div>` +
          `<div class='k'>dataset_uid</div><div class='v'><code>${{n.dataset_uid || ''}}</code></div>` +
          `<div class='k'>geometry_type</div><div class='v'>${{n.geometry_type || ''}}</div>` +
          `<div class='k'>columns</div><div class='v'>${{n.n_columns || 0}} (spatial: ${{n.n_spatial_columns || 0}})</div>` +
          `<div class='k'>labels</div><div class='v'>${{(n.labels || []).join(' | ') || 'N/A'}}</div>` +
          `<div class='k'>summary</div><div class='v'>${{n.summary || ''}}</div>`;
      }} else if (params.dataType === 'edge') {{
        edgeInfo.innerHTML =
          `<div class='k'>source</div><div class='v'><code>${{params.data.source}}</code></div>` +
          `<div class='k'>target</div><div class='v'><code>${{params.data.target}}</code></div>` +
          `<div class='k'>score</div><div class='v'>${{Number(params.data.value).toFixed(6)}}</div>`;
      }}
    }});

    citySel.addEventListener('change', refreshSampleOptions);
    sampleSel.addEventListener('change', render);
    citySel.value = cityNames[0];
    refreshSampleOptions();
  </script>
</body>
</html>"""


def main() -> int:
    script_dir = Path(__file__).resolve().parent
    ap = argparse.ArgumentParser(description="Export sampled DB graph HTML explorer.")
    ap.add_argument(
        "--sampled-jsonl",
        type=Path,
        default=script_dir / "artifacts" / "seven_city_output" / "sampled_databases.jsonl",
    )
    ap.add_argument(
        "--canonical-jsonl",
        type=Path,
        default=script_dir / "artifacts" / "seven_city_output" / "canonical_tables.jsonl",
    )
    ap.add_argument(
        "--examples-per-city",
        type=int,
        default=4,
        help="How many sampled DB examples to keep per city",
    )
    ap.add_argument(
        "--out-html",
        type=Path,
        default=script_dir / "artifacts" / "seven_city_output" / "sampled_db_graphs.html",
    )
    args = ap.parse_args()

    samples = load_jsonl(args.sampled_jsonl)
    canonical = load_jsonl(args.canonical_jsonl)
    selected = pick_examples_by_city(samples, per_city=max(1, args.examples_per_city))
    payload = build_payload(canonical, selected)
    html = build_html(payload)
    args.out_html.parent.mkdir(parents=True, exist_ok=True)
    args.out_html.write_text(html, encoding="utf-8")

    n_examples = sum(len(v) for v in selected.values())
    print(f"[done] cities={len(selected)} examples={n_examples}")
    print(f"       out_html={args.out_html.resolve()}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
