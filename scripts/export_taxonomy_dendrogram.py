#!/usr/bin/env python3
"""生成分类水平树状图 HTML（D3.js：左→右、曲线连线、空心圆节点）。

层级：城市 → ISO GGIM → utax 第三级细分 → 每张表为叶子。各级名称后「· N」为子树内表张数。
每张表只归入一个 utax：在「该 ggim 对应的候选 utax 列表」内，按数据集名称/描述与各 utax
章节标题及第三层中文标签做文本匹配，并叠加英文关键词提示；无信号时回退为字典序第一个。
（旧逻辑固定取 sorted(候选)[0] 会导致同一 ggim 下几乎永远只有一个三级节点，属展示 artifact。）
"""
from __future__ import annotations

import argparse
import json
import re
from collections import defaultdict
from pathlib import Path
from typing import Any

# 英文数据集名常见词 → 帮助划入对应 utax（与 city_ggim_layer3_taxonomy 章节语义对齐）
_UTAX_EN_HINTS: dict[str, frozenset[str]] = {
    "utax_01": frozenset(
        {
            "boundary", "boundaries", "census", "tract", "block", "district",
            "borough", "precinct", "ward", "puma", "nta", "cdta", "community",
            "council", "election", "service", "area",
        }
    ),
    "utax_02": frozenset({"population", "demographic", "socioeconomic", "density", "household"}),
    "utax_03": frozenset(
        {"zoning", "land", "use", "parcel", "planning", "historic", "mixed", "industrial", "commercial", "residential"}
    ),
    "utax_04": frozenset({"building", "footprint", "housing", "apartment", "landmark", "tower"}),
    "utax_05": frozenset(
        {
            "street", "road", "highway", "transit", "rail", "bus", "bike", "bicycle",
            "walk", "freeway", "bridge", "tunnel", "corridor", "network", "centerline",
        }
    ),
    "utax_06": frozenset(
        {
            "transit", "traffic", "signal", "parking", "metro", "station", "bus", "ferry",
            "airport", "port", "dock", "curb", "mobility",
        }
    ),
    "utax_07": frozenset(
        {
            "school", "library", "hospital", "clinic", "fire", "police", "museum",
            "theater", "sports", "community", "center", "restroom", "toilet", "defibrillator",
            "college", "university", "public",
        }
    ),
    "utax_08": frozenset(
        {"park", "playground", "trail", "green", "forest", "wetland", "waterfront", "golf", "botanical", "recreation"}
    ),
    "utax_09": frozenset(
        {
            "hydrant", "drain", "sewer", "water", "utility", "wifi", "tower", "snow",
            "pipeline", "benchmark", "infrastructure",
        }
    ),
    "utax_10": frozenset(
        {
            "water", "river", "lake", "coast", "flood", "sea", "wetland", "air", "quality",
            "evacuation", "vegetation", "shoreline", "habitat", "storm",
        }
    ),
    "utax_11": frozenset(
        {"emergency", "evacuation", "shelter", "hazard", "disaster", "safety", "police", "fire"}
    ),
    "utax_12": frozenset({"tree", "trees", "forest", "vegetation", "canopy", "planting", "urban", "woodland"}),
    "utax_13": frozenset(
        {"ortho", "imagery", "aerial", "tile", "index", "cadastre", "parcel", "basemap", "survey"}
    ),
    "utax_14": frozenset({"art", "historic", "memorial", "special", "cultural", "landmark"}),
}


def load_jsonl(path: Path) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for line in path.read_text(encoding="utf-8").splitlines():
        line = line.strip()
        if not line:
            continue
        rows.append(json.loads(line))
    return rows


def _normalize_for_match(s: str) -> str:
    s = (s or "").lower()
    s = re.sub(r"[\s·/,，。（）()]+", " ", s)
    return s.strip()


def _utax_reference_text(chapters_by_id: dict[str, Any], utax_id: str, utax_titles: dict[str, str]) -> str:
    ch = chapters_by_id.get(utax_id) or {}
    parts: list[str] = [utax_titles.get(utax_id) or str(ch.get("title_zh") or "")]
    for l3 in ch.get("layer3") or []:
        if isinstance(l3, dict) and l3.get("label_zh"):
            parts.append(str(l3["label_zh"]))
    return " ".join(parts)


def _lexical_score(blob: str, ref: str) -> int:
    """参考串中的英文词根、2 字中文片段若在 blob 中出现则加分。"""
    blob_n = _normalize_for_match(blob)
    ref_n = _normalize_for_match(ref)
    score = 0
    if ref_n:
        for m in re.finditer(r"[a-z]{3,}", ref_n):
            w = m.group(0)
            if w in blob_n:
                score += 2
        ref_compact = re.sub(r"\s+", "", ref_n)
        blob_compact = re.sub(r"\s+", "", blob_n)
        for i in range(len(ref_compact) - 1):
            bg = ref_compact[i : i + 2]
            if any("\u4e00" <= c <= "\u9fff" for c in bg) and bg in blob_compact:
                score += 1
    return score


def _en_hint_score(utax_id: str, blob: str) -> int:
    blob_l = _normalize_for_match(blob)
    words = set(re.findall(r"[a-z]{3,}", blob_l))
    hints = _UTAX_EN_HINTS.get(utax_id, frozenset())
    return 3 * len(words & hints)


def _pick_primary_utax_for_tree(
    row: dict[str, Any],
    chapters_by_id: dict[str, Any],
    utax_titles: dict[str, str],
) -> str:
    """在候选 utax 中选一个挂树：文本匹配分最高；平局取 utax id 字典序较小。"""
    tax = row.get("taxonomy") or {}
    raw = [str(x).strip() for x in (tax.get("urban_chapter_ids_for_layer3") or []) if str(x).strip()]
    if not raw:
        return "__no_utax__"
    blob = f"{row.get('dataset_name') or ''} {row.get('description') or ''}"
    best_id: str | None = None
    best_s = -1
    for u in sorted(set(raw)):
        ref = _utax_reference_text(chapters_by_id, u, utax_titles)
        s = _lexical_score(blob, ref) + _en_hint_score(u, blob)
        if s > best_s:
            best_s = s
            best_id = u
        elif s == best_s and best_id is not None and u < best_id:
            best_id = u
    if best_id is None:
        return sorted(raw)[0]
    if best_s <= 0:
        return sorted(raw)[0]
    return best_id


def _leaf_name_for_row(row: dict[str, Any], peers_names: list[str]) -> str:
    """叶子：数据集名称；同桶重名时追加 dataset_uid。"""
    name = str(row.get("dataset_name") or "").strip() or "(no_name)"
    uid = str(row.get("dataset_uid") or "").strip()
    if peers_names.count(name) > 1 and uid:
        return f"{name} · {uid}"
    return name


def build_hierarchy(
    rows: list[dict[str, Any]],
    _bridge: dict[str, list[str]],
    chapters_by_id: dict[str, Any],
    ggim_labels: dict[str, str],
    utax_titles: dict[str, str],
    *,
    depth: int,
) -> dict[str, Any]:
    """结构：城市 → ISO GGIM → 第三级 utax 细分 → 每张表为叶子。

    depth: 2=城市+GGIM（数字=该级下属表总数）；3=+utax 桶（无叶子）；4=+每张表叶子（默认）。
    各级名称后「· N」表示该节点子树内包含的表张数（叶子每张计 1）。
    """
    # city -> ggim -> utax -> [rows]
    nest: dict[str, dict[str, dict[str, list[dict[str, Any]]]]] = defaultdict(
        lambda: defaultdict(lambda: defaultdict(list))
    )

    for r in rows:
        city = str(r.get("city") or "").strip() or "(unknown)"
        tax = r.get("taxonomy") or {}
        l2 = tax.get("layer2_iso_ggim") or {}
        gid = str(l2.get("id") or "").strip() or "(no_ggim)"
        utax = _pick_primary_utax_for_tree(r, chapters_by_id, utax_titles)
        nest[city][gid][utax].append(r)

    total_tables = len(rows)
    cities_sorted = sorted(nest.keys())

    root: dict[str, Any] = {
        "name": f"Cities · {total_tables} 张表",
        "children": [],
    }

    for city in cities_sorted:
        c_total = sum(len(nest[city][g][u]) for g in nest[city] for u in nest[city][g])
        city_label = ""
        for r in rows:
            if str(r.get("city") or "").strip() == city and r.get("city_label"):
                city_label = str(r.get("city_label"))
                break
        city_name = f"{city}"
        if city_label:
            city_name = f"{city} · {city_label}"
        city_name = f"{city_name} · {c_total} 张表"
        city_node: dict[str, Any] = {"name": city_name, "children": []}

        for gid in sorted(nest[city].keys()):
            n_ggim = sum(len(nest[city][gid][u]) for u in nest[city][gid])
            g_en = ggim_labels.get(gid, "")
            ggim_name = f"{gid}"
            if g_en:
                ggim_name += f" · {g_en}"
            ggim_name += f" · {n_ggim}"
            ggim_node: dict[str, Any] = {"name": ggim_name, "children": []}

            if depth <= 2:
                city_node["children"].append(ggim_node)
                continue

            for utax in sorted(nest[city][gid].keys()):
                tbls = nest[city][gid][utax]
                n_utax = len(tbls)
                if utax == "__no_utax__":
                    title = "（无 utax 候选 / 未分层）"
                    utax_disp = "__no_utax__"
                else:
                    ch = chapters_by_id.get(utax) or {}
                    title = utax_titles.get(utax) or str(ch.get("title_zh") or "")
                    utax_disp = utax
                utax_name = utax_disp
                if title:
                    utax_name += f" · {title}"
                utax_name += f" · {n_utax}"
                utax_node: dict[str, Any] = {"name": utax_name, "children": []}

                if depth <= 3:
                    ggim_node["children"].append(utax_node)
                    continue

                names_only = [str(t.get("dataset_name") or "").strip() or "(no_name)" for t in tbls]
                for t in sorted(tbls, key=lambda x: (str(x.get("dataset_name") or ""), str(x.get("dataset_uid") or ""))):
                    utax_node["children"].append({"name": _leaf_name_for_row(t, names_only)})

                ggim_node["children"].append(utax_node)

            city_node["children"].append(ggim_node)

        root["children"].append(city_node)

    return root


def json_for_script(obj: Any) -> str:
    s = json.dumps(obj, ensure_ascii=False, indent=2)
    return s.replace("</", "<\\/")


def main() -> int:
    script_dir = Path(__file__).resolve().parent
    ap = argparse.ArgumentParser(description="导出 D3 水平 dendrogram HTML")
    ap.add_argument(
        "--input-jsonl",
        type=Path,
        default=script_dir / "artifacts" / "seven_city_output" / "ai_classification_input.jsonl",
    )
    ap.add_argument("--taxonomy-dir", type=Path, default=script_dir / "taxonomy")
    ap.add_argument(
        "--out-html",
        type=Path,
        default=script_dir / "artifacts" / "seven_city_output" / "taxonomy_dendrogram.html",
    )
    ap.add_argument(
        "--depth",
        type=int,
        default=4,
        choices=[2, 3, 4],
        help="2=城市+GGIM；3=+utax（无表叶子）；4=+每张表为叶子（默认）",
    )
    args = ap.parse_args()

    iso_path = args.taxonomy_dir / "iso_ggim_14.json"
    bridge_path = args.taxonomy_dir / "ggim_to_urban_chapters.json"
    city_path = args.taxonomy_dir / "city_ggim_layer3_taxonomy.json"
    for p in (iso_path, bridge_path, city_path):
        if not p.is_file():
            raise SystemExit(f"缺少 taxonomy 文件: {p}")

    iso = json.loads(iso_path.read_text(encoding="utf-8"))
    bridge_doc = json.loads(bridge_path.read_text(encoding="utf-8"))
    city_doc = json.loads(city_path.read_text(encoding="utf-8"))
    bridge = bridge_doc.get("mappings") or {}
    chapters_by_id = {c["id"]: c for c in (city_doc.get("urban_chapters") or []) if isinstance(c, dict) and c.get("id")}
    ggim_labels = {c["id"]: c.get("label_en", "") for c in (iso.get("categories") or []) if isinstance(c, dict) and c.get("id")}
    utax_titles = {c["id"]: c.get("title_zh", "") for c in (city_doc.get("urban_chapters") or []) if isinstance(c, dict) and c.get("id")}

    rows = load_jsonl(args.input_jsonl)
    hierarchy = build_hierarchy(
        rows,
        bridge,
        chapters_by_id,
        ggim_labels,
        utax_titles,
        depth=args.depth,
    )

    data_json = json_for_script(hierarchy)

    html_head = """<!DOCTYPE html>
<html lang="zh-CN">
<head>
  <meta charset="utf-8" />
  <meta name="viewport" content="width=device-width, initial-scale=1" />
  <title>Dataset Classification Tree (City → ISO GGIM → utax → 表)</title>
  <script src="https://cdn.jsdelivr.net/npm/d3@7.9.0/dist/d3.min.js"></script>
  <style>
    * { box-sizing: border-box; }
    body {
      margin: 0;
      font-family: "Segoe UI", "Helvetica Neue", Arial, "Microsoft YaHei", sans-serif;
      background: #fafafa;
      color: #222;
    }
    header {
      padding: 16px 24px;
      border-bottom: 1px solid #e0e0e0;
      background: #fff;
    }
    header h1 {
      margin: 0;
      font-size: 18px;
      font-weight: 600;
      color: #333;
    }
    header p {
      margin: 8px 0 0;
      font-size: 13px;
      color: #666;
    }
    #chart-wrap {
      overflow: auto;
      padding: 16px 24px 48px;
    }
    svg {
      display: block;
      min-width: 100%;
    }
    .link {
      fill: none;
      stroke: #bbb;
      stroke-opacity: 0.65;
      stroke-width: 1.2px;
    }
    .node circle {
      fill: #fff;
      stroke: #888;
      stroke-width: 1.2px;
    }
    .node text {
      font-size: 10px;
      dominant-baseline: middle;
    }
    .node--internal text {
      font-weight: 500;
    }
  </style>
</head>
<body>
  <header>
    <h1>Dataset Classification Tree (City → ISO GGIM → utax → 表)</h1>
    <p>左起：根 → 城市 → 国际标准（ggim）→ 第三级细分（utax 章节）→ <strong>每张表</strong>为一叶子。名称后「· N」为子树内表张数。第三级在「该 ggim 对应的多个候选 utax」中，按<strong>表名/描述与章节标题、第三层标签的匹配度</strong>择一（并辅以英文关键词），避免旧版「永远取字典序第一个 utax」导致二级下几乎只有一个三级节点。可缩放拖拽画布。</p>
  </header>
  <div id="chart-wrap"></div>
  <script id="raw-data" type="application/json">"""

    html_js = """
</script>
  <script>
const raw = document.getElementById("raw-data").textContent;
const data = JSON.parse(raw);

const hierarchyRoot = d3.hierarchy(data);
hierarchyRoot.sort((a, b) => (a.data.name < b.data.name ? -1 : 1));

const dx = 11;
const dy = 168;
const treeLayout = d3.tree().nodeSize([dx, dy]);
treeLayout(hierarchyRoot);

let x0 = Infinity;
let x1 = -x0;
hierarchyRoot.each((d) => {
  if (d.x > x1) x1 = d.x;
  if (d.x < x0) x0 = d.x;
});

const marginTop = 24;
const marginLeft = 120;
const width = hierarchyRoot.height * dy + marginLeft + 240;
const height = x1 - x0 + dx * 2 + marginTop;

const svg = d3.select("#chart-wrap")
  .append("svg")
  .attr("width", width)
  .attr("height", height)
  .attr("viewBox", [0, 0, width, height]);

const outerG = svg.append("g")
  .attr("transform", "translate(" + marginLeft + "," + (marginTop - x0) + ")");

const zoomG = outerG.append("g");

const link = zoomG.append("g")
  .attr("fill", "none")
  .selectAll("path")
  .data(hierarchyRoot.links())
  .join("path")
  .attr("class", "link")
  .attr("d", d3.linkHorizontal()
    .x((d) => d.y)
    .y((d) => d.x));

const node = zoomG.append("g")
  .selectAll("g")
  .data(hierarchyRoot.descendants())
  .join("g")
  .attr("class", (d) => "node" + (d.children ? " node--internal" : " node--leaf"))
  .attr("transform", (d) => "translate(" + d.y + "," + d.x + ")");

node.append("circle")
  .attr("r", 3.2);

node.append("text")
  .attr("dy", "0.32em")
  .attr("x", (d) => (d.children ? -8 : 8))
  .attr("text-anchor", (d) => (d.children ? "end" : "start"))
  .text((d) => d.data.name);

const zoom = d3.zoom()
  .scaleExtent([0.15, 4])
  .on("zoom", (ev) => {
    zoomG.attr("transform", ev.transform);
  });

svg.call(zoom);
svg.call(zoom.transform, d3.zoomIdentity.translate(20, 10).scale(0.82));
  </script>
</body>
</html>
"""

    html = html_head + data_json + html_js

    args.out_html.parent.mkdir(parents=True, exist_ok=True)
    args.out_html.write_text(html, encoding="utf-8")
    print(f"[done] wrote {args.out_html}")
    print(f"       depth={args.depth}  nodes≈{count_tree_nodes(hierarchy)}")
    return 0


def count_tree_nodes(h: dict[str, Any]) -> int:
    n = 1
    for c in h.get("children") or []:
        n += count_tree_nodes(c)
    return n


if __name__ == "__main__":
    raise SystemExit(main())
