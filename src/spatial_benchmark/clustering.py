from __future__ import annotations

from collections import Counter, defaultdict
from dataclasses import dataclass, field
import csv
import hashlib
import html
import json
import math
from pathlib import Path
import re
import sys
from typing import Any

from .taxonomy import (
    CATEGORY_TAXONOMY,
    DEFAULT_THEME_TO_GGIM_CATEGORY,
    JOIN_KEY_PATTERNS,
    SCENARIO_PROTOTYPES,
    SCENARIO_TO_GGIM_CATEGORIES,
    SPATIAL_COLUMN_HINTS,
    STOPWORDS,
    THEME_TAXONOMY,
)


DATASET_ID_RE = re.compile(r"([a-z0-9]{4}-[a-z0-9]{4})\.csv$", re.I)
DATASET_ID_FILE_RE = re.compile(r"([a-z0-9]{4}-[a-z0-9]{4})(?:\.(?:csv|geojson|json))?$", re.I)
HTML_TAG_RE = re.compile(r"<[^>]+>")
TOKEN_RE = re.compile(r"[a-z0-9]+")
WKT_PREFIXES = ("POINT", "MULTIPOINT", "LINESTRING", "MULTILINESTRING", "POLYGON", "MULTIPOLYGON")

THEME_OVERRIDES = {
    "transportation": re.compile(r"bike|bicycle|pedestrian|parking|carshare|truck|bus|taxi|fhv|street|walknyc|corridor", re.I),
    "public_safety": re.compile(r"fire|alarm|police|psa|evacuation|hurricane|hazard|emergency", re.I),
    "education": re.compile(r"school|education|pre.?k|schoolyard", re.I),
    "healthcare": re.compile(r"aed|health|hospital|medical|pharmaceutical|syringe", re.I),
    "environment": re.compile(r"flood|sea.?level|wetland|tree|hydrography|waterfront|green infrastructure|basin|outfall|shoreline|forest", re.I),
    "urban_infrastructure": re.compile(r"addresspoint|centerline|hydrant|lead service|capital project|structure|planimetric|roadbed|sidewalk|pavement", re.I),
    "demographics_boundaries": re.compile(r"census|district|boundar(y|ies)|puma|nta|cdta|modzcta|tabulation", re.I),
    "business_economy": re.compile(r"business|commercial|storefront|vendor|franchise|newsstand|food store", re.I),
    "poi_public_facilities": re.compile(r"library|park|restroom|toilet|pool|golf|beach|fountain|trail|linknyc|bench|post office|play area", re.I),
    "housing_land_use": re.compile(r"housing|nycha|zoning|landmark|building footprint|tax lot|parcel|planning|brownfield|condo|air lot|sub lot", re.I),
}

THEME_NAME_PRIORITY = {
    "transportation": re.compile(r"bike|bicycle|bus|pedestrian|parking|truck|taxi|street|walknyc|carshare", re.I),
    "public_safety": re.compile(r"fire|alarm|police|evacuation|hurricane|inundation|hazard", re.I),
    "education": re.compile(r"school|education|pre.?k|schoolyard", re.I),
    "healthcare": re.compile(r"aed|automated external defibrillator|health center|pharmaceutical|syringe|medical", re.I),
    "environment": re.compile(r"flood|sea.?level|wetland|tree|hydrography|waterfront|green infrastructure|shoreline", re.I),
    "urban_infrastructure": re.compile(r"addresspoint|centerline|hydrant|lead service|capital project|planimetric|roadbed|structure", re.I),
    "demographics_boundaries": re.compile(r"census|district|boundary|puma|nta|cdta|modzcta", re.I),
    "business_economy": re.compile(r"business|commercial|storefront|vendor|franchise|newsstand", re.I),
    "poi_public_facilities": re.compile(r"library|park|restroom|toilet|pool|golf|beach|trail|linknyc|post office|bench", re.I),
    "housing_land_use": re.compile(r"housing|nycha|zoning|landmark|building|tax lot|parcel|planning|brownfield|condo", re.I),
}

SCENARIO_OVERRIDES = {
    "traffic_mobility": re.compile(r"bike|bicycle|pedestrian|parking|truck|bus|street|corridor|walknyc|traffic|meter", re.I),
    "emergency_response": re.compile(r"fire|alarm|police|evacuation|hurricane|hazard|hydrant|aed|inundation", re.I),
    "public_service_accessibility": re.compile(r"library|school|linknyc|restroom|toilet|post office|public service|drinking fountain|seating", re.I),
    "environmental_resilience": re.compile(r"tree|wetland|flood|sea.?level|green infrastructure|sensor|basin|outfall|shoreline|waterfront", re.I),
    "urban_planning_land_use": re.compile(r"zoning|parcel|bbl|bin|building|landmark|planning|capital project|waterfront access", re.I),
    "housing_demographics": re.compile(r"housing|nycha|census|tract|nta|cdta|puma|modzcta|neighborhood", re.I),
    "parks_recreation_poi": re.compile(r"park|trail|athletic|beach|golf|kayak|canoe|fishing|rink|pool|recreation", re.I),
}

CATEGORY_OVERRIDES: dict[str, re.Pattern[str]] = {
    "ggim_1": re.compile(r"\bepsg\b|coordinate.?reference|spatial.?reference|projection|geodetic|datum|wgs.?84|srid", re.I),
    "ggim_2": re.compile(r"\baddress\b|house.?number|street.?name|postal|geocode|zip|postcode", re.I),
    "ggim_3": re.compile(r"building|settlement|residential|housing.?development|footprint|community", re.I),
    "ggim_4": re.compile(r"elevation|depth|terrain|contour|dem|bathymetr|topograph", re.I),
    "ggim_5": re.compile(r"functional.?area|service.?area|administrative|district|zone|precinct|planning.?area", re.I),
    "ggim_6": re.compile(r"geographic.?name|place.?name|toponym|street.?name|poi.?name|name.?index", re.I),
    "ggim_7": re.compile(r"geolog|soil|sediment|rock|aquifer|geotechnical", re.I),
    "ggim_8": re.compile(r"land.?cover|land.?use|impervious|vegetation|waterbody|zoning", re.I),
    "ggim_9": re.compile(r"parcel|cadastre|tax.?lot|property.?boundary|borough.?block.?lot|lot", re.I),
    "ggim_10": re.compile(r"ortho|orthophoto|aerial|satellite|imagery|raster|lidar", re.I),
    "ggim_11": re.compile(r"facility|infrastructure|utility|hospital|school|fire.?station|public.?service", re.I),
    "ggim_12": re.compile(r"population|demographic|census|density|household|tract|nta|puma|neighborhood", re.I),
    "ggim_13": re.compile(r"transport|road|route|rail|bus.?line|centerline|network|traffic|corridor", re.I),
    "ggim_14": re.compile(r"river|lake|shoreline|coast|water|hydrolog|watershed|basin|flood", re.I),
}

CATEGORY_NAME_PRIORITY: dict[str, re.Pattern[str]] = {
    "ggim_1": re.compile(r"epsg|coordinate.?reference|projection|geodetic|datum|crs", re.I),
    "ggim_2": re.compile(r"address|house.?number|street.?name|geocode|zip|postcode", re.I),
    "ggim_3": re.compile(r"building|settlement|residential|footprint|housing", re.I),
    "ggim_4": re.compile(r"elevation|depth|terrain|contour|dem|topograph", re.I),
    "ggim_5": re.compile(r"functional.?area|service.?area|district|zone|precinct|administrative", re.I),
    "ggim_6": re.compile(r"geographic.?name|place.?name|toponym|poi.?name", re.I),
    "ggim_7": re.compile(r"geology|soil|sediment|rock|aquifer", re.I),
    "ggim_8": re.compile(r"land.?cover|land.?use|impervious|vegetation|zoning", re.I),
    "ggim_9": re.compile(r"parcel|cadastre|tax.?lot|property.?boundary|lot", re.I),
    "ggim_10": re.compile(r"ortho|orthophoto|aerial|satellite|imagery|raster|lidar", re.I),
    "ggim_11": re.compile(r"facility|infrastructure|utility|hospital|school|fire.?station", re.I),
    "ggim_12": re.compile(r"population|demographic|census|density|household|tract|nta|puma", re.I),
    "ggim_13": re.compile(r"transport|road|route|rail|bus|street|centerline|network", re.I),
    "ggim_14": re.compile(r"river|lake|shoreline|coast|water|hydrolog|watershed|basin|flood", re.I),
}

DEFAULT_SCENARIO_BY_THEME = {
    "transportation": "traffic_mobility",
    "public_safety": "emergency_response",
    "education": "public_service_accessibility",
    "healthcare": "public_service_accessibility",
    "environment": "environmental_resilience",
    "urban_infrastructure": "urban_planning_land_use",
    "demographics_boundaries": "housing_demographics",
    "business_economy": "traffic_mobility",
    "poi_public_facilities": "parks_recreation_poi",
    "housing_land_use": "urban_planning_land_use",
}

field_limit = sys.maxsize
while True:
    try:
        csv.field_size_limit(field_limit)
        break
    except OverflowError:
        field_limit = int(field_limit / 10)


@dataclass
class TableProfile:
    city: str
    city_label: str
    file_name: str
    dataset_id: str
    dataset_name: str
    description: str
    tags: list[str]
    asset_url: str
    last_updated: str
    views: int
    columns: list[str]
    n_columns: int
    spatial_columns: list[str]
    has_spatial: bool
    geometry_type: str
    join_keys: list[str]
    theme: str = ""
    theme_confidence: float = 0.0
    theme_explanation: str = ""
    scenario: str = ""
    scenario_confidence: float = 0.0
    scenario_explanation: str = ""
    scenario_candidates: list[str] = field(default_factory=list)
    scenario_memberships: list[str] = field(default_factory=list)
    primary_ggim_category: str = ""
    ggim_category_confidence: float = 0.0
    ggim_category_explanation: str = ""
    ggim_category_memberships: list[str] = field(default_factory=list)
    ggim_category_candidates: list[str] = field(default_factory=list)
    allowed_scenarios_from_ggim: list[str] = field(default_factory=list)
    token_weights: Counter[str] = field(default_factory=Counter, repr=False)
    normalized_name_blob: str = field(default="", repr=False)
    normalized_desc_blob: str = field(default="", repr=False)
    normalized_columns_blob: str = field(default="", repr=False)


def normalize_text(text: str) -> str:
    text = html.unescape(text or "")
    text = HTML_TAG_RE.sub(" ", text)
    text = text.replace("&nbsp;", " ")
    text = re.sub(r"[\r\n\t/]", " ", text)
    text = re.sub(r"[_\-]+", " ", text)
    text = re.sub(r"\s+", " ", text)
    return text.strip().lower()


def tokenize(text: str) -> list[str]:
    return [tok for tok in TOKEN_RE.findall(normalize_text(text)) if tok not in STOPWORDS and len(tok) > 1]


def weighted_tokens(name: str, description: str, tags: list[str], columns: list[str], file_name: str) -> Counter[str]:
    counter: Counter[str] = Counter()
    for token in tokenize(name):
        counter[token] += 3
    for token in tokenize(file_name):
        counter[token] += 2
    for token in tokenize(description):
        counter[token] += 1
    for tag in tags:
        for token in tokenize(tag):
            counter[token] += 2
    for column in columns:
        for token in tokenize(column):
            counter[token] += 1
    return counter


def load_metadata(metadata_path: Path) -> dict[str, dict[str, Any]]:
    if not metadata_path.exists():
        return {}
    metadata = json.loads(metadata_path.read_text(encoding="utf-8"))
    if isinstance(metadata, dict):
        items = metadata.get("datasets") or metadata.get("records") or metadata.get("downloads") or []
    else:
        items = metadata

    by_file: dict[str, dict[str, Any]] = {}
    flattened_items: list[dict[str, Any]] = []
    for item in items:
        if not isinstance(item, dict):
            continue
        nested = item.get("datasets")
        if isinstance(nested, list):
            for dataset_item in nested:
                if isinstance(dataset_item, dict):
                    merged = dict(dataset_item)
                    merged.setdefault("city", item.get("city_id") or item.get("City"))
                    flattened_items.append(merged)
            continue
        flattened_items.append(item)

    for item in flattened_items:
        names = [
            item.get("geojson_filename"),
            item.get("csv_filename"),
        ]
        for key in ("geojson_path", "path", "csv_path"):
            value = item.get(key)
            if value:
                names.append(Path(str(value)).name)
        for name in names:
            if isinstance(name, str) and name:
                by_file[name] = item
    return by_file


def load_socrata_manifest(manifest_path: Path) -> tuple[dict[str, dict[str, Any]], str]:
    payload = json.loads(manifest_path.read_text(encoding="utf-8"))
    domain = payload.get("meta", {}).get("domain", "")
    by_file: dict[str, dict[str, Any]] = {}
    for item in payload.get("datasets", []):
        data_path = item.get("path") or item.get("csv_path") or ""
        if not data_path:
            continue
        file_name = Path(data_path).name
        merged = dict(item)
        merged["data_path"] = data_path
        by_file[file_name] = merged
    return by_file, domain


def read_header(data_path: Path) -> list[str]:
    if data_path.suffix.lower() in {".geojson", ".json"}:
        try:
            payload = json.loads(data_path.read_text(encoding="utf-8"))
            features = payload.get("features") or []
            if not features:
                return []
            props = (features[0] or {}).get("properties") or {}
            cols = list(props.keys())
            if "geometry" not in cols:
                cols.append("geometry")
            return cols
        except Exception:  # noqa: BLE001
            return []
    for encoding in ("utf-8-sig", "utf-8", "latin-1"):
        try:
            with data_path.open("r", encoding=encoding, newline="") as handle:
                return next(csv.reader(handle))
        except UnicodeDecodeError:
            continue
    raise UnicodeDecodeError("utf-8", b"", 0, 1, f"Could not read header for {data_path}")


def sample_row(data_path: Path, max_lines: int = 5) -> dict[str, str]:
    if data_path.suffix.lower() in {".geojson", ".json"}:
        try:
            payload = json.loads(data_path.read_text(encoding="utf-8"))
            features = payload.get("features") or []
            if not features:
                return {}
            feature = features[0] or {}
            props = (feature.get("properties") or {}).copy()
            geom = feature.get("geometry") or {}
            props["geometry"] = str(geom.get("type", ""))
            return {str(k): str(v) for k, v in props.items()}
        except Exception:  # noqa: BLE001
            return {}
    for encoding in ("utf-8-sig", "utf-8", "latin-1"):
        try:
            with data_path.open("r", encoding=encoding, newline="") as handle:
                reader = csv.DictReader(handle)
                for idx, row in enumerate(reader):
                    if idx >= max_lines:
                        break
                    if row and any(value for value in row.values()):
                        return {key: (value or "") for key, value in row.items()}
        except UnicodeDecodeError:
            continue
    return {}


def detect_spatial_columns(columns: list[str]) -> list[str]:
    hits: list[str] = []
    for column in columns:
        lower = column.lower()
        if any(hint == lower or hint in lower for hint in SPATIAL_COLUMN_HINTS):
            hits.append(column)
    return hits


def infer_geometry_type(columns: list[str], row: dict[str, str]) -> str:
    for column in columns:
        value = (row.get(column, "") or "").strip()
        upper = value.upper()
        for prefix in WKT_PREFIXES:
            if upper.startswith(prefix):
                return prefix
    lowered = {key.lower(): value for key, value in row.items()}
    if any(key in lowered for key in ("latitude", "longitude", "point_x", "point_y")) or "lat/long" in lowered:
        return "POINT_COORDINATES"
    return "NON_SPATIAL"


def infer_join_keys(columns: list[str], text_blob: str) -> list[str]:
    combined = " | ".join(columns) + " | " + text_blob
    return sorted([key for key, pattern in JOIN_KEY_PATTERNS.items() if pattern.search(combined)])


def dataset_id_from_name(file_name: str) -> str:
    match = DATASET_ID_RE.search(file_name) or DATASET_ID_FILE_RE.search(Path(file_name).name)
    if match:
        return match.group(1)
    stem = Path(file_name).name
    stem = stem.removesuffix(".csv").removesuffix(".geojson").removesuffix(".json")
    return stem


def _iter_city_data_files(city_root: Path) -> list[Path]:
    """递归扫描城市目录下全部 CSV / GeoJSON（排除 manifest 等）。"""
    if not city_root.is_dir():
        return []
    paths: list[Path] = []
    for pattern in ("*.csv", "*.geojson"):
        paths.extend(city_root.rglob(pattern))
    seen: set[str] = set()
    out: list[Path] = []
    for p in sorted(paths, key=lambda x: str(x).lower()):
        if not p.is_file() or p.name.startswith("."):
            continue
        if p.name.lower() == "manifest.json":
            continue
        key = str(p.resolve())
        if key in seen:
            continue
        seen.add(key)
        out.append(p)
    return out


def build_profiles_from_local_directory(
    city_root: Path,
    city: str,
    city_label: str,
    *,
    domain: str = "",
) -> list[TableProfile]:
    """以磁盘文件为准：递归扫描 city_root 下每个数据文件并生成 TableProfile（不经 manifest 过滤）。"""
    profiles: list[TableProfile] = []
    used_dataset_ids: set[str] = set()

    for data_path in _iter_city_data_files(city_root):
        if data_path.suffix.lower() in {".geojson", ".json"}:
            try:
                payload = json.loads(data_path.read_text(encoding="utf-8"))
                if isinstance(payload, dict) and payload.get("type") == "FeatureCollection":
                    feats = payload.get("features") or []
                    if len(feats) == 0:
                        continue
            except Exception:  # noqa: BLE001
                continue
        try:
            columns = read_header(data_path)
        except Exception:  # noqa: BLE001
            continue
        if not columns:
            continue
        row = sample_row(data_path)
        spatial_columns = detect_spatial_columns(columns)
        description = ""
        stem = data_path.stem
        dataset_name = stem.replace("__", " ").replace("_", " ").strip() or data_path.name
        did = dataset_id_from_name(data_path.name)
        if did in used_dataset_ids:
            did = f"{did}__{hashlib.md5(str(data_path.resolve()).encode('utf-8')).hexdigest()[:10]}"
        if did in used_dataset_ids:
            did = f"{did}_{len(used_dataset_ids)}"
        used_dataset_ids.add(did)

        asset_url = f"https://{domain}/d/{did}" if domain else f"file://{data_path.resolve().as_posix()}"
        profile = TableProfile(
            city=city,
            city_label=city_label,
            file_name=data_path.name,
            dataset_id=did,
            dataset_name=dataset_name,
            description=description,
            tags=[],
            asset_url=asset_url,
            last_updated="",
            views=0,
            columns=columns,
            n_columns=len(columns),
            spatial_columns=spatial_columns,
            has_spatial=bool(spatial_columns),
            geometry_type=infer_geometry_type(columns, row),
            join_keys=infer_join_keys(columns, f"{dataset_name} {description}"),
        )
        profile.normalized_name_blob = normalize_text(f"{dataset_name} {data_path.stem}")
        profile.normalized_desc_blob = description
        profile.normalized_columns_blob = normalize_text(" ".join(columns))
        profile.token_weights = weighted_tokens(dataset_name, description, profile.tags, columns, data_path.name)
        profile.theme, profile.theme_confidence, profile.theme_explanation = classify_theme(profile)
        classify_ggim_categories(profile)
        profiles.append(profile)

    return profiles


def phrase_match_score(phrase: str, name_blob: str, desc_blob: str, columns_blob: str) -> tuple[float, str | None]:
    if phrase in name_blob:
        return 3.0, phrase
    if phrase in desc_blob:
        return 1.75, phrase
    if phrase in columns_blob:
        return 1.1, phrase
    return 0.0, None


def confidence_from_scores(top_score: float, second_score: float) -> float:
    signal = min(1.0, top_score / 10.0)
    margin = max(0.0, top_score - second_score) / max(top_score, 1.0)
    return round(max(0.22, min(0.98, 0.28 + (0.47 * signal) + (0.23 * margin))), 2)


def classify_theme(profile: TableProfile) -> tuple[str, float, str]:
    scores: dict[str, float] = {}
    reasons: dict[str, list[str]] = {}
    whole_blob = " ".join([profile.normalized_name_blob, profile.normalized_desc_blob, profile.normalized_columns_blob])
    for config in THEME_TAXONOMY:
        score = 0.0
        matched_terms: list[str] = []
        for keyword in config.keywords:
            delta, reason = phrase_match_score(keyword, profile.normalized_name_blob, profile.normalized_desc_blob, profile.normalized_columns_blob)
            score += delta
            if reason:
                matched_terms.append(reason)
        for keyword in config.column_keywords:
            if keyword in profile.normalized_columns_blob:
                score += 0.8
                matched_terms.append(f"column:{keyword}")
        for pattern in config.patterns:
            if re.search(pattern, whole_blob):
                score += 1.4
                matched_terms.append(f"pattern:{pattern}")
        for join_key in profile.join_keys:
            if join_key in config.join_key_bonus:
                score += 0.35
        if THEME_OVERRIDES[config.id].search(whole_blob):
            score += 2.0
        if THEME_NAME_PRIORITY[config.id].search(profile.normalized_name_blob):
            score += 3.2
            matched_terms.append("name-priority")
        if profile.geometry_type in {"POLYGON", "MULTIPOLYGON"} and config.id in {"demographics_boundaries", "housing_land_use", "environment"}:
            score += 0.35
        if profile.geometry_type in {"LINESTRING", "MULTILINESTRING"} and config.id == "transportation":
            score += 0.45
        scores[config.id] = round(score, 4)
        reasons[config.id] = matched_terms

    ranked = sorted(scores.items(), key=lambda item: item[1], reverse=True)
    top_theme, top_score = ranked[0]
    second_score = ranked[1][1] if len(ranked) > 1 else 0.0
    if re.search(r"automated external defibrillator|\baed\b|pharmaceutical|health center", profile.normalized_name_blob):
        top_theme = "healthcare"
        top_score = max(top_score, second_score + 3.5)
    if top_score < 1.5:
        top_theme = "housing_land_use" if "building" in whole_blob else "poi_public_facilities"
        top_score = max(top_score, 1.5)
    confidence = confidence_from_scores(top_score, second_score)
    matched_reason = ", ".join(list(dict.fromkeys(reasons[top_theme]))[:4]) if reasons[top_theme] else "weak lexical evidence"
    spatial_note = "spatial columns detected" if profile.has_spatial else "no explicit spatial columns detected"
    explanation = f"Assigned to {top_theme} from lexical cues ({matched_reason}), join keys {profile.join_keys or ['none']}, and geometry type {profile.geometry_type}; {spatial_note}."
    return top_theme, confidence, explanation


def allowed_scenarios_for_ggim_categories(category_ids: list[str]) -> set[str]:
    """Business scenarios whose declared GGIM bridge intersects any of the table's categories."""
    cat_set = set(category_ids)
    if not cat_set:
        return set()
    allowed: set[str] = set()
    for scenario_id, ggim_cats in SCENARIO_TO_GGIM_CATEGORIES.items():
        if cat_set.intersection(ggim_cats):
            allowed.add(scenario_id)
    return allowed


def classify_ggim_categories(profile: TableProfile) -> None:
    """First-level ISO/UN-GGIM style categories; same scoring style as classify_theme (rule-based, explainable)."""
    max_memberships = 3
    relative_membership_threshold = 0.72
    absolute_membership_threshold = 2.0
    scores: dict[str, float] = {}
    reasons: dict[str, list[str]] = {}
    whole_blob = " ".join([profile.normalized_name_blob, profile.normalized_desc_blob, profile.normalized_columns_blob])
    for config in CATEGORY_TAXONOMY:
        score = 0.0
        matched_terms: list[str] = []
        for keyword in config.keywords:
            delta, reason = phrase_match_score(keyword, profile.normalized_name_blob, profile.normalized_desc_blob, profile.normalized_columns_blob)
            score += delta
            if reason:
                matched_terms.append(reason)
        for keyword in config.column_keywords:
            if keyword in profile.normalized_columns_blob:
                score += 0.8
                matched_terms.append(f"column:{keyword}")
        for pattern in config.patterns:
            if re.search(pattern, whole_blob):
                score += 1.4
                matched_terms.append(f"pattern:{pattern}")
        for join_key in profile.join_keys:
            if join_key in config.join_key_bonus:
                score += 0.35
        if profile.theme in config.preferred_themes:
            score += 2.8
            matched_terms.append(f"theme:{profile.theme}")
        if CATEGORY_OVERRIDES[config.id].search(whole_blob):
            score += 2.0
            matched_terms.append(f"override:{config.id}")
        if CATEGORY_NAME_PRIORITY[config.id].search(profile.normalized_name_blob):
            score += 3.2
            matched_terms.append("name-priority")
        if profile.geometry_type in {"POLYGON", "MULTIPOLYGON"} and config.id in {"ggim_3", "ggim_5", "ggim_7", "ggim_8", "ggim_9", "ggim_12", "ggim_14"}:
            score += 0.35
        if profile.geometry_type in {"LINESTRING", "MULTILINESTRING"} and config.id in {"ggim_13", "ggim_14"}:
            score += 0.45
        scores[config.id] = round(score, 4)
        reasons[config.id] = matched_terms

    ranked = sorted(scores.items(), key=lambda item: item[1], reverse=True)
    top_cat, top_score = ranked[0]
    second_score = ranked[1][1] if len(ranked) > 1 else 0.0
    if re.search(r"automated external defibrillator|\baed\b|pharmaceutical|health center", profile.normalized_name_blob):
        top_cat = "ggim_11"
        top_score = max(top_score, second_score + 2.5)
    if top_score < 1.5:
        top_cat = DEFAULT_THEME_TO_GGIM_CATEGORY.get(profile.theme, "ggim_11")
        top_score = max(top_score, 1.5)
    confidence = confidence_from_scores(top_score, second_score)
    matched_reason = ", ".join(list(dict.fromkeys(reasons[top_cat]))[:4]) if reasons[top_cat] else "weak lexical evidence"
    spatial_note = "spatial columns detected" if profile.has_spatial else "no explicit spatial columns detected"
    explanation = (
        f"Assigned to {top_cat} ({next(c.label for c in CATEGORY_TAXONOMY if c.id == top_cat)}) from lexical cues "
        f"({matched_reason}), theme {profile.theme}, join keys {profile.join_keys or ['none']}, geometry {profile.geometry_type}; {spatial_note}."
    )
    membership_threshold = max(absolute_membership_threshold, top_score * relative_membership_threshold)
    memberships = [cid for cid, sc in ranked if sc >= membership_threshold]
    if top_cat not in memberships:
        memberships.insert(0, top_cat)
    profile.primary_ggim_category = top_cat
    profile.ggim_category_confidence = confidence
    profile.ggim_category_explanation = explanation
    profile.ggim_category_memberships = memberships[:max_memberships]
    profile.ggim_category_candidates = [cid for cid, _sc in ranked[:3]]


def counter_cosine(left: Counter[str], right: Counter[str]) -> float:
    if not left or not right:
        return 0.0
    common = set(left).intersection(right)
    numerator = sum(left[token] * right[token] for token in common)
    left_norm = math.sqrt(sum(value * value for value in left.values()))
    right_norm = math.sqrt(sum(value * value for value in right.values()))
    if left_norm == 0.0 or right_norm == 0.0:
        return 0.0
    return numerator / (left_norm * right_norm)


def profile_similarity(left: TableProfile, right: TableProfile) -> float:
    semantic = counter_cosine(left.token_weights, right.token_weights)
    left_keys = set(left.join_keys)
    right_keys = set(right.join_keys)
    union = left_keys | right_keys
    join_overlap = (len(left_keys & right_keys) / len(union)) if union else 0.0
    left_cols = {normalize_text(column) for column in left.columns}
    right_cols = {normalize_text(column) for column in right.columns}
    union_cols = left_cols | right_cols
    column_overlap = (len(left_cols & right_cols) / len(union_cols)) if union_cols else 0.0
    geom_bonus = 0.2 if left.geometry_type == right.geometry_type else 0.1 if {left.geometry_type, right.geometry_type} & {"POINT", "POLYGON", "MULTIPOLYGON"} else 0.0
    theme_bonus = 0.15 if left.theme == right.theme else 0.0
    return (0.5 * semantic) + (0.25 * join_overlap) + (0.15 * column_overlap) + geom_bonus + theme_bonus


def classify_scenarios(profiles: list[TableProfile]) -> None:
    """Second-level business scenarios; scoring unchanged, then restricted to scenarios allowed by GGIM memberships."""
    max_memberships = 3
    relative_membership_threshold = 0.72
    absolute_membership_threshold = 2.0
    base_scores: dict[str, dict[str, float]] = {}
    base_reasons: dict[str, dict[str, list[str]]] = {}
    all_scenario_ids = {prototype.id for prototype in SCENARIO_PROTOTYPES}
    for profile in profiles:
        whole_blob = " ".join([profile.normalized_name_blob, profile.normalized_desc_blob, profile.normalized_columns_blob])
        scenario_scores: dict[str, float] = {}
        scenario_reasons: dict[str, list[str]] = {}
        for prototype in SCENARIO_PROTOTYPES:
            score = 0.0
            reasons: list[str] = []
            if profile.theme in prototype.preferred_themes:
                score += 2.8
                reasons.append(f"theme:{profile.theme}")
            for keyword in prototype.keywords:
                delta, reason = phrase_match_score(keyword, profile.normalized_name_blob, profile.normalized_desc_blob, profile.normalized_columns_blob)
                if delta:
                    score += delta
                    reasons.append(reason or keyword)
            for join_key in profile.join_keys:
                if join_key in prototype.join_keys:
                    score += 0.45
                    reasons.append(f"join:{join_key}")
            if profile.geometry_type in prototype.geometry_preferences:
                score += 0.4
            if SCENARIO_OVERRIDES[prototype.id].search(whole_blob):
                score += 1.8
                reasons.append(f"override:{prototype.id}")
            scenario_scores[prototype.id] = round(score, 4)
            scenario_reasons[prototype.id] = reasons
        base_scores[profile.file_name] = scenario_scores
        base_reasons[profile.file_name] = scenario_reasons

    seed_tables: dict[str, list[TableProfile]] = {}
    for prototype in SCENARIO_PROTOTYPES:
        ranked_seeds = sorted(profiles, key=lambda item: base_scores[item.file_name][prototype.id], reverse=True)
        seed_tables[prototype.id] = ranked_seeds[: min(12, len(ranked_seeds))]

    for profile in profiles:
        cats = profile.ggim_category_memberships or ([profile.primary_ggim_category] if profile.primary_ggim_category else [])
        if not cats:
            cats = [DEFAULT_THEME_TO_GGIM_CATEGORY.get(profile.theme, "ggim_11")]
        allowed = allowed_scenarios_for_ggim_categories(cats)
        if not allowed:
            allowed = set(all_scenario_ids)
        profile.allowed_scenarios_from_ggim = sorted(allowed)

        final_scores: dict[str, float] = {}
        explanations: dict[str, str] = {}
        for prototype in SCENARIO_PROTOTYPES:
            base = base_scores[profile.file_name][prototype.id]
            similarities = [
                profile_similarity(profile, candidate)
                for candidate in seed_tables[prototype.id]
                if candidate.file_name != profile.file_name
            ]
            support = (sum(sorted(similarities, reverse=True)[:3]) / max(1, min(3, len(similarities)))) if similarities else 0.0
            final_scores[prototype.id] = round(base + (2.4 * support), 4)
            seed_examples = [candidate.dataset_name for candidate in seed_tables[prototype.id][:3] if candidate.file_name != profile.file_name]
            matched = ", ".join(list(dict.fromkeys(base_reasons[profile.file_name][prototype.id]))[:4]) or "weak lexical evidence"
            explanations[prototype.id] = (
                f"Scenario evidence: {matched}; graph neighbors resemble {seed_examples[:2] or ['no strong neighbors']}; "
                f"allowed by GGIM layer: {sorted(allowed)}."
            )

        healthcare_re = re.search(
            r"automated external defibrillator|\baed\b|pharmaceutical|health center",
            profile.normalized_name_blob,
        )
        if profile.theme == "healthcare" and healthcare_re:
            ps = "public_service_accessibility"
            final_scores[ps] = round(final_scores.get(ps, 0.0) + 2.5, 4)

        masked_scores = {sid: (sc if sid in allowed else float("-inf")) for sid, sc in final_scores.items()}
        ranked = sorted(masked_scores.items(), key=lambda item: item[1], reverse=True)
        top_scenario, top_score = ranked[0]
        if not math.isfinite(top_score):
            ranked = sorted(final_scores.items(), key=lambda item: item[1], reverse=True)
            top_scenario, top_score = ranked[0]

        if top_score < 2.0:
            default_sid = DEFAULT_SCENARIO_BY_THEME.get(profile.theme, "urban_planning_land_use")
            if default_sid in allowed:
                top_scenario = default_sid
                masked_scores[default_sid] = max(masked_scores.get(default_sid, float("-inf")), 2.0)
            else:
                top_scenario = max(allowed, key=lambda sid: final_scores.get(sid, float("-inf")))
                masked_scores[top_scenario] = final_scores[top_scenario]
            ranked = sorted(masked_scores.items(), key=lambda item: item[1], reverse=True)
            top_scenario, top_score = ranked[0]

        second_score = ranked[1][1] if len(ranked) > 1 and math.isfinite(ranked[1][1]) else 0.0

        membership_threshold = max(absolute_membership_threshold, top_score * relative_membership_threshold)
        memberships = [sid for sid, sc in ranked if math.isfinite(sc) and sc >= membership_threshold]
        if top_scenario not in memberships:
            memberships.insert(0, top_scenario)
        profile.scenario_memberships = memberships[:max_memberships]
        profile.scenario = top_scenario
        profile.scenario_confidence = confidence_from_scores(top_score, second_score if math.isfinite(second_score) else 0.0)
        profile.scenario_explanation = explanations[top_scenario]
        profile.scenario_candidates = [scenario_id for scenario_id, _score in ranked[:3] if math.isfinite(_score)]


def build_profiles(raw_dir: Path) -> list[TableProfile]:
    metadata_path = raw_dir / "nyc_opendata_maps.json"
    if not metadata_path.exists():
        metadata_path = raw_dir / "manifest.json"
    if not metadata_path.exists():
        metadata_path = raw_dir.parent / "metadata.json"
    metadata_by_file = load_metadata(metadata_path)
    profiles: list[TableProfile] = []
    for data_path in _iter_city_data_files(raw_dir):
        if data_path.name.startswith("._"):
            continue
        metadata = metadata_by_file.get(data_path.name, {})
        columns = read_header(data_path)
        row = sample_row(data_path)
        spatial_columns = detect_spatial_columns(columns)
        description = normalize_text(metadata.get("description", ""))
        dataset_name = metadata.get("name", data_path.stem.replace("_", " "))
        profile = TableProfile(
            city="nyc",
            city_label="New York City",
            file_name=data_path.name,
            dataset_id=dataset_id_from_name(data_path.name),
            dataset_name=dataset_name,
            description=description,
            tags=[normalize_text(tag) for tag in metadata.get("tags", []) if tag],
            asset_url=metadata.get("asset_url", ""),
            last_updated=metadata.get("last_updated", ""),
            views=int(metadata.get("views", 0) or 0),
            columns=columns,
            n_columns=len(columns),
            spatial_columns=spatial_columns,
            has_spatial=bool(spatial_columns),
            geometry_type=infer_geometry_type(columns, row),
            join_keys=infer_join_keys(columns, f"{dataset_name} {description}"),
        )
        profile.normalized_name_blob = normalize_text(f"{dataset_name} {data_path.stem}")
        profile.normalized_desc_blob = description
        profile.normalized_columns_blob = normalize_text(" ".join(columns))
        profile.token_weights = weighted_tokens(dataset_name, description, profile.tags, columns, data_path.name)
        profile.theme, profile.theme_confidence, profile.theme_explanation = classify_theme(profile)
        classify_ggim_categories(profile)
        profiles.append(profile)
    classify_scenarios(profiles)
    return profiles


def build_profiles_from_socrata_manifest(manifest_path: Path, city: str, city_label: str) -> list[TableProfile]:
    metadata_by_file, domain = load_socrata_manifest(manifest_path)
    profiles: list[TableProfile] = []
    for file_name, metadata in metadata_by_file.items():
        data_path = Path(metadata.get("data_path", ""))
        if not data_path.exists():
            continue
        columns = read_header(data_path)
        row = sample_row(data_path)
        spatial_columns = detect_spatial_columns(columns)
        description = normalize_text(metadata.get("description", ""))
        dataset_name = metadata.get("name", data_path.stem.replace("_", " "))
        profile = TableProfile(
            city=city,
            city_label=city_label,
            file_name=data_path.name,
            dataset_id=str(metadata.get("id", dataset_id_from_name(data_path.name))),
            dataset_name=dataset_name,
            description=description,
            tags=[normalize_text(tag) for tag in metadata.get("tags", []) if tag],
            asset_url=f"https://{domain}/d/{metadata.get('id', '')}" if domain else "",
            last_updated=metadata.get("updatedAt", ""),
            views=int(metadata.get("views", 0) or 0),
            columns=columns,
            n_columns=len(columns),
            spatial_columns=spatial_columns,
            has_spatial=bool(spatial_columns),
            geometry_type=infer_geometry_type(columns, row),
            join_keys=infer_join_keys(columns, f"{dataset_name} {description}"),
        )
        profile.normalized_name_blob = normalize_text(f"{dataset_name} {data_path.stem}")
        profile.normalized_desc_blob = description
        profile.normalized_columns_blob = normalize_text(" ".join(columns))
        profile.token_weights = weighted_tokens(dataset_name, description, profile.tags, columns, data_path.name)
        profile.theme, profile.theme_confidence, profile.theme_explanation = classify_theme(profile)
        classify_ggim_categories(profile)
        profiles.append(profile)
    classify_scenarios(profiles)
    return profiles


def profile_to_record(profile: TableProfile) -> dict[str, Any]:
    return {
        "city": profile.city,
        "city_label": profile.city_label,
        "file_name": profile.file_name,
        "dataset_id": profile.dataset_id,
        "dataset_name": profile.dataset_name,
        "theme_label": profile.theme,
        "theme_confidence": profile.theme_confidence,
        "theme_explanation": profile.theme_explanation,
        "primary_scenario": profile.scenario,
        "scenario_confidence": profile.scenario_confidence,
        "scenario_explanation": profile.scenario_explanation,
        "scenario_candidates": profile.scenario_candidates,
        "scenario_memberships": profile.scenario_memberships,
        "primary_ggim_category": profile.primary_ggim_category,
        "ggim_category_confidence": profile.ggim_category_confidence,
        "ggim_category_explanation": profile.ggim_category_explanation,
        "ggim_category_memberships": profile.ggim_category_memberships,
        "ggim_category_candidates": profile.ggim_category_candidates,
        "allowed_scenarios_from_ggim": profile.allowed_scenarios_from_ggim,
        "columns": profile.columns,
        "n_columns": profile.n_columns,
        "joinable_keys": profile.join_keys,
        "has_spatial_columns": profile.has_spatial,
        "spatial_columns": profile.spatial_columns,
        "geometry_type": profile.geometry_type,
        "asset_url": profile.asset_url,
        "last_updated": profile.last_updated,
        "views": profile.views,
    }


def write_json(path: Path, payload: Any) -> None:
    path.write_text(json.dumps(payload, indent=2, ensure_ascii=False), encoding="utf-8")


def write_csv_catalog(path: Path, profiles: list[TableProfile]) -> None:
    rows = [profile_to_record(profile) for profile in profiles]
    fieldnames = [
        "city", "city_label",
        "file_name", "dataset_id", "dataset_name", "theme_label", "theme_confidence", "theme_explanation",
        "primary_ggim_category", "ggim_category_confidence", "ggim_category_explanation",
        "ggim_category_candidates", "ggim_category_memberships",
        "primary_scenario", "scenario_confidence", "scenario_explanation", "scenario_candidates", "scenario_memberships",
        "allowed_scenarios_from_ggim",
        "columns",
        "n_columns", "joinable_keys", "has_spatial_columns", "spatial_columns", "geometry_type", "asset_url",
        "last_updated", "views",
    ]
    with path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames)
        writer.writeheader()
        for row in rows:
            row["ggim_category_candidates"] = " | ".join(row["ggim_category_candidates"])
            row["ggim_category_memberships"] = " | ".join(row["ggim_category_memberships"])
            row["allowed_scenarios_from_ggim"] = " | ".join(row["allowed_scenarios_from_ggim"])
            row["scenario_candidates"] = " | ".join(row["scenario_candidates"])
            row["scenario_memberships"] = " | ".join(row["scenario_memberships"])
            row["columns"] = " | ".join(row["columns"])
            row["joinable_keys"] = " | ".join(row["joinable_keys"])
            row["spatial_columns"] = " | ".join(row["spatial_columns"])
            writer.writerow(row)


def build_unified_inventory(profiles: list[TableProfile]) -> list[dict[str, Any]]:
    return [
        {
            "dataset_uid": f"{p.city}:{p.dataset_id}",
            "city": p.city,
            "city_label": p.city_label,
            "dataset_id": p.dataset_id,
            "dataset_name": p.dataset_name,
            "file_name": p.file_name,
            "description": p.description,
            "asset_url": p.asset_url,
            "last_updated": p.last_updated,
            "has_spatial_columns": p.has_spatial,
            "geometry_type": p.geometry_type,
            "joinable_keys": list(p.join_keys),
        }
        for p in profiles
    ]


def write_unified_inventory_csv(path: Path, payload: list[dict[str, Any]]) -> None:
    fieldnames = [
        "dataset_uid",
        "city",
        "city_label",
        "dataset_id",
        "dataset_name",
        "file_name",
        "description",
        "asset_url",
        "last_updated",
        "has_spatial_columns",
        "geometry_type",
        "joinable_keys",
    ]
    with path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames)
        writer.writeheader()
        for row in payload:
            out = dict(row)
            out["joinable_keys"] = " | ".join(out["joinable_keys"])
            writer.writerow(out)


def infer_spatial_relationships(members: list[TableProfile]) -> list[str]:
    geometry_types = {member.geometry_type for member in members}
    relationships: list[str] = []
    if "POINT" in geometry_types or "POINT_COORDINATES" in geometry_types:
        if {"POLYGON", "MULTIPOLYGON"} & geometry_types:
            relationships.append("point-in-polygon containment")
        if {"LINESTRING", "MULTILINESTRING"} & geometry_types:
            relationships.append("point-to-line proximity")
    if len({"POLYGON", "MULTIPOLYGON"} & geometry_types) >= 1:
        relationships.append("polygon overlay / intersection")
    if len(geometry_types) > 1:
        relationships.append("mixed-geometry spatial joins")
    return relationships or ["attribute joins with optional spatial enrichment"]


def build_scenario_clusters(profiles: list[TableProfile]) -> list[dict[str, Any]]:
    grouped: dict[str, list[TableProfile]] = defaultdict(list)
    for profile in profiles:
        memberships = profile.scenario_memberships or [profile.scenario]
        for scenario_id in memberships:
            grouped[scenario_id].append(profile)

    scenario_label = {prototype.id: prototype.label for prototype in SCENARIO_PROTOTYPES}
    scenario_desc = {prototype.id: prototype.description for prototype in SCENARIO_PROTOTYPES}
    clusters: list[dict[str, Any]] = []
    for scenario_id, members in sorted(grouped.items(), key=lambda item: item[0]):
        join_counts = Counter(join_key for member in members for join_key in member.join_keys)
        theme_counts = Counter(member.theme for member in members)
        geometry_counts = Counter(member.geometry_type for member in members)
        clusters.append(
            {
                "scenario_id": scenario_id,
                "scenario_label": scenario_label[scenario_id],
                "scenario_description": scenario_desc[scenario_id],
                "n_tables": len(members),
                "shared_entities": [key for key, _count in join_counts.most_common(5)],
                "joinable_keys": [{"key": key, "count": count} for key, count in join_counts.most_common(8)],
                "theme_mix": [{"theme": theme, "count": count} for theme, count in theme_counts.most_common()],
                "geometry_mix": [{"geometry_type": geom, "count": count} for geom, count in geometry_counts.most_common()],
                "common_spatial_relationships": infer_spatial_relationships(members),
                "overlapping_geographic_regions": [key for key in join_counts if key in {"borough", "community_district", "census_tract", "nta", "zip", "puma"}],
                "why_meaningful_for_text_to_sql": f"This cluster combines {len(members)} semantically aligned tables with shared keys such as {', '.join(key for key, _ in join_counts.most_common(3)) or 'spatial predicates'}, supporting multi-table joins and compositional spatial reasoning.",
                "tables": [
                    {
                        "file_name": member.file_name,
                        "dataset_name": member.dataset_name,
                        "theme": member.theme,
                        "theme_confidence": member.theme_confidence,
                        "scenario_confidence": member.scenario_confidence,
                        "joinable_keys": member.join_keys,
                        "geometry_type": member.geometry_type,
                    }
                    for member in sorted(members, key=lambda item: (-item.scenario_confidence, item.file_name))
                ],
            }
        )
    return clusters


def build_category_clusters(profiles: list[TableProfile]) -> list[dict[str, Any]]:
    """One cluster per GGIM category id; tables may appear in multiple clusters (multi-label)."""
    labels = {c.id: c.label for c in CATEGORY_TAXONOMY}
    grouped: dict[str, list[TableProfile]] = defaultdict(list)
    for profile in profiles:
        memberships = profile.ggim_category_memberships or (
            [profile.primary_ggim_category] if profile.primary_ggim_category else []
        )
        for cat_id in memberships:
            grouped[cat_id].append(profile)

    clusters: list[dict[str, Any]] = []
    for cat_id, members in sorted(grouped.items(), key=lambda item: item[0]):
        join_counts = Counter(join_key for member in members for join_key in member.join_keys)
        theme_counts = Counter(member.theme for member in members)
        geometry_counts = Counter(member.geometry_type for member in members)
        clusters.append(
            {
                "category_id": cat_id,
                "category_label": labels.get(cat_id, cat_id),
                "n_tables": len(members),
                "shared_entities": [key for key, _count in join_counts.most_common(5)],
                "joinable_keys": [{"key": key, "count": count} for key, count in join_counts.most_common(8)],
                "theme_mix": [{"theme": theme, "count": count} for theme, count in theme_counts.most_common()],
                "geometry_mix": [{"geometry_type": geom, "count": count} for geom, count in geometry_counts.most_common()],
                "common_spatial_relationships": infer_spatial_relationships(members),
                "overlapping_geographic_regions": [
                    key for key in join_counts if key in {"borough", "community_district", "census_tract", "nta", "zip", "puma"}
                ],
                "why_meaningful_for_text_to_sql": (
                    f"This GGIM/ISO-aligned category groups {len(members)} tables with shared keys "
                    f"{', '.join(key for key, _ in join_counts.most_common(3)) or 'spatial predicates'} for compositional spatial reasoning."
                ),
                "tables": [
                    {
                        "file_name": member.file_name,
                        "dataset_name": member.dataset_name,
                        "theme": member.theme,
                        "theme_confidence": member.theme_confidence,
                        "ggim_category_confidence": member.ggim_category_confidence,
                        "primary_scenario": member.scenario,
                        "scenario_confidence": member.scenario_confidence,
                        "joinable_keys": member.join_keys,
                        "geometry_type": member.geometry_type,
                    }
                    for member in sorted(members, key=lambda item: (-item.ggim_category_confidence, item.file_name))
                ],
            }
        )
    return clusters


def build_table_category_scenario_mapping(profiles: list[TableProfile]) -> list[dict[str, Any]]:
    """Flat join view: theme + GGIM layer + allowed scenarios + primary/multi scenario memberships."""
    return [
        {
            "file_name": p.file_name,
            "dataset_id": p.dataset_id,
            "dataset_name": p.dataset_name,
            "theme_label": p.theme,
            "theme_confidence": p.theme_confidence,
            "primary_ggim_category": p.primary_ggim_category,
            "ggim_category_memberships": list(p.ggim_category_memberships),
            "ggim_category_confidence": p.ggim_category_confidence,
            "ggim_category_candidates": list(p.ggim_category_candidates),
            "allowed_scenarios_from_ggim": list(p.allowed_scenarios_from_ggim),
            "primary_scenario": p.scenario,
            "scenario_memberships": list(p.scenario_memberships),
            "scenario_confidence": p.scenario_confidence,
            "scenario_candidates": list(p.scenario_candidates),
        }
        for p in profiles
    ]


def build_dataset_hierarchy_map(profiles: list[TableProfile]) -> list[dict[str, Any]]:
    category_labels = {c.id: c.label for c in CATEGORY_TAXONOMY}
    scenario_labels = {s.id: s.label for s in SCENARIO_PROTOTYPES}
    return [
        {
            "dataset_uid": f"{p.city}:{p.dataset_id}",
            "city": p.city,
            "city_label": p.city_label,
            "dataset_id": p.dataset_id,
            "dataset_name": p.dataset_name,
            "ggim_iso_code": p.primary_ggim_category,
            "ggim_iso_label": category_labels.get(p.primary_ggim_category, p.primary_ggim_category),
            "scenario_id": p.scenario,
            "scenario_label": scenario_labels.get(p.scenario, p.scenario),
            "confidence": round((p.ggim_category_confidence + p.scenario_confidence) / 2.0, 2),
            "classification_source": "legacy_nyc_rules" if p.city == "nyc" else "cross_city_rules",
            "ggim_candidates": list(p.ggim_category_candidates),
            "scenario_candidates": list(p.scenario_candidates),
        }
        for p in profiles
    ]


def write_dataset_hierarchy_csv(path: Path, payload: list[dict[str, Any]]) -> None:
    fieldnames = [
        "dataset_uid",
        "city",
        "city_label",
        "dataset_id",
        "dataset_name",
        "ggim_iso_code",
        "ggim_iso_label",
        "scenario_id",
        "scenario_label",
        "confidence",
        "classification_source",
        "ggim_candidates",
        "scenario_candidates",
    ]
    with path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames)
        writer.writeheader()
        for row in payload:
            out = dict(row)
            out["ggim_candidates"] = " | ".join(out["ggim_candidates"])
            out["scenario_candidates"] = " | ".join(out["scenario_candidates"])
            writer.writerow(out)


def build_quality_report(profiles: list[TableProfile]) -> tuple[dict[str, Any], list[dict[str, Any]]]:
    total = len(profiles)
    missing = [
        p for p in profiles
        if not p.primary_ggim_category or not p.scenario
    ]
    conflicts: list[TableProfile] = []
    for p in profiles:
        multi_ggim = len(set(p.ggim_category_candidates[:2])) > 1
        multi_scenario = len(set(p.scenario_candidates[:2])) > 1
        if (multi_ggim and p.ggim_category_confidence < 0.65) or (multi_scenario and p.scenario_confidence < 0.65):
            conflicts.append(p)

    by_keyword: dict[str, dict[str, set[str]]] = defaultdict(lambda: defaultdict(set))
    for p in profiles:
        for token in tokenize(p.dataset_name)[:5]:
            by_keyword[token][p.city].add(p.primary_ggim_category)
    inconsistent_keywords = [
        {
            "keyword": kw,
            "city_categories": {city: sorted(vals) for city, vals in city_map.items()},
        }
        for kw, city_map in by_keyword.items()
        if len({cat for vals in city_map.values() for cat in vals}) >= 3 and len(city_map) >= 2
    ]

    review_queue = []
    for p in profiles:
        if p in missing or p in conflicts or p.ggim_category_confidence < 0.55 or p.scenario_confidence < 0.55:
            review_queue.append(
                {
                    "dataset_uid": f"{p.city}:{p.dataset_id}",
                    "city": p.city,
                    "dataset_name": p.dataset_name,
                    "ggim_iso_code": p.primary_ggim_category,
                    "scenario_id": p.scenario,
                    "ggim_confidence": p.ggim_category_confidence,
                    "scenario_confidence": p.scenario_confidence,
                    "priority": "high" if min(p.ggim_category_confidence, p.scenario_confidence) < 0.45 else "medium",
                }
            )

    report = {
        "total_datasets": total,
        "coverage_rate": round((total - len(missing)) / max(total, 1), 4),
        "conflict_rate": round(len(conflicts) / max(total, 1), 4),
        "missing_count": len(missing),
        "conflict_count": len(conflicts),
        "cross_city_inconsistency_count": len(inconsistent_keywords),
        "city_distribution": dict(Counter(p.city for p in profiles)),
    }
    return report, review_queue


def build_clustering_summary_markdown(
    profiles: list[TableProfile],
    scenario_clusters: list[dict[str, Any]],
    category_clusters: list[dict[str, Any]],
) -> str:
    theme_counts = Counter(profile.theme for profile in profiles)
    scenario_counts = Counter(
        scenario_id
        for profile in profiles
        for scenario_id in (profile.scenario_memberships or [profile.scenario])
    )
    ggim_counts = Counter(
        cat_id
        for profile in profiles
        for cat_id in (profile.ggim_category_memberships or [profile.primary_ggim_category])
    )
    lines = [
        "# Spatial Benchmark Clustering Summary",
        "",
        "## Dataset Profiling",
        f"- Total tables profiled: {len(profiles)}",
        f"- Tables with spatial columns: {sum(1 for profile in profiles if profile.has_spatial)}",
        f"- Distinct primary themes: {len(theme_counts)}",
        "",
        "### Theme Distribution",
    ]
    for theme, count in theme_counts.most_common():
        lines.append(f"- {theme}: {count}")
    lines.extend(["", "### Scenario Distribution (multi-label)"])
    for scenario, count in scenario_counts.most_common():
        lines.append(f"- {scenario}: {count}")
    lines.extend(["", "### GGIM / ISO Category Distribution (multi-label)"])
    for cat_id, count in ggim_counts.most_common():
        label = next((c.label for c in CATEGORY_TAXONOMY if c.id == cat_id), cat_id)
        lines.append(f"- {cat_id} ({label}): {count}")
    lines.extend(["", "## GGIM Category Clusters"])
    for cluster in category_clusters:
        lines.append(
            f"- {cluster['category_id']}: {cluster['n_tables']} tables; "
            f"shared entities {', '.join(cluster['shared_entities'][:4]) or 'none'}"
        )
    lines.extend(["", "## Scenario Clusters"])
    for cluster in scenario_clusters:
        lines.append(f"- {cluster['scenario_id']}: {cluster['n_tables']} tables; shared entities {', '.join(cluster['shared_entities'][:4]) or 'none'}")
    return "\n".join(lines) + "\n"


def load_scenario_clusters(artifacts_dir: Path) -> list[dict[str, Any]]:
    clusters_path = artifacts_dir / "scenario_clusters.json"
    if not clusters_path.exists():
        raise FileNotFoundError(
            f"Missing {clusters_path}. Run `python scripts/cluster_tables.py --raw-dir <raw_dir> --artifacts-dir {artifacts_dir}` first."
        )
    return json.loads(clusters_path.read_text(encoding="utf-8"))


def cluster_table_membership(clusters: list[dict[str, Any]]) -> dict[str, set[str]]:
    return {
        cluster["scenario_id"]: {table["file_name"] for table in cluster.get("tables", [])}
        for cluster in clusters
    }


def run_clustering_pipeline(raw_dir: Path, artifacts_dir: Path) -> dict[str, Any]:
    artifacts_dir.mkdir(parents=True, exist_ok=True)
    profiles = build_profiles(raw_dir)
    scenario_clusters = build_scenario_clusters(profiles)
    category_clusters = build_category_clusters(profiles)
    table_mapping = build_table_category_scenario_mapping(profiles)
    unified_inventory = build_unified_inventory(profiles)
    hierarchy_map = build_dataset_hierarchy_map(profiles)
    quality_report, review_queue = build_quality_report(profiles)

    write_json(artifacts_dir / "table_catalog.json", [profile_to_record(profile) for profile in profiles])
    write_csv_catalog(artifacts_dir / "table_catalog.csv", profiles)
    write_json(artifacts_dir / "scenario_clusters.json", scenario_clusters)
    write_json(artifacts_dir / "category_clusters.json", category_clusters)
    write_json(artifacts_dir / "table_category_scenario_mapping.json", table_mapping)
    write_json(artifacts_dir / "unified_inventory.json", unified_inventory)
    write_unified_inventory_csv(artifacts_dir / "unified_inventory.csv", unified_inventory)
    write_json(artifacts_dir / "dataset_hierarchy_map.json", hierarchy_map)
    write_dataset_hierarchy_csv(artifacts_dir / "dataset_hierarchy_map.csv", hierarchy_map)
    write_json(artifacts_dir / "classification_quality_report.json", quality_report)
    write_json(artifacts_dir / "review_queue.json", review_queue)
    (artifacts_dir / "clustering_summary.md").write_text(
        build_clustering_summary_markdown(profiles, scenario_clusters, category_clusters),
        encoding="utf-8",
    )

    return {
        "n_tables": len(profiles),
        "n_clusters": len(scenario_clusters),
        "n_category_clusters": len(category_clusters),
        "table_catalog": str((artifacts_dir / "table_catalog.json").resolve()),
        "scenario_clusters": str((artifacts_dir / "scenario_clusters.json").resolve()),
        "category_clusters": str((artifacts_dir / "category_clusters.json").resolve()),
        "table_category_scenario_mapping": str((artifacts_dir / "table_category_scenario_mapping.json").resolve()),
        "unified_inventory": str((artifacts_dir / "unified_inventory.json").resolve()),
        "dataset_hierarchy_map": str((artifacts_dir / "dataset_hierarchy_map.json").resolve()),
        "classification_quality_report": str((artifacts_dir / "classification_quality_report.json").resolve()),
    }


def run_multi_city_clustering_pipeline(
    *,
    nyc_raw_dir: Path,
    artifacts_dir: Path,
    chicago_manifest_path: Path | None = None,
    lacity_manifest_path: Path | None = None,
    seattle_manifest_path: Path | None = None,
    boston_manifest_path: Path | None = None,
    sf_manifest_path: Path | None = None,
    phoenix_manifest_path: Path | None = None,
    local_socrata_city_roots: dict[str, Path] | None = None,
) -> dict[str, Any]:
    """若提供 local_socrata_city_roots[city_key]，则该城以磁盘递归扫描为准，不再使用 manifest 子集。"""
    artifacts_dir.mkdir(parents=True, exist_ok=True)
    profiles = build_profiles(nyc_raw_dir)

    domain_by_city = {
        "chicago": "data.cityofchicago.org",
        "lacity": "data.lacity.org",
        "seattle": "data.seattle.gov",
        "boston": "data.boston.gov",
        "sf": "data.sfgov.org",
        "phoenix": "www.phoenixopendata.com",
    }
    local_roots = local_socrata_city_roots or {}
    city_specs: list[tuple[str, Path | None, str, str]] = [
        ("chicago", chicago_manifest_path, "chicago", "Chicago"),
        ("lacity", lacity_manifest_path, "lacity", "Los Angeles"),
        ("seattle", seattle_manifest_path, "seattle", "Seattle"),
        ("boston", boston_manifest_path, "boston", "Boston"),
        ("sf", sf_manifest_path, "sf", "San Francisco"),
        ("phoenix", phoenix_manifest_path, "phoenix", "Phoenix"),
    ]
    for city_key, manifest, city_id, city_label in city_specs:
        root = local_roots.get(city_key)
        if root is not None and root.is_dir():
            profiles.extend(
                build_profiles_from_local_directory(
                    root,
                    city_id,
                    city_label,
                    domain=domain_by_city.get(city_key, ""),
                )
            )
        elif manifest and manifest.exists():
            profiles.extend(build_profiles_from_socrata_manifest(manifest, city_id, city_label))

    classify_scenarios(profiles)
    scenario_clusters = build_scenario_clusters(profiles)
    category_clusters = build_category_clusters(profiles)
    table_mapping = build_table_category_scenario_mapping(profiles)
    unified_inventory = build_unified_inventory(profiles)
    hierarchy_map = build_dataset_hierarchy_map(profiles)
    quality_report, review_queue = build_quality_report(profiles)

    write_json(artifacts_dir / "table_catalog.json", [profile_to_record(profile) for profile in profiles])
    write_csv_catalog(artifacts_dir / "table_catalog.csv", profiles)
    write_json(artifacts_dir / "scenario_clusters.json", scenario_clusters)
    write_json(artifacts_dir / "category_clusters.json", category_clusters)
    write_json(artifacts_dir / "table_category_scenario_mapping.json", table_mapping)
    write_json(artifacts_dir / "unified_inventory.json", unified_inventory)
    write_unified_inventory_csv(artifacts_dir / "unified_inventory.csv", unified_inventory)
    write_json(artifacts_dir / "dataset_hierarchy_map.json", hierarchy_map)
    write_dataset_hierarchy_csv(artifacts_dir / "dataset_hierarchy_map.csv", hierarchy_map)
    write_json(artifacts_dir / "classification_quality_report.json", quality_report)
    write_json(artifacts_dir / "review_queue.json", review_queue)
    (artifacts_dir / "clustering_summary.md").write_text(
        build_clustering_summary_markdown(profiles, scenario_clusters, category_clusters),
        encoding="utf-8",
    )

    return {
        "n_tables": len(profiles),
        "n_clusters": len(scenario_clusters),
        "n_category_clusters": len(category_clusters),
        "city_distribution": dict(Counter(p.city for p in profiles)),
        "table_catalog": str((artifacts_dir / "table_catalog.json").resolve()),
        "scenario_clusters": str((artifacts_dir / "scenario_clusters.json").resolve()),
        "category_clusters": str((artifacts_dir / "category_clusters.json").resolve()),
        "table_category_scenario_mapping": str((artifacts_dir / "table_category_scenario_mapping.json").resolve()),
        "unified_inventory": str((artifacts_dir / "unified_inventory.json").resolve()),
        "dataset_hierarchy_map": str((artifacts_dir / "dataset_hierarchy_map.json").resolve()),
        "classification_quality_report": str((artifacts_dir / "classification_quality_report.json").resolve()),
        "review_queue": str((artifacts_dir / "review_queue.json").resolve()),
    }
