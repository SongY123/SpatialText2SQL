from __future__ import annotations

from dataclasses import dataclass
from typing import Pattern
import re


@dataclass(frozen=True)
class ThemeConfig:
    id: str
    label: str
    keywords: tuple[str, ...]
    column_keywords: tuple[str, ...]
    patterns: tuple[str, ...]
    join_key_bonus: tuple[str, ...] = ()


@dataclass(frozen=True)
class ScenarioPrototype:
    id: str
    label: str
    description: str
    preferred_themes: tuple[str, ...]
    keywords: tuple[str, ...]
    join_keys: tuple[str, ...]
    geometry_preferences: tuple[str, ...]


@dataclass(frozen=True)
class CategoryConfig:
    """UN-GGIM / ISO 19115-1 aligned first-level category (Table 2 style)."""

    id: str
    label: str
    keywords: tuple[str, ...]
    column_keywords: tuple[str, ...]
    patterns: tuple[str, ...]
    join_key_bonus: tuple[str, ...] = ()
    preferred_themes: tuple[str, ...] = ()


STOPWORDS = {
    "a", "about", "all", "an", "and", "any", "app", "apple", "are", "archive",
    "area", "areas", "available", "based", "been", "between", "big", "boundaries",
    "boundary", "bytes", "can", "city", "cityofnewyork", "click", "contains", "csv",
    "data", "dataset", "datasets", "description", "download", "each", "for", "from",
    "gis", "have", "href", "html", "https", "index", "information", "into", "is",
    "it", "its", "layer", "layers", "link", "links", "map", "maps", "metadata",
    "nyc", "new", "open", "opendata", "page", "please", "previously", "released",
    "resource", "rows", "see", "set", "shape", "shapefile", "site", "sorts", "table",
    "tables", "that", "the", "their", "these", "this", "through", "url", "used",
    "using", "versions", "view", "with", "within", "year", "york",
}


JOIN_KEY_PATTERNS: dict[str, Pattern[str]] = {
    "borough": re.compile(r"\bboro(ugh|code|name)?\b|borough", re.I),
    "community_district": re.compile(r"community.?district|boro.?cd|borocd|cdta", re.I),
    "census_tract": re.compile(r"census.?tract|tractce|tract\b", re.I),
    "census_block": re.compile(r"census.?block|blockce|block\b", re.I),
    "nta": re.compile(r"\bnta\b|neighborhood.?tabulation", re.I),
    "puma": re.compile(r"\bpuma\b|public.?use.?microdata", re.I),
    "zip": re.compile(r"\bzip\b|modzcta|zcta", re.I),
    "council_district": re.compile(r"coun.?dist|city.?council", re.I),
    "assembly_district": re.compile(r"assem(dist|bly)|state.?assembly", re.I),
    "senate_district": re.compile(r"st.?sen(dist)?|senate", re.I),
    "congressional_district": re.compile(r"cong(dist|ress)", re.I),
    "election_district": re.compile(r"election.?district", re.I),
    "health_district": re.compile(r"health.?center.?district", re.I),
    "school_zone": re.compile(r"school.?zone", re.I),
    "address": re.compile(r"address|house.?number|street.?name|full.?street", re.I),
    "bin": re.compile(r"\bbin\b|building.?ident", re.I),
    "bbl": re.compile(r"\bbbl\b|tax.?lot|borough.?block.?lot", re.I),
}


SPATIAL_COLUMN_HINTS = (
    "the_geom", "geom", "geometry", "shape", "wkt", "latitude", "longitude", "lat",
    "lon", "point_x", "point_y", "x_coord", "y_coord",
)


THEME_TAXONOMY = (
    ThemeConfig(
        id="transportation",
        label="transportation",
        keywords=("bike", "bicycle", "bus", "carshare", "crossing", "mobility", "parking", "pedestrian", "plaza", "route", "sidewalk", "signal", "street", "taxi", "traffic", "transit", "truck", "vision zero", "walk"),
        column_keywords=("corridor", "intersection", "route", "segment", "signal", "street"),
        patterns=(r"bike|bicycle|pedestrian|truck|bus|parking|meter|lane|corridor|walknyc|street seat|slow zone|fhv|taxi",),
        join_key_bonus=("borough", "community_district", "council_district"),
    ),
    ThemeConfig(
        id="public_safety",
        label="public safety",
        keywords=("alarm", "battalion", "evacuation", "fire", "hazard", "hurricane", "inundation", "police", "priority", "psa", "rescue", "safety", "security"),
        column_keywords=("battalion", "company", "hazard", "priority", "zone"),
        patterns=(r"fire|alarm|police|evacuation|hurricane|inundation|hazard|emergency",),
        join_key_bonus=("borough", "community_district"),
    ),
    ThemeConfig(
        id="education",
        label="education",
        keywords=("adult education", "continuing education", "education", "pre-k", "school", "schoolyard", "student", "universal pre-k"),
        column_keywords=("school", "grade", "student", "zone"),
        patterns=(r"school|education|pre.?k|student|schoolyard",),
        join_key_bonus=("school_zone", "borough"),
    ),
    ThemeConfig(
        id="healthcare",
        label="healthcare",
        keywords=("aed", "automated external defibrillator", "clinic", "health", "hospital", "medical", "pharmaceutical", "syringe", "wellness"),
        column_keywords=("health", "medical", "pharmacy"),
        patterns=(r"health|hospital|aed|medical|pharmaceutical|syringe",),
        join_key_bonus=("health_district", "borough", "zip"),
    ),
    ThemeConfig(
        id="environment",
        label="environment",
        keywords=("basin", "beach", "catch", "climate", "ecological", "flood", "forest", "green", "habitat", "high water", "hydrography", "infrastructure", "inundation", "outfall", "resiliency", "sea level", "shoreline", "squirrel", "stormwater", "tree", "waterfront", "wetland"),
        column_keywords=("canopy", "flood", "habitat", "tree", "water"),
        patterns=(r"tree|forest|wetland|hydrography|flood|sea.?level|waterfront|storm|green infrastructure|basin|outfall|shoreline",),
        join_key_bonus=("borough", "census_tract", "nta"),
    ),
    ThemeConfig(
        id="urban_infrastructure",
        label="urban infrastructure",
        keywords=("address", "asset", "basemap", "building elevation", "capital project", "centerline", "curb", "hydrant", "lead service", "outfall", "pavement", "planimetric", "roadbed", "sidewalk", "structure", "utility"),
        column_keywords=("asset", "bin", "bbl", "centerline", "hydrant", "utility"),
        patterns=(r"addresspoint|centerline|hydrant|catch basin|lead service|capital project|structure|roadbed|curb|pavement|digital city map",),
        join_key_bonus=("borough", "address", "bin", "bbl"),
    ),
    ThemeConfig(
        id="demographics_boundaries",
        label="demographics / boundaries",
        keywords=("assembly district", "borough boundary", "boundary", "cdta", "census", "community district", "congressional district", "district", "election district", "modzcta", "nta", "puma", "senate district", "tabulation area", "tract"),
        column_keywords=("district", "population", "tract", "zcta"),
        patterns=(r"census|district|boundary|puma|nta|modzcta|borough boundaries|community district|tabulation",),
        join_key_bonus=("borough", "community_district", "census_tract", "census_block", "nta", "puma", "zip", "council_district"),
    ),
    ThemeConfig(
        id="business_economy",
        label="business / economy",
        keywords=("business", "commercial", "district", "economy", "food store", "franchise", "market", "newsstand", "permit", "retail", "storefront", "vendor", "waste zone"),
        column_keywords=("business", "commercial", "permit", "vendor"),
        patterns=(r"business improvement|commercial|storefront|vendor|food store|newsstand|franchise",),
        join_key_bonus=("borough", "community_district", "zip"),
    ),
    ThemeConfig(
        id="poi_public_facilities",
        label="POI / public facilities",
        keywords=("amenity", "bench", "botanical", "facility", "fountain", "garden", "golf", "kayak", "library", "linknyc", "park", "play area", "plaza", "pool", "post office", "public toilet", "restroom", "seating", "toilet", "trail"),
        column_keywords=("facility", "park", "playground", "restroom", "trail"),
        patterns=(r"library|park|play area|playground|restroom|toilet|fountain|pool|golf|beach|botanical|trail|linknyc|bench|post office",),
        join_key_bonus=("borough", "community_district", "nta"),
    ),
    ThemeConfig(
        id="housing_land_use",
        label="housing / land use",
        keywords=("affordable", "brownfield", "building", "condo", "footprint", "historic district", "housing", "inclusionary", "land use", "landmark", "lot", "nycha", "parcel", "planning", "tax lot", "waterfront access plan", "zoning"),
        column_keywords=("bbl", "bin", "lot", "parcel", "zoning"),
        patterns=(r"housing|nycha|zoning|landmark|building footprint|tax lot|parcel|planning|inclusionary|brownfield|air lot|sub lot|condo",),
        join_key_bonus=("borough", "bbl", "bin", "community_district", "census_tract", "nta"),
    ),
)


SCENARIO_PROTOTYPES = (
    ScenarioPrototype(
        id="traffic_mobility",
        label="urban traffic management",
        description="Street operations, corridor performance, curb assets, and multimodal movement.",
        preferred_themes=("transportation", "urban_infrastructure", "business_economy"),
        keywords=("bike", "bicycle", "bus", "carshare", "corridor", "intersection", "lane", "meter", "mobility", "parking", "pedestrian", "signal", "street", "traffic", "truck", "walk"),
        join_keys=("borough", "community_district", "nta", "council_district"),
        geometry_preferences=("POINT", "LINESTRING", "MULTILINESTRING", "POLYGON"),
    ),
    ScenarioPrototype(
        id="emergency_response",
        label="emergency response",
        description="Public safety assets, service areas, hazard zones, and emergency access.",
        preferred_themes=("public_safety", "healthcare", "environment", "urban_infrastructure"),
        keywords=("aed", "alarm", "battalion", "emergency", "evacuation", "fire", "flood", "hazard", "hurricane", "hydrant", "inundation", "police", "psa", "rescue"),
        join_keys=("borough", "community_district", "health_district", "zip"),
        geometry_preferences=("POINT", "POLYGON", "MULTIPOLYGON"),
    ),
    ScenarioPrototype(
        id="public_service_accessibility",
        label="public service accessibility",
        description="Access to libraries, schools, health assets, communications infrastructure, and civic amenities.",
        preferred_themes=("poi_public_facilities", "education", "healthcare"),
        keywords=("aed", "drinking fountain", "facility", "health", "library", "linknyc", "post office", "public", "restroom", "school", "seating", "service", "toilet"),
        join_keys=("borough", "community_district", "nta", "zip", "school_zone"),
        geometry_preferences=("POINT", "POLYGON"),
    ),
    ScenarioPrototype(
        id="environmental_resilience",
        label="environmental monitoring",
        description="Ecological assets, green infrastructure, flood risk, and climate resilience analysis.",
        preferred_themes=("environment", "urban_infrastructure"),
        keywords=("basin", "catch", "flood", "forest", "green", "hydrography", "infrastructure", "outfall", "resiliency", "sea level", "sensor", "shoreline", "tree", "waterfront", "wetland"),
        join_keys=("borough", "census_tract", "nta", "community_district"),
        geometry_preferences=("POINT", "POLYGON", "MULTIPOLYGON"),
    ),
    ScenarioPrototype(
        id="urban_planning_land_use",
        label="urban planning",
        description="Zoning, parcels, landmarks, capital projects, and land-use designations.",
        preferred_themes=("housing_land_use", "urban_infrastructure", "demographics_boundaries"),
        keywords=("bbl", "bin", "brownfield", "building", "capital project", "landmark", "lot", "parcel", "planning", "project", "tax", "waterfront access", "zoning"),
        join_keys=("borough", "community_district", "bbl", "bin", "census_tract"),
        geometry_preferences=("POLYGON", "MULTIPOLYGON", "POINT"),
    ),
    ScenarioPrototype(
        id="housing_demographics",
        label="housing and neighborhood equity",
        description="Housing supply, NYCHA developments, census geographies, and neighborhood-level analysis.",
        preferred_themes=("housing_land_use", "demographics_boundaries", "education"),
        keywords=("affordable", "cdta", "census", "community district", "development", "housing", "inclusionary", "modzcta", "neighborhood", "nta", "nycha", "puma", "tract"),
        join_keys=("borough", "community_district", "census_tract", "nta", "puma", "zip"),
        geometry_preferences=("POINT", "POLYGON", "MULTIPOLYGON"),
    ),
    ScenarioPrototype(
        id="parks_recreation_poi",
        label="tourism / POI recommendation",
        description="Parks, recreation facilities, leisure assets, and waterfront access points.",
        preferred_themes=("poi_public_facilities", "environment"),
        keywords=("access", "athletic", "beach", "canoe", "facility", "fishing", "golf", "kayak", "park", "pool", "recreation", "rink", "trail", "waterfront"),
        join_keys=("borough", "community_district", "nta", "zip"),
        geometry_preferences=("POINT", "POLYGON", "LINESTRING", "MULTILINESTRING"),
    ),
)


# ---------------------------------------------------------------------------
# UN-GGIM first-level categories (14 global fundamental geospatial themes).
# ids use ggim_1..ggim_14 for stable downstream processing.
# ---------------------------------------------------------------------------
CATEGORY_TAXONOMY: tuple[CategoryConfig, ...] = (
    CategoryConfig(
        id="ggim_1",
        label="Global Geodetic Reference Frame",
        keywords=(
            "crs",
            "epsg",
            "projection",
            "coordinate reference system",
            "spatial reference",
            "datum",
            "geodetic",
            "wgs84",
        ),
        column_keywords=("crs", "srid", "epsg", "projection", "datum"),
        patterns=(
            r"\bepsg\b|coordinate.?reference|spatial.?reference|projection|geodetic|datum|wgs.?84|srid",
        ),
        join_key_bonus=(),
        preferred_themes=("urban_infrastructure",),
    ),
    CategoryConfig(
        id="ggim_2",
        label="Addresses",
        keywords=(
            "address",
            "house number",
            "street name",
            "postal address",
            "geocode",
            "zip code",
            "location point",
        ),
        column_keywords=("address", "house_number", "street", "zip", "postcode", "latitude", "longitude"),
        patterns=(
            r"\baddress\b|house.?number|street.?name|postal|geocode|zip|postcode",
        ),
        join_key_bonus=("address", "zip", "borough"),
        preferred_themes=("urban_infrastructure",),
    ),
    CategoryConfig(
        id="ggim_3",
        label="Buildings and Settlements",
        keywords=(
            "building",
            "settlement",
            "residential",
            "neighborhood",
            "community",
            "housing development",
            "footprint",
        ),
        column_keywords=("building", "bin", "footprint", "residential", "settlement"),
        patterns=(r"building|settlement|residential|housing.?development|footprint|community",),
        join_key_bonus=("borough", "bin", "community_district", "census_tract", "nta"),
        preferred_themes=("housing_land_use", "demographics_boundaries"),
    ),
    CategoryConfig(
        id="ggim_4",
        label="Elevation and Depth",
        keywords=(
            "elevation",
            "depth",
            "terrain",
            "contour",
            "dem",
            "bathymetry",
            "topography",
        ),
        column_keywords=("elevation", "depth", "z", "dem", "contour"),
        patterns=(r"elevation|depth|terrain|contour|dem|bathymetr|topograph",),
        join_key_bonus=("borough", "census_tract"),
        preferred_themes=("environment",),
    ),
    CategoryConfig(
        id="ggim_5",
        label="Functional Areas",
        keywords=(
            "district",
            "service area",
            "administrative area",
            "planning area",
            "school zone",
            "police precinct",
            "election district",
        ),
        column_keywords=("district", "zone", "boundary", "area", "precinct"),
        patterns=(r"functional.?area|service.?area|administrative|district|zone|precinct|planning.?area",),
        join_key_bonus=("borough", "community_district", "council_district", "school_zone", "zip"),
        preferred_themes=("demographics_boundaries", "public_safety"),
    ),
    CategoryConfig(
        id="ggim_6",
        label="Geographical Names",
        keywords=(
            "place name",
            "geographical name",
            "street name",
            "poi name",
            "name index",
            "toponym",
        ),
        column_keywords=("name", "place", "street", "poi"),
        patterns=(r"geographic.?name|place.?name|toponym|street.?name|poi.?name|name.?index",),
        join_key_bonus=("address", "borough"),
        preferred_themes=("poi_public_facilities", "urban_infrastructure"),
    ),
    CategoryConfig(
        id="ggim_7",
        label="Geology and Soils",
        keywords=(
            "geology",
            "soil",
            "sediment",
            "rock",
            "aquifer",
            "soil type",
            "geotechnical",
        ),
        column_keywords=("soil", "geology", "sediment", "rock", "aquifer"),
        patterns=(r"geolog|soil|sediment|rock|aquifer|geotechnical",),
        join_key_bonus=("borough", "census_tract"),
        preferred_themes=("environment",),
    ),
    CategoryConfig(
        id="ggim_8",
        label="Land Cover and Land Use",
        keywords=(
            "land cover",
            "land use",
            "vegetation",
            "impervious",
            "waterbody",
            "zoning",
            "classification",
        ),
        column_keywords=("land_use", "land_cover", "class", "zoning", "vegetation"),
        patterns=(r"land.?cover|land.?use|impervious|vegetation|waterbody|zoning",),
        join_key_bonus=("borough", "bbl", "census_tract"),
        preferred_themes=("housing_land_use", "environment"),
    ),
    CategoryConfig(
        id="ggim_9",
        label="Land Parcels",
        keywords=("parcel", "cadastre", "tax lot", "property boundary", "lot"),
        column_keywords=("parcel", "bbl", "lot", "block"),
        patterns=(r"parcel|cadastre|tax.?lot|property.?boundary|borough.?block.?lot|lot",),
        join_key_bonus=("bbl", "borough", "bin"),
        preferred_themes=("housing_land_use",),
    ),
    CategoryConfig(
        id="ggim_10",
        label="Orthoimagery",
        keywords=("orthoimagery", "orthophoto", "aerial", "satellite", "imagery", "raster", "lidar"),
        column_keywords=("raster", "pixel", "tile", "imagery", "ortho"),
        patterns=(r"ortho|orthophoto|aerial|satellite|imagery|raster|lidar",),
        join_key_bonus=("borough",),
        preferred_themes=("environment", "urban_infrastructure"),
    ),
    CategoryConfig(
        id="ggim_11",
        label="Physical Infrastructure",
        keywords=("facility", "hospital", "school", "fire station", "utility", "public service", "infrastructure"),
        column_keywords=("facility", "infrastructure", "utility", "hospital", "school"),
        patterns=(r"facility|infrastructure|utility|hospital|school|fire.?station|public.?service",),
        join_key_bonus=("borough", "community_district", "zip"),
        preferred_themes=("poi_public_facilities", "healthcare", "education", "urban_infrastructure"),
    ),
    CategoryConfig(
        id="ggim_12",
        label="Population Distribution",
        keywords=("population", "demographic", "census", "density", "household", "neighborhood"),
        column_keywords=("population", "census", "tract", "nta", "puma", "household"),
        patterns=(r"population|demographic|census|density|household|tract|nta|puma|neighborhood",),
        join_key_bonus=("census_tract", "nta", "puma", "borough"),
        preferred_themes=("demographics_boundaries", "housing_land_use"),
    ),
    CategoryConfig(
        id="ggim_13",
        label="Transport Networks",
        keywords=("road", "transport", "route", "rail", "bus line", "street centerline", "network"),
        column_keywords=("route", "line", "segment", "street", "corridor"),
        patterns=(r"transport|road|route|rail|bus.?line|centerline|network|traffic|corridor",),
        join_key_bonus=("borough", "community_district"),
        preferred_themes=("transportation",),
    ),
    CategoryConfig(
        id="ggim_14",
        label="Water",
        keywords=("river", "lake", "shoreline", "coastline", "water", "hydrology", "watershed", "flood"),
        column_keywords=("water", "shoreline", "river", "basin", "hydro"),
        patterns=(r"river|lake|shoreline|coast|water|hydrolog|watershed|basin|flood",),
        join_key_bonus=("borough", "census_tract"),
        preferred_themes=("environment",),
    ),
)


# Each business scenario may draw from one or more GGIM categories (Table 2 bridge).
SCENARIO_TO_GGIM_CATEGORIES: dict[str, tuple[str, ...]] = {
    "traffic_mobility": ("ggim_13", "ggim_11", "ggim_5"),
    "emergency_response": ("ggim_11", "ggim_14", "ggim_5"),
    "public_service_accessibility": ("ggim_11", "ggim_2", "ggim_6"),
    "environmental_resilience": ("ggim_4", "ggim_7", "ggim_8", "ggim_14", "ggim_10"),
    "urban_planning_land_use": ("ggim_8", "ggim_9", "ggim_3", "ggim_5"),
    "housing_demographics": ("ggim_3", "ggim_12", "ggim_2"),
    "parks_recreation_poi": ("ggim_11", "ggim_14", "ggim_6", "ggim_8"),
}


DEFAULT_THEME_TO_GGIM_CATEGORY: dict[str, str] = {
    "transportation": "ggim_13",
    "public_safety": "ggim_5",
    "education": "ggim_11",
    "healthcare": "ggim_11",
    "environment": "ggim_14",
    "urban_infrastructure": "ggim_11",
    "demographics_boundaries": "ggim_5",
    "business_economy": "ggim_12",
    "poi_public_facilities": "ggim_11",
    "housing_land_use": "ggim_8",
}
