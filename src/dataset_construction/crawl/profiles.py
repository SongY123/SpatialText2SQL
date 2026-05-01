"""City portal configuration for the unified open-data map crawler."""
from __future__ import annotations

from dataclasses import dataclass
from typing import Literal


PortalType = Literal["socrata", "ckan"]


@dataclass(frozen=True, slots=True)
class CityProfile:
    city_id: str
    label: str
    portal_type: PortalType
    output_dir_name: str
    browse_url: str
    domain: str = ""
    base_url: str = ""
    ckan_fq: str = ""
    skip_dataset_names: tuple[str, ...] = ()


CITY_PROFILES: dict[str, CityProfile] = {
    "nyc": CityProfile(
        city_id="nyc",
        label="New York City",
        portal_type="socrata",
        domain="data.cityofnewyork.us",
        output_dir_name="new_york_city",
        browse_url="https://data.cityofnewyork.us/browse?limitTo=maps",
    ),
    "lacity": CityProfile(
        city_id="lacity",
        label="Los Angeles",
        portal_type="socrata",
        domain="data.lacity.org",
        output_dir_name="los_angeles",
        browse_url="https://data.lacity.org/browse?sortBy=relevance&pageSize=20&limitTo=maps",
    ),
    "chicago": CityProfile(
        city_id="chicago",
        label="Chicago",
        portal_type="socrata",
        domain="data.cityofchicago.org",
        output_dir_name="chicago",
        browse_url="https://data.cityofchicago.org/browse?limitTo=maps&sortBy=relevance",
    ),
    "seattle": CityProfile(
        city_id="seattle",
        label="Seattle",
        portal_type="socrata",
        domain="data.seattle.gov",
        output_dir_name="seattle",
        browse_url="https://data.seattle.gov/browse?limitTo=maps",
    ),
    "sf": CityProfile(
        city_id="sf",
        label="San Francisco",
        portal_type="socrata",
        domain="data.sfgov.org",
        output_dir_name="san_francisco",
        browse_url="https://data.sfgov.org/browse?limitTo=maps",
    ),
    "boston": CityProfile(
        city_id="boston",
        label="Boston",
        portal_type="ckan",
        base_url="https://data.boston.gov",
        output_dir_name="boston",
        browse_url="https://data.boston.gov/dataset/",
        ckan_fq="organization:boston-maps",
        skip_dataset_names=("2011 Contours- 1ft",),
    ),
    "phoenix": CityProfile(
        city_id="phoenix",
        label="Phoenix",
        portal_type="ckan",
        base_url="https://www.phoenixopendata.com",
        output_dir_name="phoenix",
        browse_url="https://www.phoenixopendata.com/dataset/",
        ckan_fq="groups:mapping",
    ),
}


DEFAULT_CITY_ORDER = ("nyc", "lacity", "chicago", "seattle", "sf", "boston", "phoenix")


def _normalize_dataset_name(value: str) -> str:
    return " ".join(value.strip().casefold().split())


def should_skip_dataset_name(profile: CityProfile, dataset_name: str) -> bool:
    """Return True when a dataset name matches the city's skip list."""
    normalized_name = _normalize_dataset_name(dataset_name)
    if not normalized_name:
        return False
    return normalized_name in {_normalize_dataset_name(name) for name in profile.skip_dataset_names}


def parse_city_list(value: str) -> list[CityProfile]:
    """Resolve a comma-separated city list into profiles."""
    if not value.strip() or value.strip().lower() == "all":
        return [CITY_PROFILES[city_id] for city_id in DEFAULT_CITY_ORDER]

    profiles: list[CityProfile] = []
    unknown: list[str] = []
    for raw_city in value.split(","):
        city_id = raw_city.strip().lower()
        if not city_id:
            continue
        profile = CITY_PROFILES.get(city_id)
        if profile is None:
            unknown.append(city_id)
        else:
            profiles.append(profile)

    if unknown:
        choices = ", ".join(DEFAULT_CITY_ORDER)
        raise ValueError(f"Unknown city id(s): {', '.join(unknown)}. Choices: {choices}.")
    return profiles
