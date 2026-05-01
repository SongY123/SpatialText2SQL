"""Shared helpers for open-data map crawlers."""
from __future__ import annotations

import json
import os
import re
import time
from pathlib import Path
from typing import Any, Mapping
from urllib.error import HTTPError, URLError
from urllib.parse import urlencode, urljoin, urlsplit, urlunsplit, unquote
from urllib.request import HTTPRedirectHandler, Request, build_opener, urlopen


class CrawlError(RuntimeError):
    """Raised when a catalog or download operation cannot be completed."""


REDIRECT_STATUS_CODES = {301, 302, 303, 307, 308}
FIRST_DOWNLOAD_CHUNK_SIZE = 4096
STREAM_DOWNLOAD_CHUNK_SIZE = 128 * 1024


class _NoRedirectHandler(HTTPRedirectHandler):
    def redirect_request(self, req, fp, code, msg, headers, newurl):  # type: ignore[override]
        return None


def sanitize_filename(value: str, max_len: int = 90) -> str:
    """Create a stable filesystem-safe slug."""
    slug = re.sub(r"[^\w\s-]", "", value, flags=re.UNICODE)
    slug = re.sub(r"[-\s]+", "_", slug.strip()).strip("_").lower()
    return (slug[:max_len] or "dataset").lower()


def make_geojson_filename(name: str, dataset_id: str) -> str:
    """Build a deterministic GeoJSON filename for a downloaded dataset."""
    return f"{sanitize_filename(name)}_{dataset_id}.geojson"


def default_headers(
    *,
    accept: str = "application/geo+json,application/json,text/plain,*/*",
    include_socrata_token: bool = True,
) -> dict[str, str]:
    """Headers shared by all outbound HTTP requests."""
    headers = {
        "User-Agent": (
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
            "AppleWebKit/537.36 (KHTML, like Gecko) "
            "Chrome/124.0.0.0 Safari/537.36"
        ),
        "Accept": accept,
        "Accept-Language": "en-US,en;q=0.9",
    }
    token = os.environ.get("SOCRATA_APP_TOKEN", "").strip()
    if include_socrata_token and token:
        headers["X-App-Token"] = token
    return headers


def http_get_json(
    url: str,
    params: Mapping[str, Any] | None = None,
    *,
    headers: Mapping[str, str] | None = None,
    timeout: float = 120.0,
) -> dict[str, Any]:
    """Perform a GET request and parse a JSON object response."""
    if params:
        url = f"{url}?{urlencode(params, doseq=True)}"
    request = Request(url, headers=dict(headers or default_headers(accept="application/json,*/*")), method="GET")
    try:
        with urlopen(request, timeout=timeout) as response:
            payload = response.read()
    except (HTTPError, URLError) as exc:
        raise CrawlError(f"GET {url} failed: {exc}") from exc

    try:
        data = json.loads(payload.decode("utf-8"))
    except json.JSONDecodeError as exc:
        raise CrawlError(f"GET {url} did not return valid JSON.") from exc
    if not isinstance(data, dict):
        raise CrawlError(f"GET {url} returned unexpected JSON structure.")
    return data


def _normalize_browser_redirect_url(url: str) -> str:
    """Normalize redirect URLs the way browsers do for default ports."""
    parts = urlsplit(url)
    netloc = parts.netloc
    if parts.scheme == "https" and netloc.endswith(":443"):
        netloc = netloc[:-4]
    elif parts.scheme == "http" and netloc.endswith(":80"):
        netloc = netloc[:-3]
    return urlunsplit((parts.scheme, netloc, parts.path, parts.query, parts.fragment))


def _open_with_browser_redirects(
    url: str,
    *,
    headers: Mapping[str, str],
    timeout: float,
    max_redirects: int = 8,
):
    """Open a URL while manually normalizing redirects like a browser."""
    opener = build_opener(_NoRedirectHandler)
    current_url = url
    for _ in range(max_redirects + 1):
        request = Request(current_url, headers=dict(headers), method="GET")
        try:
            return opener.open(request, timeout=timeout)
        except HTTPError as exc:
            if exc.code not in REDIRECT_STATUS_CODES:
                raise
            location = exc.headers.get("Location")
            exc.close()
            if not location:
                raise CrawlError(f"Redirect from {current_url} did not include a Location header.")
            current_url = _normalize_browser_redirect_url(urljoin(current_url, location))
    raise CrawlError(f"Too many redirects while downloading {url}.")


def _raise_for_non_geojson_response(url: str, content_type: str, first_chunk: bytes) -> None:
    if not first_chunk:
        raise CrawlError(f"GeoJSON export {url} returned an empty body.")

    sample = first_chunk.lstrip().lower()
    if "html" in content_type or sample.startswith(b"<"):
        raise CrawlError(f"GeoJSON export {url} returned HTML instead of GeoJSON.")
    compact = re.sub(rb"\s+", b"", sample[:96])
    if compact.startswith((b'{"code"', b'{"error"', b'{"message"')):
        raise CrawlError(f"GeoJSON export {url} returned an error JSON payload.")


def _extract_filename_from_headers(headers: Mapping[str, str], default_name: str = "download") -> str:
    """Extract filename from Content-Disposition header if available."""
    disposition = headers.get("Content-Disposition", "")
    if not disposition:
        return default_name

    # Parse filename*=utf-8''encoded_name or filename="name" or filename=name
    for part in disposition.split(";"):
        part = part.strip()
        if part.startswith("filename*="):
            # RFC 5987 encoded filename
            try:
                _, encoded = part.split("=", 1)
                if "''" in encoded:
                    _, name = encoded.split("''", 1)
                    return unquote(name)
            except Exception:
                pass
        elif part.startswith("filename="):
            # Standard filename parameter
            filename = part[9:].strip('"\'')
            if filename:
                return filename
    return default_name


def download_geojson(
    url: str,
    destination: Path,
    *,
    headers: Mapping[str, str] | None = None,
    timeout: float = 300.0,
) -> tuple[int, str]:
    """Stream a GeoJSON export to disk and return (byte size, filename from server).
    
    Args:
        url: The URL to download from
        destination: Path where to save the file
        headers: HTTP headers to use
        timeout: Request timeout in seconds

    Returns:
        Tuple of (bytes_downloaded, filename_from_server)
    """
    request_headers = dict(headers or default_headers())
    try:
        with _open_with_browser_redirects(url, headers=request_headers, timeout=timeout) as response:
            total = int(response.headers.get("Content-Length") or 0)
            content_type = (response.headers.get("Content-Type") or "").lower()
            first_chunk = response.read(FIRST_DOWNLOAD_CHUNK_SIZE)
            _raise_for_non_geojson_response(url, content_type, first_chunk)

            # Extract filename from server response if available
            server_filename = _extract_filename_from_headers(response.headers, destination.name)

            destination.parent.mkdir(parents=True, exist_ok=True)
            downloaded = len(first_chunk)
            last_show = 0.0

            with destination.open("wb") as sink:
                sink.write(first_chunk)

                while True:
                    chunk = response.read(STREAM_DOWNLOAD_CHUNK_SIZE)
                    if not chunk:
                        break

                    sink.write(chunk)
                    downloaded += len(chunk)

                    now = time.time()
                    if now - last_show >= 0.35:
                        if total > 0:
                            pct = downloaded * 100.0 / total
                            print(
                                f"      -> {downloaded / 1024 / 1024:.2f}MB / "
                                f"{total / 1024 / 1024:.2f}MB ({pct:.1f}%)",
                                end="\r",
                                flush=True,
                            )
                        else:
                            print(f"      -> {downloaded / 1024 / 1024:.2f}MB", end="\r", flush=True)
                        last_show = now
            print(" " * 95, end="\r")
            return downloaded, server_filename
    except (HTTPError, URLError) as exc:
        raise CrawlError(f"GeoJSON download failed for {url}: {exc}") from exc
