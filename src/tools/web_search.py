from __future__ import annotations

import json
import os
from typing import Dict, List, Optional
from urllib.parse import urlencode
from urllib.request import urlopen
from urllib.error import HTTPError, URLError


class GoogleWebSearcher:
    """Google Web searcher based on Google Custom Search JSON API."""

    BASE_URL = "https://www.googleapis.com/customsearch/v1"

    def __init__(
        self,
        api_key: Optional[str] = None,
        cse_id: Optional[str] = None,
        timeout: int = 15,
    ) -> None:
        self.api_key = api_key or os.getenv("GOOGLE_API_KEY")
        self.cse_id = cse_id or os.getenv("GOOGLE_CSE_ID")
        self.timeout = int(timeout)

        if not self.api_key:
            raise ValueError("Google API key is required. Set GOOGLE_API_KEY or pass api_key.")
        if not self.cse_id:
            raise ValueError("Google CSE ID is required. Set GOOGLE_CSE_ID or pass cse_id.")

    def search(
        self,
        query: str,
        top_k: int = 10,
        start: int = 1,
        language: Optional[str] = None,
    ) -> List[Dict]:
        q = str(query or "").strip()
        if not q:
            raise ValueError("query must not be empty.")

        n = max(1, min(int(top_k), 10))
        st = max(1, int(start))

        params = {
            "key": self.api_key,
            "cx": self.cse_id,
            "q": q,
            "num": n,
            "start": st,
        }
        if language:
            params["lr"] = f"lang_{language}"

        url = f"{self.BASE_URL}?{urlencode(params)}"
        payload = self._get_json(url)
        items = payload.get("items", []) or []

        results: List[Dict] = []
        for item in items:
            results.append(
                {
                    "title": item.get("title"),
                    "link": item.get("link"),
                    "snippet": item.get("snippet"),
                    "display_link": item.get("displayLink"),
                }
            )
        return results

    def _get_json(self, url: str) -> Dict:
        try:
            with urlopen(url, timeout=self.timeout) as resp:
                body = resp.read().decode("utf-8")
            return json.loads(body)
        except HTTPError as exc:
            detail = exc.read().decode("utf-8", errors="ignore")
            raise RuntimeError(f"Google search HTTP error: {exc.code} {detail}") from exc
        except URLError as exc:
            raise RuntimeError(f"Google search connection error: {exc}") from exc
