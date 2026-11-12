from __future__ import annotations

import hashlib
import json
import os
import tempfile
import urllib.error
import urllib.request
from dataclasses import dataclass
from pathlib import Path
from typing import Any


@dataclass(frozen=True)
class SourceBatch:
    source: str
    records: list[Any]
    raw_body: bytes
    content_type: str


class FixtureSource:
    """Reads deterministic local JSON input without applying domain rules."""

    def fetch(self, path: str) -> SourceBatch:
        raw_body = Path(path).read_bytes()
        value = json.loads(raw_body.decode("utf-8"))
        if not isinstance(value, list):
            raise ValueError("fixture must contain a JSON list")
        return SourceBatch(f"fixture://{Path(path).name}", value, raw_body, "application/json")


class HttpJsonSource:
    """Fetches JSON records and keeps authentication outside source code."""

    def __init__(self, url: str, timeout: float = 30.0, records_key: str | None = None) -> None:
        if not url.startswith(("http://", "https://")):
            raise ValueError("url must use http:// or https://")
        if timeout <= 0:
            raise ValueError("timeout must be positive")
        self.url = url
        self.timeout = timeout
        self.records_key = records_key

    def _headers(self) -> dict[str, str]:
        headers = {"Accept": "application/json", "User-Agent": "data-engineering-adapter/0.1"}
        token = os.getenv("DATA_SOURCE_BEARER_TOKEN")
        api_key = os.getenv("DATA_SOURCE_API_KEY")
        if token:
            headers["Authorization"] = f"Bearer {token}"
        if api_key:
            headers["X-API-Key"] = api_key
        return headers

    def fetch(self) -> SourceBatch:
        request = urllib.request.Request(self.url, headers=self._headers(), method="GET")
        try:
            with urllib.request.urlopen(request, timeout=self.timeout) as response:
                raw_body = response.read()
                content_type = response.headers.get("Content-Type", "application/json")
        except urllib.error.HTTPError as exc:
            raise RuntimeError(f"source returned HTTP {exc.code}") from exc
        except urllib.error.URLError as exc:
            raise RuntimeError(f"source request failed: {exc.reason}") from exc
        try:
            value = json.loads(raw_body.decode("utf-8"))
        except (UnicodeDecodeError, json.JSONDecodeError) as exc:
            raise ValueError("source response is not valid UTF-8 JSON") from exc
        selected = value
        if self.records_key:
            for part in self.records_key.split("."):
                if not isinstance(selected, dict) or part not in selected:
                    raise ValueError(f"records key not found: {self.records_key}")
                selected = selected[part]
        elif isinstance(selected, dict):
            for key in ("records", "data", "items", "results"):
                if isinstance(selected.get(key), list):
                    selected = selected[key]
                    break
        if not isinstance(selected, list):
            raise ValueError("source response must contain a JSON list of records")
        return SourceBatch(self.url, selected, raw_body, content_type)


class RawLanding:
    """Stores immutable content-addressed source responses when configured."""

    def __init__(self, directory: str | None) -> None:
        self.directory = Path(directory) if directory else None

    def save(self, batch: SourceBatch) -> tuple[str, str | None]:
        digest = hashlib.sha256(batch.raw_body).hexdigest()
        if self.directory is None:
            return digest, None
        self.directory.mkdir(parents=True, exist_ok=True)
        target = self.directory / f"{digest}.json"
        if not target.exists():
            with tempfile.NamedTemporaryFile(mode="wb", dir=self.directory, delete=False, prefix=f".{digest}.") as handle:
                temporary = Path(handle.name)
                handle.write(batch.raw_body)
            temporary.replace(target)
        return digest, str(target)
