"""Client HTTP JSON commun : timeout, retries avec backoff exponentiel, rate limit."""

import time

import httpx

from integrations.errors import NotFound, RateLimited, UpstreamError
from integrations.ratelimit import NoBudget

RETRYABLE_STATUSES = {429, 500, 502, 503, 504}


class JsonHttpClient:
    def __init__(
        self,
        base_url: str,
        *,
        limiter,
        budget=None,
        timeout: float = 15,
        max_retries: int = 3,
        headers: dict | None = None,
        auth: tuple[str, str] | None = None,
        backoff_seconds: float = 1.0,
        sleep=time.sleep,
    ):
        self._client = httpx.Client(base_url=base_url, timeout=timeout, headers=headers, auth=auth)
        self._limiter = limiter
        self._budget = budget or NoBudget()
        self._max_retries = max_retries
        self._backoff = backoff_seconds
        self._sleep = sleep

    def get(self, path: str, params: dict | None = None) -> dict | list:
        return self._request("GET", path, params=params)

    def post(self, path: str, json: dict) -> dict | list:
        return self._request("POST", path, json=json)

    def _request(self, method: str, path: str, **kwargs) -> dict | list:
        attempt = 0
        while True:
            self._budget.consume()
            self._limiter.acquire()
            status: int | None = None
            error: Exception | None = None
            try:
                response = self._client.request(method, path, **kwargs)
            except httpx.TransportError as exc:
                error = exc
            else:
                status = response.status_code
                if status == 404:
                    raise NotFound(f"{method} {path} : introuvable")
                if status < 400:
                    return response.json()
                if status not in RETRYABLE_STATUSES:
                    raise UpstreamError(f"{method} {path} : HTTP {status}")
            if attempt >= self._max_retries:
                if status == 429:
                    raise RateLimited(f"{method} {path} : HTTP 429")
                raise UpstreamError(f"{method} {path} : {status or error}")
            self._sleep(self._backoff * 2**attempt)
            attempt += 1
