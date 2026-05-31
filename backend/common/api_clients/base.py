"""Base API client."""
import requests
from typing import Optional, Dict, Any
import time


class BaseAPIClient:
    """Base class for external API clients."""

    def __init__(self, api_key: Optional[str] = None, base_url: str = ""):
        self.api_key = api_key
        self.base_url = base_url
        self.session = requests.Session()

    def _get_headers(self) -> Dict[str, str]:
        """Get headers for API requests."""
        headers = {"Content-Type": "application/json"}
        if self.api_key:
            headers["Authorization"] = f"Bearer {self.api_key}"
        return headers

    def _request(
        self,
        method: str,
        endpoint: str,
        params: Optional[Dict] = None,
        data: Optional[Dict] = None,
        retry: int = 3,
    ) -> Any:
        """Make API request with retry logic."""
        url = f"{self.base_url}{endpoint}"

        for attempt in range(retry):
            try:
                response = self.session.request(
                    method=method,
                    url=url,
                    headers=self._get_headers(),
                    params=params,
                    json=data,
                    timeout=30,
                )
                response.raise_for_status()
                return response.json()

            except requests.exceptions.RequestException as e:
                if attempt == retry - 1:
                    raise
                time.sleep(2 ** attempt)

    def get(self, endpoint: str, params: Optional[Dict] = None) -> Any:
        """GET request."""
        return self._request("GET", endpoint, params=params)

    def post(self, endpoint: str, data: Optional[Dict] = None) -> Any:
        """POST request."""
        return self._request("POST", endpoint, data=data)
