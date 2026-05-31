"""Dune Analytics API client."""
from django.conf import settings
from .base import BaseAPIClient
from typing import Dict, Any, List
import time


class DuneClient(BaseAPIClient):
    """Client for Dune Analytics API."""

    def __init__(self):
        api_key = settings.DUNE_API_KEY if hasattr(settings, 'DUNE_API_KEY') else None
        super().__init__(api_key=api_key, base_url="https://api.dune.com/api/v1/")

    def _get_headers(self) -> Dict[str, str]:
        """Override headers for Dune API."""
        return {
            "Content-Type": "application/json",
            "X-Dune-API-Key": self.api_key
        }

    def execute_query(self, query_id: int, params: Dict[str, Any] = None) -> str:
        """Execute a Dune query and return execution ID."""
        endpoint = f"query/{query_id}/execute"
        data = {}
        if params:
            data["query_parameters"] = params

        response = self.post(endpoint, data=data)
        return response.get('execution_id')

    def get_execution_status(self, execution_id: str) -> Dict[str, Any]:
        """Get status of a query execution."""
        endpoint = f"execution/{execution_id}/status"
        return self.get(endpoint)

    def get_execution_results(self, execution_id: str) -> List[Dict]:
        """Get results of a completed query execution."""
        endpoint = f"execution/{execution_id}/results"
        response = self.get(endpoint)
        return response.get('result', {}).get('rows', [])

    def execute_and_wait(
        self,
        query_id: int,
        params: Dict[str, Any] = None,
        max_wait: int = 300
    ) -> List[Dict]:
        """Execute query and wait for results."""
        execution_id = self.execute_query(query_id, params)

        start_time = time.time()
        while time.time() - start_time < max_wait:
            status = self.get_execution_status(execution_id)
            state = status.get('state')

            if state == 'QUERY_STATE_COMPLETED':
                return self.get_execution_results(execution_id)
            elif state == 'QUERY_STATE_FAILED':
                raise Exception(f"Query failed: {status}")

            time.sleep(5)

        raise TimeoutError(f"Query execution timed out after {max_wait}s")
