import os
import time
import requests


def get_json(url: str, params: dict | None = None) -> dict:
    """Fetch JSON data from Riot API with retry and basic error handling."""
    api_key = os.getenv("RIOT_API_KEY")
    if not api_key:
        raise ValueError("RIOT_API_KEY environment variable is not set.")

    headers = {
        "Accept": "application/json",
        "X-Riot-Token": api_key,
    }

    while True:
        response = requests.get(
            url,
            headers=headers,
            params=params,
            timeout=10,
        )

        if response.status_code == 401:
            raise PermissionError("Unauthorized: Check your RIOT_API_KEY.")

        if response.status_code == 403:
            raise PermissionError("Forbidden: Invalid or expired API key.")

        if response.status_code == 429:
            retry_after = int(response.headers.get("Retry-After", 1))
            time.sleep(retry_after)
            continue

        response.raise_for_status()

        return response.json()
