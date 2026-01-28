import os


def get_rank_entries(
    queue: str,
    tier: str,
    division: str,
    page: int | None = None,
) -> str:
    """Construct the endpoint URL for fetching ranked entries
    using league-exp-v4.
    """
    base_url = os.getenv("BASE_URL")
    if not base_url:
        raise ValueError("BASE_URL environment variable is not set.")

    url = f"{base_url}/lol/league-exp/v4/entries/{queue}/{tier}/{division}"

    if page is not None:
        url += f"?page={page}"

    return url
