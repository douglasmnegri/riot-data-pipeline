import os
from typing import Optional

from dotenv import load_dotenv

load_dotenv()


def get_rank_entries(
    queue: str,
    tier: str,
    division: str,
    page: Optional[int] = None,
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


def get_player_champion_data(player_puuid: str) -> str:
    """Construct the endpoint URL for fetching player data
    using summoner-v4.
    """
    base_url = os.getenv("BASE_URL")
    if not base_url:
        raise ValueError("BASE_URL environment variable is not set.")

    url = (
        f"{base_url}/lol/champion-mastery/v4/champion-masteries/by-puuid/{player_puuid}"
    )

    return url
