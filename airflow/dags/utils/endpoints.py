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


def get_player_challenges(player_puuid: str) -> str:
    """Construct the endpoint URL for fetching player challenges
    using challenges-v1.
    """
    base_url = os.getenv("BASE_URL")
    if not base_url:
        raise ValueError("BASE_URL environment variable is not set.")

    url = f"{base_url}/lol/challenges/v1/player-data/{player_puuid}"

    return url


def get_tft_grandmaster_leaderboard() -> str:
    """Construct the endpoint URL for fetching TFT grandmaster leaderboard
    using league-v1.
    """
    base_url = os.getenv("BASE_URL")
    if not base_url:
        raise ValueError("BASE_URL environment variable is not set.")

    return f"{base_url}/tft/league/v1/grandmaster"


def get_tft_challenger_leaderboard() -> str:
    """Construct the endpoint URL for fetching TFT challenger leaderboard
    using league-v1.
    """
    base_url = os.getenv("BASE_URL")
    if not base_url:
        raise ValueError("BASE_URL environment variable is not set.")

    return f"{base_url}/tft/league/v1/challenger"


def get_tft_entries_by_puuid(puuid: str) -> str:
    """Construct the endpoint URL for fetching TFT entries by PUUID
    using league-v1.
    """
    base_url = os.getenv("BASE_URL")
    if not base_url:
        raise ValueError("BASE_URL environment variable is not set.")

    return f"{base_url}/tft/league/v1/by-puuid/{puuid}"


def get_tft_match_by_puuid(puuid: str) -> str:
    """Construct the endpoint URL for fetching TFT match history by PUUID
    using match-v5.
    """
    base_url = os.getenv("BASE_URL_AMERICAS")
    if not base_url:
        raise ValueError("BASE_URL_AMERICAS environment variable is not set.")

    return f"{base_url}/tft/match/v1/matches/by-puuid/{puuid}/ids"
