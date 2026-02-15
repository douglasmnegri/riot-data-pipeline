import json
import time
from datetime import datetime
from pathlib import Path

from utils.client import get_json
from utils.endpoints import (
    get_rank_entries,
    get_player_champion_data,
    get_player_challenges,
    get_tft_grandmaster_leaderboard,
    get_tft_challenger_leaderboard,
    get_tft_entries_by_puuid,
    get_tft_match_by_puuid,
    get_tft_match_details,
)


DATA_LOL_DIR = Path("data/raw/lol")
DATA_TFT_DIR = Path("data/raw/tft")


def extract_lol_rank_entries(
    queue: str,
    tier: str,
    division: str,
) -> str:
    all_entries = []
    page = 1

    while True:
        response = get_json(get_rank_entries(queue, tier, division, page=page))
        if not response:
            break
        all_entries.extend(response)
        page += 1

    ts = datetime.utcnow().strftime("%Y%m%d_%H%M%S")

    rank_dir = DATA_LOL_DIR / "rank"
    puuid_dir = DATA_LOL_DIR / "puuids"

    rank_dir.mkdir(parents=True, exist_ok=True)
    puuid_dir.mkdir(parents=True, exist_ok=True)

    rank_file = rank_dir / f"{queue}_{tier}_{division}_{ts}.json"
    puuid_file = puuid_dir / f"{queue}_{tier}_{division}_{ts}.json"

    puuids = sorted({entry["puuid"] for entry in all_entries})

    with rank_file.open("w", encoding="utf-8") as f:
        json.dump(all_entries, f, indent=2)

    with puuid_file.open("w", encoding="utf-8") as f:
        json.dump(puuids, f, indent=2)

    return str(puuid_file)


def extract_champion_mastery_entries(puuid_file: str) -> None:
    puuid_path = Path(puuid_file)

    with puuid_path.open("r", encoding="utf-8") as f:
        puuids: list[str] = json.load(f)

    mastery_dir = DATA_LOL_DIR / "champion_mastery"
    mastery_dir.mkdir(parents=True, exist_ok=True)

    for puuid in puuids:
        output = mastery_dir / f"{puuid}.json"

        # idempotency = Airflow retries won't re-hit API
        if output.exists():
            continue

        response = get_json(get_player_champion_data(puuid))
        if not response:
            continue

        with output.open("w", encoding="utf-8") as f:
            json.dump(response, f, indent=2)

        # Rate limiting protection (Riot API allows 100 requests every 2 minutes)
        time.sleep(2)


def extract_progressed_challenges() -> None:
    puuid_dir = DATA_LOL_DIR / "puuids"
    challenges_dir = DATA_LOL_DIR / "progressed_challenges"

    challenges_dir.mkdir(parents=True, exist_ok=True)

    # Iterate over ALL puuid snapshot files
    for puuid_file in puuid_dir.glob("*.json"):
        with puuid_file.open("r", encoding="utf-8") as f:
            puuids: list[str] = json.load(f)

        for puuid in puuids:
            output = challenges_dir / f"{puuid}.json"

            if output.exists():
                continue

            response = get_json(get_player_challenges(puuid))
            if not response:
                continue

            with output.open("w", encoding="utf-8") as f:
                json.dump(response, f, indent=2)

            # Riot rate limit protection
            time.sleep(2)


# The functions below needs to be refactored to avoid code duplication with the TFT leaderboard extraction.
def extract_tft_grandmaster_leaderboard() -> str:
    response = get_json(get_tft_grandmaster_leaderboard())
    if not response:
        return

    ts = datetime.utcnow().strftime("%Y%m%d_%H%M%S")

    rank_dir = DATA_TFT_DIR / "rank"
    puuid_dir = DATA_TFT_DIR / "puuids"

    rank_dir.mkdir(parents=True, exist_ok=True)
    puuid_dir.mkdir(parents=True, exist_ok=True)

    rank_file = rank_dir / f"tft_grandmaster_{ts}.json"
    puuid_file = puuid_dir / f"tft_grandmaster_{ts}.json"

    puuids = sorted({entry["puuid"] for entry in response["entries"]})

    with rank_file.open("w", encoding="utf-8") as f:
        json.dump(response, f, indent=2)

    with puuid_file.open("w", encoding="utf-8") as f:
        json.dump(puuids, f, indent=2)

    return str(puuid_file)


def extract_tft_challenger_leaderboard() -> str:
    response = get_json(get_tft_challenger_leaderboard())
    if not response:
        return

    ts = datetime.utcnow().strftime("%Y%m%d_%H%M%S")

    rank_dir = DATA_TFT_DIR / "rank"
    puuid_dir = DATA_TFT_DIR / "puuids"

    rank_dir.mkdir(parents=True, exist_ok=True)
    puuid_dir.mkdir(parents=True, exist_ok=True)

    rank_file = rank_dir / f"tft_challenger_{ts}.json"
    puuid_file = puuid_dir / f"tft_challenger_{ts}.json"

    puuids = sorted({entry["puuid"] for entry in response["entries"]})

    with rank_file.open("w", encoding="utf-8") as f:
        json.dump(response, f, indent=2)

    with puuid_file.open("w", encoding="utf-8") as f:
        json.dump(puuids, f, indent=2)

    return str(puuid_file)


def extract_tft_entries_by_puuid(puuid_file: str) -> None:
    puuid_path = Path(puuid_file)

    with puuid_path.open("r", encoding="utf-8") as f:
        puuids: list[str] = json.load(f)

    player_details = DATA_TFT_DIR / "player_details"
    player_details.mkdir(parents=True, exist_ok=True)

    for puuid in puuids:
        output = player_details / f"{puuid}.json"

        if output.exists():
            continue

        response = get_json(get_tft_entries_by_puuid(puuid))
        if not response:
            continue

        with output.open("w", encoding="utf-8") as f:
            json.dump(response, f, indent=2)

        # Rate protection (Same as before)
        time.sleep(2)


def extract_tft_matches_by_puuid(puuid_file: str) -> None:
    puuid_path = Path(puuid_file)

    with puuid_path.open("r", encoding="utf-8") as f:
        puuids: list[str] = json.load(f)

    match_history_dir = DATA_TFT_DIR / "match_history"
    match_history_dir.mkdir(parents=True, exist_ok=True)

    for puuid in puuids:
        output = match_history_dir / f"{puuid}.json"

        if output.exists():
            continue

        response = get_json(get_tft_match_by_puuid(puuid))
        if not response:
            continue

        with output.open("w", encoding="utf-8") as f:
            json.dump(response, f, indent=2)

        # Rate protection (Same as before)
        time.sleep(2)


def extract_tft_match_details(match_history_file: str) -> None:
    match_history_path = Path(match_history_file)

    with match_history_path.open("r", encoding="utf-8") as f:
        match_ids: list[str] = json.load(f)

    match_details_dir = DATA_TFT_DIR / "match_details"
    match_details_dir.mkdir(parents=True, exist_ok=True)

    for match_id in match_ids:
        output = match_details_dir / f"{match_id}.json"

        if output.exists():
            continue

        response = get_json(get_tft_match_details(match_id))
        if not response:
            continue

        with output.open("w", encoding="utf-8") as f:
            json.dump(response, f, indent=2)

        # Rate protection (Same as before)
        time.sleep(2)
