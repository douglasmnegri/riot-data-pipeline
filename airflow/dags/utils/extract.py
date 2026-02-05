import json
import time
from datetime import datetime
from pathlib import Path

from utils.client import get_json
from utils.endpoints import get_rank_entries
from utils.endpoints import get_player_champion_data
from utils.endpoints import get_player_challenges
from utils.endpoints import get_tft_grandmaster_leaderboard

DATA_DIR = Path("data/raw")


def extract_rank_entries(
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

    rank_dir = DATA_DIR / "rank"
    puuid_dir = DATA_DIR / "puuids"

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

    mastery_dir = DATA_DIR / "champion_mastery"
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

        # Rate liminting protection (Riot API allows 100 requests every 2 minutes)
        time.sleep(2)


def extract_progressed_challenges() -> None:
    puuid_dir = DATA_DIR / "puuids"
    challenges_dir = DATA_DIR / "progressed_challenges"

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


def extract_tft_grandmaster_leaderboard() -> None:
    leaderboard_dir = DATA_DIR / "tft_grandmaster_leaderboard"
    leaderboard_dir.mkdir(parents=True, exist_ok=True)

    output = leaderboard_dir / "grandmaster_leaderboard.json"

    if output.exists():
        return

    response = get_json(get_tft_grandmaster_leaderboard())
    if not response:
        return

    with output.open("w", encoding="utf-8") as f:
        json.dump(response, f, indent=2)
