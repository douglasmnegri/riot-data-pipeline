import json
from datetime import datetime
from pathlib import Path

from utils.client import get_json
from utils.endpoints import get_rank_entries
from utils.endpoints import get_player_champion_data

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

    DATA_DIR.mkdir(parents=True, exist_ok=True)

    ts = datetime.utcnow().strftime("%Y%m%d_%H%M%S")
    output = DATA_DIR / f"{queue}_{tier}_{division}_{ts}.json"

    with output.open("w") as f:
        json.dump(all_entries, f, indent=2)

    return str(output)


def extract_champion_mastery_entries(player_puuid: str) -> str:
    DATA_DIR.mkdir(parents=True, exist_ok=True)

    response = get_json(get_player_champion_data(player_puuid))

    if not response:
        raise ValueError(f"No champion mastery data for player {player_puuid}")

    output = DATA_DIR / f"{player_puuid}.json"

    with output.open("w", encoding="utf-8") as f:
        json.dump(response, f, indent=2)

    return str(output)
