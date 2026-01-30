import json
from datetime import datetime
from pathlib import Path

from riot.client import get_json
from riot.endpoints import get_rank_entries


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
