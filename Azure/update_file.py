import os
import random
from pathlib import Path
from azure.storage.blob import BlobServiceClient
from dotenv import load_dotenv


load_dotenv()


LOCAL_DIR = Path("../data/raw/puuids")


def main():
    files = list(LOCAL_DIR.glob("*.json"))

    if not files:
        raise RuntimeError(f"No JSON files found in {LOCAL_DIR}")

    file = random.choice(files)
    blob_name = f"{os.getenv('BLOB_PREFIX')}/{file.name}"

    print(f"Uploading: {file} → {blob_name}")

    service = BlobServiceClient(
        account_url=f"https://{os.getenv('STORAGE_ACCOUNT')}.blob.core.windows.net",
        credential=os.getenv("ACCOUNT_ACCESS_KEY"),
    )

    container = service.get_container_client(os.getenv("CONTAINER_NAME"))
    with file.open("rb") as f:
        container.upload_blob(
            name=blob_name,
            data=f,
            overwrite=True,
        )

    print("Upload successful!")


if __name__ == "__main__":
    main()
