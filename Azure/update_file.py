import random
from pathlib import Path
from azure.storage.blob import BlobServiceClient

# =====================
# CONFIG
# =====================
STORAGE_ACCOUNT = "riotdata"
CONTAINER_NAME = "riot-data-pipeline"

# ⚠️ STORAGE ACCOUNT KEY (not container key)
ACCOUNT_KEY = "PASTE_YOUR_STORAGE_ACCOUNT_KEY_HERE"

LOCAL_DIR = Path("data/raw/puuids")
BLOB_PREFIX = "test_uploads"


def main():
    files = list(LOCAL_DIR.glob("*.json"))

    if not files:
        raise RuntimeError(f"No JSON files found in {LOCAL_DIR}")

    file = random.choice(files)
    blob_name = f"{BLOB_PREFIX}/{file.name}"

    print(f"Uploading: {file} → {blob_name}")

    service = BlobServiceClient(
        account_url=f"https://{STORAGE_ACCOUNT}.blob.core.windows.net",
        credential=ACCOUNT_KEY,
    )

    container = service.get_container_client(CONTAINER_NAME)

    with file.open("rb") as f:
        container.upload_blob(
            name=blob_name,
            data=f,
            overwrite=True,
        )

    print("✅ Upload successful!")


if __name__ == "__main__":
    main()
