import os
from pathlib import Path
from azure.storage.blob import BlobServiceClient
from dotenv import load_dotenv


load_dotenv()


DATA_DIR = Path("data/raw")


def upload_all_raw_data_to_blob() -> None:
    storage_account = os.getenv("STORAGE_ACCOUNT")
    container_name = os.getenv("CONTAINER_NAME")
    access_key = os.getenv("ACCOUNT_ACCESS_KEY")
    blob_prefix = os.getenv("BLOB_PREFIX", "raw")

    if not all([storage_account, container_name, access_key]):
        raise RuntimeError("Missing Azure Blob Storage environment variables")

    service = BlobServiceClient(
        account_url=f"https://{storage_account}.blob.core.windows.net",
        credential=access_key,
    )

    container = service.get_container_client(container_name)

    for file in DATA_DIR.rglob("*.json"):
        relative_path = file.relative_to(DATA_DIR)
        blob_name = f"{blob_prefix}/{relative_path.as_posix()}"

        print(f"Uploading {file} → {blob_name}")

        with file.open("rb") as f:
            container.upload_blob(
                name=blob_name,
                data=f,
                overwrite=True,
            )

    print("All raw data uploaded successfully")
