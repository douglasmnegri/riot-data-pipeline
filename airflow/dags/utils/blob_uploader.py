from pathlib import Path
from azure.storage.blob import BlobServiceClient
import os
from dotenv import load_dotenv


load_dotenv()


def upload_directory(
    local_dir: Path,
    blob_prefix: str,
):
    service = BlobServiceClient(
        account_url=f"https://{os.getenv('STORAGE_ACCOUNT')}.blob.core.windows.net",
        credential=os.getenv("ACCOUNT_ACCESS_KEY"),
    )

    container = service.get_container_client(os.getenv("CONTAINER_NAME"))

    files = list(local_dir.glob("*.json"))
    if not files:
        raise RuntimeError(f"No files found in {local_dir}")

    for file in files:
        blob_name = f"{blob_prefix}/{file.name}"

        with file.open("rb") as f:
            container.upload_blob(
                name=blob_name,
                data=f,
                overwrite=True,
            )
