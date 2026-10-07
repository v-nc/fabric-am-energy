"""Uploads the generated history (data/history) to the Lakehouse Files area in OneLake, through the ADLS Gen2 API that
OneLake speaks. Files that already exist with the same size are skipped, so the upload can be re-run.

    uv run --no-project --with azure-storage-file-datalake --with azure-identity python scripts/upload_history.py

Authenticates with the Azure CLI login (`az login`).
"""

from __future__ import annotations

import argparse
from pathlib import Path

from azure.identity import AzureCliCredential
from azure.storage.filedatalake import DataLakeServiceClient

REPO = Path(__file__).resolve().parents[1]


def main() -> None:
    p = argparse.ArgumentParser()
    p.add_argument("--workspace", default="am-energy-dev")
    p.add_argument("--lakehouse", default="lh_energy")
    p.add_argument("--source", default=str(REPO / "data" / "history"))
    p.add_argument("--target", default="Files/landing/history")
    args = p.parse_args()

    service = DataLakeServiceClient("https://onelake.dfs.fabric.microsoft.com", credential=AzureCliCredential())
    fs = service.get_file_system_client(args.workspace)
    root = f"{args.lakehouse}.Lakehouse/{args.target}"

    source = Path(args.source)
    files = sorted(f for f in source.rglob("*") if f.is_file() and f.suffix in {".parquet", ".csv"})
    if not files:
        raise SystemExit(f"nothing to upload in {source} (run `npm run history` in edge/)")
    for f in files:
        remote = f"{root}/{f.relative_to(source).as_posix()}"
        client = fs.get_file_client(remote)
        if client.exists() and client.get_file_properties().size == f.stat().st_size:
            print(f"skip   {remote}")
            continue
        with f.open("rb") as data:
            client.upload_data(data, overwrite=True)
        print(f"upload {remote} ({f.stat().st_size / 1e6:.1f} MB)")


if __name__ == "__main__":
    main()
