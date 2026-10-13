"""Runs T-SQL against the project's Warehouse with the Azure CLI login.

    uv run --no-project --with mssql-python python scripts/sql.py "SELECT COUNT(*) AS n FROM ref.day_ahead_price"
    uv run --no-project --with mssql-python python scripts/sql.py @warehouse/setup.sql   # batches separated by GO

Another Warehouse: set WAREHOUSE_SERVER to its SQL connection string (Warehouse settings in Fabric) and
WAREHOUSE_DATABASE if renamed.
"""
from __future__ import annotations

import json
import os
import re
import struct
import subprocess
import sys

import mssql_python

SERVER = os.environ.get(
    "WAREHOUSE_SERVER", "pryhkamp5gfu7jrym6b5y3ebci-yc4b6gtb6qsubk4jubf47yyhky.datawarehouse.fabric.microsoft.com"
)
DATABASE = os.environ.get("WAREHOUSE_DATABASE", "wh_am_energy")
SQL_COPT_SS_ACCESS_TOKEN = 1256  # ODBC connection attribute that takes an Entra access token


def access_token() -> bytes:
    token = subprocess.run(
        ["az", "account", "get-access-token", "--resource", "https://database.windows.net/",
         "--query", "accessToken", "-o", "tsv"],
        check=True, capture_output=True, text=True,
    ).stdout.strip()
    raw = token.encode("utf-16-le")
    return struct.pack(f"<I{len(raw)}s", len(raw), raw)


def batches(script: str) -> list[str]:
    return [b.strip() for b in re.split(r"^\s*GO\s*$", script, flags=re.IGNORECASE | re.MULTILINE) if b.strip()]


def main() -> None:
    text = sys.argv[1]
    if text.startswith("@"):
        with open(text[1:], encoding="utf-8") as f:
            text = f.read()
    conn = mssql_python.connect(
        f"Server={SERVER},1433;Database={DATABASE};Encrypt=yes;TrustServerCertificate=no",
        attrs_before={SQL_COPT_SS_ACCESS_TOKEN: access_token()},
        autocommit=True,
    )
    cur = conn.cursor()
    for batch in batches(text):
        cur.execute(batch)
        while True:
            if cur.description:
                cols = [c[0] for c in cur.description]
                for row in cur.fetchall():
                    print(json.dumps(dict(zip(cols, row)), default=str))
            if not cur.nextset():
                break
    conn.close()


if __name__ == "__main__":
    main()
