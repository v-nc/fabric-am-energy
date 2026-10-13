"""Runs KQL against the project's Eventhouse with the Azure CLI login.

    python3 scripts/kql.py query "meter_events | count"
    python3 scripts/kql.py mgmt  ".show tables"
    python3 scripts/kql.py mgmt  @kql/setup.kql        # a file; commands separated by blank lines

Another Eventhouse: set KUSTO_URI to its Query URI (Eventhouse overview in Fabric) and KUSTO_DATABASE if renamed.
"""
from __future__ import annotations

import json
import os
import subprocess
import sys
import urllib.error
import urllib.request

CLUSTER = os.environ.get("KUSTO_URI", "https://trd-2czftksj0qxw9pe2x5.z1.kusto.fabric.microsoft.com")
DATABASE = os.environ.get("KUSTO_DATABASE", "eh_am_energy")


def token() -> str:
    return subprocess.run(["az", "account", "get-access-token", "--resource", CLUSTER, "--query", "accessToken", "-o", "tsv"],
                          check=True, capture_output=True, text=True).stdout.strip()


def run(kind: str, csl: str, tok: str) -> list[dict]:
    req = urllib.request.Request(f"{CLUSTER}/v1/rest/{kind}", method="POST",
                                 data=json.dumps({"db": DATABASE, "csl": csl}).encode(),
                                 headers={"Authorization": f"Bearer {tok}", "Content-Type": "application/json; charset=utf-8"})
    try:
        with urllib.request.urlopen(req, timeout=120) as r:
            tables = json.load(r)["Tables"]
    except urllib.error.HTTPError as e:
        raise SystemExit(f"HTTP {e.code}: {e.read().decode()[:1500]}")
    t = tables[0]
    cols = [c["ColumnName"] for c in t["Columns"]]
    return [dict(zip(cols, row)) for row in t["Rows"]]


def main() -> None:
    kind, text = sys.argv[1], sys.argv[2]
    if text.startswith("@"):
        text = open(text[1:]).read()
    tok = token()
    commands = [text]
    if kind == "mgmt":
        chunks = ["\n".join(l for l in c.strip().splitlines() if not l.lstrip().startswith("//")).strip() for c in text.split("\n\n")]
        commands = [c for c in chunks if c]
    for csl in commands:
        rows = run(kind, csl, tok)
        if kind == "mgmt" and len(commands) > 1:
            print(f"-- {csl.splitlines()[0][:90]}  → {len(rows)} row(s)")
            continue
        for r in rows[:50]:
            print(json.dumps(r, default=str, ensure_ascii=False))


if __name__ == "__main__":
    main()
