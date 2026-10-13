"""Static checks on the Fabric item definitions in fabric/, run in CI before anything reaches a workspace.

- every JSON file (including .platform) parses;
- report pages: pages.json lists exactly the page folders, and page and visual names match their folders;
- every table, column and measure that a visual, a filter or the report uses exists in the semantic model (TMDL);
- every relationship joins columns that exist.

    python3 scripts/check_fabric.py [fabric_dir]
"""
from __future__ import annotations

import json
import re
import sys
from pathlib import Path

ROOT = Path(sys.argv[1]) if len(sys.argv) > 1 else Path(__file__).resolve().parents[1] / "fabric"
MODEL = ROOT / "sm_am_energy.SemanticModel" / "definition"
REPORT = ROOT / "rpt_am_energy.Report" / "definition"
NAME = r"('(?:[^']|'')+'|[^\s=]+)"  # a TMDL name: quoted, or a bare token


def unquote(name: str) -> str:
    return name[1:-1].replace("''", "'") if name.startswith("'") else name


def model_fields() -> dict[str, set[str]]:
    """Table -> its column and measure names, read from the TMDL table files."""
    fields: dict[str, set[str]] = {}
    for f in sorted((MODEL / "tables").glob("*.tmdl")):
        table, names = None, set()
        for line in f.read_text(encoding="utf-8").splitlines():
            if m := re.match(rf"^table {NAME}", line):
                table = unquote(m.group(1))
            elif m := re.match(rf"^\t(?:column|measure) {NAME}", line):
                names.add(unquote(m.group(1)))
        if table:
            fields[table] = names
    return fields


def field_refs(node, aliases: dict[str, str] | None = None):
    """Yields (entity, property) for every Column or Measure reference in a PBIR JSON tree."""
    aliases = dict(aliases or {})
    if isinstance(node, dict):
        for source in node.get("From", []) if isinstance(node.get("From"), list) else []:
            if isinstance(source, dict) and "Name" in source and "Entity" in source:
                aliases[source["Name"]] = source["Entity"]
        for kind in ("Column", "Measure"):
            ref = node.get(kind)
            if isinstance(ref, dict) and "Property" in ref:
                src = ref.get("Expression", {}).get("SourceRef", {})
                entity = src.get("Entity") or aliases.get(src.get("Source", ""))
                yield entity, ref["Property"]
        for value in node.values():
            yield from field_refs(value, aliases)
    elif isinstance(node, list):
        for value in node:
            yield from field_refs(value, aliases)


def main() -> int:
    errors: list[str] = []
    documents: dict[Path, object] = {}
    for f in sorted(ROOT.rglob("*")):
        if f.is_file() and (f.suffix == ".json" or f.name == ".platform"):
            try:
                documents[f] = json.loads(f.read_text(encoding="utf-8"))
            except json.JSONDecodeError as e:
                errors.append(f"{f.relative_to(ROOT)}: invalid JSON ({e})")

    pages_dir = REPORT / "pages"
    order = documents.get(pages_dir / "pages.json", {}).get("pageOrder", [])
    folders = sorted(p.name for p in pages_dir.iterdir() if p.is_dir())
    if sorted(order) != folders:
        errors.append(f"pages.json pageOrder {sorted(order)} does not match the page folders {folders}")
    for page in folders:
        if documents.get(pages_dir / page / "page.json", {}).get("name") != page:
            errors.append(f"page {page}: page.json name differs from its folder")
        for v in sorted((pages_dir / page / "visuals").glob("*/visual.json")):
            if documents.get(v, {}).get("name") != v.parent.name:
                errors.append(f"{v.relative_to(ROOT)}: name differs from its folder")

    fields = model_fields()
    checked = 0
    for f, doc in documents.items():
        if REPORT not in f.parents:
            continue
        for entity, prop in field_refs(doc):
            checked += 1
            if entity not in fields:
                errors.append(f"{f.relative_to(ROOT)}: table '{entity}' is not in the semantic model")
            elif prop not in fields[entity]:
                errors.append(f"{f.relative_to(ROOT)}: '{entity}'[{prop}] is not a column or measure of the model")

    for line in (MODEL / "relationships.tmdl").read_text(encoding="utf-8").splitlines():
        if m := re.match(r"^\t(?:fromColumn|toColumn): (.+)$", line):
            table, _, column = m.group(1).rpartition(".")
            if column not in fields.get(unquote(table), set()):
                errors.append(f"relationships.tmdl: {m.group(1)} does not exist")

    for e in errors:
        print("ERROR", e)
    print(f"{len(documents)} JSON documents, {len(folders)} report pages, {checked} field references checked, "
          f"{len(fields)} model tables: {'OK' if not errors else f'{len(errors)} error(s)'}")
    return 1 if errors else 0


if __name__ == "__main__":
    sys.exit(main())
