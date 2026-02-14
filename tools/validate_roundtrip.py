import json
import os
import runpy
from pathlib import Path
from typing import Any, List, Tuple

from py2rocket import from_json


def _is_workflow_json(data: Any) -> bool:
    return (
        isinstance(data, dict)
        and "pipelineGraph" in data
        and "nodes" in data.get("pipelineGraph", {})
    )


def _compare(a: Any, b: Any, path: str = "") -> List[str]:
    diffs: List[str] = []
    if path.endswith("/arity"):
        return diffs
    if type(a) != type(b):
        return [f"{path}: type {type(a).__name__} != {type(b).__name__}"]

    if isinstance(a, dict):
        a_keys = set(a.keys())
        b_keys = set(b.keys())
        for k in sorted(a_keys - b_keys):
            diffs.append(f"{path}/{k}: missing in rebuilt")
        for k in sorted(b_keys - a_keys):
            diffs.append(f"{path}/{k}: unexpected in rebuilt")
        for k in sorted(a_keys & b_keys):
            diffs.extend(_compare(a[k], b[k], f"{path}/{k}"))
        return diffs

    if isinstance(a, list):
        if len(a) != len(b):
            return [f"{path}: list length {len(a)} != {len(b)}"]
        for i, (ai, bi) in enumerate(zip(a, b)):
            diffs.extend(_compare(ai, bi, f"{path}[{i}]"))
        return diffs

    if a != b:
        return [f"{path}: {a!r} != {b!r}"]

    return diffs


def main() -> Tuple[int, List[str]]:
    repo_root = Path(__file__).resolve().parents[1]
    ref_root = repo_root / "ref_workflows"

    json_files = sorted(ref_root.rglob("*.json"))
    failures: List[str] = []
    processed = 0

    for json_path in json_files:
        stem_lower = json_path.stem.lower()
        if "rebuilt" in stem_lower or "roundtrip" in stem_lower:
            continue

        try:
            original = json.loads(json_path.read_text(encoding="utf-8"))
        except Exception:
            continue

        if not _is_workflow_json(original):
            continue

        processed += 1
        output_py = json_path.with_suffix(".roundtrip.py")

        from_json(str(json_path), output_file=str(output_py))

        previous_cwd = Path.cwd()
        try:
            os.chdir(json_path.parent)
            runpy.run_path(str(output_py), run_name="__main__")
        finally:
            os.chdir(previous_cwd)

        rebuilt_path = json_path.with_name(json_path.stem + "_rebuilt.json")
        if not rebuilt_path.exists():
            failures.append(f"{json_path}: rebuilt json missing")
            continue

        rebuilt = json.loads(rebuilt_path.read_text(encoding="utf-8"))
        diffs = _compare(original, rebuilt)
        if diffs:
            failures.append(f"{json_path}: {diffs[0]}")

    if processed == 0:
        failures.append("No workflow JSON files found under ref_workflows")

    return processed, failures


if __name__ == "__main__":
    total, errors = main()
    print(f"Workflows procesados: {total}")
    if errors:
        print("❌ Diferencias encontradas:")
        for err in errors:
            print(f"- {err}")
        raise SystemExit(1)
    print("✅ Sin diferencias detectadas")
