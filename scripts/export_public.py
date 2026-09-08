"""Export a fresh, allowlisted source tree and ZIP without Git history or market data."""
from __future__ import annotations

import argparse
import ast
import hashlib
import json
from pathlib import Path
import re
import shutil
import zipfile

ROOT = Path(__file__).resolve().parents[1]
MANIFEST = ROOT / "config/public_release.json"


def release_files(root=ROOT):
    files = json.loads((root / "config/public_release.json").read_text(encoding="utf-8"))["files"]
    if len(files) != len(set(files)):
        raise ValueError("duplicate release paths")
    for name in files:
        path = root / name
        if Path(name).is_absolute() or ".." in Path(name).parts or not path.is_file():
            raise ValueError(f"invalid release path: {name}")
        if any(parent.is_symlink() for parent in [path, *path.parents]) or not path.resolve().is_relative_to(root.resolve()):
            raise ValueError(f"symlink/outside release root: {name}")
    return sorted(files)


def validate(root, files):
    selected = set(files)
    for name in files:
        text = (root / name).read_text(encoding="utf-8-sig")
        if re.search(r"(?:[A-Z]:[/\\](?:Users|agent)|" + "@" + r"session:|-----" + r"BEGIN .*PRIVATE KEY-----|ghp_[A-Za-z0-9]{30,})", text):
            raise ValueError(f"private content candidate in {name}")
        if not name.endswith(".py"):
            continue
        tree = ast.parse(text)
        for node in ast.walk(tree):
            names = [node.module] if isinstance(node, ast.ImportFrom) and node.module else (
                [alias.name for alias in node.names] if isinstance(node, ast.Import) else [])
            for module in names:
                if module.split('.')[0] not in {"scripts", "validation", "api", "models", "utils", "pipeline"}:
                    continue
                path = module.replace('.', '/')
                if path + '.py' not in selected and path + '/__init__.py' not in selected:
                    raise ValueError(f"missing local import {module} in {name}")


def export(output: Path):
    files = release_files()
    validate(ROOT, files)
    output = output.resolve()
    if output.exists():
        raise FileExistsError("output must be a new directory; existing files are never overwritten")
    output.mkdir(parents=True)
    tree = output / "SVU"
    hashes = {}
    for name in files:
        destination = tree / name
        destination.parent.mkdir(parents=True, exist_ok=True)
        shutil.copyfile(ROOT / name, destination)
        hashes[name] = hashlib.sha256(destination.read_bytes()).hexdigest()
    (output / "SHA256.json").write_text(json.dumps(hashes, indent=2, sort_keys=True), encoding="utf-8")
    with zipfile.ZipFile(output / "SVU-source.zip", "w", zipfile.ZIP_DEFLATED) as archive:
        for name in files:
            archive.write(tree / name, "SVU/" + name)
    return tree


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    print(export(args.output))


if __name__ == "__main__":
    main()
