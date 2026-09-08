"""Rebuild the supported official-data research path in dependency order."""
from __future__ import annotations

import argparse
import importlib
import hashlib
import json
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
BATCHES = [
    ("fetch_public_data", "load_public_database"),
    ("fetch_supplemental_data", "load_supplemental_database"),
    ("fetch_official_fx_batch", "load_official_fx_batch"),
    ("fetch_ecb_fx_batch", "load_ecb_fx_batch"),
    ("fetch_ecb_discovered", "load_ecb_discovered"),
    ("fetch_official_equity", "load_official_equity"),
    ("fetch_official_energy", "load_official_energy"),
]

INPUTS = {
    'load_public_database': ('public_manifest.json', [('normalized_file', 'normalized_sha256', 'public_prices.csv'), ('macro_file', 'macro_sha256', 'macro_indicators.csv')]),
    'load_supplemental_database': ('supplemental_manifest.json', [('processed_file', 'processed_sha256', 'supplemental_prices.csv')]),
    'load_official_fx_batch': ('official_fx_manifest.json', [('processed_file', 'processed_sha256', 'official_fx.csv')]),
    'load_ecb_fx_batch': ('ecb_fx_manifest.json', [('processed_file', 'processed_sha256', 'ecb_fx.csv')]),
    'load_ecb_discovered': ('ecb_discovered_manifest.json', [('processed_file', 'processed_sha256', 'ecb_discovered_fx.csv')]),
    'load_official_equity': ('official_equity_manifest.json', [('processed_file', 'processed_sha256', 'official_equity.csv')]),
    'load_official_energy': ('official_energy_manifest.json', [('processed_file', 'processed_sha256', 'official_energy.csv')]),
}


def validate_batch(root, loader):
    manifest_name, files = INPUTS[loader]
    folder = root / 'data/processed'
    manifest = json.loads((folder / manifest_name).read_text(encoding='utf-8'))
    for path_key, hash_key, expected_name in files:
        path = (root / manifest[path_key]).resolve()
        expected = (folder / expected_name).resolve()
        if path != expected or not path.is_relative_to(root.resolve()):
            raise ValueError(f'unexpected input path in {manifest_name}')
        if hashlib.sha256(path.read_bytes()).hexdigest() != manifest[hash_key]:
            raise ValueError(f'input hash mismatch: {expected_name}')


def rebuild(download=False, runner=None):
    from scripts.publication import operation_lock
    with operation_lock(ROOT / "reports" / ".database.lock"):
        return _rebuild(download, runner)


def _rebuild(download=False, runner=None):
    """Load local official files by default; downloading is explicit."""
    if runner is None:
        runner = lambda name: importlib.import_module(name).main()
    (ROOT / "reports").mkdir(parents=True, exist_ok=True)
    for fetcher, loader in BATCHES:
        if download:
            runner(f"scripts.{fetcher}")
        validate_batch(ROOT, loader)
        runner(f"scripts.{loader}")
    for name in ["scripts.create_unique_views", "scripts.create_entity_dedup_layer"]:
        runner(name)
    from scripts.daily_official_refresh import rebuild_outputs
    results = rebuild_outputs()
    if any(row.get("status") == "failed" for row in results):
        raise RuntimeError(f"research outputs incomplete: {results}")
    return results


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--download", action="store_true", help="download official batches before loading")
    args = parser.parse_args()
    rebuild(download=args.download)


if __name__ == "__main__":
    main()
