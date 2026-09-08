"""Generate a complete hash lock for the already selected constraint versions."""
import argparse
from pathlib import Path
import re

import requests

ROOT = Path(__file__).resolve().parents[1]


def generate(constraints, output):
    lines = ['# Generated from constraints.txt using PyPI release SHA-256 digests.',
             '# Includes wheels and source archives across platforms; versions are not auto-upgraded.']
    for item in constraints.read_text(encoding='utf-8').splitlines():
        if not item.strip() or item.startswith('#'):
            continue
        match = re.fullmatch(r'([A-Za-z0-9_.-]+)==([A-Za-z0-9_.+!-]+)', item.strip())
        if not match:
            raise ValueError('constraints must contain exact versions only')
        name, version = match.groups()
        response = requests.get(f'https://pypi.org/pypi/{name}/{version}/json', timeout=(10, 30))
        response.raise_for_status()
        hashes = sorted({row['digests']['sha256'] for row in response.json()['urls'] if not row.get('yanked')})
        if not hashes or not all(re.fullmatch('[a-f0-9]{64}', value) for value in hashes):
            raise ValueError(f'no valid hashes for {name}=={version}')
        lines.append(item.strip() + ' \\')
        lines.extend('    --hash=sha256:' + value + (' \\' if i < len(hashes)-1 else '') for i, value in enumerate(hashes))
    output.write_text('\n'.join(lines) + '\n', encoding='utf-8')


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--output', type=Path, default=ROOT / 'requirements.lock')
    args = parser.parse_args()
    generate(ROOT / 'constraints.txt', args.output)


if __name__ == '__main__':
    main()
