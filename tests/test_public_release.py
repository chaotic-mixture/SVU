import json
from pathlib import Path
import zipfile

import pytest

from scripts.export_public import ROOT, export, release_files, validate


def test_export_contains_only_allowlisted_files_and_no_history(tmp_path):
    tree = export(tmp_path / "candidate")
    expected = set(release_files())
    actual = {p.relative_to(tree).as_posix() for p in tree.rglob('*') if p.is_file()}
    assert actual == expected
    assert not any(x.startswith(('api/', 'models/', 'reports/', '.git/')) or x.endswith('.db') for x in actual)
    hashes = json.loads((tree.parent / 'SHA256.json').read_text())
    assert set(hashes) == expected
    with zipfile.ZipFile(tree.parent / 'SVU-source.zip') as archive:
        assert set(archive.namelist()) == {'SVU/' + x for x in expected}
    with pytest.raises(FileExistsError): export(tree.parent)


def test_missing_local_dependency_is_rejected(tmp_path):
    (tmp_path / 'broken.py').write_text('from api.research_api import app\n')
    with pytest.raises(ValueError, match='missing local import'):
        validate(tmp_path, ['broken.py'])


def test_hash_lock_matches_all_constrained_versions():
    import re
    constraints = {line.lower() for line in (ROOT / 'constraints.txt').read_text().splitlines() if line and not line.startswith('#')}
    locked = {match.group(0).lower() for match in re.finditer(r'^[A-Za-z0-9_.-]+==[^\s]+', (ROOT / 'requirements.lock').read_text(), re.M)}
    assert constraints == locked
