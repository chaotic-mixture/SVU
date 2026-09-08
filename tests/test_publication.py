import json
import sqlite3
from contextlib import closing

import pytest

from scripts.publication import current_directory, operation_lock, publish


def init_db(root):
    with sqlite3.connect(root / 'svu.db') as con:
        con.execute('create table example(value)')
        con.execute('insert into example values(1)')


def builders():
    return [('daily', lambda out, db: (out / 'svu_daily.html').write_text('complete')),
            ('snapshot', lambda out, db: (out / 'svu_v03_snapshot.json').write_text('{}'))]


def test_failed_generation_keeps_previous_pointer(tmp_path):
    init_db(tmp_path)
    publish(tmp_path, builders())
    previous = (tmp_path / 'reports/current.json').read_bytes()
    def fail(out, db):
        (out / 'svu_daily.html').write_text('partial')
        raise RuntimeError('synthetic builder failure')
    with pytest.raises(RuntimeError, match='synthetic'):
        publish(tmp_path, [('daily', fail)])
    assert (tmp_path / 'reports/current.json').read_bytes() == previous
    assert (current_directory(tmp_path / 'reports') / 'svu_daily.html').read_text() == 'complete'
    assert not list((tmp_path / 'reports').glob('.input-*.db'))


def test_builders_share_a_consistent_database_snapshot(tmp_path):
    init_db(tmp_path)
    def mutate(out, db):
        with sqlite3.connect(tmp_path / 'svu.db') as con:
            con.execute('insert into example values(2)')
        (out / 'svu_daily.html').write_text('complete')
    def read(out, db):
        with closing(sqlite3.connect(db)) as con:
            assert con.execute('select count(*) from example').fetchone()[0] == 1
        (out / 'svu_v03_snapshot.json').write_text('{}')
    publish(tmp_path, [('daily', mutate), ('snapshot', read)])
    assert current_directory(tmp_path / 'reports').is_dir()


def test_concurrent_writer_is_rejected_and_lock_releases(tmp_path):
    with operation_lock(tmp_path / 'lock'):
        with pytest.raises(RuntimeError, match='another research operation'):
            with operation_lock(tmp_path / 'lock'):
                pass
    with operation_lock(tmp_path / 'lock'):
        pass


def test_pointer_rejects_path_traversal(tmp_path):
    (tmp_path / 'current.json').write_text(json.dumps({'generation': '../private'}))
    with pytest.raises(ValueError):
        current_directory(tmp_path)
