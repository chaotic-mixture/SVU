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


def test_stable_entry_links_follow_generation_without_mutating_history(tmp_path):
    init_db(tmp_path)
    page='<a href="domains/currency.html">FX</a><a href="../../labor.html">Labor</a>'
    steps=[('daily',lambda out,db:(out/'svu_daily.html').write_text(page)),
           ('snapshot',lambda out,db:(out/'svu_v03_snapshot.json').write_text('{}'))]
    publish(tmp_path,steps)
    reports=tmp_path/'reports';first=current_directory(reports)
    publish(tmp_path,steps)
    latest=current_directory(reports)
    assert first!=latest and (first/'svu_daily.html').read_text()==page
    for name in ['index.html','svu_daily.html','svu_v03_dashboard.html']:
        content=(reports/name).read_text()
        assert f'./generations/{latest.name}/domains/currency.html' in content
        assert 'href="./labor.html"' in content


def test_entry_write_failure_does_not_invalidate_committed_generation(tmp_path,monkeypatch):
    init_db(tmp_path)
    def fail(reports): raise OSError('entry locked')
    monkeypatch.setattr('scripts.publication.write_entry_page',fail)
    result=publish(tmp_path,builders())
    current=current_directory(tmp_path/'reports')
    assert (current/'svu_daily.html').read_text()=='complete'
    assert not (current/'failed.json').exists()
    assert result[-1]['status']=='failed' and result[-1]['publication_complete'] is True
