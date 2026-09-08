"""Publish a complete immutable report generation through one atomic pointer."""
from contextlib import closing, contextmanager
from datetime import datetime, timezone
import json
import os
from pathlib import Path
import re
import sqlite3
import tempfile
import uuid


@contextmanager
def operation_lock(path):
    """Process lock released by the OS even when a process crashes."""
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open('a+b') as handle:
        if handle.tell() == 0:
            handle.write(b'0')
            handle.flush()
        handle.seek(0)
        if os.name == 'nt':
            import msvcrt
            lock = lambda: msvcrt.locking(handle.fileno(), msvcrt.LK_NBLCK, 1)
            unlock = lambda: msvcrt.locking(handle.fileno(), msvcrt.LK_UNLCK, 1)
        else:
            import fcntl
            lock = lambda: fcntl.flock(handle, fcntl.LOCK_EX | fcntl.LOCK_NB)
            unlock = lambda: fcntl.flock(handle, fcntl.LOCK_UN)
        try:
            lock()
        except OSError as exc:
            raise RuntimeError('another research operation is running') from exc
        try:
            yield
        finally:
            handle.seek(0)
            unlock()


def current_directory(reports):
    pointer = json.loads((reports / 'current.json').read_text(encoding='utf-8'))
    generation = pointer['generation']
    if not re.fullmatch(r'[0-9]{8}T[0-9]{6}Z-[a-f0-9]{32}', generation):
        raise ValueError('invalid report generation')
    directory = reports / 'generations' / generation
    if not directory.is_dir() or directory.is_symlink() or not directory.resolve().is_relative_to(reports.resolve()):
        raise ValueError('report generation unavailable')
    marker = json.loads((directory / 'generation.json').read_text(encoding='utf-8'))
    if marker.get('generation') != generation or marker.get('status') != 'complete':
        raise ValueError('report generation is not complete')
    return directory


def publish(root, builders=None):
    root = Path(root)
    reports = root / 'reports'
    reports.mkdir(parents=True, exist_ok=True)
    with operation_lock(reports / '.publish.lock'):
        generation = datetime.now(timezone.utc).strftime('%Y%m%dT%H%M%SZ-') + uuid.uuid4().hex
        output = reports / 'generations' / generation
        output.mkdir(parents=True)
        fd, filename = tempfile.mkstemp(prefix='.input-', suffix='.db', dir=reports)
        os.close(fd)
        snapshot = Path(filename)
        results = []
        try:
            with closing(sqlite3.connect((root / 'svu.db').resolve().as_uri() + '?mode=ro', uri=True)) as source:
                with closing(sqlite3.connect(snapshot)) as destination:
                    source.backup(destination)
            if builders is None:
                from validation.build_domain_dashboard import build
                from validation.build_daily_dashboard import main as daily
                from validation.build_v03_snapshot import main as summary
                builders = [(domain, lambda out, db, name=domain: build(name, output_dir=out, db_path=db))
                            for domain in ['currency', 'energy', 'monetary_hedge', 'equity_index']]
                builders += [('daily', lambda out, db: daily(output_dir=out, db_path=db)),
                             ('snapshot', lambda out, db: summary(output_dir=out, db_path=db))]
            for name, builder in builders:
                builder(output, snapshot)
                results.append({'domain_key': name, 'status': 'ok', 'generation': generation})
            if not (output / 'svu_daily.html').is_file() or not (output / 'svu_v03_snapshot.json').is_file():
                raise ValueError('generation lacks dashboard or snapshot')
            # A leaked builder connection must fail before switching the pointer.
            snapshot.unlink()
            pointer = {'generation': generation, 'status': 'complete', 'built_at_utc': datetime.now(timezone.utc).isoformat()}
            (output / 'generation.json').write_text(json.dumps(pointer), encoding='utf-8')
            temporary = reports / f'.current-{uuid.uuid4().hex}.json'
            try:
                with temporary.open('w', encoding='utf-8') as handle:
                    json.dump(pointer, handle)
                    handle.flush()
                    os.fsync(handle.fileno())
                os.replace(temporary, reports / 'current.json')
            finally:
                temporary.unlink(missing_ok=True)
            return results
        except Exception as exc:
            failure = {'status': 'failed', 'error': str(exc)[:500], 'previous_generation_preserved': True}
            (output / 'failed.json').write_text(json.dumps(failure), encoding='utf-8')
            raise
        finally:
            snapshot.unlink(missing_ok=True)
