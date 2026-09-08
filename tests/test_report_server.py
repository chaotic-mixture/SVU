import argparse
from datetime import datetime, timezone
from http.client import HTTPConnection
from http.server import ThreadingHTTPServer
import sqlite3
from threading import Thread

import pytest

from scripts.publication import current_directory, publish
from scripts.serve_reports import handler_for
from scripts.serve_reports import refresh_time, seconds_until_refresh


def test_report_server_redirects_to_complete_generation_and_hides_database(tmp_path):
    with sqlite3.connect(tmp_path / 'svu.db') as con:
        con.execute('create table example(value)')
    builders = [('daily', lambda out, db: (out / 'svu_daily.html').write_text('complete')),
                ('snapshot', lambda out, db: (out / 'svu_v03_snapshot.json').write_text('{}'))]
    publish(tmp_path, builders)
    reports = tmp_path / 'reports'
    directory = current_directory(reports)
    with ThreadingHTTPServer(('127.0.0.1', 0), handler_for(reports)) as server:
        thread = Thread(target=server.serve_forever, daemon=True)
        thread.start()
        try:
            def request(path):
                client = HTTPConnection('127.0.0.1', server.server_port, timeout=5)
                try:
                    client.request('GET', path)
                    response = client.getresponse()
                    return response.status, response.getheader('Location'), response.read()
                finally:
                    client.close()
            code, location, _ = request('/')
            assert code == 302
            assert request(location)[::2] == (200, b'complete')
            assert request('/svu.db')[0] == 404
            assert request('/generations/' + directory.name + '/../../../svu.db')[0] == 404
            assert request('/generations/' + directory.name + '/')[0] == 404
            incomplete = reports / 'generations/unpublished'
            incomplete.mkdir()
            (incomplete / 'partial.html').write_text('partial')
            assert request('/generations/unpublished/partial.html')[0] == 404
        finally:
            server.shutdown()
            thread.join(timeout=5)


def test_local_refresh_time_parsing_and_next_day_schedule():
    assert refresh_time('20:00') == (20, 0)
    assert refresh_time('00:05') == (0, 5)
    with pytest.raises(argparse.ArgumentTypeError):
        refresh_time('24:00')
    now = datetime(2026, 9, 7, 20, 0, 1, tzinfo=timezone.utc)
    assert seconds_until_refresh(20, 0, now) == pytest.approx(86399)


def test_refresh_loop_stops_and_survives_failed_run(monkeypatch):
    import scripts.serve_reports as site
    calls = []
    class Stop:
        def wait(self, seconds):
            return len(calls) >= 2
    def refresh():
        calls.append('refresh')
        if len(calls) == 1:
            raise RuntimeError('provider unavailable')
        return 0
    monkeypatch.setattr(site, 'seconds_until_refresh', lambda *args: 0)
    site.refresh_loop(Stop(), 20, 0, refresh)
    assert calls == ['refresh', 'refresh']
