"""Serve complete report generations and refresh official data while this local site runs."""
import argparse
from datetime import datetime, timedelta
from http.server import SimpleHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from threading import Event, Thread
from urllib.parse import unquote, urlsplit

from scripts.publication import current_directory

ROOT = Path(__file__).resolve().parents[1]


def refresh_time(value: str) -> tuple[int, int]:
    """Parse a local wall-clock refresh time in HH:MM format."""
    try:
        hour, minute = (int(part) for part in value.split(':', 1))
    except ValueError as exc:
        raise argparse.ArgumentTypeError('refresh time must be HH:MM') from exc
    if not 0 <= hour <= 23 or not 0 <= minute <= 59:
        raise argparse.ArgumentTypeError('refresh time must be HH:MM')
    return hour, minute


def seconds_until_refresh(hour: int, minute: int, now=None) -> float:
    now = now or datetime.now().astimezone()
    target = now.replace(hour=hour, minute=minute, second=0, microsecond=0)
    if target <= now:
        target += timedelta(days=1)
    return (target - now).total_seconds()


def refresh_loop(stop: Event, hour: int, minute: int, refresher=None) -> None:
    """Run one local refresh per day until the web server stops."""
    while not stop.wait(seconds_until_refresh(hour, minute)):
        try:
            if refresher is None:
                # Keep report-only serving available even if refresh dependencies
                # are not installed; import them only when a refresh is due.
                from scripts.daily_official_refresh import main as default_refresher
                status = default_refresher()
            else:
                status = refresher()
            print(f'Official data refresh finished with status {status}', flush=True)
        except Exception as exc:
            # The refresh script retains the current complete generation on failure.
            print(f'Official data refresh failed: {exc}', flush=True)


def handler_for(reports):
    class Handler(SimpleHTTPRequestHandler):
        def __init__(self, *args, **kwargs):
            super().__init__(*args, directory=str(reports), **kwargs)

        def do_GET(self):
            try:
                current = current_directory(reports)
            except (OSError, ValueError, KeyError):
                self.send_error(503, 'No complete research generation available')
                return
            path = unquote(urlsplit(self.path).path)
            if path == '/':
                self.send_response(302)
                self.send_header('Location', '/' + current.relative_to(reports).as_posix() + '/svu_daily.html')
                self.end_headers()
                return
            target = (reports / path.lstrip('/')).resolve()
            generations = (reports / 'generations').resolve()
            if not target.is_relative_to(generations) or target.suffix not in {'.html', '.json', '.md'} or not target.is_file():
                self.send_error(404)
                return
            # Failed generations remain diagnostic artifacts, not published pages.
            relative = target.relative_to(generations)
            if len(relative.parts) < 2 or not (generations / relative.parts[0] / 'generation.json').is_file():
                self.send_error(404)
                return
            super().do_GET()

        def do_HEAD(self):
            self.send_error(405)

    return Handler


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--port', type=int, default=8000)
    parser.add_argument('--refresh-time', type=refresh_time, default=(20, 0), metavar='HH:MM',
                        help='local daily official-data refresh time (default: 20:00)')
    parser.add_argument('--no-refresh', action='store_true', help='serve reports without the local refresh scheduler')
    args = parser.parse_args()
    stop = Event()
    worker = None
    # Bind and validate dependencies before announcing an active scheduler.
    server = ThreadingHTTPServer(('127.0.0.1', args.port), handler_for(ROOT / 'reports'))
    if not args.no_refresh:
        try:
            from scripts.daily_official_refresh import main as refresher
        except ImportError:
            server.server_close()
            parser.error('refresh dependencies missing; install requirements.lock or use --no-refresh')
        hour, minute = args.refresh_time
        worker = Thread(target=refresh_loop, args=(stop, hour, minute, refresher), daemon=False,
                        name='svu-official-data-refresh')
        worker.start()
        print(f'Official data refresh scheduled daily at {hour:02d}:{minute:02d} local time.', flush=True)
    try:
        with server:
            print(f'Open http://127.0.0.1:{args.port}/', flush=True)
            server.serve_forever()
    except KeyboardInterrupt:
        pass
    finally:
        stop.set()
        if worker:
            # Finish an in-flight database transaction/publication before exit.
            worker.join()


if __name__ == '__main__':
    main()
