#!/usr/bin/env python
"""Start a local PostgreSQL for development / verification without Docker.

Downloads nothing at runtime: it uses the ``pgserver`` package (a dev dependency)
which ships PostgreSQL binaries. The database keeps running after this script
exits; re-running it reuses the same data directory.

Usage:
    python scripts/local_postgres.py                 # start + print DATABASE_URL
    python scripts/local_postgres.py --print-env     # machine readable output
    python scripts/local_postgres.py --stop          # stop the server

Docker Compose remains the canonical way to run the stack; this is the fallback
for environments where the Docker daemon is unavailable.
"""

from __future__ import annotations

import argparse
import pathlib
import sys

import pgserver

DEFAULT_DATA_DIR = pathlib.Path(".e2e/pgdata")
DATABASE_NAME = "webhooks"


def _server(data_dir: pathlib.Path) -> pgserver.PostgresServer:
    data_dir.mkdir(parents=True, exist_ok=True)
    # cleanup_mode=None leaves PostgreSQL running when this script exits.
    return pgserver.get_server(data_dir, cleanup_mode=None)


def _ensure_database(server: pgserver.PostgresServer, name: str) -> None:
    exists = server.psql(f"SELECT 1 FROM pg_database WHERE datname = '{name}'")
    if "1 row" not in exists and "(0 rows)" in exists:
        server.psql(f'CREATE DATABASE "{name}"')


def _database_url(server: pgserver.PostgresServer, name: str) -> str:
    uri = server.get_uri()
    # pgserver returns postgresql://...?host=/path/to/socketdir (unix socket).
    query = uri.partition("?")[2]
    return f"postgresql+asyncpg://postgres@/{name}?{query}"


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--data-dir", type=pathlib.Path, default=DEFAULT_DATA_DIR)
    parser.add_argument("--database", default=DATABASE_NAME)
    parser.add_argument("--stop", action="store_true", help="stop the server and exit")
    parser.add_argument("--print-env", action="store_true", help="print only DATABASE_URL")
    args = parser.parse_args()

    server = _server(args.data_dir)
    if args.stop:
        server.cleanup()
        print("stopped", file=sys.stderr)
        return 0

    _ensure_database(server, args.database)
    url = _database_url(server, args.database)

    env_file = args.data_dir.parent / "database-url"
    env_file.parent.mkdir(parents=True, exist_ok=True)
    env_file.write_text(url + "\n", encoding="utf-8")

    if args.print_env:
        print(url)
        return 0

    print(f"PostgreSQL is running (data dir: {args.data_dir})", file=sys.stderr)
    print(f"DATABASE_URL={url}", file=sys.stderr)
    print(f"written to {env_file}", file=sys.stderr)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
