"""Start isolated real Redis/API/worker processes; use a temporary PostgreSQL schema.

Requires TEST_REDIS_SERVER (or redis-server on PATH) and TEST_DATABASE_URL.
No existing Redis keys or PostgreSQL tables are reset.
"""

import argparse
import asyncio
import json
import os
from pathlib import Path
import shutil
import socket
import subprocess
import sys
import tempfile
import time
from uuid import uuid4

import httpx
import psycopg
from psycopg import sql
from psycopg.conninfo import make_conninfo
from redis import Redis
from redis.exceptions import RedisError

from scripts.load_test import benchmark


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--requests", type=int, default=1000)
    parser.add_argument("--concurrency", type=int, default=50)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    if args.output.exists():
        parser.error("Output already exists; choose a new report path")
    binary = os.getenv("TEST_REDIS_SERVER") or shutil.which("redis-server")
    database_url = os.getenv("TEST_DATABASE_URL")
    if not binary or not database_url:
        parser.error("Set TEST_REDIS_SERVER and TEST_DATABASE_URL")
    root = Path(__file__).resolve().parents[1]
    schema = f"benchmark_{uuid4().hex}"
    with psycopg.connect(database_url, autocommit=True) as admin:
        admin.execute(sql.SQL("CREATE SCHEMA {}").format(sql.Identifier(schema)))
        try:
            with tempfile.TemporaryDirectory(prefix="fss-bench-", dir="/tmp") as directory:
                processes, logs = [], []
                def start(name, command, env=None):
                    log = open(Path(directory) / f"{name}.log", "w+")
                    logs.append(log)
                    process = subprocess.Popen(command, cwd=root, env=env, stdout=log, stderr=subprocess.STDOUT)
                    processes.append(process)
                    return process
                try:
                    socket_path = str(Path(directory) / "redis.sock")
                    start("redis", [binary, str(root / "redis.conf"), "--dir", directory,
                                    "--port", "0", "--unixsocket", socket_path, "--unixsocketperm", "700"])
                    with Redis(unix_socket_path=socket_path, socket_timeout=1) as redis:
                        for _ in range(100):
                            try:
                                redis.ping()
                                break
                            except RedisError:
                                time.sleep(0.05)
                        else:
                            raise RuntimeError("Redis did not start")
                        redis_version = redis.info()["redis_version"]
                    env = dict(os.environ, REDIS_URL=f"unix://{socket_path}",
                               DATABASE_URL=make_conninfo(database_url, options=f"-c search_path={schema}"))
                    start("worker", [sys.executable, "-m", "app.worker"], env)
                    with socket.socket() as sock:
                        sock.bind(("127.0.0.1", 0))
                        port = sock.getsockname()[1]
                    start("api", [sys.executable, "-m", "uvicorn", "app.main:app", "--host", "127.0.0.1",
                                  "--port", str(port), "--no-access-log"], env)
                    base_url = f"http://127.0.0.1:{port}"
                    with httpx.Client(timeout=2) as client:
                        for _ in range(100):
                            if any(process.poll() is not None for process in processes):
                                raise RuntimeError("A service exited during startup")
                            try:
                                if client.get(base_url + "/ready").status_code == 200:
                                    break
                            except httpx.HTTPError:
                                pass
                            time.sleep(0.1)
                        else:
                            raise RuntimeError("API dependencies did not become ready")
                    report = asyncio.run(benchmark(base_url, args.requests, args.concurrency))
                    report["services"] = {"redis": redis_version, "postgres": admin.execute("SHOW server_version").fetchone()[0],
                                          "api_processes": 1, "worker_processes": 1,
                                          "placement": "load generator and all services on one host"}
                    with psycopg.connect(env["DATABASE_URL"]) as connection:
                        count = connection.execute("SELECT count(*) FROM orders").fetchone()[0]
                    report["checks"]["database_row_count_matches_stock"] = count == 100
                    report["passed"] = all(report["checks"].values())
                    args.output.parent.mkdir(parents=True, exist_ok=True)
                    with args.output.open("x") as output:
                        json.dump(report, output, indent=2)
                        output.write("\n")
                    print(json.dumps(report, indent=2))
                except Exception:
                    for log in logs:
                        log.flush()
                        log.seek(0)
                        print(log.read()[-4000:], file=sys.stderr)
                    raise
                finally:
                    for process in reversed(processes):
                        if process.poll() is None:
                            process.terminate()
                            try:
                                process.wait(timeout=10)
                            except subprocess.TimeoutExpired:
                                process.kill()
                                process.wait(timeout=5)
                    for log in logs:
                        log.close()
        finally:
            admin.execute(sql.SQL("DROP SCHEMA {} CASCADE").format(sql.Identifier(schema)))
    raise SystemExit(0 if report["passed"] else 1)


if __name__ == "__main__":
    main()
