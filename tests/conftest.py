"""Isolated real-service fixtures. Never flush or restart a supplied Redis URL."""

import os
from pathlib import Path
import shutil
import subprocess
import tempfile
import time
from uuid import uuid4

import psycopg
from psycopg import sql
from psycopg.conninfo import make_conninfo
import pytest
from redis import Redis
from redis.exceptions import ConnectionError

from app.orders import PostgresOrderStore


@pytest.fixture
def store():
    database_url = os.getenv("TEST_DATABASE_URL")
    if not database_url:
        pytest.skip("Set TEST_DATABASE_URL to run real PostgreSQL tests")
    schema = f"test_orders_{uuid4().hex}"
    with psycopg.connect(database_url, autocommit=True) as connection:
        connection.execute(sql.SQL("CREATE SCHEMA {}").format(sql.Identifier(schema)))
        try:
            store = PostgresOrderStore(make_conninfo(database_url, options=f"-c search_path={schema}"))
            store.initialize()
            yield store
        finally:
            connection.execute(sql.SQL("DROP SCHEMA {} CASCADE").format(sql.Identifier(schema)))


class RedisProcess:
    def __init__(self, binary, directory):
        self.binary = binary
        self.directory = Path(directory)
        self.process = None
        self.log = None
        self.client = Redis(
            unix_socket_path=str(self.directory / "redis.sock"),
            decode_responses=True, socket_timeout=5,
        )

    def start(self):
        config = Path(__file__).resolve().parents[1] / "redis.conf"
        self.log = (self.directory / "redis.log").open("ab")
        self.process = subprocess.Popen(
            [self.binary, str(config), "--dir", str(self.directory), "--port", "0",
             "--unixsocket", str(self.directory / "redis.sock"), "--unixsocketperm", "700"],
            stdout=self.log, stderr=subprocess.STDOUT,
        )
        deadline = time.monotonic() + 10
        while time.monotonic() < deadline:
            if self.process.poll() is not None:
                raise RuntimeError((self.directory / "redis.log").read_text())
            try:
                if self.client.ping():
                    return
            except ConnectionError:
                time.sleep(0.05)
        raise RuntimeError("Temporary Redis did not become ready")

    def stop(self, abrupt=False):
        if self.process is not None and self.process.poll() is None:
            if abrupt:
                self.process.kill()
            else:
                self.process.terminate()
            try:
                self.process.wait(timeout=10)
            except subprocess.TimeoutExpired:
                self.process.kill()
                self.process.wait(timeout=5)
        self.client.close()
        if self.log:
            self.log.close()

    def restart(self, abrupt=False):
        self.stop(abrupt=abrupt)
        self.start()


@pytest.fixture
def real_redis():
    binary = os.getenv("TEST_REDIS_SERVER") or shutil.which("redis-server")
    if not binary:
        pytest.skip("Set TEST_REDIS_SERVER or install redis-server 7.4+ for real Redis tests")
    # A short socket path avoids macOS/Unix socket pathname limits.
    with tempfile.TemporaryDirectory(prefix="fss-redis-", dir="/tmp") as directory:
        server = RedisProcess(binary, directory)
        try:
            server.start()
            yield server
        finally:
            server.stop()
