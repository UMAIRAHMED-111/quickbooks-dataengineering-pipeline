"""Shared bounded psycopg2 connection pooling for Supabase/Postgres access."""

from __future__ import annotations

import atexit
from contextlib import contextmanager
from threading import Lock
from typing import Iterator

from psycopg2 import extensions
from psycopg2.pool import SimpleConnectionPool

_POOL_MIN_CONNECTIONS = 1
_POOL_MAX_CONNECTIONS = 4

_CONN_POOLS: dict[str, SimpleConnectionPool] = {}
_CONN_POOLS_LOCK = Lock()


def _close_all_pools() -> None:
    with _CONN_POOLS_LOCK:
        for pool in _CONN_POOLS.values():
            pool.closeall()
        _CONN_POOLS.clear()


atexit.register(_close_all_pools)


def connection_pool(conninfo: str) -> SimpleConnectionPool:
    with _CONN_POOLS_LOCK:
        pool = _CONN_POOLS.get(conninfo)
        if pool is not None:
            return pool
        pool = SimpleConnectionPool(
            _POOL_MIN_CONNECTIONS,
            _POOL_MAX_CONNECTIONS,
            dsn=conninfo,
        )
        _CONN_POOLS[conninfo] = pool
        return pool


@contextmanager
def pooled_connection(conninfo: str, *, autocommit: bool = False) -> Iterator[extensions.connection]:
    pool = connection_pool(conninfo)
    conn = pool.getconn()
    conn.autocommit = autocommit
    try:
        yield conn
    finally:
        try:
            if (
                not autocommit
                and conn.get_transaction_status() != extensions.TRANSACTION_STATUS_IDLE
            ):
                conn.rollback()
        finally:
            conn.autocommit = False
            pool.putconn(conn)
