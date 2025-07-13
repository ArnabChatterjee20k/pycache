import sqlite3 as sqlite
import asyncio
from threading import Thread
from queue import SimpleQueue
from dataclasses import dataclass
from functools import partial, wraps
from .Adapter import Adapter
from ..datatypes.Datatype import Datatype
from ..sql import SQL, Composed, Identifier, Placeholder, Literal


@dataclass
class OperationPayload:
    future: asyncio.Future
    action: callable


_STOP_RUNNING_EXECUTOR = object()


def _async_op(fn):
    @wraps(fn)
    async def wrapper(self: "SQLite", *args, **kwargs):
        # binding the fn to the SQLite scope
        return await self._execute(fn.__get__(self), *args, **kwargs)

    return wrapper


# Factory class
class SQLite(Adapter):
    _operation_queue: SimpleQueue[OperationPayload] = None
    _executor = None
    _running = None
    _in_transaction = False

    def __init__(self, *args):
        super().__init__(*args)

    # HACK: the connect() returns the SQLiteSession
    # and SQLiteSession will have the local scope of the state and no state will be shared
    async def connect(self) -> "SQLiteSession":
        # Return a fresh SQLiteSession on connect
        session = SQLiteSession(self._connection_uri, self._tablename)
        await session.connect()
        return session

    async def __aenter__(self):
        # return await self
        return await self.connect()

    # HACK: not using it anymore and directly using the aenter
    """
        we cant use the following any more
        db = await SQLite("db.db")
        await db.set("test2","arnab")
        print(await db.get("test2"))

        we need to use the followings
            db = await SQLite("db.db").connect()
            await db.set("test2","arnab")
            print(await db.get("test2"))
        or
            with async with SQLite("db.db") as con:
                pass
        as remove __await__ and return `await self` from __aenter__ which was returing iterator and __await__ scheduling that iterator 
        on the event loop
    """
    # def __await__(self):
    #     return self.connect().__await__()

    async def __aexit__(self, *args):
        await self.close()

    def _stop(self):
        self._running = False
        self._operation_queue.put_nowait(_STOP_RUNNING_EXECUTOR)

    async def _execute(self, fn, *args, **kwargs):
        if not self._running:
            raise ValueError("Connection is closed")
        future = asyncio.get_event_loop().create_future()
        action = partial(fn, *args, **kwargs)
        self._operation_queue.put_nowait(OperationPayload(future=future, action=action))
        return await future

    def _run_executor(self):
        def set_future_result(future: asyncio.Future, result):
            future.set_result(result)

        def set_future_exception(future: asyncio.Future, error):
            future.set_exception(error)

        while True:
            try:
                payload = self._operation_queue.get()
                if payload is _STOP_RUNNING_EXECUTOR:
                    break
                result = payload.action()
                payload.future.get_loop().call_soon_threadsafe(
                    set_future_result, payload.future, result
                )
            except BaseException as e:
                payload.future.get_loop().call_soon_threadsafe(
                    set_future_exception, payload.future, e
                )

    async def close(self):
        try:
            await self._execute(self._db.close)
        except BaseException as e:
            raise e
        finally:
            self._stop()
            self._executor.join()

    @_async_op
    def create(self) -> None:
        stmt = Composed(
            [
                SQL("CREATE TABLE IF NOT EXISTS {table}").format(
                    table=Identifier(self._tablename)
                ),
                SQL(
                    """
                        (
                            {id} INTEGER PRIMARY KEY AUTOINCREMENT,
                            {key} TEXT NOT NULL UNIQUE,
                            {value} BLOB NOT NULL,
                            {created_at} DATETIME DEFAULT CURRENT_TIMESTAMP,
                            {expires_at} DATETIME NULL,
                            {ttl} INTEGER NULL
                            CHECK ({expires_at} IS NULL OR {expires_at} > {created_at})
                        )
                    """
                ).format(
                    id=Identifier("id"),
                    key=Identifier("key"),
                    value=Identifier("value"),
                    created_at=Identifier("created_at"),
                    expires_at=Identifier("expires_at"),
                    ttl=Identifier("ttl"),
                ),
            ]
        ).to_string()
        db: sqlite.Connection = self._db
        db.execute(stmt)
        db.commit()

    @_async_op
    def create_index(self) -> None:
        index_name = f"{self._tablename}_expires_at_index"
        # index automatically created on the key as it is unique
        stmt = (
            SQL("SELECT {name} FROM {table} WHERE type={index} AND name={index_name}")
            .format(
                name=Identifier("name"),
                table=Identifier("sqlite_master"),
                index=Literal("index"),
                index_name=Placeholder("?"),
            )
            .to_string()
        )

        db: sqlite.Connection = self._db
        cursor = db.cursor()
        existing_index = cursor.execute(stmt, (index_name,)).fetchone()
        if existing_index:
            return

        stmt = (
            SQL("CREATE INDEX {name} ON {table}({expires_at})")
            .format(
                name=Identifier(index_name),
                table=Identifier(self._tablename),
                expires_at=Identifier("expires_at"),
            )
            .to_string()
        )
        cursor.execute(stmt)
        self._db.commit()
        # index info
        # cursor = db.cursor()
        # cursor.execute("SELECT name FROM sqlite_master WHERE type='index' ")
        # print(cursor.fetchone()[0])

        # cursor.execute("PRAGMA index_info('sqlite_autoindex_kv-store_1')")
        # for row in cursor.fetchall():
        #     print(row)

    @_async_op
    def get(self, key: str) -> any:
        # SQLite doesn't provide select for update to lock table, we need to lock the entire database
        # TODO: need to use distributed locks may be with a separate locks table
        stmt = Composed(
            [
                SQL("SELECT {value} FROM {table} WHERE {key} = {key_name}").format(
                    value=Identifier("value"),
                    table=Identifier(self._tablename),
                    key=Identifier("key"),
                    key_name=Placeholder("?"),
                ),
                SQL(
                    "AND ({expires_at} IS NULL OR {expires_at} > CURRENT_TIMESTAMP)"
                ).format(expires_at=Identifier("expires_at")),
            ]
        ).to_string()

        cursor = self._db.cursor()
        row = cursor.execute(stmt, (key,)).fetchone()
        return self.to_value(row[0]) if row else None

    @_async_op
    def set(self, key: str, value: bytes) -> int:
        # || is the concatenation operation
        # SELECT DATETIME(CURRENT_TIMESTAMP, '+' || 30 || ' seconds') -> current time + 30 seconds
        # SELECT DATETIME(CURRENT_TIMESTAMP, '+' || NULL || ' seconds') -> NULL
        # here 30 , NULL are the ttl column which will be reference internally
        stmt = Composed(
            [
                SQL("INSERT INTO {table} ({key}, {value})").format(
                    table=Identifier(self._tablename),
                    key=Identifier("key"),
                    value=Identifier("value"),
                ),
                SQL("VALUES ({user_key}, {user_value})").format(
                    user_key=Placeholder("?"),
                    user_value=Placeholder("?"),
                ),
                SQL(
                    """ON CONFLICT({key}) DO UPDATE SET 
                        {value} = excluded.{value}, 
                        {created_at} = CURRENT_TIMESTAMP, 
                        {expires_at} = DATETIME(CURRENT_TIMESTAMP,'+' || {ttl} || ' seconds')
                        """
                ).format(
                    ttl=Identifier("ttl"),
                    key=Identifier("key"),
                    value=Identifier("value"),
                    created_at=Identifier("created_at"),
                    expires_at=Identifier("expires_at"),
                ),
            ]
        ).to_string()
        cursor = self._db.cursor()
        cursor.execute(stmt, (key, self.to_bytes(value)))
        if not self._in_transaction:
            self._db.commit()

        stmt = SQL("SELECT last_insert_rowid()").to_string()
        cursor = self._db.cursor()
        cursor.execute(stmt)
        row = cursor.fetchone()
        return row[0] if row else None

    @_async_op
    def batch_get(self, keys: list[str]) -> list:
        stmt = Composed(
            [
                SQL("SELECT {key}, {value} FROM {table}").format(
                    key=Identifier("key"),
                    table=Identifier(self._tablename),
                    value=Identifier("value"),
                ),
                SQL(
                    "WHERE {key} in ({keys}) AND ({expires_at} IS NULL OR {expires_at} > CURRENT_TIMESTAMP)"
                ).format(
                    key=Identifier("key"),
                    keys=Placeholder("?", len(keys)),
                    expires_at=Identifier("expires_at"),
                ),
            ]
        ).to_string()
        cursor = self._db.cursor()
        rows = cursor.execute(stmt, keys).fetchall()
        return {row[0]: self.to_value(row[1]) for row in rows}

    @_async_op
    def batch_set(self, key_values: dict[str, Datatype]) -> int:
        stmt = (
            SQL(
                """
            INSERT INTO {table} ({key}, {value}) 
            VALUES (?, ?)
            ON CONFLICT({key}) DO UPDATE SET {value} = excluded.{value}
        """
            )
            .format(
                table=Identifier(self._tablename),
                key=Identifier("key"),
                value=Identifier("value"),
            )
            .to_string()
        )

        data = [(k, self.to_bytes(v.value)) for k, v in key_values.items()]

        cursor = self._db.cursor()
        cursor.executemany(stmt, data)
        row = cursor.rowcount
        if not self._in_transaction:
            self._db.commit()
        return row

    @_async_op
    def delete(self, key: str) -> int:
        stmt = Composed(
            [
                SQL("DELETE FROM {table} WHERE {key} = ").format(
                    table=Identifier(self._tablename), key=Identifier("key")
                ),
                Placeholder("?"),
            ]
        ).to_string()
        cursor = self._db.cursor()
        cursor.execute(stmt, (key,))
        deleted_rows = cursor.rowcount
        if not self._in_transaction:
            self._db.commit()
        return deleted_rows

    @_async_op
    def exists(self, key: str) -> bool:
        stmt = Composed(
            [
                SQL("SELECT 1 FROM {table} WHERE {key} = {key_name}").format(
                    table=Identifier(self._tablename),
                    key=Identifier("key"),
                    key_name=Placeholder("?"),
                ),
                SQL(
                    "AND ({expires_at} IS NULL OR {expires_at} > CURRENT_TIMESTAMP)"
                ).format(expires_at=Identifier("expires_at")),
            ]
        ).to_string()
        cursor = self._db.cursor()
        row = cursor.execute(stmt, (key,)).fetchone()
        return row is not None

    @_async_op
    def keys(self) -> list:
        stmt = (
            SQL(
                "SELECT {key} FROM {table} WHERE {expires_at} IS NULL OR {expires_at} > CURRENT_TIMESTAMP"
            )
            .format(
                key=Identifier("key"),
                table=Identifier(self._tablename),
                expires_at=Identifier("expires_at"),
            )
            .to_string()
        )
        cursor = self._db.cursor()
        rows = cursor.execute(stmt).fetchall()
        return [row[0] for row in rows]

    @_async_op
    def set_expire(self, key: str, ttl: int) -> int:
        expires_at = self.get_expires_at(ttl)
        stmt = (
            SQL(
                "UPDATE {table} SET {expires_at} = {expires_at_value}, {ttl} = {ttl_value} WHERE {key} = {key_value}"
            )
            .format(
                table=Identifier(self._tablename),
                expires_at=Identifier("expires_at"),
                expires_at_value=Placeholder("?"),
                ttl=Identifier("ttl"),
                ttl_value=Placeholder("?"),
                key=Identifier("key"),
                key_value=Placeholder("?"),
            )
            .to_string()
        )
        cursor = self._db.cursor()
        cursor.execute(stmt, (expires_at, ttl, key))
        row = cursor.rowcount
        if not self._in_transaction:
            self._db.commit()
        return row

    @_async_op
    def begin(self):
        self._in_transaction = True
        self._db.cursor().execute("BEGIN")

    @_async_op
    def commit(self):
        self._in_transaction = False
        self._db.commit()

    @_async_op
    def rollback(self):
        self._in_transaction = False
        self._db.rollback()

    @_async_op
    def get_expire(self, key) -> int | None:
        stmt = (
            SQL("SELECT ttl from {table} where key={key}")
            .format(table=Identifier(self._tablename), key=Placeholder("?"))
            .to_string()
        )
        cursor = self._db.cursor()
        cursor.execute(stmt, (key,))
        row = cursor.fetchone()
        return row[0] if row else None

    def get_datetime_format(self):
        return "%Y-%m-%d %H:%M:%S"

    def delete_expired_attributes(self):
        """Delete all expired keys."""
        with sqlite.connect(self._connection_uri) as db:
            stmt = (
                SQL(
                    "DELETE FROM {table} WHERE {expires_at} IS NOT NULL AND {expires_at} <= CURRENT_TIMESTAMP"
                )
                .format(
                    table=Identifier(self._tablename),
                    expires_at=Identifier("expires_at"),
                )
                .to_string()
            )
            cursor = db.cursor()
            cursor.execute(stmt)
            db.commit()

    def count_expired_keys(self) -> int:
        """Count the number of expired keys in the database."""
        with sqlite.connect(self._connection_uri) as db:
            stmt = (
                SQL(
                    "SELECT COUNT(*) FROM {table} WHERE {expires_at} IS NOT NULL AND {expires_at} <= CURRENT_TIMESTAMP"
                )
                .format(
                    table=Identifier(self._tablename),
                    expires_at=Identifier("expires_at"),
                )
                .to_string()
            )
            cursor = db.cursor()
            row = cursor.execute(stmt).fetchone()
            return row[0] if row else 0

    def get_all_keys_with_expiry(self) -> list[tuple[str, str]]:
        """Get all keys with their expiry information for testing purposes."""
        with sqlite.connect(self._connection_uri) as db:
            stmt = (
                SQL("SELECT {key}, {expires_at} FROM {table} ORDER BY {key}")
                .format(
                    key=Identifier("key"),
                    expires_at=Identifier("expires_at"),
                    table=Identifier(self._tablename),
                )
                .to_string()
            )
            cursor = db.cursor()
            rows = cursor.execute(stmt).fetchall()
            return [(row[0], row[1]) for row in rows]


# Actual session
class SQLiteSession(SQLite):
    def __init__(self, *args):
        super().__init__(*args)

        self._operation_queue: SimpleQueue[OperationPayload] = SimpleQueue()
        self._executor = Thread(target=self._run_executor, daemon=True)
        self._running = False
        self._in_transaction = False

    async def connect(self) -> "SQLite":
        self._running = True
        self._executor.start()

        def connector():
            return sqlite.connect(self._connection_uri)

        self._db = await self._execute(connector)
        return self
