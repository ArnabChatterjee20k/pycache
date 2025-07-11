import sqlite3 as sqlite
import asyncio
from threading import Thread
from queue import SimpleQueue
from dataclasses import dataclass
from functools import partial, wraps
from .Adapter import Adapter
from ..datatypes.Datatype import Datatype
from ..sql import SQL, Composed, Identifier, Placeholder


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


class SQLite(Adapter):
    def __init__(self, *args):
        super().__init__(*args)
        self._operation_queue: SimpleQueue[OperationPayload] = SimpleQueue()
        self._executor = Thread(target=self._run_executor, daemon=True)
        self._running = False

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

    async def connect(self):
        self._running = True
        self._executor.start()

        def connector():
            return sqlite.connect(self._connection_uri)

        self._db = await self._execute(connector)
        return self

    async def close(self):
        try:
            await self._execute(self._db.close)
        except BaseException as e:
            raise e
        finally:
            self._stop()
            self._executor.join()

    @_async_op
    def create(self):
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
                            {expires_at} DATETIME NULL
                        )
                    """
                ).format(
                    id=Identifier("id"),
                    key=Identifier("key"),
                    value=Identifier("value"),
                    created_at=Identifier("created_at"),
                    expires_at=Identifier("expires_at"),
                ),
            ]
        ).to_string()
        db: sqlite.Connection = self._db
        db.execute(stmt)
        db.commit()

    @_async_op
    def create_index(self):
        pass

    @_async_op
    def get(self, key: str):
        stmt = Composed(
            [
                SQL("SELECT {value} FROM {table} WHERE {key} = ").format(
                    value=Identifier("value"),
                    table=Identifier(self._tablename),
                    key=Identifier("key"),
                ),
                Placeholder("?"),
            ]
        ).to_string()

        cursor = self._db.cursor()
        row = cursor.execute(stmt, (key,)).fetchone()
        return self.to_value(row[0]) if row else None

    @_async_op
    def set(self, key: str, value: bytes) -> None:
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
                    "ON CONFLICT({key}) DO UPDATE SET {value} = excluded.{value}"
                ).format(key=Identifier("key"), value=Identifier("value")),
            ]
        ).to_string()
        cursor = self._db.cursor()
        cursor.execute(stmt, (key, self.to_bytes(value)))
        self._db.commit()

        stmt = SQL("SELECT last_insert_rowid()").to_string()
        cursor = self._db.cursor()
        cursor.execute(stmt)
        row = cursor.fetchone()
        return row[0] if row else None

    @_async_op
    def batch_get(self, keys: list[str]):
        stmt = Composed(
            [
                SQL("SELECT {key}, {value} FROM {table}").format(
                    key=Identifier("key"),
                    table=Identifier(self._tablename),
                    value=Identifier("value"),
                ),
                SQL("WHERE {key} in ({keys})").format(
                    key=Identifier("key"), keys=Placeholder("?", len(keys))
                ),
            ]
        ).to_string()
        cursor = self._db.cursor()
        rows = cursor.execute(stmt, keys).fetchall()
        return {row[0]: self.to_value(row[1]) for row in rows}

    @_async_op
    def batch_set(self, key_values: dict[str, Datatype]) -> None:
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
        self._db.commit()

    @_async_op
    def delete(self, key: str) -> None:
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
        self._db.commit()

    @_async_op
    def exists(self, key: str) -> bool:
        stmt = Composed(
            [
                SQL("SELECT 1 FROM {table} WHERE {key} = ").format(
                    table=Identifier(self._tablename), key=Identifier("key")
                ),
                Placeholder("?"),
            ]
        ).to_string()
        cursor = self._db.cursor()
        row = cursor.execute(stmt, (key,)).fetchone()
        return row is not None

    @_async_op
    def keys(self) -> list:
        stmt = (
            SQL("SELECT {key} FROM {table}")
            .format(key=Identifier("key"), table=Identifier(self._tablename))
            .to_string()
        )
        cursor = self._db.cursor()
        rows = cursor.execute(stmt).fetchall()
        return [row[0] for row in rows]

    @_async_op
    def set_expire(self, key: str, expires_at) -> None:
        stmt = Composed(
            [
                SQL("UPDATE {table} SET {expires_at} = ").format(
                    table=Identifier(self._tablename),
                    expires_at=Identifier("expires_at"),
                ),
                Placeholder("?"),
                SQL(" WHERE {key} = ").format(key=Identifier("key")),
                Placeholder("?"),
            ]
        ).to_string()
        cursor = self._db.cursor()
        cursor.execute(stmt, (expires_at, key))
        self._db.commit()
