import sqlite3 as sqlite
from .Adapter import Adapter
from ..datatypes.Datatype import Datatype
from ..sql import SQL, Composed, Identifier, Literal, Placeholder


class SQLite(Adapter):
    def connect(self):
        self._db = sqlite.connect(self._connection_uri)

    def close(self):
        self._db.close()

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

    def create_index(self):
        pass

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
                SQL("ON CONFLICT({key}) DO UPDATE SET {key} = excluded.{key}").format(
                    key=Identifier("key")
                ),
            ]
        ).to_string()
        cursor = self._db.cursor()
        cursor.execute(stmt, (key, self.to_bytes(value)))
        self._db.commit()

    def batch_get(self, keys: list[str]):
        stmt = Composed(
            [
                SQL("SELECT {value} FROM {table}").format(
                    table=Identifier(self._tablename), value=Identifier("value")
                ),
                SQL("WHERE {key} in ({keys})").format(
                    key=Identifier("key"), keys=Placeholder("?", len(keys))
                ),
            ]
        ).to_string()
        cursor = self._db.cursor()
        rows = cursor.execute(stmt, keys).fetchall()
        return [self.to_value(row[0]) for row in rows]

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

    def delete(self, key: str) -> None:
        pass

    def exists(self, key: str) -> bool:
        pass

    def keys(self) -> list:
        pass

    def set_expire(self):
        pass
