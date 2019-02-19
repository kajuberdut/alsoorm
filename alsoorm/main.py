import asyncio
import json
from datetime import date, datetime
from pathlib import Path
from typing import Dict, List, Optional

import asyncpg
from asyncpg.exceptions import UndefinedTableError
from asyncpg.pool import Pool
from tenacity import retry, stop_after_delay

import alsoorm
from dataclasses import dataclass, field

schema_query = """SELECT c.table_schema schema_name
                , c.table_name
                , JSON_AGG(('{"column_name": "' || c.column_name::varchar || '",' ||
                '"pg_type": "' || c.data_type::varchar || '",' ||
                '"primary_key": ' || CASE WHEN
                        kcu.column_name IS NULL THEN false
                        ELSE True END::bool || ',' ||
                '"default": "' || COALESCE(c.column_default::varchar, '') || '",' ||
                '"nullable": ' || c.is_nullable::bool || ',' ||
                '"ordinal": ' || c.ordinal_position::int || '}')::json) as columns
                FROM INFORMATION_SCHEMA.columns c
                LEFT JOIN INFORMATION_SCHEMA.TABLE_CONSTRAINTS tc
                    ON tc.table_catalog = c.table_catalog
                    AND tc.table_schema = c.table_schema
                    AND tc.table_name = C.table_name
                    AND tc.constraint_type = 'PRIMARY KEY'
                LEFT JOIN INFORMATION_SCHEMA.KEY_COLUMN_USAGE kcu
                    ON kcu.table_catalog = tc.table_catalog
                    AND kcu.table_schema = tc.table_schema
                    AND kcu.table_name = tc.table_name
                    AND kcu.constraint_name = tc.constraint_name
                    AND kcu.column_name = c.column_name
                WHERE c.table_schema = $1
                GROUP BY c.table_schema, c.table_name"""

data_types = {
    "integer": int,
    "bigint": int,
    "timestamp with time zone": datetime,
    "date": date,
    "character varying": str,
    "text": str,
    "boolean": bool,
    "tsvector": str,
}


@dataclass(frozen=True)
class Column:
    """Class for representing Postgres column elements"""

    __slots__ = [
        "column_name",
        "pg_type",
        "py_type",
        "primary_key",
        "nullable",
        "ordinal",
        "default",
    ]
    column_name: str
    pg_type: str
    py_type: type
    primary_key: bool
    nullable: bool
    ordinal: int
    default: str


@dataclass
class Secondary:
    """A class for storing custom queries able to be joined to table results."""

    join: str
    columns: List[str]


@dataclass
class Table:
    """Class for representing Postgres table elements"""

    schema_name: str
    table_name: str
    columns: List[Column] = field(default_factory=list)

    @property
    def primary_key(self) -> List[Column]:
        return [column for column in self.columns if column.primary_key]

    @property
    def insertable(self) -> List[Column]:
        return [
            column
            for column in self.columns
            if not column.primary_key
            and column.column_name not in alsoorm.config.system_maintained
        ]

    def __getattr__(self, attr):
        if attr in self.columns:
            return self.columns[attr]
        else:
            raise AttributeError(f"No attribute {attr}")

    @property
    def order_column(self):
        return next(
            [
                column
                for column in self.columns
                if column.column_name == f"{self.table_name}_order"
            ],
            None,
        )


@dataclass
class Schema:
    schema_name: str
    tables: Dict[str, Table] = field(default_factory=dict)

    def __getattr__(self, attr):
        if attr in self.tables:
            return self.tables[attr]
        else:
            raise AttributeError(f"No attribute {attr}")


async def init_connection(conn):
    await conn.set_type_codec(
        "json", encoder=json.dumps, decoder=json.loads, schema="pg_catalog"
    )


@dataclass
class DB:
    db_url: str
    config: alsoorm.Config = alsoorm.config
    setup_schema: Optional[str] = None
    schemas: Dict[str, Schema] = field(default_factory=dict)
    connection_pool: Pool = None

    @retry(stop=stop_after_delay(60))
    async def connect(self):
        if self.connection_pool is None:
            self.connection_pool = await asyncpg.create_pool(
                self.db_url, init=init_connection
            )

    async def setup_database(self):
        await self.connect()

        cwd = Path().cwd()
        with open(cwd / "testline" / "schema.sql") as s:
            schema_sql = s.read()

        async with self.connection_pool.acquire() as connection:
            try:
                version = await connection.fetchrow(
                    "SELECT * FROM sys.version WHERE version_object = 'db'"
                )
                version = version["version"]
            except UndefinedTableError:
                version = 0
            if version == 0:
                await connection.execute(schema_sql)

    async def get_schema(self, schema_name: str) -> Optional[Schema]:
        if schema_name not in self.schemas:
            self.schemas[schema_name] = await self.reflect_schema(schema_name)
        return self.schemas.get(schema_name)

    async def reflect_schema(self, schema_name: str = "public") -> Schema:
        async with self.connection_pool.acquire() as connection:
            result = await connection.fetch(schema_query, schema_name)

        schema = Schema(schema_name)

        for row in result:
            table_name = row["table_name"]
            table = Table(table_name=table_name, schema_name=schema_name)
            for col in row["columns"]:
                col["py_type"] = data_types[col["pg_type"]]
                table.columns.append(Column(**col))
            schema.tables[table_name] = table

        return schema


def setup_database(db_url, loop=None):
    db = DB(db_url)
    if loop is None:
        loop = asyncio.get_event_loop()
    loop.run_until_complete(db.setup_database())
    return db


@dataclass
class Config:
    _system_maintained: List[str] = ["updated_on", "created_on"]

    @property
    def system_maintained(self):
        return self._system_maintained

    def mark_system_column(self, column_name: str):
        """ Mark a column name that should be treated as system maintained. """
        self._system_maintained.append(column_name)
