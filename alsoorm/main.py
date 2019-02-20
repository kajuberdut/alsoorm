import asyncio
import json
from datetime import date, datetime
from typing import Any, Callable, Dict, List, Optional

import asyncpg

# from asyncpg.exceptions import UndefinedTableError
from asyncpg.pool import Pool
from tenacity import retry, stop_after_delay

import dataclasses

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


@dataclasses.dataclass(frozen=True)
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

    @property
    def system_maintained(self):
        return self.column_name in AlsoConfig().system_maintained

    def coerce(self, value):
        return self.py_type(value)


@dataclasses.dataclass
class Row:
    columns: List[Column]
    values: List[Any]

    def asdict(self) -> Dict[str, Any]:
        return {
            col.column_name: col.coerce(val)
            for col, val in zip(self.columns, self.values)
        }


@dataclasses.dataclass
class Secondary:
    """A class for storing custom queries able to be joined to table results."""

    join: str
    columns: List[str]


@dataclasses.dataclass
class Table:
    """Class for representing Postgres table elements"""

    schema: Any
    table_name: str
    columns: List[Column] = dataclasses.field(default_factory=list)
    secondaries: List[Secondary] = dataclasses.field(default_factory=list)

    @property
    def primary_key(self) -> List[Column]:
        return [column for column in self.columns if column.primary_key]

    @property
    def full_name(self) -> str:
        return f'"{self.schema.schema_name}"."{self.table_name}"'

    @property
    def pk_name(self):
        if len(self.primary_key) != 1:
            raise NotImplementedError(
                "Multi column primary keys are not yet supported."
            )
        return self.primary_key[0].column_name

    @property
    def insertable(self) -> List[Column]:
        return [
            column
            for column in self.columns
            if not column.primary_key and not column.system_maintained
        ]

    def __getattr__(self, attr):
        if attr in self.columns:
            return self.columns[attr]
        else:
            raise AttributeError(f"No attribute {attr}")

    async def insert(self, data: Dict[str, Any]) -> Dict[str, Any]:
        insertable = self.insertable
        obj = await self.schema.db.fetchrow(
            f""" INSERT INTO {self.table_name} ({','.join([i.column_name for i in insertable])})
                    VALUES ({','.join([f'${i+1}' for i in range(len(insertable))])})
                    RETURNING *""",
            *[self.get_values(i.column_name, data) for i in insertable],
        )
        return dict(obj)


@dataclasses.dataclass
class Schema:
    schema_name: str
    tables: Dict[str, Table] = dataclasses.field(default_factory=dict)
    db: Any = None

    def __getattr__(self, attr):
        if attr in self.tables:
            return self.tables[attr]
        else:
            raise AttributeError(f"No attribute {attr}")


async def init_connection(conn):
    await conn.set_type_codec(
        "json", encoder=json.dumps, decoder=json.loads, schema="pg_catalog"
    )


@dataclasses.dataclass
class DB:
    db_url: str
    schemas: Dict[str, Schema] = dataclasses.field(default_factory=dict)
    connection_pool: Pool = None

    @property
    def config(self):
        return AlsoConfig()

    async def setup_database(self):
        await self.connect()

    @retry(stop=stop_after_delay(60))
    async def connect(self):
        if self.connection_pool is None:
            self.connection_pool = await asyncpg.create_pool(
                self.db_url, init=init_connection
            )

    async def get_schema(self, schema_name: str) -> Schema:
        if schema_name not in self.schemas:
            self.schemas[schema_name] = await self.reflect_schema(schema_name)
        return self.schemas[schema_name]

    async def reflect_schema(self, schema_name: str = "public") -> Schema:
        async with self.connection_pool.acquire() as connection:
            result = await connection.fetch(schema_query, schema_name)

        schema = Schema(schema_name, db=self)

        for row in result:
            table_name = row["table_name"]
            table = Table(table_name=table_name, schema=schema)
            for col in row["columns"]:
                col["py_type"] = self.config.data_types[col["pg_type"]]
                table.columns.append(Column(**col))
            schema.tables[table_name] = table

        return schema

    async def fetch(self, command, *args) -> Optional[Any]:
        async with self.connection_pool.acquire() as connection:
            result = await connection.fetch(command, *args)
        return result

    async def fetchrow(self, command, *args) -> Optional[Any]:
        async with self.connection_pool.acquire() as connection:
            result = await connection.fetchrow(command, *args)
        return result


def setup_database(db_url, loop=None):
    db = DB(db_url)
    if loop is None:
        loop = asyncio.get_event_loop()
    loop.run_until_complete(db.setup_database())
    return db


# Singleton/SingletonMetaClass.py
class SingletonMetaClass(type):
    def __init__(cls, name, bases, dict):
        super(SingletonMetaClass, cls).__init__(name, bases, dict)
        original_new = cls.__new__

        def my_new(cls, *args, **kwds):
            if cls.instance is None:
                cls.instance = original_new(cls, *args, **kwds)
            return cls.instance

        cls.instance = None
        cls.__new__ = staticmethod(my_new)


def default_data_types() -> Dict[str, Any]:
    return {
        "integer": int,
        "bigint": int,
        "timestamp with time zone": datetime,
        "date": date,
        "character varying": str,
        "text": str,
        "boolean": bool,
        "tsvector": str,
    }


@dataclasses.dataclass
class ConfigHolder:
    system_columns: List[str] = dataclasses.field(default_factory=list)
    data_types: Dict[str, Any] = dataclasses.field(default_factory=default_data_types)
    default_schema: str = "public"


class AlsoConfig(metaclass=SingletonMetaClass):
    conf: ConfigHolder

    def __init__(self):
        self.conf = ConfigHolder()

    @property
    def system_maintained(self):
        return self.conf.system_columns

    def mark_system_column(self, column_name: str):
        """ Mark a column name that should be treated as system maintained. """
        self.conf.system_columns.append(column_name)

    @property
    def data_types(self):
        return self.conf.data_types

    def add_type(self, pg_type: str, py_type: Callable[[str], object]) -> None:
        self.data_types[pg_type] = py_type
