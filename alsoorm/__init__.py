from alsoorm.orm import DB, Column, Table, Schema, setup_database, Config  # noqa: F401

__version__ = 1.0
__all__ = ["DB", "Column", "Table", "Schema", "config"]

_config: Config = None

config = Config()
