from pathlib import Path

import aiosqlite

from app.core.config import settings
from app.db.models import SCHEMA


def _dict_row(cursor, row):
    """Row factory that returns dicts instead of tuples."""
    return dict(zip([col[0] for col in cursor.description], row))


async def get_db():
    """FastAPI dependency — yields an async read-only SQLite connection."""
    db = await aiosqlite.connect(settings.database_path)
    db.row_factory = _dict_row
    await db.execute("PRAGMA journal_mode=WAL")
    await db.execute("PRAGMA synchronous=NORMAL")
    await db.execute("PRAGMA cache_size=-64000")   # 64 MB
    await db.execute("PRAGMA query_only=ON")
    try:
        yield db
    finally:
        await db.close()


async def init_db():
    """Create the database file and all tables if they don't exist."""
    db_path = Path(settings.database_path)
    db_path.parent.mkdir(parents=True, exist_ok=True)
    async with aiosqlite.connect(str(db_path)) as db:
        await db.execute("PRAGMA journal_mode=WAL")
        await db.executescript(SCHEMA)
        await db.commit()
