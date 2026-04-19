from contextlib import asynccontextmanager
from pathlib import Path

import aiosqlite
from fastapi import FastAPI
from fastapi.responses import JSONResponse

from app.core.config import settings
from app.db.database import init_db
from app.routers import cnpj
from app.scheduler.jobs import scheduler, setup_scheduler


@asynccontextmanager
async def lifespan(app: FastAPI):
    await init_db()
    setup_scheduler()
    scheduler.start()
    yield
    scheduler.shutdown(wait=False)


app = FastAPI(
    title="CNPJ API",
    description=(
        "Consulta de dados públicos de CNPJ — "
        "Receita Federal do Brasil (dados abertos)"
    ),
    version="1.0.0",
    lifespan=lifespan,
)

app.include_router(cnpj.router)


@app.get("/", include_in_schema=False)
async def root():
    return {
        "name": "CNPJ API",
        "version": "1.0.0",
        "docs": "/docs",
    }


@app.get("/status")
async def status():
    """Health-check endpoint (unauthenticated — used by marketplaces)."""
    last_import = None
    db_path = Path(settings.database_path)
    if db_path.exists():
        try:
            async with aiosqlite.connect(str(db_path)) as db:
                db.row_factory = lambda cur, row: dict(
                    zip([c[0] for c in cur.description], row)
                )
                cur = await db.execute(
                    "SELECT batch, status, started_at, finished_at "
                    "FROM import_runs ORDER BY id DESC LIMIT 1"
                )
                last_import = await cur.fetchone()
        except Exception:
            pass

    return {"status": "ok", "last_import": last_import}
