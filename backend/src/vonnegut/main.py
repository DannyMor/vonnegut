# backend/src/vonnegut/main.py
from contextlib import asynccontextmanager

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

from vonnegut.config import Settings
from vonnegut.database import Database
from vonnegut.encryption import get_or_create_key
from vonnegut.adapters.factory import DefaultAdapterFactory
from vonnegut.routers.ai import router as ai_router
from vonnegut.routers.connections import router as connections_router
from vonnegut.routers.explorer import router as explorer_router
from vonnegut.routers.migrations import router as migrations_router
from vonnegut.routers.pipeline_steps import router as pipeline_steps_router
from vonnegut.routers.transformations import router as transformations_router
from vonnegut.services.connection_manager import ConnectionManager


def create_app(
    db: Database | None = None,
    encryption_key: str | None = None,
    settings: Settings | None = None,
    adapter_factory=None,
) -> FastAPI:
    settings = settings or Settings()

    @asynccontextmanager
    async def lifespan(app: FastAPI):
        # Startup: initialize DB if not injected (i.e. production mode)
        if app.state.db is None:
            db_instance = Database(settings.database_url)
            await db_instance.initialize()
            app.state.db = db_instance
            app.state.connection_manager = ConnectionManager(
                db=db_instance, encryption_key=app.state.encryption_key,
            )
        yield
        # Shutdown: close DB
        if app.state.db is not None:
            await app.state.db.close()

    app = FastAPI(title="Vonnegut", version="0.1.0", lifespan=lifespan)

    app.add_middleware(
        CORSMiddleware,
        allow_origins=settings.cors_origins,
        allow_credentials=True,
        allow_methods=["*"],
        allow_headers=["*"],
    )

    app.state.db = db
    app.state.encryption_key = encryption_key or get_or_create_key()
    app.state.settings = settings
    app.state.adapter_factory = adapter_factory or DefaultAdapterFactory()
    app.state.connection_manager = ConnectionManager(
        db=app.state.db, encryption_key=app.state.encryption_key,
    )

    @app.get("/api/health")
    def health():
        return {"status": "ok"}

    app.include_router(ai_router, prefix="/api/v1")
    app.include_router(connections_router, prefix="/api/v1")
    app.include_router(explorer_router, prefix="/api/v1")
    app.include_router(migrations_router, prefix="/api/v1")
    app.include_router(pipeline_steps_router, prefix="/api/v1")
    app.include_router(transformations_router, prefix="/api/v1")

    return app


# uvicorn entry point: `uvicorn vonnegut.main:app --factory`
app = create_app
